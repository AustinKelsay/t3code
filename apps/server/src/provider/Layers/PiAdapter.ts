/**
 * PiAdapter — live adapter for Pi RPC sessions.
 *
 * @module provider/Layers/PiAdapter
 */
import {
  EventId,
  type PiSettings,
  ProviderDriverKind,
  ProviderInstanceId,
  type ProviderRuntimeEvent,
  ThreadId,
  TurnId,
} from "@t3tools/contracts";
import * as Crypto from "effect/Crypto";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Queue from "effect/Queue";
import * as Stream from "effect/Stream";
import { ChildProcessSpawner } from "effect/unstable/process";
import {
  ProviderAdapterRequestError,
  ProviderAdapterSessionNotFoundError,
  ProviderAdapterValidationError,
} from "../Errors.ts";
import { ServerConfig } from "../../config.ts";
import {
  createHandlePiExtensionUiRequest,
  respondToPiApproval,
  respondToPiUserInput,
  sendPiExtensionUiResponse,
} from "../pi/PiAdapterExtensionUi.ts";
import { createPiAdapterLifecycle } from "../pi/PiAdapterLifecycle.ts";
import type { PiSessionContext } from "../pi/PiAdapterSessionHelpers.ts";
import { makePiRpcClient, type PiRpcClient } from "../pi/PiRpcClient.ts";
import type { PiSpawnOptions } from "../pi/PiSpawn.ts";
import { type PiAdapterShape } from "../Services/PiAdapter.ts";
import { type EventNdjsonLogger, makeEventNdjsonLogger } from "./EventNdjsonLogger.ts";

const PROVIDER = ProviderDriverKind.make("piAgent");

export interface PiAdapterLiveOptions {
  readonly instanceId?: ProviderInstanceId;
  readonly environment?: NodeJS.ProcessEnv;
  readonly nativeEventLogPath?: string;
  readonly nativeEventLogger?: EventNdjsonLogger;
  readonly createRpcClient?: (input: PiSpawnOptions) => Effect.Effect<PiRpcClient>;
}

const nowIso = Effect.map(DateTime.now, DateTime.formatIso);

export const makePiAdapter = Effect.fn("makePiAdapter")(function* (
  piSettings: PiSettings,
  options?: PiAdapterLiveOptions,
) {
  const boundInstanceId = options?.instanceId ?? ProviderInstanceId.make("piAgent");
  const serverConfig = yield* ServerConfig;
  const fileSystem = yield* FileSystem.FileSystem;
  const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
  const crypto = yield* Crypto.Crypto;
  const nativeEventLogger =
    options?.nativeEventLogger ??
    (options?.nativeEventLogPath !== undefined
      ? yield* makeEventNdjsonLogger(options.nativeEventLogPath, { stream: "native" })
      : undefined);
  const managedNativeEventLogger =
    options?.nativeEventLogger === undefined ? nativeEventLogger : undefined;
  const runtimeEvents = yield* Queue.unbounded<ProviderRuntimeEvent>();
  const sessions = new Map<ThreadId, PiSessionContext>();
  const createRpcClient =
    options?.createRpcClient ??
    ((input: Parameters<typeof makePiRpcClient>[0]) =>
      makePiRpcClient(input).pipe(
        Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner),
      ));
  const randomUUIDv4 = crypto.randomUUIDv4.pipe(
    Effect.mapError(
      (cause) =>
        new ProviderAdapterRequestError({
          provider: PROVIDER,
          method: "crypto/randomUUIDv4",
          detail: "Failed to generate Pi runtime identifier.",
          cause,
        }),
    ),
  );

  const buildEventBase = (input: {
    readonly threadId: ThreadId;
    readonly turnId?: TurnId;
    readonly itemId?: string;
    readonly createdAt?: string;
    readonly raw?: unknown;
  }) =>
    Effect.all({
      eventId: randomUUIDv4.pipe(Effect.map(EventId.make)),
      createdAt: input.createdAt === undefined ? nowIso : Effect.succeed(input.createdAt),
    }).pipe(
      Effect.map(({ eventId, createdAt }) => ({
        eventId,
        provider: PROVIDER,
        threadId: input.threadId,
        createdAt,
        ...(input.turnId ? { turnId: input.turnId } : {}),
        ...(input.raw !== undefined
          ? { raw: { source: "pi.rpc.event" as const, payload: input.raw } }
          : {}),
      })),
      Effect.orDie,
    );

  const emit = (event: ProviderRuntimeEvent) =>
    Queue.offer(runtimeEvents, event).pipe(Effect.asVoid);

  const logNativeEvent = (threadId: ThreadId, payload: unknown) =>
    Effect.gen(function* () {
      if (!nativeEventLogger) {
        return;
      }
      const observedAt = yield* nowIso;
      yield* nativeEventLogger.write(
        {
          observedAt,
          event: {
            id: yield* randomUUIDv4,
            kind: "notification",
            provider: PROVIDER,
            createdAt: observedAt,
            method: "pi.event",
            threadId,
            payload,
          },
        },
        threadId,
      );
    }).pipe(Effect.catchCause(() => Effect.void));

  const handlePiExtensionUiRequest = createHandlePiExtensionUiRequest({
    emit,
    buildEventBase,
    sendPiExtensionUiResponse: (context, response) =>
      sendPiExtensionUiResponse(context.client, response),
  });

  const { ensureSessionContext, stopPiContext, startSession, sendTurn, interruptTurn } =
    createPiAdapterLifecycle({
      provider: PROVIDER,
      piSettings,
      serverConfig,
      fileSystem,
      sessions,
      boundInstanceId,
      createRpcClient,
      ...(options?.environment ? { environment: options.environment } : {}),
      emit,
      buildEventBase,
      logNativeEvent,
      nowIso,
      randomUUIDv4,
      handlePiExtensionUiRequest,
    });

  yield* Effect.addFinalizer(() =>
    Effect.gen(function* () {
      const contexts = [...sessions.values()];
      sessions.clear();
      yield* Effect.forEach(contexts, (context) => Effect.ignoreCause(stopPiContext(context)), {
        concurrency: "unbounded",
        discard: true,
      });
      if (managedNativeEventLogger !== undefined) {
        yield* managedNativeEventLogger.close();
      }
    }).pipe(Effect.ensuring(Queue.shutdown(runtimeEvents))),
  );

  const stopSession: PiAdapterShape["stopSession"] = Effect.fn("stopSession")(function* (threadId) {
    const context = sessions.get(threadId);
    if (!context) {
      return yield* new ProviderAdapterSessionNotFoundError({ provider: PROVIDER, threadId });
    }
    const stopped = yield* stopPiContext(context);
    sessions.delete(threadId);
    if (!stopped) {
      return;
    }
    yield* emit({
      ...(yield* buildEventBase({ threadId })),
      type: "session.exited",
      payload: {
        reason: "Session stopped.",
        recoverable: false,
        exitKind: "graceful",
      },
    });
  });

  const rollbackThread: PiAdapterShape["rollbackThread"] = Effect.fn("rollbackThread")(
    function* (threadId, _numTurns) {
      yield* ensureSessionContext(threadId);
      return yield* new ProviderAdapterValidationError({
        provider: PROVIDER,
        operation: "rollbackThread",
        issue: "Pi does not support thread rollback.",
      });
    },
  );

  return {
    provider: PROVIDER,
    capabilities: { sessionModelSwitch: "in-session" },
    startSession,
    sendTurn,
    interruptTurn,
    respondToRequest: Effect.fn("respondToRequest")(function* (threadId, requestId, decision) {
      const context = yield* ensureSessionContext(threadId);
      yield* respondToPiApproval(context, requestId, decision);
    }),
    respondToUserInput: Effect.fn("respondToUserInput")(function* (threadId, requestId, answers) {
      const context = yield* ensureSessionContext(threadId);
      yield* respondToPiUserInput(context, requestId, answers);
    }),
    stopSession,
    listSessions: () => Effect.sync(() => [...sessions.values()].map((context) => context.session)),
    hasSession: (threadId) => Effect.sync(() => sessions.has(threadId)),
    readThread: (threadId) =>
      Effect.gen(function* () {
        const context = yield* ensureSessionContext(threadId);
        return { threadId, turns: context.turns };
      }),
    rollbackThread,
    stopAll: () =>
      Effect.gen(function* () {
        const contexts = [...sessions.values()];
        sessions.clear();
        yield* Effect.forEach(contexts, (context) => Effect.ignoreCause(stopPiContext(context)), {
          concurrency: "unbounded",
          discard: true,
        });
      }),
    get streamEvents() {
      return Stream.fromQueue(runtimeEvents);
    },
  } satisfies PiAdapterShape;
});
