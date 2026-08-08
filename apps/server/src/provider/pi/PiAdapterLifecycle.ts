/**
 * PiAdapterLifecycle — event pump, session bootstrap, and session lifecycle handlers.
 *
 * @module provider/pi/PiAdapterLifecycle
 */
import {
  EventId,
  type PiSettings,
  type ProviderDriverKind,
  type ProviderInstanceId,
  type ProviderRuntimeEvent,
  type ProviderSession,
  ThreadId,
  TurnId,
} from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import * as Effect from "effect/Effect";
import type * as FileSystem from "effect/FileSystem";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Ref from "effect/Ref";
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import {
  ProviderAdapterProcessError,
  ProviderAdapterRequestError,
  ProviderAdapterSessionClosedError,
  ProviderAdapterSessionNotFoundError,
  ProviderAdapterValidationError,
} from "../Errors.ts";
import type { ServerConfig } from "../../config.ts";
import { cancelPendingExtensionUiRequests } from "./PiAdapterExtensionUi.ts";
import { createPiAdapterTurns } from "./PiAdapterTurns.ts";
import {
  decodePiSessionState,
  modelSlugFromState,
  type PiSessionContext,
} from "./PiAdapterSessionHelpers.ts";
import { PiRpcClientError, type PiRpcClient } from "./PiRpcClient.ts";
import { mapPiRpcClientErrorForMethod } from "./PiRpcErrors.ts";
import {
  buildPiResumeCursor,
  parsePiModelSelection,
  parsePiResumeCursor,
  validatePiResumeCursor,
} from "./PiRpcProtocol.ts";
import type { PiSpawnOptions } from "./PiSpawn.ts";
import { mapPiAgentEvent, parsePiExtensionUiRequest } from "./PiRuntimeEvents.ts";
import type { PiRpcExtensionUiRequest } from "./PiRpcProtocol.ts";
import type { PiAdapterShape } from "../Services/PiAdapter.ts";
import * as Schema from "effect/Schema";

const isPiRpcClientError = Schema.is(PiRpcClientError);

export interface PiAdapterLifecycleDeps {
  readonly provider: ProviderDriverKind;
  readonly piSettings: PiSettings;
  readonly serverConfig: ServerConfig["Service"];
  readonly fileSystem: FileSystem.FileSystem;
  readonly sessions: Map<ThreadId, PiSessionContext>;
  readonly boundInstanceId: ProviderInstanceId;
  readonly createRpcClient: (input: PiSpawnOptions) => Effect.Effect<PiRpcClient>;
  readonly environment?: NodeJS.ProcessEnv;
  readonly emit: (event: ProviderRuntimeEvent) => Effect.Effect<void>;
  readonly buildEventBase: (input: {
    readonly threadId: ThreadId;
    readonly turnId?: TurnId;
    readonly itemId?: string;
    readonly createdAt?: string;
    readonly raw?: unknown;
  }) => Effect.Effect<{
    readonly eventId: EventId;
    readonly provider: ProviderDriverKind;
    readonly threadId: ThreadId;
    readonly createdAt: string;
    readonly turnId?: TurnId;
    readonly raw?: { readonly source: "pi.rpc.event"; readonly payload: unknown };
  }>;
  readonly logNativeEvent: (threadId: ThreadId, payload: unknown) => Effect.Effect<void>;
  readonly nowIso: Effect.Effect<string>;
  readonly randomUUIDv4: Effect.Effect<string, ProviderAdapterRequestError>;
  readonly handlePiExtensionUiRequest: (
    context: PiSessionContext,
    request: PiRpcExtensionUiRequest,
  ) => Effect.Effect<void>;
}

/**
 * Create Pi adapter lifecycle handlers for session start, turns, interrupts, and the event pump.
 */
export function createPiAdapterLifecycle(deps: PiAdapterLifecycleDeps) {
  const {
    provider: PROVIDER,
    piSettings,
    serverConfig,
    fileSystem,
    sessions,
    boundInstanceId,
    createRpcClient,
    environment,
    emit,
    buildEventBase,
    logNativeEvent,
    nowIso,
    randomUUIDv4,
    handlePiExtensionUiRequest,
  } = deps;

  const ensureSessionContext = Effect.fn("ensureSessionContext")(function* (threadId: ThreadId) {
    const context = sessions.get(threadId);
    if (!context) {
      return yield* new ProviderAdapterSessionNotFoundError({ provider: PROVIDER, threadId });
    }
    if (yield* Ref.get(context.stopped)) {
      return yield* new ProviderAdapterSessionClosedError({ provider: PROVIDER, threadId });
    }
    return context;
  });

  const stopPiContext = Effect.fn("stopPiContext")(function* (context: PiSessionContext) {
    if (yield* Ref.getAndSet(context.stopped, true)) {
      return false;
    }
    yield* cancelPendingExtensionUiRequests(context);
    yield* Fiber.interrupt(context.eventFiber).pipe(Effect.ignore);
    yield* context.client.stop();
    yield* Scope.close(context.sessionScope, Exit.void);
    return true;
  });

  const startEventPump = Effect.fn("startEventPump")(function* (context: PiSessionContext) {
    return yield* context.client.events.pipe(
      Stream.runForEach((event) =>
        Effect.gen(function* () {
          yield* logNativeEvent(context.threadId, event);

          const extensionUiRequest = parsePiExtensionUiRequest(event);
          if (extensionUiRequest) {
            yield* handlePiExtensionUiRequest(context, extensionUiRequest);
            return;
          }

          if (event.type === "agent_start") {
            context.isStreaming = true;
          }

          const settledTurnId = context.activeTurnId ?? context.lastRunningTurnId;
          const mapped = yield* mapPiAgentEvent(
            event,
            {
              threadId: context.threadId,
              turnId: settledTurnId,
              interruptedTurnIds: context.interruptedTurnIds,
              createdAt: yield* nowIso,
              nextEventId: () => randomUUIDv4.pipe(Effect.map(EventId.make), Effect.orDie),
            },
            {
              assistantTextByPartId: context.assistantTextByPartId,
              completedAssistantPartIds: context.completedAssistantPartIds,
            },
          );
          for (const runtimeEvent of mapped) {
            yield* emit(runtimeEvent);
          }

          if (event.type === "agent_settled") {
            context.isStreaming = false;
            if (settledTurnId && !context.interruptedTurnIds.has(settledTurnId)) {
              context.activeTurnId = undefined;
              context.session = {
                ...context.session,
                status: "ready",
                updatedAt: yield* nowIso,
              };
              delete (context.session as { activeTurnId?: TurnId }).activeTurnId;
            }
            if (settledTurnId && context.interruptedTurnIds.has(settledTurnId)) {
              context.interruptedTurnIds.delete(settledTurnId);
            }
            if (settledTurnId) {
              context.lastRunningTurnId = undefined;
            }
          }
        }),
      ),
      Effect.orDie,
      Effect.forkIn(context.sessionScope),
    );
  });

  const startSession: PiAdapterShape["startSession"] = Effect.fn("startSession")(function* (input) {
    const directory = input.cwd ?? serverConfig.cwd;
    const existing = sessions.get(input.threadId);
    if (existing) {
      yield* stopPiContext(existing);
      sessions.delete(input.threadId);
    }

    const sessionScope = yield* Scope.make();
    const resume = parsePiResumeCursor(input.resumeCursor);
    const resumeIssue = validatePiResumeCursor(resume);
    if (resumeIssue) {
      yield* Scope.close(sessionScope, Exit.void).pipe(Effect.ignore);
      return yield* new ProviderAdapterValidationError({
        provider: PROVIDER,
        operation: "startSession",
        issue: resumeIssue,
      });
    }

    const startedExit = yield* Effect.exit(
      Effect.gen(function* () {
        const client = (yield* createRpcClient({
          settings: piSettings,
          cwd: directory,
          ...(environment ? { environment } : {}),
        })) as PiRpcClient;
        yield* client.start().pipe(
          Effect.mapError(
            (cause) =>
              new ProviderAdapterProcessError({
                provider: PROVIDER,
                threadId: input.threadId,
                detail: isPiRpcClientError(cause) ? cause.detail : String(cause),
                cause,
              }),
          ),
        );

        if (resume?.sessionFile) {
          yield* client
            .sendCommand({ type: "switch_session", sessionPath: resume.sessionFile })
            .pipe(Effect.mapError(mapPiRpcClientErrorForMethod(PROVIDER, "switch_session")));
        }

        const stateResponse = yield* client
          .sendCommand({ type: "get_state" })
          .pipe(Effect.mapError(mapPiRpcClientErrorForMethod(PROVIDER, "get_state")));
        const state = decodePiSessionState(stateResponse.data);
        const selectedModel = parsePiModelSelection(
          input.modelSelection?.model,
          piSettings.defaultProvider,
          piSettings.defaultModel,
        );
        if (selectedModel) {
          yield* client
            .sendCommand({
              type: "set_model",
              provider: selectedModel.provider,
              modelId: selectedModel.modelId,
            })
            .pipe(Effect.mapError(mapPiRpcClientErrorForMethod(PROVIDER, "set_model")));
        }

        return { client, state, selectedModel };
      }).pipe(Effect.provideService(Scope.Scope, sessionScope)),
    );

    if (Exit.isFailure(startedExit)) {
      yield* Scope.close(sessionScope, Exit.void).pipe(Effect.ignore);
      const cause = Cause.squash(startedExit.cause);
      return yield* new ProviderAdapterProcessError({
        provider: PROVIDER,
        threadId: input.threadId,
        detail: cause instanceof Error ? cause.message : String(cause),
        cause,
      });
    }

    const { client, state, selectedModel } = startedExit.value;
    const createdAt = yield* nowIso;
    const modelSlug = selectedModel
      ? `${selectedModel.provider}/${selectedModel.modelId}`
      : modelSlugFromState(state);
    const session: ProviderSession = {
      provider: PROVIDER,
      providerInstanceId: boundInstanceId,
      status: "ready",
      runtimeMode: input.runtimeMode,
      cwd: directory,
      ...(modelSlug ? { model: modelSlug } : {}),
      threadId: input.threadId,
      resumeCursor: buildPiResumeCursor(state),
      createdAt,
      updatedAt: createdAt,
    };

    const context: PiSessionContext = {
      threadId: input.threadId,
      session,
      client,
      sessionScope,
      eventFiber: undefined as unknown as Fiber.Fiber<void, never>,
      activeTurnId: undefined,
      lastRunningTurnId: undefined,
      activeModelSlug: modelSlug,
      isStreaming: state?.isStreaming === true,
      interruptedTurnIds: new Set(),
      stopped: yield* Ref.make(false),
      assistantTextByPartId: new Map(),
      completedAssistantPartIds: new Set(),
      pendingApprovals: new Map(),
      pendingUserInputs: new Map(),
      turns: [],
    };
    context.eventFiber = yield* startEventPump(context);
    sessions.set(input.threadId, context);

    yield* emit({
      ...(yield* buildEventBase({ threadId: input.threadId })),
      type: "session.started",
      payload: { message: "Pi session started" },
    });
    yield* emit({
      ...(yield* buildEventBase({ threadId: input.threadId })),
      type: "thread.started",
      payload: {
        providerThreadId: state?.sessionId ?? input.threadId,
      },
    });

    return session;
  });

  const { sendTurn, interruptTurn } = createPiAdapterTurns({
    provider: PROVIDER,
    piSettings,
    serverConfig,
    fileSystem,
    ensureSessionContext,
    emit,
    buildEventBase,
    logNativeEvent,
    nowIso,
    randomUUIDv4,
  });

  return {
    ensureSessionContext,
    stopPiContext,
    startEventPump,
    startSession,
    sendTurn,
    interruptTurn,
  };
}
