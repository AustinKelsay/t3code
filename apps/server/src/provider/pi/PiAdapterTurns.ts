/**
 * PiAdapterTurns — send-turn and interrupt-turn handlers for the Pi provider adapter.
 *
 * @module provider/pi/PiAdapterTurns
 */
import {
  EventId,
  type PiSettings,
  type ProviderDriverKind,
  type ProviderRuntimeEvent,
  TurnId,
  type ThreadId,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import type * as FileSystem from "effect/FileSystem";
import * as Schema from "effect/Schema";

import {
  ProviderAdapterRequestError,
  ProviderAdapterSessionClosedError,
  ProviderAdapterSessionNotFoundError,
  ProviderAdapterValidationError,
} from "../Errors.ts";
import type { ServerConfig } from "../../config.ts";
import { cancelPendingExtensionUiRequests } from "./PiAdapterExtensionUi.ts";
import {
  decodePiSessionState,
  refreshPiSessionFromState,
  resolvePiTurnCommand,
  type PiSessionContext,
} from "./PiAdapterSessionHelpers.ts";
import { resolvePiTurnImages } from "./PiAttachments.ts";
import { mapPiRpcClientErrorForMethod } from "./PiRpcErrors.ts";
import { parsePiModelSelection, type PiRpcSessionState } from "./PiRpcProtocol.ts";
import type { PiAdapterShape } from "../Services/PiAdapter.ts";

const isProviderAdapterRequestError = Schema.is(ProviderAdapterRequestError);

export interface PiAdapterTurnsDeps {
  readonly provider: ProviderDriverKind;
  readonly piSettings: PiSettings;
  readonly serverConfig: ServerConfig["Service"];
  readonly fileSystem: FileSystem.FileSystem;
  readonly ensureSessionContext: (
    threadId: ThreadId,
  ) => Effect.Effect<
    PiSessionContext,
    ProviderAdapterSessionNotFoundError | ProviderAdapterSessionClosedError
  >;
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
}

/**
 * Create Pi adapter turn send and interrupt handlers.
 */
export function createPiAdapterTurns(deps: PiAdapterTurnsDeps) {
  const {
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
  } = deps;

  const refreshSessionFromState = (
    context: PiSessionContext,
    state: PiRpcSessionState | undefined,
  ) => refreshPiSessionFromState(context, state, nowIso);

  const abortTurnAfterSendFailure = Effect.fn("abortTurnAfterSendFailure")(function* (input: {
    readonly context: PiSessionContext;
    readonly threadId: ThreadId;
    readonly turnId: TurnId;
    readonly reason: string;
  }) {
    input.context.activeTurnId = undefined;
    input.context.lastRunningTurnId = undefined;
    input.context.session = {
      ...input.context.session,
      status: "ready",
      updatedAt: yield* nowIso,
    };
    delete (input.context.session as { activeTurnId?: TurnId }).activeTurnId;
    yield* emit({
      ...(yield* buildEventBase({ threadId: input.threadId, turnId: input.turnId })),
      type: "turn.aborted",
      payload: { reason: input.reason },
    });
  });

  const sendTurn: PiAdapterShape["sendTurn"] = Effect.fn("sendTurn")(function* (input) {
    const context = yield* ensureSessionContext(input.threadId);
    const text = input.input?.trim() ?? "";
    const images = yield* resolvePiTurnImages(fileSystem, {
      attachmentsDir: serverConfig.attachmentsDir,
      attachments: input.attachments,
      operation: "sendTurn",
    });
    if (text.length === 0 && images.length === 0) {
      return yield* new ProviderAdapterValidationError({
        provider: PROVIDER,
        operation: "sendTurn",
        issue: "Pi turns require text input or at least one attachment.",
      });
    }

    const selectedModel = parsePiModelSelection(
      input.modelSelection?.model ?? context.session.model,
      piSettings.defaultProvider,
      piSettings.defaultModel,
    );
    const nextModelSlug = selectedModel
      ? `${selectedModel.provider}/${selectedModel.modelId}`
      : context.activeModelSlug;
    if (
      selectedModel &&
      nextModelSlug !== context.activeModelSlug &&
      context.activeModelSlug !== undefined
    ) {
      yield* context.client
        .sendCommand({
          type: "set_model",
          provider: selectedModel.provider,
          modelId: selectedModel.modelId,
        })
        .pipe(Effect.mapError(mapPiRpcClientErrorForMethod(PROVIDER, "set_model")));
      context.activeModelSlug = nextModelSlug;
    }

    const stateBeforeSend = yield* context.client
      .sendCommand({ type: "get_state" })
      .pipe(Effect.mapError(mapPiRpcClientErrorForMethod(PROVIDER, "get_state")));
    const stateBefore = decodePiSessionState(stateBeforeSend.data);
    context.isStreaming = stateBefore?.isStreaming === true;
    yield* refreshSessionFromState(context, stateBefore);

    const steeringTurnId = context.isStreaming ? context.activeTurnId : undefined;
    const turnId = steeringTurnId ?? TurnId.make(`pi-turn-${yield* randomUUIDv4}`);
    const commandType = resolvePiTurnCommand({
      isStreaming: context.isStreaming,
      hasActiveTurn: steeringTurnId !== undefined,
      wantsInterruptSteer: false,
    });
    context.activeTurnId = turnId;
    context.lastRunningTurnId = turnId;
    context.session = {
      ...context.session,
      status: "running",
      activeTurnId: turnId,
      ...(nextModelSlug ? { model: nextModelSlug } : {}),
      updatedAt: yield* nowIso,
    };

    if (steeringTurnId === undefined) {
      yield* emit({
        ...(yield* buildEventBase({ threadId: input.threadId, turnId })),
        type: "turn.started",
        payload: {
          ...(nextModelSlug ? { model: nextModelSlug } : {}),
        },
      });
    }

    const command = {
      type: commandType,
      message: text,
      ...(images.length > 0 ? { images } : {}),
    } as const;
    yield* logNativeEvent(input.threadId, { direction: "outbound", command });
    yield* context.client.sendCommand(command).pipe(
      Effect.mapError(mapPiRpcClientErrorForMethod(PROVIDER, commandType)),
      Effect.tapError((requestError) =>
        steeringTurnId !== undefined
          ? Effect.void
          : abortTurnAfterSendFailure({
              context,
              threadId: input.threadId,
              turnId,
              reason: isProviderAdapterRequestError(requestError)
                ? requestError.detail
                : String(requestError),
            }),
      ),
    );

    const stateResponse = yield* context.client
      .sendCommand({ type: "get_state" })
      .pipe(Effect.mapError(mapPiRpcClientErrorForMethod(PROVIDER, "get_state")));
    const state = decodePiSessionState(stateResponse.data);
    const resumeCursor = yield* refreshSessionFromState(context, state);

    return {
      threadId: input.threadId,
      turnId,
      ...(resumeCursor ? { resumeCursor } : {}),
    };
  });

  const interruptTurn: PiAdapterShape["interruptTurn"] = Effect.fn("interruptTurn")(
    function* (threadId, turnId) {
      const context = yield* ensureSessionContext(threadId);
      yield* cancelPendingExtensionUiRequests(context);
      yield* context.client
        .sendCommand({ type: "abort" })
        .pipe(Effect.mapError(mapPiRpcClientErrorForMethod(PROVIDER, "abort")));
      const activeTurnId = turnId ?? context.activeTurnId;
      if (activeTurnId) {
        context.interruptedTurnIds.add(activeTurnId);
        context.lastRunningTurnId = activeTurnId;
      }
      context.activeTurnId = undefined;
      context.isStreaming = false;
      if (activeTurnId) {
        yield* emit({
          ...(yield* buildEventBase({ threadId, turnId: activeTurnId })),
          type: "turn.aborted",
          payload: { reason: "Interrupted by user." },
        });
      }
    },
  );

  return {
    sendTurn,
    interruptTurn,
  };
}
