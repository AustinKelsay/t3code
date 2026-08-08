/**
 * PiAdapterExtensionUi — extension UI approvals, user-input prompts, and respond helpers.
 *
 * @module provider/pi/PiAdapterExtensionUi
 */
import {
  ApprovalRequestId,
  type ProviderApprovalDecision,
  type ProviderRuntimeEvent,
  type ProviderUserInputAnswers,
  RuntimeRequestId,
  type TurnId,
  type ThreadId,
} from "@t3tools/contracts";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";

import { ProviderAdapterRequestError } from "../Errors.ts";
import {
  buildPiExtensionUiCancelResponse,
  buildPiExtensionUiConfirmedResponse,
  buildPiExtensionUiValueResponse,
  isPiExtensionUiDialogMethod,
  isPiExtensionUiFireAndForgetMethod,
  mapPiConfirmToRequestOpenedPayload,
  mapPiEditorToUserInputQuestions,
  mapPiInputToUserInputQuestions,
  mapPiNotifyToRuntimeWarning,
  mapPiSelectToUserInputQuestions,
} from "./PiRuntimeEvents.ts";
import type { PiRpcExtensionUiRequest, PiRpcExtensionUiResponse } from "./PiRpcProtocol.ts";
import { ProviderDriverKind } from "@t3tools/contracts";
import type {
  PendingApproval,
  PendingUserInput,
  PendingUserInputResolution,
  PiSessionContext,
} from "./PiAdapterSessionHelpers.ts";
import type { PiRpcClient } from "./PiRpcClient.ts";

const PROVIDER = ProviderDriverKind.make("piAgent");

export function settlePendingApprovalsAsCancelled(
  pendingApprovals: ReadonlyMap<ApprovalRequestId, PendingApproval>,
): Effect.Effect<void> {
  return Effect.forEach(
    Array.from(pendingApprovals.values()),
    (pending) => Deferred.succeed(pending.decision, "cancel").pipe(Effect.ignore),
    { discard: true },
  );
}

export function settlePendingUserInputsAsCancelled(
  pendingUserInputs: ReadonlyMap<ApprovalRequestId, PendingUserInput>,
): Effect.Effect<void> {
  return Effect.forEach(
    Array.from(pendingUserInputs.values()),
    (pending) => Deferred.succeed(pending.resolution, { _tag: "cancelled" }).pipe(Effect.ignore),
    { discard: true },
  );
}

/**
 * Cancel all blocking extension UI requests for a session.
 */
export function cancelPendingExtensionUiRequests(context: PiSessionContext): Effect.Effect<void> {
  return Effect.gen(function* () {
    yield* settlePendingApprovalsAsCancelled(context.pendingApprovals);
    yield* settlePendingUserInputsAsCancelled(context.pendingUserInputs);
  });
}

export interface PiExtensionUiHandlerDeps {
  readonly emit: (event: ProviderRuntimeEvent) => Effect.Effect<void>;
  readonly buildEventBase: (input: {
    readonly threadId: ThreadId;
    readonly turnId?: TurnId;
    readonly createdAt?: string;
    readonly raw?: unknown;
  }) => Effect.Effect<
    {
      readonly eventId: import("@t3tools/contracts").EventId;
      readonly provider: typeof PROVIDER;
      readonly threadId: ThreadId;
      readonly createdAt: string;
      readonly turnId?: TurnId;
      readonly raw?: { readonly source: "pi.rpc.event"; readonly payload: unknown };
    },
    never,
    never
  >;
  readonly sendPiExtensionUiResponse: (
    context: PiSessionContext,
    response: PiRpcExtensionUiResponse,
  ) => Effect.Effect<void>;
}

function extensionUiEventBase(
  deps: PiExtensionUiHandlerDeps,
  context: PiSessionContext,
  raw: PiRpcExtensionUiRequest,
) {
  return deps.buildEventBase({
    threadId: context.threadId,
    ...(context.activeTurnId ? { turnId: context.activeTurnId } : {}),
    raw,
  });
}

/**
 * Handle one Pi extension UI RPC request and emit the matching runtime events.
 */
export const createHandlePiExtensionUiRequest = (deps: PiExtensionUiHandlerDeps) =>
  Effect.fn("handlePiExtensionUiRequest")(function* (
    context: PiSessionContext,
    request: PiRpcExtensionUiRequest,
  ) {
    if (isPiExtensionUiFireAndForgetMethod(request.method)) {
      if (request.method === "notify") {
        const warning = mapPiNotifyToRuntimeWarning(request);
        if (warning) {
          yield* deps.emit({
            ...(yield* extensionUiEventBase(deps, context, request)),
            type: "runtime.warning",
            payload: warning,
          });
        }
      }
      return;
    }

    if (!isPiExtensionUiDialogMethod(request.method)) {
      return;
    }

    const requestId = ApprovalRequestId.make(request.id);
    const runtimeRequestId = RuntimeRequestId.make(requestId);

    if (request.method === "confirm") {
      const openedPayload = mapPiConfirmToRequestOpenedPayload(request);
      const decision = yield* Deferred.make<ProviderApprovalDecision>();
      context.pendingApprovals.set(requestId, {
        piRequestId: request.id,
        decision,
      });
      yield* deps.emit({
        ...(yield* extensionUiEventBase(deps, context, request)),
        type: "request.opened",
        requestId: runtimeRequestId,
        payload: openedPayload,
      });
      const resolved = yield* Deferred.await(decision);
      context.pendingApprovals.delete(requestId);
      const response =
        resolved === "cancel"
          ? buildPiExtensionUiCancelResponse(request.id)
          : buildPiExtensionUiConfirmedResponse(
              request.id,
              resolved === "accept" || resolved === "acceptForSession",
            );
      yield* deps.sendPiExtensionUiResponse(context, response);
      yield* deps.emit({
        ...(yield* extensionUiEventBase(deps, context, request)),
        type: "request.resolved",
        requestId: runtimeRequestId,
        payload: {
          requestType: openedPayload.requestType,
          decision: resolved,
        },
      });
      return;
    }

    const mapped =
      request.method === "select"
        ? mapPiSelectToUserInputQuestions(request)
        : request.method === "input"
          ? mapPiInputToUserInputQuestions(request)
          : mapPiEditorToUserInputQuestions(request);
    const resolution = yield* Deferred.make<PendingUserInputResolution>();
    context.pendingUserInputs.set(requestId, {
      piRequestId: request.id,
      questionId: mapped.questionId,
      resolution,
    });
    yield* deps.emit({
      ...(yield* extensionUiEventBase(deps, context, request)),
      type: "user-input.requested",
      requestId: runtimeRequestId,
      payload: { questions: mapped.questions },
    });
    const resolved = yield* Deferred.await(resolution);
    context.pendingUserInputs.delete(requestId);
    const answers = resolved._tag === "answered" ? resolved.answers : {};
    const answerValue = answers[mapped.questionId];
    const response =
      resolved._tag === "cancelled" || typeof answerValue !== "string" || answerValue.length === 0
        ? buildPiExtensionUiCancelResponse(request.id)
        : buildPiExtensionUiValueResponse(request.id, answerValue);
    yield* deps.sendPiExtensionUiResponse(context, response);
    yield* deps.emit({
      ...(yield* extensionUiEventBase(deps, context, request)),
      type: "user-input.resolved",
      requestId: runtimeRequestId,
      payload: { answers },
    });
  });

/**
 * Resolve a pending Pi approval dialog.
 */
export const respondToPiApproval = Effect.fn("respondToPiApproval")(function* (
  context: PiSessionContext,
  requestId: ApprovalRequestId,
  decision: ProviderApprovalDecision,
) {
  const pending = context.pendingApprovals.get(requestId);
  if (!pending) {
    return yield* new ProviderAdapterRequestError({
      provider: PROVIDER,
      method: "extension_ui_response",
      detail: `Unknown pending approval request: ${requestId}`,
    });
  }
  yield* Deferred.succeed(pending.decision, decision);
});

/**
 * Resolve a pending Pi user-input dialog.
 */
export const respondToPiUserInput = Effect.fn("respondToPiUserInput")(function* (
  context: PiSessionContext,
  requestId: ApprovalRequestId,
  answers: ProviderUserInputAnswers,
) {
  const pending = context.pendingUserInputs.get(requestId);
  if (!pending) {
    return yield* new ProviderAdapterRequestError({
      provider: PROVIDER,
      method: "extension_ui_response",
      detail: `Unknown pending user-input request: ${requestId}`,
    });
  }
  yield* Deferred.succeed(pending.resolution, { _tag: "answered", answers });
});

/**
 * Write an extension UI response line to the Pi RPC client.
 */
export function sendPiExtensionUiResponse(
  client: PiRpcClient,
  response: PiRpcExtensionUiResponse,
): Effect.Effect<void> {
  return client.writeLine(response).pipe(Effect.ignore);
}
