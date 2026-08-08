/**
 * PiAdapterSessionHelpers — session state decoding, turn routing, and resume helpers.
 *
 * @module provider/pi/PiAdapterSessionHelpers
 */
import type { ProviderSession, TurnId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Ref from "effect/Ref";
import * as Scope from "effect/Scope";

import type { PiRpcClient } from "./PiRpcClient.ts";
import { buildPiResumeCursor, type PiRpcSessionState } from "./PiRpcProtocol.ts";
import type { ApprovalRequestId } from "@t3tools/contracts";
import type { ProviderApprovalDecision, ProviderUserInputAnswers } from "@t3tools/contracts";
import * as Deferred from "effect/Deferred";
import type { ThreadId } from "@t3tools/contracts";

export interface PiSessionContext {
  readonly threadId: ThreadId;
  session: ProviderSession;
  readonly client: PiRpcClient;
  readonly sessionScope: Scope.Closeable;
  eventFiber: Fiber.Fiber<void, never>;
  activeTurnId: TurnId | undefined;
  lastRunningTurnId: TurnId | undefined;
  activeModelSlug: string | undefined;
  isStreaming: boolean;
  readonly interruptedTurnIds: Set<TurnId>;
  readonly stopped: Ref.Ref<boolean>;
  readonly assistantTextByPartId: Map<string, string>;
  readonly completedAssistantPartIds: Set<string>;
  readonly pendingApprovals: Map<ApprovalRequestId, PendingApproval>;
  readonly pendingUserInputs: Map<ApprovalRequestId, PendingUserInput>;
  turns: Array<{ id: TurnId; items: Array<unknown> }>;
}

export type PendingUserInputResolution =
  | { readonly _tag: "answered"; readonly answers: ProviderUserInputAnswers }
  | { readonly _tag: "cancelled" };

export interface PendingApproval {
  readonly piRequestId: string;
  readonly decision: Deferred.Deferred<ProviderApprovalDecision>;
}

export interface PendingUserInput {
  readonly piRequestId: string;
  readonly questionId: string;
  readonly resolution: Deferred.Deferred<PendingUserInputResolution>;
}

/**
 * Decode a Pi `get_state` payload into a typed session snapshot.
 */
export function decodePiSessionState(data: unknown): PiRpcSessionState | undefined {
  if (typeof data !== "object" || data === null) {
    return undefined;
  }
  const record = data as Record<string, unknown>;
  if (typeof record.sessionId !== "string") {
    return undefined;
  }
  return {
    sessionId: record.sessionId,
    ...(typeof record.sessionFile === "string" ? { sessionFile: record.sessionFile } : {}),
    ...(typeof record.sessionName === "string" ? { sessionName: record.sessionName } : {}),
    ...(record.isStreaming === true ? { isStreaming: true } : {}),
  };
}

/**
 * Format a Pi model selection as `provider/modelId`.
 */
export function modelSlugFromState(state: PiRpcSessionState | undefined): string | undefined {
  if (!state?.model) {
    return undefined;
  }
  return `${state.model.provider}/${state.model.id}`;
}

export type PiTurnRpcCommandType = "prompt" | "steer" | "follow_up";

/**
 * Choose the Pi RPC command for a user turn.
 * Mid-stream sends queue via `follow_up`; `steer` is reserved for interrupt-style steering.
 */
export function resolvePiTurnCommand(input: {
  readonly isStreaming: boolean;
  readonly hasActiveTurn: boolean;
  readonly wantsInterruptSteer: boolean;
}): PiTurnRpcCommandType {
  if (input.isStreaming && input.hasActiveTurn) {
    return input.wantsInterruptSteer ? "steer" : "follow_up";
  }
  return "prompt";
}

/**
 * Refresh the in-memory session from a Pi RPC state snapshot.
 */
export const refreshPiSessionFromState = Effect.fn("refreshPiSessionFromState")(function* (
  context: PiSessionContext,
  state: PiRpcSessionState | undefined,
  nowIso: Effect.Effect<string>,
) {
  const resumeCursor = buildPiResumeCursor(state);
  context.isStreaming = state?.isStreaming === true;
  if (resumeCursor) {
    context.session = {
      ...context.session,
      resumeCursor,
      updatedAt: yield* nowIso,
    };
  }
  return resumeCursor;
});
