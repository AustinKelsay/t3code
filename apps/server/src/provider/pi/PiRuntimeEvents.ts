/**
 * Map Pi agent session events into canonical provider runtime events.
 *
 * Pi emits user `message_start`/`message_end` for the echoed prompt, then
 * streams assistant text under `message_update` (`text_delta` /
 * `thinking_delta`) with numeric `contentIndex` values, and finally a
 * role=assistant `message_end`. Mapping must:
 * - ignore non-assistant messages (otherwise the user prompt appears as
 *   fold/reasoning commentary in the UI)
 * - use stable part ids so streamed deltas and the final `message_end` share
 *   one assistant_text / reasoning_text stream (mismatched ids duplicated the
 *   full reply: "HelloHello")
 *
 * @module provider/pi/PiRuntimeEvents
 */
import {
  EventId,
  ProviderDriverKind,
  type ProviderRuntimeEvent,
  RuntimeItemId,
  type ToolLifecycleItemType,
  TurnId,
  type ThreadId,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";

import type { PiAgentSessionEvent } from "./PiRpcProtocol.ts";

const PROVIDER = ProviderDriverKind.make("piAgent");

/** Stable runtime item id suffix for streamed/final assistant prose. */
export const PI_ASSISTANT_TEXT_PART_ID = "assistant-text";

/** Stable runtime item id suffix for streamed/final assistant thinking. */
export const PI_ASSISTANT_REASONING_PART_ID = "assistant-reasoning";

/**
 * Build a turn-scoped runtime item id so consecutive Pi turns do not reuse the
 * same orchestration message id (projector appends streaming deltas by id).
 */
export function piAssistantPartId(kind: "text" | "reasoning", turnId: TurnId | undefined): string {
  const suffix = kind === "text" ? PI_ASSISTANT_TEXT_PART_ID : PI_ASSISTANT_REASONING_PART_ID;
  return turnId ? `${turnId}:${suffix}` : suffix;
}

export interface PiRuntimeEventContext {
  readonly threadId: ThreadId;
  readonly turnId?: TurnId | undefined;
  readonly interruptedTurnIds?: ReadonlySet<TurnId>;
  readonly createdAt: string;
  readonly nextEventId: () => Effect.Effect<EventId>;
}

export interface PiAssistantTextState {
  readonly partId: string;
  readonly previousText: string;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function readString(record: Record<string, unknown>, key: string): string | undefined {
  const value = record[key];
  return typeof value === "string" && value.trim().length > 0 ? value : undefined;
}

function toToolLifecycleItemType(toolName: string): ToolLifecycleItemType {
  const normalized = toolName.toLowerCase();
  if (normalized.includes("bash") || normalized.includes("command")) {
    return "command_execution";
  }
  if (normalized.includes("edit") || normalized.includes("write") || normalized.includes("patch")) {
    return "file_change";
  }
  if (normalized.includes("web")) {
    return "web_search";
  }
  return "dynamic_tool_call";
}

function readMessageRole(message: unknown): string | undefined {
  return isRecord(message) ? readString(message, "role") : undefined;
}

/**
 * Join text content parts from a Pi agent message.
 */
function extractTextParts(message: unknown, partType: "text" | "thinking"): string | undefined {
  if (!isRecord(message)) {
    return undefined;
  }
  const content = message.content;
  if (!Array.isArray(content)) {
    return undefined;
  }
  const parts = content
    .map((entry) => {
      if (!isRecord(entry)) {
        return undefined;
      }
      if (entry.type !== partType) {
        return undefined;
      }
      if (partType === "text" && typeof entry.text === "string") {
        return entry.text;
      }
      if (partType === "thinking") {
        if (typeof entry.thinking === "string") {
          return entry.thinking;
        }
        if (typeof entry.text === "string") {
          return entry.text;
        }
      }
      return undefined;
    })
    .filter((entry): entry is string => entry !== undefined);
  const joined = parts.join("");
  return joined.length > 0 ? joined : undefined;
}

function extractAssistantText(message: unknown): string | undefined {
  return extractTextParts(message, "text");
}

function extractAssistantThinking(message: unknown): string | undefined {
  return extractTextParts(message, "thinking");
}

function commonPrefixLength(left: string, right: string): number {
  let index = 0;
  while (index < left.length && index < right.length && left[index] === right[index]) {
    index += 1;
  }
  return index;
}

/**
 * Merge streamed assistant text into the latest snapshot and delta to emit.
 *
 * Prefer growth (prefix extension) and refuse regressions: a shorter or
 * unrelated `nextText` must not wipe a longer streamed buffer. Pi's
 * `message_end` can otherwise hand us a truncated thinking snapshot that
 * collapses accumulated reasoning to a single character.
 */
export function mergePiAssistantText(
  previousText: string | undefined,
  nextText: string,
): {
  readonly latestText: string;
  readonly deltaToEmit: string;
} {
  const previous = previousText ?? "";
  let latestText = nextText;
  if (previous.length > 0) {
    if (nextText.startsWith(previous)) {
      latestText = nextText;
    } else if (previous.startsWith(nextText) || previous.length >= nextText.length) {
      latestText = previous;
    }
  }
  return {
    latestText,
    deltaToEmit: latestText.slice(commonPrefixLength(previous, latestText)),
  };
}

/**
 * Map one Pi agent session event into zero or more provider runtime events.
 */
export const mapPiAgentEvent = Effect.fn("mapPiAgentEvent")(function* (
  event: PiAgentSessionEvent,
  context: PiRuntimeEventContext,
  state: {
    readonly assistantTextByPartId: Map<string, string>;
    readonly completedAssistantPartIds: Set<string>;
  },
) {
  const events: Array<ProviderRuntimeEvent> = [];
  const base = {
    provider: PROVIDER,
    threadId: context.threadId,
    createdAt: context.createdAt,
    ...(context.turnId ? { turnId: context.turnId } : {}),
    raw: {
      source: "pi.rpc.event" as const,
      payload: event,
    },
  };

  const push = (runtimeEvent: Omit<ProviderRuntimeEvent, "eventId">) =>
    Effect.gen(function* () {
      events.push({
        ...runtimeEvent,
        eventId: yield* context.nextEventId(),
      } as ProviderRuntimeEvent);
    });

  const emitMergedText = Effect.fn("emitMergedText")(function* (input: {
    readonly partId: string;
    readonly nextText: string;
    readonly streamKind: "assistant_text" | "reasoning_text";
    readonly complete: boolean;
    readonly completeTitle: string;
    readonly itemType: "assistant_message" | "reasoning";
  }) {
    // text_start already finalizes reasoning before prose streams. message_end
    // must not re-emit a truncated thinking snapshot afterward — that delta
    // clears the ingestion buffer and replaces the activity with ".".
    if (state.completedAssistantPartIds.has(input.partId)) {
      return;
    }
    const previousText = state.assistantTextByPartId.get(input.partId) ?? "";
    const { latestText, deltaToEmit } = mergePiAssistantText(previousText, input.nextText);
    state.assistantTextByPartId.set(input.partId, latestText);
    if (deltaToEmit.length > 0) {
      yield* push({
        ...base,
        type: "content.delta",
        itemId: RuntimeItemId.make(input.partId),
        payload: {
          streamKind: input.streamKind,
          delta: deltaToEmit,
        },
      });
    }
    if (input.complete && latestText.length > 0) {
      state.completedAssistantPartIds.add(input.partId);
      yield* push({
        ...base,
        type: "item.completed",
        itemId: RuntimeItemId.make(input.partId),
        payload: {
          itemType: input.itemType,
          status: "completed",
          title: input.completeTitle,
          detail: latestText,
        },
      });
    }
  });

  const record = event as Record<string, unknown>;
  const type = readString(record, "type");
  if (!type) {
    return events;
  }

  switch (type) {
    case "agent_start":
    case "turn_start": {
      // Part-id maps are session-scoped; clear at turn boundaries so a later
      // turn cannot inherit completion flags or merge against prior prose.
      state.assistantTextByPartId.clear();
      state.completedAssistantPartIds.clear();
      break;
    }
    case "message_update": {
      const assistantMessageEvent = record.assistantMessageEvent;
      if (!isRecord(assistantMessageEvent)) {
        break;
      }
      const deltaType = readString(assistantMessageEvent, "type");
      const textPartId = piAssistantPartId("text", context.turnId);
      const reasoningPartId = piAssistantPartId("reasoning", context.turnId);

      if (deltaType === "text_start") {
        // Close the thinking commentary message before assistant prose starts
        // so the UI can fold reasoning under "Worked for ..." while text streams.
        const reasoningText = state.assistantTextByPartId.get(reasoningPartId);
        if (
          reasoningText &&
          reasoningText.length > 0 &&
          !state.completedAssistantPartIds.has(reasoningPartId)
        ) {
          state.completedAssistantPartIds.add(reasoningPartId);
          yield* push({
            ...base,
            type: "item.completed",
            itemId: RuntimeItemId.make(reasoningPartId),
            payload: {
              itemType: "reasoning",
              status: "completed",
              title: "Reasoning",
              detail: reasoningText,
            },
          });
        }
        break;
      }

      if (deltaType === "text_delta" || deltaType === "thinking_delta") {
        const delta = readString(assistantMessageEvent, "delta");
        if (!delta || delta.length === 0) {
          break;
        }
        const isThinking = deltaType === "thinking_delta";
        const partId = isThinking ? reasoningPartId : textPartId;
        if (state.completedAssistantPartIds.has(partId)) {
          break;
        }
        const previousText = state.assistantTextByPartId.get(partId) ?? "";
        const nextText = previousText + delta;
        state.assistantTextByPartId.set(partId, nextText);
        yield* push({
          ...base,
          type: "content.delta",
          itemId: RuntimeItemId.make(partId),
          payload: {
            streamKind: isThinking ? "reasoning_text" : "assistant_text",
            delta,
          },
        });
      }
      break;
    }
    case "message_end": {
      const message = record.message;
      // Pi echoes the user prompt as message_start/message_end. Mapping those
      // as assistant content made the prompt show up under "Worked for ...".
      if (readMessageRole(message) !== "assistant") {
        break;
      }
      const text = extractAssistantText(message);
      if (text) {
        // Non-streaming backends may skip message_update text_delta and only
        // deliver the final message_end payload. When deltas already ran, this
        // merge is a no-op (shared part id).
        yield* emitMergedText({
          partId: piAssistantPartId("text", context.turnId),
          nextText: text,
          streamKind: "assistant_text",
          complete: true,
          completeTitle: "Assistant message",
          itemType: "assistant_message",
        });
      }
      const thinking = extractAssistantThinking(message);
      if (thinking) {
        yield* emitMergedText({
          partId: piAssistantPartId("reasoning", context.turnId),
          nextText: thinking,
          streamKind: "reasoning_text",
          complete: true,
          completeTitle: "Reasoning",
          itemType: "reasoning",
        });
      }
      break;
    }
    case "tool_execution_start": {
      const toolCallId = readString(record, "toolCallId") ?? "tool";
      const toolName = readString(record, "toolName") ?? "tool";
      yield* push({
        ...base,
        type: "item.started",
        itemId: RuntimeItemId.make(toolCallId),
        payload: {
          itemType: toToolLifecycleItemType(toolName),
          status: "inProgress",
          title: toolName,
        },
      });
      break;
    }
    case "tool_execution_update": {
      const toolCallId = readString(record, "toolCallId") ?? "tool";
      const toolName = readString(record, "toolName") ?? "tool";
      yield* push({
        ...base,
        type: "item.updated",
        itemId: RuntimeItemId.make(toolCallId),
        payload: {
          itemType: toToolLifecycleItemType(toolName),
          status: "inProgress",
          title: toolName,
        },
      });
      break;
    }
    case "tool_execution_end": {
      const toolCallId = readString(record, "toolCallId") ?? "tool";
      const toolName = readString(record, "toolName") ?? "tool";
      const isError = record.isError === true;
      yield* push({
        ...base,
        type: "item.completed",
        itemId: RuntimeItemId.make(toolCallId),
        payload: {
          itemType: toToolLifecycleItemType(toolName),
          status: isError ? "failed" : "completed",
          title: toolName,
        },
      });
      break;
    }
    case "session_info_changed": {
      const name = isRecord(record) ? readString(record, "name") : undefined;
      if (name) {
        yield* push({
          ...base,
          type: "thread.metadata.updated",
          payload: {
            name,
            metadata: {},
          },
        });
      }
      break;
    }
    case "agent_settled": {
      if (context.turnId && !context.interruptedTurnIds?.has(context.turnId)) {
        yield* push({
          ...base,
          type: "turn.completed",
          payload: {
            state: "completed",
          },
        });
      }
      break;
    }
    case "extension_error": {
      const error = readString(record, "error") ?? "Pi extension error.";
      yield* push({
        ...base,
        type: "runtime.error",
        payload: {
          message: error,
          class: "provider_error",
          detail: {
            extensionPath: record.extensionPath,
            event: record.event,
          },
        },
      });
      break;
    }
    case "auto_retry_end": {
      if (record.success === false) {
        const finalError = readString(record, "finalError") ?? "Pi agent retry failed.";
        yield* push({
          ...base,
          type: "runtime.error",
          payload: {
            message: finalError,
            class: "provider_error",
          },
        });
      }
      break;
    }
    default:
      break;
  }

  return events;
});

export {
  buildPiExtensionUiCancelResponse,
  buildPiExtensionUiConfirmedResponse,
  buildPiExtensionUiValueResponse,
  isPiExtensionUiDialogMethod,
  isPiExtensionUiFireAndForgetMethod,
  isPiExtensionUiRequest,
  mapPiConfirmToRequestOpenedPayload,
  mapPiEditorToUserInputQuestions,
  mapPiInputToUserInputQuestions,
  mapPiNotifyToRuntimeWarning,
  mapPiSelectToUserInputQuestions,
  parsePiExtensionUiRequest,
} from "./PiExtensionUiMapping.ts";
