/**
 * Map Pi agent session events into canonical provider runtime events.
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

function extractAssistantText(message: unknown): string | undefined {
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
      if (entry.type === "text" && typeof entry.text === "string") {
        return entry.text;
      }
      return undefined;
    })
    .filter((entry): entry is string => entry !== undefined);
  const joined = parts.join("");
  return joined.length > 0 ? joined : undefined;
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
 */
export function mergePiAssistantText(
  previousText: string | undefined,
  nextText: string,
): {
  readonly latestText: string;
  readonly deltaToEmit: string;
} {
  const latestText =
    previousText && previousText.length > nextText.length && previousText.startsWith(nextText)
      ? previousText
      : nextText;
  return {
    latestText,
    deltaToEmit: latestText.slice(commonPrefixLength(previousText ?? "", latestText)),
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

  const record = event as Record<string, unknown>;
  const type = readString(record, "type");
  if (!type) {
    return events;
  }

  switch (type) {
    case "message_update": {
      const assistantMessageEvent = record.assistantMessageEvent;
      if (!isRecord(assistantMessageEvent)) {
        break;
      }
      const deltaType = readString(assistantMessageEvent, "type");
      const partId =
        readString(assistantMessageEvent, "contentIndex") ??
        readString(record, "messageId") ??
        "assistant";
      if (deltaType === "text_delta" || deltaType === "thinking_delta") {
        const delta = readString(assistantMessageEvent, "delta");
        if (!delta || delta.length === 0) {
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
            streamKind: deltaType === "thinking_delta" ? "reasoning_text" : "assistant_text",
            delta,
          },
        });
      }
      break;
    }
    case "message_end": {
      const message = record.message;
      const text = extractAssistantText(message);
      const partId = isRecord(message) ? (readString(message, "id") ?? "assistant") : "assistant";
      if (text) {
        // Some Pi backends (esp. local/non-streaming) skip message_update
        // text_delta events and only deliver the final message_end payload.
        const previousText = state.assistantTextByPartId.get(partId) ?? "";
        const { latestText, deltaToEmit } = mergePiAssistantText(previousText, text);
        state.assistantTextByPartId.set(partId, latestText);
        if (deltaToEmit.length > 0) {
          yield* push({
            ...base,
            type: "content.delta",
            itemId: RuntimeItemId.make(partId),
            payload: {
              streamKind: "assistant_text",
              delta: deltaToEmit,
            },
          });
        }
      }
      if (text && !state.completedAssistantPartIds.has(partId)) {
        state.completedAssistantPartIds.add(partId);
        yield* push({
          ...base,
          type: "item.completed",
          itemId: RuntimeItemId.make(partId),
          payload: {
            itemType: "assistant_message",
            status: "completed",
            title: "Assistant message",
            detail: text,
          },
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
