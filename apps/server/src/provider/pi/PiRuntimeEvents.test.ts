import * as NodeAssert from "node:assert/strict";
import { describe, it } from "vite-plus/test";
import * as Effect from "effect/Effect";
import { EventId, ProviderDriverKind, ThreadId, TurnId } from "@t3tools/contracts";
import type { PiAgentSessionEvent } from "./PiRpcProtocol.ts";
import {
  buildPiExtensionUiCancelResponse,
  buildPiExtensionUiConfirmedResponse,
  buildPiExtensionUiValueResponse,
  isPiExtensionUiDialogMethod,
  isPiExtensionUiFireAndForgetMethod,
  isPiExtensionUiRequest,
  mapPiConfirmToRequestOpenedPayload,
  mapPiInputToUserInputQuestions,
  mapPiNotifyToRuntimeWarning,
  mapPiSelectToUserInputQuestions,
  mapPiAgentEvent,
  mergePiAssistantText,
  parsePiExtensionUiRequest,
} from "./PiRuntimeEvents.ts";

const threadId = ThreadId.make("thread-pi-1");
const turnId = TurnId.make("turn-pi-1");

describe("PiRuntimeEvents", () => {
  it("merges assistant text deltas with a common prefix", () => {
    NodeAssert.deepEqual(mergePiAssistantText("Hello", "Hello world"), {
      latestText: "Hello world",
      deltaToEmit: " world",
    });
  });

  it("maps agent_settled to turn.completed", async () => {
    const events = await Effect.runPromise(
      mapPiAgentEvent(
        { type: "agent_settled" },
        {
          threadId,
          turnId,
          createdAt: "2026-01-01T00:00:00.000Z",
          nextEventId: () => Effect.succeed(EventId.make("evt-1")),
        },
        {
          assistantTextByPartId: new Map(),
          completedAssistantPartIds: new Set(),
        },
      ),
    );
    NodeAssert.equal(events.length, 1);
    NodeAssert.equal(events[0]?.type, "turn.completed");
    NodeAssert.equal(events[0]?.provider, ProviderDriverKind.make("piAgent"));
  });

  it("skips turn.completed for interrupted turns", async () => {
    const events = await Effect.runPromise(
      mapPiAgentEvent(
        { type: "agent_settled" },
        {
          threadId,
          turnId,
          interruptedTurnIds: new Set([turnId]),
          createdAt: "2026-01-01T00:00:00.000Z",
          nextEventId: () => Effect.succeed(EventId.make("evt-1b")),
        },
        {
          assistantTextByPartId: new Map(),
          completedAssistantPartIds: new Set(),
        },
      ),
    );
    NodeAssert.deepEqual(events, []);
  });

  it("maps extension_error to runtime.error", async () => {
    const events = await Effect.runPromise(
      mapPiAgentEvent(
        {
          type: "extension_error",
          extensionPath: "/tmp/ext.ts",
          event: "tool_call",
          error: "boom",
        } as PiAgentSessionEvent,
        {
          threadId,
          turnId,
          createdAt: "2026-01-01T00:00:00.000Z",
          nextEventId: () => Effect.succeed(EventId.make("evt-err")),
        },
        {
          assistantTextByPartId: new Map(),
          completedAssistantPartIds: new Set(),
        },
      ),
    );
    NodeAssert.equal(events[0]?.type, "runtime.error");
    if (events[0]?.type === "runtime.error") {
      NodeAssert.equal(events[0].payload.message, "boom");
    }
  });

  it("maps non-streaming message_end text to content.delta", async () => {
    const events = await Effect.runPromise(
      mapPiAgentEvent(
        {
          type: "message_end",
          message: {
            id: "msg-1",
            role: "assistant",
            content: [{ type: "text", text: "pong" }],
          },
        } as PiAgentSessionEvent,
        {
          threadId,
          turnId,
          createdAt: "2026-01-01T00:00:00.000Z",
          nextEventId: () => Effect.succeed(EventId.make("evt-3")),
        },
        {
          assistantTextByPartId: new Map(),
          completedAssistantPartIds: new Set(),
        },
      ),
    );
    NodeAssert.equal(events[0]?.type, "content.delta");
    if (events[0]?.type === "content.delta") {
      NodeAssert.equal(events[0].payload.delta, "pong");
    }
    NodeAssert.equal(events[1]?.type, "item.completed");
  });

  it("maps text_delta message_update to content.delta", async () => {
    const events = await Effect.runPromise(
      mapPiAgentEvent(
        {
          type: "message_update",
          assistantMessageEvent: {
            type: "text_delta",
            contentIndex: "0",
            delta: "Hi",
          },
        } as PiAgentSessionEvent,
        {
          threadId,
          turnId,
          createdAt: "2026-01-01T00:00:00.000Z",
          nextEventId: () => Effect.succeed(EventId.make("evt-2")),
        },
        {
          assistantTextByPartId: new Map(),
          completedAssistantPartIds: new Set(),
        },
      ),
    );
    NodeAssert.equal(events[0]?.type, "content.delta");
    if (events[0]?.type === "content.delta") {
      NodeAssert.equal(events[0].payload.delta, "Hi");
    }
  });

  it("detects extension UI requests and builds cancel responses", () => {
    NodeAssert.equal(
      isPiExtensionUiRequest({
        type: "extension_ui_request",
        id: "ui-1",
        method: "confirm",
      } as PiAgentSessionEvent),
      "ui-1",
    );
    NodeAssert.deepEqual(buildPiExtensionUiCancelResponse("ui-1"), {
      type: "extension_ui_response",
      id: "ui-1",
      cancelled: true,
    });
    NodeAssert.deepEqual(buildPiExtensionUiConfirmedResponse("ui-2", true), {
      type: "extension_ui_response",
      id: "ui-2",
      confirmed: true,
    });
    NodeAssert.deepEqual(buildPiExtensionUiValueResponse("ui-3", "Allow"), {
      type: "extension_ui_response",
      id: "ui-3",
      value: "Allow",
    });
  });

  it("classifies extension UI methods", () => {
    NodeAssert.equal(isPiExtensionUiDialogMethod("confirm"), true);
    NodeAssert.equal(isPiExtensionUiDialogMethod("notify"), false);
    NodeAssert.equal(isPiExtensionUiFireAndForgetMethod("notify"), true);
    NodeAssert.equal(isPiExtensionUiFireAndForgetMethod("select"), false);
  });

  it("maps extension UI requests into T3 payloads", () => {
    const confirm = parsePiExtensionUiRequest({
      type: "extension_ui_request",
      id: "ui-confirm",
      method: "confirm",
      title: "Allow action?",
      message: "This cannot be undone.",
    } as PiAgentSessionEvent);
    NodeAssert.ok(confirm);
    NodeAssert.deepEqual(mapPiConfirmToRequestOpenedPayload(confirm!), {
      requestType: "dynamic_tool_call",
      detail: "Allow action?: This cannot be undone.",
      args: confirm,
    });

    const select = parsePiExtensionUiRequest({
      type: "extension_ui_request",
      id: "ui-select",
      method: "select",
      title: "Pick one",
      options: ["A", "B"],
    } as PiAgentSessionEvent);
    NodeAssert.deepEqual(mapPiSelectToUserInputQuestions(select!), {
      questionId: "Pick one",
      questions: [
        {
          id: "Pick one",
          header: "Pick one",
          question: "Pick one",
          options: [
            { label: "A", description: "A" },
            { label: "B", description: "B" },
          ],
          multiSelect: false,
        },
      ],
    });

    const input = parsePiExtensionUiRequest({
      type: "extension_ui_request",
      id: "ui-input",
      method: "input",
      title: "Name",
      placeholder: "your-name",
    } as PiAgentSessionEvent);
    NodeAssert.equal(mapPiInputToUserInputQuestions(input!).questionId, "Name");

    const notify = parsePiExtensionUiRequest({
      type: "extension_ui_request",
      id: "ui-notify",
      method: "notify",
      message: "Saved",
      notifyType: "warning",
    } as PiAgentSessionEvent);
    NodeAssert.deepEqual(mapPiNotifyToRuntimeWarning(notify!), {
      message: "Warning: Saved",
      detail: notify,
    });
  });
});
