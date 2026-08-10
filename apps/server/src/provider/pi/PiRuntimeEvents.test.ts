import * as NodeAssert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
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
const fixtureDir = dirname(fileURLToPath(import.meta.url));

describe("PiRuntimeEvents", () => {
  it("merges assistant text deltas with a common prefix", () => {
    NodeAssert.deepEqual(mergePiAssistantText("Hello", "Hello world"), {
      latestText: "Hello world",
      deltaToEmit: " world",
    });
  });

  it("does not regress a longer streamed buffer to a truncated snapshot", () => {
    NodeAssert.deepEqual(mergePiAssistantText('The user says "Tell me a story".', "."), {
      latestText: 'The user says "Tell me a story".',
      deltaToEmit: "",
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
      NodeAssert.equal(events[0].itemId, "turn-pi-1:assistant-text");
    }
    NodeAssert.equal(events[1]?.type, "item.completed");
  });

  it("ignores user message_end so the prompt is not echoed as reasoning/commentary", async () => {
    const events = await Effect.runPromise(
      mapPiAgentEvent(
        {
          type: "message_end",
          message: {
            role: "user",
            content: [{ type: "text", text: "hey" }],
          },
        } as PiAgentSessionEvent,
        {
          threadId,
          turnId,
          createdAt: "2026-01-01T00:00:00.000Z",
          nextEventId: () => Effect.succeed(EventId.make("evt-user")),
        },
        {
          assistantTextByPartId: new Map(),
          completedAssistantPartIds: new Set(),
        },
      ),
    );
    NodeAssert.deepEqual(events, []);
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
      NodeAssert.equal(events[0].itemId, "turn-pi-1:assistant-text");
    }
  });

  it("does not re-emit assistant text when message_end follows streamed deltas", async () => {
    const assistantTextByPartId = new Map<string, string>();
    const completedAssistantPartIds = new Set<string>();
    let counter = 0;
    const ctx = {
      threadId,
      turnId,
      createdAt: "2026-01-01T00:00:00.000Z",
      nextEventId: () => Effect.succeed(EventId.make(`evt-${counter++}`)),
    };

    await Effect.runPromise(
      mapPiAgentEvent(
        {
          type: "message_update",
          assistantMessageEvent: {
            type: "text_delta",
            contentIndex: 0,
            delta: "Hello",
          },
        } as PiAgentSessionEvent,
        ctx,
        { assistantTextByPartId, completedAssistantPartIds },
      ),
    );
    await Effect.runPromise(
      mapPiAgentEvent(
        {
          type: "message_update",
          assistantMessageEvent: {
            type: "text_delta",
            contentIndex: 0,
            delta: " world",
          },
        } as PiAgentSessionEvent,
        ctx,
        { assistantTextByPartId, completedAssistantPartIds },
      ),
    );
    const endEvents = await Effect.runPromise(
      mapPiAgentEvent(
        {
          type: "message_end",
          message: {
            role: "assistant",
            content: [{ type: "text", text: "Hello world" }],
          },
        } as PiAgentSessionEvent,
        ctx,
        { assistantTextByPartId, completedAssistantPartIds },
      ),
    );

    NodeAssert.equal(
      endEvents.filter((event) => event.type === "content.delta").length,
      0,
      "message_end must not duplicate already-streamed assistant text",
    );
    NodeAssert.equal(endEvents[0]?.type, "item.completed");
    NodeAssert.equal(assistantTextByPartId.get("turn-pi-1:assistant-text"), "Hello world");
  });

  it("maps thinking_delta with numeric contentIndex to reasoning content.delta", async () => {
    const events = await Effect.runPromise(
      mapPiAgentEvent(
        {
          type: "message_update",
          assistantMessageEvent: {
            type: "thinking_delta",
            contentIndex: 1,
            delta: "considering",
          },
        } as PiAgentSessionEvent,
        {
          threadId,
          turnId,
          createdAt: "2026-01-01T00:00:00.000Z",
          nextEventId: () => Effect.succeed(EventId.make("evt-think")),
        },
        {
          assistantTextByPartId: new Map(),
          completedAssistantPartIds: new Set(),
        },
      ),
    );
    NodeAssert.equal(events[0]?.type, "content.delta");
    if (events[0]?.type === "content.delta") {
      NodeAssert.equal(events[0].payload.streamKind, "reasoning_text");
      NodeAssert.equal(events[0].payload.delta, "considering");
      NodeAssert.equal(events[0].itemId, "turn-pi-1:assistant-reasoning");
    }
  });

  it("scopes assistant item ids per turn so consecutive turns do not share a message id", async () => {
    const assistantTextByPartId = new Map<string, string>();
    const completedAssistantPartIds = new Set<string>();
    let counter = 0;
    const mapWithTurn = (turn: string, delta: string) =>
      Effect.runPromise(
        mapPiAgentEvent(
          {
            type: "message_update",
            assistantMessageEvent: {
              type: "text_delta",
              contentIndex: 0,
              delta,
            },
          } as PiAgentSessionEvent,
          {
            threadId,
            turnId: TurnId.make(turn),
            createdAt: "2026-01-01T00:00:00.000Z",
            nextEventId: () => Effect.succeed(EventId.make(`evt-${counter++}`)),
          },
          { assistantTextByPartId, completedAssistantPartIds },
        ),
      );

    const first = await mapWithTurn("turn-a", "one");
    const second = await mapWithTurn("turn-b", "two");
    NodeAssert.equal(first[0]?.type, "content.delta");
    NodeAssert.equal(second[0]?.type, "content.delta");
    if (first[0]?.type === "content.delta" && second[0]?.type === "content.delta") {
      NodeAssert.equal(first[0].itemId, "turn-a:assistant-text");
      NodeAssert.equal(second[0].itemId, "turn-b:assistant-text");
      NodeAssert.notEqual(first[0].itemId, second[0].itemId);
    }
  });

  it("replays a captured hey turn without user-echo or duplicated assistant text", async () => {
    const captured = JSON.parse(
      readFileSync(join(fixtureDir, "fixtures/pi-hey-turn.json"), "utf8"),
    ) as Array<PiAgentSessionEvent>;
    const assistantTextByPartId = new Map<string, string>();
    const completedAssistantPartIds = new Set<string>();
    let counter = 0;
    const mapped = [];
    for (const event of captured) {
      const runtimeEvents = await Effect.runPromise(
        mapPiAgentEvent(
          event,
          {
            threadId,
            turnId,
            createdAt: "2026-01-01T00:00:00.000Z",
            nextEventId: () => Effect.succeed(EventId.make(`evt-${counter++}`)),
          },
          { assistantTextByPartId, completedAssistantPartIds },
        ),
      );
      mapped.push(...runtimeEvents);
    }

    const assistantDeltas = mapped.filter(
      (event) => event.type === "content.delta" && event.payload.streamKind === "assistant_text",
    );
    const reasoningDeltas = mapped.filter(
      (event) => event.type === "content.delta" && event.payload.streamKind === "reasoning_text",
    );
    const joinedAssistant = assistantDeltas
      .map((event) => (event.type === "content.delta" ? event.payload.delta : ""))
      .join("");

    NodeAssert.equal(
      joinedAssistant.includes("heyhey") || joinedAssistant.startsWith("hey"),
      false,
      `user prompt must not appear in assistant stream: ${joinedAssistant}`,
    );
    NodeAssert.equal(
      joinedAssistant,
      "Hey! What can I help you with?",
      "assistant text must appear exactly once (no stream + message_end duplication)",
    );
    NodeAssert.equal(
      reasoningDeltas.length,
      0,
      "this model emitted no thinking_delta; fake reasoning must not be invented",
    );
    NodeAssert.ok(mapped.some((event) => event.type === "turn.completed"));
  });

  it("replays a captured story turn with thinking_delta as reasoning_text", async () => {
    const captured = JSON.parse(
      readFileSync(join(fixtureDir, "fixtures/pi-story-thinking.json"), "utf8"),
    ) as Array<PiAgentSessionEvent>;
    const assistantTextByPartId = new Map<string, string>();
    const completedAssistantPartIds = new Set<string>();
    let counter = 0;
    const mapped = [];
    for (const event of captured) {
      const runtimeEvents = await Effect.runPromise(
        mapPiAgentEvent(
          event,
          {
            threadId,
            turnId,
            createdAt: "2026-01-01T00:00:00.000Z",
            nextEventId: () => Effect.succeed(EventId.make(`evt-story-${counter++}`)),
          },
          { assistantTextByPartId, completedAssistantPartIds },
        ),
      );
      mapped.push(...runtimeEvents);
    }

    const reasoning = mapped.filter(
      (event) => event.type === "content.delta" && event.payload.streamKind === "reasoning_text",
    );
    const assistant = mapped.filter(
      (event) => event.type === "content.delta" && event.payload.streamKind === "assistant_text",
    );
    NodeAssert.ok(reasoning.length > 0, "expected thinking_delta → reasoning_text");
    NodeAssert.ok(assistant.length > 0, "expected text_delta → assistant_text");
    NodeAssert.ok(
      mapped.some(
        (event) => event.type === "item.completed" && event.payload.itemType === "reasoning",
      ),
      "text_start should complete the reasoning item before prose",
    );
    if (reasoning[0]?.type === "content.delta" && assistant[0]?.type === "content.delta") {
      NodeAssert.notEqual(reasoning[0].itemId, assistant[0].itemId);
    }
  });

  it("ignores truncated message_end thinking after reasoning was already completed", async () => {
    const assistantTextByPartId = new Map<string, string>();
    const completedAssistantPartIds = new Set<string>();
    let counter = 0;
    const context = {
      threadId,
      turnId,
      createdAt: "2026-01-01T00:00:00.000Z",
      nextEventId: () => Effect.succeed(EventId.make(`evt-trunc-${counter++}`)),
    };
    const state = { assistantTextByPartId, completedAssistantPartIds };

    await Effect.runPromise(
      mapPiAgentEvent(
        {
          type: "message_update",
          assistantMessageEvent: {
            type: "thinking_delta",
            contentIndex: 0,
            delta: "The user says tell me a story.",
          },
        } as PiAgentSessionEvent,
        context,
        state,
      ),
    );
    const afterTextStart = await Effect.runPromise(
      mapPiAgentEvent(
        {
          type: "message_update",
          assistantMessageEvent: {
            type: "text_start",
            contentIndex: 1,
          },
        } as PiAgentSessionEvent,
        context,
        state,
      ),
    );
    NodeAssert.ok(
      afterTextStart.some(
        (event) => event.type === "item.completed" && event.payload.itemType === "reasoning",
      ),
    );

    const afterMessageEnd = await Effect.runPromise(
      mapPiAgentEvent(
        {
          type: "message_end",
          message: {
            role: "assistant",
            content: [
              { type: "thinking", thinking: "." },
              { type: "text", text: "Once upon a time." },
            ],
          },
        } as PiAgentSessionEvent,
        context,
        state,
      ),
    );
    const lateReasoning = afterMessageEnd.filter(
      (event) => event.type === "content.delta" && event.payload.streamKind === "reasoning_text",
    );
    NodeAssert.equal(
      lateReasoning.length,
      0,
      "truncated thinking on message_end must not emit after reasoning completed",
    );
    NodeAssert.equal(
      assistantTextByPartId.get(`${turnId}:assistant-reasoning`),
      "The user says tell me a story.",
    );
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
