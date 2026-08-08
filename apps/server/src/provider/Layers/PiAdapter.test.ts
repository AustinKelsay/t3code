import * as NodeAssert from "node:assert/strict";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as PubSub from "effect/PubSub";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";

import {
  ApprovalRequestId,
  PiSettings,
  ProviderDriverKind,
  ProviderInstanceId,
  type ProviderRuntimeEvent,
  ThreadId,
} from "@t3tools/contracts";
import { attachmentRelativePath } from "../../attachmentStore.ts";
import { ServerConfig } from "../../config.ts";
import { makePiAdapter } from "./PiAdapter.ts";
import type { PiAgentSessionEvent, PiRpcCommand, PiRpcResponse } from "../pi/PiRpcProtocol.ts";
import type { PiRpcClient } from "../pi/PiRpcClient.ts";
import { PiRpcClientError } from "../pi/PiRpcClient.ts";

const decodePiSettings = Schema.decodeSync(PiSettings);
const asThreadId = (value: string): ThreadId => ThreadId.make(value);

interface FakePiRpcClientState {
  readonly commands: Array<PiRpcCommand>;
  readonly writes: Array<unknown>;
  readonly events: PubSub.PubSub<PiAgentSessionEvent>;
  isStreaming: boolean;
  switchSessionShouldFail: boolean;
}

function makeFakePiRpcClient(state: FakePiRpcClientState): Effect.Effect<PiRpcClient> {
  return Effect.gen(function* () {
    return {
      start: () => Effect.void,
      stop: () => Effect.void,
      sendCommand: (command) => {
        state.commands.push(command);
        if (command.type === "switch_session" && state.switchSessionShouldFail) {
          return Effect.fail(
            new PiRpcClientError({
              detail: "session file missing",
            }),
          );
        }
        return Effect.sync(() => {
          const responseId = command.id;
          if (command.type === "get_state") {
            return {
              type: "response",
              id: responseId,
              command: "get_state",
              success: true,
              data: {
                sessionId: "pi-session-1",
                sessionFile: "/tmp/pi-session.jsonl",
                isStreaming: state.isStreaming,
              },
            } satisfies PiRpcResponse;
          }
          if (
            command.type === "prompt" ||
            command.type === "follow_up" ||
            command.type === "steer"
          ) {
            state.isStreaming = true;
          }
          return {
            type: "response",
            id: responseId,
            command: command.type,
            success: true,
          } satisfies PiRpcResponse;
        });
      },
      writeLine: (value) =>
        Effect.sync(() => {
          state.writes.push(value);
        }),
      events: Stream.fromPubSub(state.events),
      getStderr: () => "",
    };
  });
}

const TestLayer = Layer.provideMerge(
  ServerConfig.layerTest(process.cwd(), process.cwd()),
  NodeServices.layer,
);

it.layer(TestLayer)("PiAdapter", (it) => {
  it.effect("starts a session, sends prompts, rejects rollback, and stops", () =>
    Effect.gen(function* () {
      const events = yield* PubSub.unbounded<PiAgentSessionEvent>();
      const state: FakePiRpcClientState = {
        commands: [],
        writes: [],
        events,
        isStreaming: false,
        switchSessionShouldFail: false,
      };
      const adapter = yield* makePiAdapter(decodePiSettings({}), {
        instanceId: ProviderInstanceId.make("piAgent"),
        createRpcClient: () => makeFakePiRpcClient(state),
      });

      const threadId = asThreadId("thread-pi");
      const session = yield* adapter.startSession({
        threadId,
        runtimeMode: "full-access",
        cwd: process.cwd(),
      });
      NodeAssert.equal(session.provider, ProviderDriverKind.make("piAgent"));
      NodeAssert.equal(session.status, "ready");
      NodeAssert.deepEqual(session.resumeCursor, {
        schemaVersion: 1,
        sessionId: "pi-session-1",
        sessionFile: "/tmp/pi-session.jsonl",
      });

      const turn = yield* adapter.sendTurn({
        threadId,
        input: "Implement feature X",
      });
      NodeAssert.match(turn.turnId, /pi-turn-/);

      const promptCommands = state.commands.filter((command) => command.type === "prompt");
      NodeAssert.equal(promptCommands.length, 1);
      NodeAssert.equal(promptCommands[0]?.message, "Implement feature X");

      yield* adapter.interruptTurn(threadId, turn.turnId);
      NodeAssert.ok(state.commands.some((command) => command.type === "abort"));

      const rollbackExit = yield* adapter.rollbackThread(threadId, 1).pipe(Effect.exit);
      NodeAssert.ok(Exit.isFailure(rollbackExit));

      yield* adapter.stopSession(threadId);
    }),
  );

  it.effect("rejects resume without sessionFile", () =>
    Effect.gen(function* () {
      const events = yield* PubSub.unbounded<PiAgentSessionEvent>();
      const state: FakePiRpcClientState = {
        commands: [],
        writes: [],
        events,
        isStreaming: false,
        switchSessionShouldFail: false,
      };
      const adapter = yield* makePiAdapter(decodePiSettings({}), {
        createRpcClient: () => makeFakePiRpcClient(state),
      });
      const exit = yield* adapter
        .startSession({
          threadId: asThreadId("thread-pi-resume-missing-file"),
          runtimeMode: "full-access",
          cwd: process.cwd(),
          resumeCursor: {
            schemaVersion: 1,
            sessionId: "pi-session-1",
          },
        })
        .pipe(Effect.exit);
      NodeAssert.ok(Exit.isFailure(exit));
    }),
  );

  it.effect("fails loudly when switch_session fails", () =>
    Effect.gen(function* () {
      const events = yield* PubSub.unbounded<PiAgentSessionEvent>();
      const state: FakePiRpcClientState = {
        commands: [],
        writes: [],
        events,
        isStreaming: false,
        switchSessionShouldFail: true,
      };
      const adapter = yield* makePiAdapter(decodePiSettings({}), {
        createRpcClient: () => makeFakePiRpcClient(state),
      });
      const exit = yield* adapter
        .startSession({
          threadId: asThreadId("thread-pi-switch-fail"),
          runtimeMode: "full-access",
          cwd: process.cwd(),
          resumeCursor: {
            schemaVersion: 1,
            sessionId: "pi-session-1",
            sessionFile: "/tmp/missing-session.jsonl",
          },
        })
        .pipe(Effect.exit);
      NodeAssert.ok(Exit.isFailure(exit));
      NodeAssert.ok(state.commands.some((command) => command.type === "switch_session"));
    }),
  );

  it.effect("uses follow_up while streaming and reuses the active turn id", () =>
    Effect.gen(function* () {
      const events = yield* PubSub.unbounded<PiAgentSessionEvent>();
      const state: FakePiRpcClientState = {
        commands: [],
        writes: [],
        events,
        isStreaming: true,
        switchSessionShouldFail: false,
      };
      const adapter = yield* makePiAdapter(decodePiSettings({}), {
        createRpcClient: () => makeFakePiRpcClient(state),
      });
      const threadId = asThreadId("thread-pi-follow-up");
      yield* adapter.startSession({
        threadId,
        runtimeMode: "full-access",
        cwd: process.cwd(),
      });

      const firstTurn = yield* adapter.sendTurn({
        threadId,
        input: "first prompt",
      });
      state.isStreaming = true;
      const secondTurn = yield* adapter.sendTurn({
        threadId,
        input: "queued while running",
      });

      NodeAssert.equal(String(secondTurn.turnId), String(firstTurn.turnId));
      NodeAssert.ok(state.commands.some((command) => command.type === "follow_up"));
      NodeAssert.equal(state.commands.filter((command) => command.type === "follow_up").length, 1);
    }),
  );

  it.effect("maps image attachments onto Pi RPC images", () =>
    Effect.gen(function* () {
      const events = yield* PubSub.unbounded<PiAgentSessionEvent>();
      const state: FakePiRpcClientState = {
        commands: [],
        writes: [],
        events,
        isStreaming: false,
        switchSessionShouldFail: false,
      };
      const adapter = yield* makePiAdapter(decodePiSettings({}), {
        createRpcClient: () => makeFakePiRpcClient(state),
      });
      const serverConfig = yield* ServerConfig;
      const threadId = asThreadId("thread-pi-attachment");
      const attachment = {
        type: "image" as const,
        id: `${threadId}-00000000-0000-4000-8000-000000000001`,
        name: "diagram.png",
        mimeType: "image/png",
        sizeBytes: 8,
      };
      const relativePath = attachmentRelativePath(attachment);
      const path = yield* Path.Path;
      const fileSystem = yield* FileSystem.FileSystem;
      const absolutePath = path.join(serverConfig.attachmentsDir, relativePath);
      yield* fileSystem.makeDirectory(path.dirname(absolutePath), { recursive: true });
      yield* fileSystem.writeFile(absolutePath, Buffer.from("fake-png"));

      yield* adapter.startSession({
        threadId,
        runtimeMode: "full-access",
        cwd: process.cwd(),
      });
      yield* adapter.sendTurn({
        threadId,
        attachments: [attachment],
      });

      const prompt = state.commands.find((command) => command.type === "prompt");
      NodeAssert.ok(prompt);
      NodeAssert.equal(prompt?.message, "");
      NodeAssert.equal(prompt?.images?.length, 1);
      NodeAssert.equal(prompt?.images?.[0]?.mimeType, "image/png");
      NodeAssert.equal(prompt?.images?.[0]?.type, "image");
    }),
  );

  it.effect("ignores late agent_settled for interrupted turns", () =>
    Effect.gen(function* () {
      const events = yield* PubSub.unbounded<PiAgentSessionEvent>();
      const state: FakePiRpcClientState = {
        commands: [],
        writes: [],
        events,
        isStreaming: false,
        switchSessionShouldFail: false,
      };
      const nativeWrites: Array<unknown> = [];
      const adapter = yield* makePiAdapter(decodePiSettings({}), {
        createRpcClient: () => makeFakePiRpcClient(state),
        nativeEventLogger: {
          filePath: "memory://pi-native-events",
          write: (record) =>
            Effect.sync(() => {
              nativeWrites.push(record);
            }),
          close: () => Effect.void,
        },
      });

      const runtimeEvents: Array<ProviderRuntimeEvent> = [];
      const runtimeFiber = yield* Stream.runForEach(adapter.streamEvents, (event) =>
        Effect.sync(() => {
          runtimeEvents.push(event);
        }),
      ).pipe(Effect.forkChild);

      const threadId = asThreadId("thread-pi-interrupt-settle");
      yield* adapter.startSession({
        threadId,
        runtimeMode: "full-access",
        cwd: process.cwd(),
      });
      const turn = yield* adapter.sendTurn({
        threadId,
        input: "work",
      });
      yield* adapter.interruptTurn(threadId, turn.turnId);
      yield* PubSub.publish(events, { type: "agent_settled" });
      for (let attempt = 0; attempt < 8; attempt += 1) {
        yield* Effect.yieldNow;
      }

      const completed = runtimeEvents.filter(
        (event) => event.type === "turn.completed" && String(event.turnId) === String(turn.turnId),
      );
      const aborted = runtimeEvents.filter(
        (event) => event.type === "turn.aborted" && String(event.turnId) === String(turn.turnId),
      );
      NodeAssert.equal(completed.length, 0);
      NodeAssert.equal(aborted.length, 1);
      NodeAssert.ok(nativeWrites.length > 0);

      yield* Fiber.interrupt(runtimeFiber);
      yield* adapter.stopSession(threadId);
    }),
  );

  it.effect("settles a prompt turn when Pi emits agent_settled", () =>
    Effect.gen(function* () {
      const events = yield* PubSub.unbounded<PiAgentSessionEvent>();
      const state: FakePiRpcClientState = {
        commands: [],
        writes: [],
        events,
        isStreaming: false,
        switchSessionShouldFail: false,
      };
      const adapter = yield* makePiAdapter(decodePiSettings({}), {
        createRpcClient: () => makeFakePiRpcClient(state),
      });

      const runtimeEvents: Array<ProviderRuntimeEvent> = [];
      const runtimeFiber = yield* Stream.runForEach(adapter.streamEvents, (event) =>
        Effect.sync(() => {
          runtimeEvents.push(event);
        }),
      ).pipe(Effect.forkChild);

      const threadId = asThreadId("thread-pi-settle");
      yield* adapter.startSession({
        threadId,
        runtimeMode: "full-access",
        cwd: process.cwd(),
      });
      const turn = yield* adapter.sendTurn({
        threadId,
        input: "finish this",
      });
      for (let attempt = 0; attempt < 12; attempt += 1) {
        yield* Effect.yieldNow;
      }
      yield* PubSub.publish(events, { type: "agent_start" });
      yield* PubSub.publish(events, { type: "agent_settled" });
      for (let attempt = 0; attempt < 50; attempt += 1) {
        yield* Effect.yieldNow;
        const sessions = yield* adapter.listSessions();
        if (sessions[0]?.status === "ready") {
          break;
        }
      }

      const sessions = yield* adapter.listSessions();
      NodeAssert.equal(sessions[0]?.status, "ready");
      NodeAssert.equal(sessions[0]?.activeTurnId, undefined);
      NodeAssert.ok(
        runtimeEvents.some(
          (event) =>
            event.type === "turn.completed" && String(event.turnId) === String(turn.turnId),
        ),
        `expected turn.completed for ${String(turn.turnId)}, got ${runtimeEvents.map((event) => event.type).join(",")}`,
      );

      yield* Fiber.interrupt(runtimeFiber);
      yield* adapter.stopSession(threadId);
    }),
  );

  it.effect("routes confirm extension_ui to respondToRequest", () =>
    Effect.gen(function* () {
      const events = yield* PubSub.unbounded<PiAgentSessionEvent>();
      const state: FakePiRpcClientState = {
        commands: [],
        writes: [],
        events,
        isStreaming: false,
        switchSessionShouldFail: false,
      };
      const adapter = yield* makePiAdapter(decodePiSettings({}), {
        createRpcClient: () => makeFakePiRpcClient(state),
      });

      const runtimeEvents: Array<ProviderRuntimeEvent> = [];
      const runtimeFiber = yield* Stream.runForEach(adapter.streamEvents, (event) =>
        Effect.sync(() => {
          runtimeEvents.push(event);
        }),
      ).pipe(Effect.forkChild);

      const threadId = asThreadId("thread-pi-confirm");
      yield* adapter.startSession({
        threadId,
        runtimeMode: "full-access",
        cwd: process.cwd(),
      });
      yield* adapter.sendTurn({ threadId, input: "needs approval" });

      const confirmRequest = {
        type: "extension_ui_request" as const,
        id: "ui-confirm-1",
        method: "confirm" as const,
        title: "Allow action?",
        message: "Proceed?",
      };
      const publishFiber = yield* Effect.forkChild(
        Effect.gen(function* () {
          yield* PubSub.publish(events, confirmRequest);
          for (let attempt = 0; attempt < 12; attempt += 1) {
            yield* Effect.yieldNow;
            const opened = runtimeEvents.find((event) => event.type === "request.opened");
            if (opened?.type === "request.opened") {
              yield* adapter.respondToRequest(
                threadId,
                ApprovalRequestId.make("ui-confirm-1"),
                "accept",
              );
              return;
            }
          }
          throw new Error("request.opened never arrived");
        }),
      );
      yield* Fiber.join(publishFiber);
      for (let attempt = 0; attempt < 20; attempt += 1) {
        yield* Effect.yieldNow;
      }

      NodeAssert.ok(
        state.writes.some(
          (write) =>
            typeof write === "object" &&
            write !== null &&
            "type" in write &&
            write.type === "extension_ui_response" &&
            "confirmed" in write &&
            write.confirmed === true,
        ),
      );
      NodeAssert.ok(runtimeEvents.some((event) => event.type === "request.resolved"));

      yield* Fiber.interrupt(runtimeFiber);
      yield* adapter.stopSession(threadId);
    }),
  );

  it.effect("routes select and input extension_ui to respondToUserInput", () =>
    Effect.gen(function* () {
      const events = yield* PubSub.unbounded<PiAgentSessionEvent>();
      const state: FakePiRpcClientState = {
        commands: [],
        writes: [],
        events,
        isStreaming: false,
        switchSessionShouldFail: false,
      };
      const adapter = yield* makePiAdapter(decodePiSettings({}), {
        createRpcClient: () => makeFakePiRpcClient(state),
      });

      const runtimeEvents: Array<ProviderRuntimeEvent> = [];
      const runtimeFiber = yield* Stream.runForEach(adapter.streamEvents, (event) =>
        Effect.sync(() => {
          runtimeEvents.push(event);
        }),
      ).pipe(Effect.forkChild);

      const threadId = asThreadId("thread-pi-user-input");
      yield* adapter.startSession({
        threadId,
        runtimeMode: "full-access",
        cwd: process.cwd(),
      });
      yield* adapter.sendTurn({ threadId, input: "pick and type" });

      const selectRequest = {
        type: "extension_ui_request" as const,
        id: "ui-select-1",
        method: "select" as const,
        title: "Pick one",
        options: ["Alpha", "Beta"],
      };
      const selectFiber = yield* Effect.forkChild(
        Effect.gen(function* () {
          yield* PubSub.publish(events, selectRequest);
          for (let attempt = 0; attempt < 12; attempt += 1) {
            yield* Effect.yieldNow;
            if (runtimeEvents.some((event) => event.type === "user-input.requested")) {
              yield* adapter.respondToUserInput(threadId, ApprovalRequestId.make("ui-select-1"), {
                "Pick one": "Alpha",
              });
              return;
            }
          }
          throw new Error("user-input.requested never arrived for select");
        }),
      );
      yield* Fiber.join(selectFiber);
      for (let attempt = 0; attempt < 12; attempt += 1) {
        yield* Effect.yieldNow;
      }
      NodeAssert.ok(
        state.writes.some(
          (write) =>
            typeof write === "object" &&
            write !== null &&
            "value" in write &&
            write.value === "Alpha",
        ),
      );

      const inputRequest = {
        type: "extension_ui_request" as const,
        id: "ui-input-1",
        method: "input" as const,
        title: "Name",
        placeholder: "your-name",
      };
      const inputFiber = yield* Effect.forkChild(
        Effect.gen(function* () {
          yield* PubSub.publish(events, inputRequest);
          for (let attempt = 0; attempt < 12; attempt += 1) {
            yield* Effect.yieldNow;
            const requested = runtimeEvents.filter(
              (event) => event.type === "user-input.requested",
            );
            if (requested.length >= 2) {
              yield* adapter.respondToUserInput(threadId, ApprovalRequestId.make("ui-input-1"), {
                Name: "Ada",
              });
              return;
            }
          }
          throw new Error("user-input.requested never arrived for input");
        }),
      );
      yield* Fiber.join(inputFiber);
      for (let attempt = 0; attempt < 12; attempt += 1) {
        yield* Effect.yieldNow;
      }
      NodeAssert.ok(
        state.writes.some(
          (write) =>
            typeof write === "object" &&
            write !== null &&
            "value" in write &&
            write.value === "Ada",
        ),
      );

      yield* Fiber.interrupt(runtimeFiber);
      yield* adapter.stopSession(threadId);
    }),
  );
});
