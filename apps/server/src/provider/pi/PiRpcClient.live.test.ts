/**
 * Live regression tests for Pi RPC client stdin framing.
 *
 * The original bug closed child stdin after every `sendCommand` because
 * `Stream.run(Stream.make(payload), child.stdin)` ends the Node writable
 * (`endOnDone` defaults true). That hung `get_state` forever and wedged the
 * shared provider turn reactor — freezing Pi *and* other harnesses.
 *
 * This suite drives a real `pi --mode rpc` process through `makePiRpcClient`
 * and asserts the exact symptom: successive RPC commands must complete quickly,
 * and a prompt must emit session events (message_update / agent_settled).
 *
 * @module provider/pi/PiRpcClient.live.test
 */
import * as NodeAssert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { describe, it } from "@effect/vitest";
import { PiSettings } from "@t3tools/contracts";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";

import { makePiRpcClient, withPiRpcClient } from "./PiRpcClient.ts";
import { mapPiAgentEvent } from "./PiRuntimeEvents.ts";
import { EventId, ThreadId, TurnId } from "@t3tools/contracts";

const decodePiSettings = Schema.decodeSync(PiSettings);

const piBinary = process.env.PI_BINARY_PATH?.trim() || "/opt/homebrew/bin/pi";
const piAvailable =
  spawnSync(piBinary, ["--version"], { encoding: "utf8", timeout: 5_000 }).status === 0;

const TestLayer = NodeServices.layer;

/**
 * Assert get_state succeeds twice — second call fails if stdin was closed by the first write.
 */
describe.skipIf(!piAvailable)("PiRpcClient live (real pi binary)", () => {
  it.live(
    "keeps stdin open across successive get_state commands",
    () =>
      withPiRpcClient(
        {
          cwd: process.cwd(),
          settings: decodePiSettings({
            binaryPath: piBinary,
            approveProjectResources: false,
          }),
        },
        (client) =>
          Effect.gen(function* () {
            const first = yield* client
              .sendCommand({ type: "get_state" })
              .pipe(Effect.timeout(Duration.seconds(10)));
            NodeAssert.equal(first.success, true);
            NodeAssert.equal(first.command, "get_state");

            const second = yield* client
              .sendCommand({ type: "get_state" })
              .pipe(Effect.timeout(Duration.seconds(10)));
            NodeAssert.equal(second.success, true);
            NodeAssert.equal(second.command, "get_state");
          }),
      ).pipe(Effect.provide(TestLayer)),
    { timeout: 60_000 },
  );

  it.live(
    "streams agent session events for a short prompt",
    () =>
      Effect.gen(function* () {
        const client = yield* makePiRpcClient({
          cwd: process.cwd(),
          settings: decodePiSettings({
            binaryPath: piBinary,
            approveProjectResources: false,
          }),
        });
        yield* client.start();

        const collector = yield* Effect.forkChild(
          client.events.pipe(
            Stream.takeUntil((event) => event.type === "agent_settled"),
            Stream.runCollect,
          ),
        );

        yield* client.sendCommand({
          type: "prompt",
          message: "Reply with exactly the word: pong",
        });

        const events = yield* Fiber.join(collector).pipe(Effect.timeout(Duration.seconds(90)));
        yield* client.stop();

        NodeAssert.ok(
          events.some((event) => event.type === "agent_settled"),
          `expected agent_settled; got types: ${events.map((e) => e.type).join(",")}`,
        );
        const hasTextFeedback = events.some(
          (event) => event.type === "message_update" || event.type === "message_end",
        );
        NodeAssert.ok(
          hasTextFeedback,
          `expected message_update or message_end feedback; got types: ${events
            .map((e) => e.type)
            .join(",")}`,
        );

        let eventCounter = 0;
        const assistantTextByPartId = new Map<string, string>();
        const completedAssistantPartIds = new Set<string>();
        const mapped = [];
        for (const event of events) {
          const runtimeEvents = yield* mapPiAgentEvent(
            event,
            {
              threadId: ThreadId.make("live-pi-thread"),
              turnId: TurnId.make("live-pi-turn"),
              createdAt: new Date().toISOString(),
              nextEventId: () => Effect.succeed(EventId.make(`evt_${eventCounter++}`)),
            },
            { assistantTextByPartId, completedAssistantPartIds },
          );
          mapped.push(...runtimeEvents);
        }
        NodeAssert.ok(
          mapped.some(
            (event) =>
              event.type === "content.delta" &&
              (event.payload.streamKind === "assistant_text" ||
                event.payload.streamKind === "reasoning_text"),
          ),
          `expected mapped content.delta for UI feedback; mapped types: ${mapped
            .map((e) => e.type)
            .join(",")}`,
        );
      }).pipe(Effect.provide(TestLayer)),
    { timeout: 120_000 },
  );
});
