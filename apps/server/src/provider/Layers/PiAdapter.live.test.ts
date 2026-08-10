/**
 * Live PiAdapter regression: startSession must return quickly and turns must
 * emit content.delta. This catches the stdin-close hang that previously
 * wedged the shared provider turn reactor for all harnesses.
 *
 * @module provider/Layers/PiAdapter.live.test
 */
import * as NodeAssert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { describe, it } from "@effect/vitest";
import { PiSettings, ProviderInstanceId, ThreadId } from "@t3tools/contracts";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";

import { ServerConfig } from "../../config.ts";
import { makePiAdapter } from "./PiAdapter.ts";

const decodePiSettings = Schema.decodeSync(PiSettings);
const piBinary = process.env.PI_BINARY_PATH?.trim() || "/opt/homebrew/bin/pi";
const piAvailable =
  spawnSync(piBinary, ["--version"], { encoding: "utf8", timeout: 5_000 }).status === 0;

const TestLayer = Layer.provideMerge(
  ServerConfig.layerTest(process.cwd(), process.cwd()),
  NodeServices.layer,
);

describe.skipIf(!piAvailable)("PiAdapter live (real pi binary)", () => {
  it.live(
    "starts a session quickly and streams content.delta for a prompt",
    () =>
      Effect.gen(function* () {
        const adapter = yield* makePiAdapter(
          decodePiSettings({
            binaryPath: piBinary,
            approveProjectResources: false,
          }),
          { instanceId: ProviderInstanceId.make("piAgent") },
        );

        const threadId = ThreadId.make(`live-adapter-${Date.now()}`);
        const eventsFiber = yield* Effect.forkChild(
          adapter.streamEvents.pipe(
            Stream.takeUntil(
              (event) =>
                event.type === "turn.completed" ||
                event.type === "turn.failed" ||
                event.type === "session.exited",
            ),
            Stream.runCollect,
          ),
        );

        const startedAt = Date.now();
        yield* adapter
          .startSession({
            threadId,
            runtimeMode: "full-access",
            cwd: process.cwd(),
          })
          .pipe(Effect.timeout(Duration.seconds(15)));
        NodeAssert.ok(
          Date.now() - startedAt < 15_000,
          "startSession hung — likely stdin closed after RPC write",
        );

        yield* adapter.sendTurn({
          threadId,
          input: "Reply with exactly the word: pong",
        });

        const events = yield* Fiber.join(eventsFiber).pipe(Effect.timeout(Duration.seconds(90)));
        yield* adapter.stopSession(threadId);

        NodeAssert.ok(
          events.some((event) => event.type === "content.delta"),
          `expected content.delta UI feedback; got: ${events.map((e) => e.type).join(",")}`,
        );
        NodeAssert.ok(
          events.some((event) => event.type === "turn.completed"),
          `expected turn.completed; got: ${events.map((e) => e.type).join(",")}`,
        );
      }).pipe(Effect.scoped, Effect.provide(TestLayer)),
    { timeout: 120_000 },
  );
});
