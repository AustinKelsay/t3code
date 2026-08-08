// @effect-diagnostics nodeBuiltinImport:off
import * as NodePath from "node:path";
import * as NodeOS from "node:os";
import * as NodeFS from "node:fs";

import * as NodeServices from "@effect/platform-node/NodeServices";
import { it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import { createModelSelection } from "@t3tools/shared/model";
import { expect } from "vite-plus/test";
import { PiSettings, ProviderInstanceId } from "@t3tools/contracts";

import * as ServerConfig from "../config.ts";
import * as TextGeneration from "./TextGeneration.ts";
import { makePiTextGeneration } from "./PiTextGeneration.ts";

const decodePiSettings = Schema.decodeSync(PiSettings);

const PiTextGenerationTestLayer = ServerConfig.ServerConfig.layerTest(process.cwd(), {
  prefix: "t3code-pi-text-generation-test-",
}).pipe(Layer.provideMerge(NodeServices.layer));

function writePiJsonPrintMock(dir: string, stdoutJsonl: string): string {
  const binDir = NodePath.join(dir, "bin");
  const piPath = NodePath.join(binDir, "pi");
  const mockPath = NodePath.join(dir, "pi-json-print.mjs");
  NodeFS.mkdirSync(binDir, { recursive: true });
  NodeFS.writeFileSync(
    mockPath,
    [
      "const payload = ",
      JSON.stringify(stdoutJsonl),
      ";",
      "const args = process.argv.slice(2);",
      'if (args.includes("--version")) {',
      '  process.stdout.write("pi-coding-agent 0.0.1\\n");',
      "  process.exit(0);",
      "}",
      'if (args.includes("--mode") && args.includes("json") && args.includes("-p")) {',
      '  process.stdout.write(payload + "\\n");',
      "  process.exit(0);",
      "}",
      'process.stderr.write("unexpected args: " + process.argv.slice(2).join(" ") + "\\n");',
      "process.exit(11);",
      "",
    ].join("\n"),
    "utf8",
  );
  NodeFS.writeFileSync(
    piPath,
    [
      "#!/bin/sh",
      `exec ${JSON.stringify(process.execPath)} ${JSON.stringify(mockPath)} "$@"`,
      "",
    ].join("\n"),
    "utf8",
  );
  NodeFS.chmodSync(piPath, 0o755);
  return piPath;
}

function withFakePiPrint<A, E, R>(
  stdoutJsonl: string,
  effectFn: (textGeneration: TextGeneration.TextGeneration["Service"]) => Effect.Effect<A, E, R>,
) {
  return Effect.gen(function* () {
    const tempDir = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3code-pi-text-"));
    yield* Effect.addFinalizer(() =>
      Effect.sync(() => {
        NodeFS.rmSync(tempDir, { recursive: true, force: true });
      }),
    );
    const binaryPath = writePiJsonPrintMock(tempDir, stdoutJsonl);
    const config = decodePiSettings({ binaryPath });
    const textGeneration = yield* makePiTextGeneration(config);
    return yield* effectFn(textGeneration);
  }).pipe(Effect.scoped);
}

it.layer(PiTextGenerationTestLayer)("PiTextGeneration", (it) => {
  it.effect("generates a thread title from mocked Pi JSONL output", () =>
    withFakePiPrint(
      JSON.stringify({
        type: "message_end",
        message: {
          role: "assistant",
          content: [{ type: "text", text: JSON.stringify({ title: "Fix flaky Pi tests" }) }],
        },
      }),
      (textGeneration) =>
        Effect.gen(function* () {
          const generated = yield* textGeneration.generateThreadTitle({
            cwd: process.cwd(),
            message: "tests are red again",
            modelSelection: createModelSelection(
              ProviderInstanceId.make("piAgent"),
              "anthropic/claude-sonnet-4-5",
            ),
          });
          expect(generated.title).toBe("Fix flaky Pi tests");
        }),
    ),
  );

  it.effect("generates a commit message from mocked Pi JSONL output", () =>
    withFakePiPrint(
      JSON.stringify({
        type: "message_end",
        message: {
          role: "assistant",
          content: [
            {
              type: "text",
              text: JSON.stringify({
                subject: "Add Pi text generation tests",
                body: "Mock `pi -p --mode json` for thread titles and commit messages.",
              }),
            },
          ],
        },
      }),
      (textGeneration) =>
        Effect.gen(function* () {
          const generated = yield* textGeneration.generateCommitMessage({
            cwd: process.cwd(),
            branch: "feature/pi-provider",
            stagedSummary: "M apps/server/src/textGeneration/PiTextGeneration.test.ts",
            stagedPatch: "diff --git a/.../PiTextGeneration.test.ts b/.../PiTextGeneration.test.ts",
            modelSelection: createModelSelection(
              ProviderInstanceId.make("piAgent"),
              "anthropic/claude-sonnet-4-5",
            ),
          });
          expect(generated.subject).toBe("Add Pi text generation tests");
          expect(generated.body).toContain("Mock `pi -p --mode json`");
        }),
    ),
  );
});
