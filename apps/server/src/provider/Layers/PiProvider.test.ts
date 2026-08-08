// @effect-diagnostics nodeBuiltinImport:off
import * as NodePath from "node:path";
import * as NodeOS from "node:os";
import * as NodeFS from "node:fs";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { describe, expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import { PiSettings } from "@t3tools/contracts";

import { buildInitialPiProviderSnapshot, checkPiProviderStatus } from "./PiProvider.ts";

const decodePiSettings = Schema.decodeSync(PiSettings);

function writePiRpcMock(binaryPath: string, mode: "success" | "failure"): void {
  const dir = NodePath.dirname(binaryPath);
  const rpcMockPath = NodePath.join(dir, "pi-rpc-handler.mjs");
  const responseBody =
    mode === "success"
      ? `{ type: "response", id: command.id, command: command.type, success: true, data: { models: [{ provider: "anthropic", id: "claude-sonnet-4-5", name: "Sonnet" }] } }`
      : `{ type: "response", id: command.id, command: command.type, success: false, error: "rpc unavailable" }`;
  NodeFS.writeFileSync(
    rpcMockPath,
    [
      'import readline from "node:readline";',
      "const rl = readline.createInterface({ input: process.stdin });",
      'rl.on("line", (line) => {',
      "  if (!line.trim()) return;",
      "  const command = JSON.parse(line);",
      `  process.stdout.write(JSON.stringify(${responseBody}) + "\\n");`,
      "});",
      "",
    ].join("\n"),
    "utf8",
  );
  NodeFS.writeFileSync(
    binaryPath,
    [
      "#!/bin/sh",
      'for arg in "$@"; do',
      '  if [ "$arg" = "--version" ]; then',
      '    printf "pi-coding-agent 1.2.3\\n"',
      "    exit 0",
      "  fi",
      "done",
      'for arg in "$@"; do',
      '  if [ "$arg" = "rpc" ]; then',
      `    exec ${JSON.stringify(process.execPath)} ${JSON.stringify(rpcMockPath)}`,
      "  fi",
      "done",
      'printf "unexpected args: %s\\n" "$*" >&2',
      "exit 11",
      "",
    ].join("\n"),
    "utf8",
  );
  NodeFS.chmodSync(binaryPath, 0o755);
}

describe("buildInitialPiProviderSnapshot", () => {
  it.effect("returns a disabled snapshot when settings.enabled is false", () =>
    Effect.gen(function* () {
      const snapshot = yield* buildInitialPiProviderSnapshot(decodePiSettings({ enabled: false }));
      expect(snapshot.enabled).toBe(false);
      expect(snapshot.status).toBe("disabled");
      expect(snapshot.installed).toBe(false);
      expect(snapshot.message).toContain("disabled");
    }),
  );

  it.effect("returns a pending snapshot by default", () =>
    Effect.gen(function* () {
      const snapshot = yield* buildInitialPiProviderSnapshot(decodePiSettings({}));
      expect(snapshot.enabled).toBe(true);
      expect(snapshot.installed).toBe(true);
      expect(snapshot.status).toBe("warning");
      expect(snapshot.version).toBeNull();
      expect(snapshot.message).toContain("Checking Pi");
    }),
  );
});

it.layer(NodeServices.layer)("checkPiProviderStatus", (it) => {
  it.effect("reports the binary as missing when the binary path does not resolve", () =>
    Effect.gen(function* () {
      const snapshot = yield* checkPiProviderStatus(
        decodePiSettings({
          enabled: true,
          binaryPath: "/definitely/not/installed/pi-binary",
        }),
        process.cwd(),
      );
      expect(snapshot.enabled).toBe(true);
      expect(snapshot.installed).toBe(false);
      expect(snapshot.status).toBe("error");
      expect(snapshot.message).toMatch(/not installed|not on PATH|Failed to execute/);
    }),
  );

  it.effect("reports an installed CLI as unhealthy when --version exits non-zero", () =>
    Effect.gen(function* () {
      const secretStderr = "broken pi install: secret-token-value";
      const snapshot = yield* Effect.scoped(
        Effect.gen(function* () {
          const fs = yield* FileSystem.FileSystem;
          const path = yield* Path.Path;
          const dir = yield* fs.makeTempDirectoryScoped({ prefix: "t3code-pi-version-" });
          const piPath = path.join(dir, "pi");
          yield* fs.writeFileString(
            piPath,
            ["#!/bin/sh", `printf "%s\\n" "${secretStderr}" >&2`, "exit 2", ""].join("\n"),
          );
          yield* fs.chmod(piPath, 0o755);

          return yield* checkPiProviderStatus(
            decodePiSettings({ enabled: true, binaryPath: piPath }),
            process.cwd(),
          );
        }),
      );

      expect(snapshot.enabled).toBe(true);
      expect(snapshot.installed).toBe(true);
      expect(snapshot.status).toBe("error");
      expect(snapshot.message).toBe("Pi CLI is installed but failed to run.");
      expect(snapshot.message).not.toContain(secretStderr);
    }),
  );

  it.effect("reports an error when RPC model discovery is unavailable", () =>
    Effect.gen(function* () {
      const tempDir = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3code-pi-rpc-fail-"));
      const piPath = NodePath.join(tempDir, "pi");
      writePiRpcMock(piPath, "failure");
      const snapshot = yield* checkPiProviderStatus(
        decodePiSettings({ enabled: true, binaryPath: piPath }),
        process.cwd(),
      );

      expect(snapshot.enabled).toBe(true);
      expect(snapshot.installed).toBe(true);
      expect(snapshot.status).toBe("error");
      expect(snapshot.version).toBe("1.2.3");
      expect(snapshot.message).toContain("RPC startup failed");
    }),
  );

  it.effect("reports ready when version and RPC model discovery succeed", () =>
    Effect.gen(function* () {
      const tempDir = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3code-pi-success-"));
      const piPath = NodePath.join(tempDir, "pi");
      writePiRpcMock(piPath, "success");
      const snapshot = yield* checkPiProviderStatus(
        decodePiSettings({ enabled: true, binaryPath: piPath }),
        process.cwd(),
      );

      expect(snapshot.enabled).toBe(true);
      expect(snapshot.installed).toBe(true);
      expect(snapshot.status).toBe("ready");
      expect(snapshot.version).toBe("1.2.3");
      expect(snapshot.models.map((model) => model.slug)).toContain("anthropic/claude-sonnet-4-5");
    }),
  );
});
