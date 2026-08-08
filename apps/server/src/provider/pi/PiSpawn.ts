/**
 * Spawn helpers for the Pi coding agent CLI in RPC mode.
 *
 * @module provider/pi/PiSpawn
 */
import type { PiSettings } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import { ChildProcess } from "effect/unstable/process";
import { resolveSpawnCommand } from "@t3tools/shared/shell";

import { expandHomePath } from "../../pathExpansion.ts";
import { nonEmptyTrimmed } from "../providerSnapshot.ts";

export interface PiSpawnOptions {
  readonly cwd: string;
  readonly environment?: NodeJS.ProcessEnv;
  readonly settings: PiSettings;
  readonly extraArgs?: ReadonlyArray<string>;
}

/**
 * Build argv for `pi --mode rpc` with instance settings applied.
 */
export function buildPiRpcSpawnArgs(settings: PiSettings): ReadonlyArray<string> {
  const args = ["--mode", "rpc"];
  const provider = nonEmptyTrimmed(settings.defaultProvider);
  const model = nonEmptyTrimmed(settings.defaultModel);
  if (provider) {
    args.push("--provider", provider);
  }
  if (model) {
    args.push("--model", model);
  }
  if (settings.approveProjectResources) {
    args.push("--approve");
  } else {
    args.push("--no-approve");
  }
  return args;
}

/**
 * Build the process environment for a Pi RPC session.
 */
export function buildPiProcessEnvironment(
  settings: PiSettings,
  environment: NodeJS.ProcessEnv = process.env,
): NodeJS.ProcessEnv {
  const agentDir = nonEmptyTrimmed(settings.agentDir);
  if (!agentDir) {
    return environment;
  }
  return {
    ...environment,
    PI_CODING_AGENT_DIR: expandHomePath(agentDir),
  };
}

/**
 * Resolve a Pi RPC child process command for spawning.
 */
export const resolvePiRpcCommand = Effect.fn("resolvePiRpcCommand")(function* (
  options: PiSpawnOptions,
) {
  const binaryPath = nonEmptyTrimmed(options.settings.binaryPath) ?? "pi";
  const args = [...buildPiRpcSpawnArgs(options.settings), ...(options.extraArgs ?? [])];
  const env = buildPiProcessEnvironment(options.settings, options.environment);
  const spawnCommand = yield* resolveSpawnCommand(binaryPath, args, { env });
  return {
    binaryPath,
    command: ChildProcess.make(spawnCommand.command, spawnCommand.args, {
      cwd: options.cwd,
      env,
      shell: spawnCommand.shell,
    }),
    env,
  };
});

/**
 * Resolve argv for one-shot Pi print invocations (`pi -p`).
 */
export const resolvePiPrintCommand = Effect.fn("resolvePiPrintCommand")(function* (input: {
  readonly settings: PiSettings;
  readonly cwd: string;
  readonly environment?: NodeJS.ProcessEnv;
  readonly provider?: string;
  readonly model?: string;
  readonly prompt: string;
}) {
  const binaryPath = nonEmptyTrimmed(input.settings.binaryPath) ?? "pi";
  const args = [
    "--mode",
    "json",
    "-p",
    "--no-session",
    ...(input.settings.approveProjectResources ? ["--approve"] : ["--no-approve"]),
    ...(input.provider ? ["--provider", input.provider] : []),
    ...(input.model ? ["--model", input.model] : []),
    input.prompt,
  ];
  const env = buildPiProcessEnvironment(input.settings, input.environment);
  const spawnCommand = yield* resolveSpawnCommand(binaryPath, args, { env });
  return {
    binaryPath,
    command: ChildProcess.make(spawnCommand.command, spawnCommand.args, {
      cwd: input.cwd,
      env,
      shell: spawnCommand.shell,
    }),
  };
});
