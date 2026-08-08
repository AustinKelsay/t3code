/**
 * Effect-based Pi RPC client over strict JSONL stdin/stdout framing.
 *
 * @module provider/pi/PiRpcClient
 */
import * as Cause from "effect/Cause";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as PubSub from "effect/PubSub";
import * as Ref from "effect/Ref";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import * as PlatformError from "effect/PlatformError";
import type * as Sink from "effect/Sink";
import { ChildProcessSpawner } from "effect/unstable/process";

import { createJsonlLineReader, serializeJsonLine } from "./PiJsonl.ts";
import type { PiAgentSessionEvent, PiRpcCommand, PiRpcResponse } from "./PiRpcProtocol.ts";
import { resolvePiRpcCommand, type PiSpawnOptions } from "./PiSpawn.ts";

function parseJsonLine(line: string): unknown | undefined {
  try {
    return JSON.parse(line) as unknown;
  } catch {
    return undefined;
  }
}

export class PiRpcClientError extends Schema.TaggedErrorClass<PiRpcClientError>()(
  "PiRpcClientError",
  {
    detail: Schema.String,
    cause: Schema.optional(Schema.Defect()),
  },
) {
  override get message(): string {
    return this.detail;
  }
}

export interface PiRpcClient {
  readonly start: () => Effect.Effect<void, PiRpcClientError>;
  readonly stop: () => Effect.Effect<void>;
  readonly sendCommand: (command: PiRpcCommand) => Effect.Effect<PiRpcResponse, PiRpcClientError>;
  readonly writeLine: (value: unknown) => Effect.Effect<void, PiRpcClientError>;
  readonly events: Stream.Stream<PiAgentSessionEvent>;
  readonly getStderr: () => string;
}

interface PendingRequest {
  readonly deferred: Deferred.Deferred<PiRpcResponse, PiRpcClientError>;
}

interface PiChildProcess {
  readonly stdout: Stream.Stream<Uint8Array, PlatformError.PlatformError>;
  readonly stderr: Stream.Stream<Uint8Array, PlatformError.PlatformError>;
  readonly stdin: Sink.Sink<void, Uint8Array, never, PlatformError.PlatformError>;
  readonly kill: (signal: string) => Effect.Effect<void, PlatformError.PlatformError>;
}

/**
 * Create a scoped Pi RPC client backed by a child `pi --mode rpc` process.
 */
export const makePiRpcClient = (options: PiSpawnOptions) =>
  Effect.gen(function* () {
    const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
    const sessionScope = yield* Scope.make();
    let child: PiChildProcess | null = null;
    let stderr = "";
    const requestCounterRef = yield* Ref.make(0);
    const pendingRef = yield* Ref.make(new Map<string, PendingRequest>());
    const stoppedRef = yield* Ref.make(false);
    let readerFiber: Fiber.Fiber<void, never> | null = null;
    const eventsPubSub = yield* PubSub.unbounded<PiAgentSessionEvent>();

    const failPending = (error: PiRpcClientError) =>
      Ref.getAndSet(pendingRef, new Map()).pipe(
        Effect.flatMap((pending) =>
          Effect.forEach([...pending.values()], (entry) => Deferred.fail(entry.deferred, error), {
            discard: true,
          }),
        ),
      );

    const handleLine = (line: string) =>
      Effect.gen(function* () {
        if (line.trim().length === 0) {
          return;
        }
        const data = parseJsonLine(line);
        if (data === undefined) {
          return;
        }
        if (
          typeof data === "object" &&
          data !== null &&
          "type" in data &&
          data.type === "response" &&
          "id" in data &&
          typeof data.id === "string"
        ) {
          const response = data as PiRpcResponse;
          const pending = yield* Ref.modify(pendingRef, (map) => {
            const entry = map.get(response.id!);
            if (!entry) {
              return [undefined, map] as const;
            }
            const next = new Map(map);
            next.delete(response.id!);
            return [entry, next] as const;
          });
          if (pending) {
            if (response.success) {
              yield* Deferred.succeed(pending.deferred, response);
            } else {
              yield* Deferred.fail(
                pending.deferred,
                new PiRpcClientError({
                  detail: response.error ?? `Pi RPC command '${response.command}' failed.`,
                }),
              );
            }
          }
          return;
        }
        if (typeof data === "object" && data !== null && "type" in data) {
          yield* PubSub.publish(eventsPubSub, data as PiAgentSessionEvent);
        }
      });

    const writePayload = (
      activeChild: PiChildProcess,
      payload: string,
    ): Effect.Effect<void, PiRpcClientError> =>
      Stream.run(Stream.encodeText(Stream.make(payload)), activeChild.stdin).pipe(
        Effect.mapError(
          (cause) =>
            new PiRpcClientError({
              detail: "Failed to write Pi RPC payload.",
              cause,
            }),
        ),
      );

    yield* Scope.addFinalizer(
      sessionScope,
      Effect.gen(function* () {
        yield* Ref.set(stoppedRef, true);
        if (readerFiber) {
          yield* Fiber.interrupt(readerFiber).pipe(Effect.ignore);
        }
        yield* failPending(new PiRpcClientError({ detail: "Pi RPC client stopped." }));
        if (child) {
          yield* child.kill("SIGTERM").pipe(Effect.ignore);
          child = null;
        }
      }),
    );

    const start = (): Effect.Effect<void, PiRpcClientError> =>
      Effect.gen(function* () {
        if (child !== null) {
          return;
        }
        const resolved = yield* resolvePiRpcCommand(options).pipe(
          Effect.mapError(
            (cause) =>
              new PiRpcClientError({
                detail: "Failed to resolve Pi RPC command.",
                cause,
              }),
          ),
        );
        const spawned = yield* spawner.spawn(resolved.command).pipe(
          Effect.mapError(
            (cause) =>
              new PiRpcClientError({
                detail: "Failed to spawn Pi RPC process.",
                cause,
              }),
          ),
        );
        child = spawned as unknown as PiChildProcess;

        const reader = createJsonlLineReader();
        readerFiber = yield* spawned.stdout.pipe(
          Stream.decodeText(),
          Stream.runForEach((chunk) => {
            const lines = reader.push(chunk);
            return Effect.forEach(lines, (line) => handleLine(line), { discard: true });
          }),
          Effect.catchCause((cause) =>
            Effect.gen(function* () {
              if (yield* Ref.get(stoppedRef)) {
                return;
              }
              yield* failPending(
                new PiRpcClientError({
                  detail: "Pi RPC stdout reader failed.",
                  cause: Cause.squash(cause),
                }),
              );
            }),
          ),
          Effect.forkIn(sessionScope),
        );

        yield* spawned.stderr
          .pipe(
            Stream.decodeText(),
            Stream.runForEach((chunk) =>
              Effect.sync(() => {
                stderr += chunk;
              }),
            ),
            Effect.forkIn(sessionScope),
          )
          .pipe(Effect.asVoid);
      }).pipe(Effect.provideService(Scope.Scope, sessionScope));

    const stop: PiRpcClient["stop"] = () =>
      Scope.close(sessionScope, Exit.void).pipe(Effect.catchCause(() => Effect.void));

    const sendCommand = (command: PiRpcCommand) =>
      Effect.gen(function* () {
        if (!child) {
          return yield* new PiRpcClientError({ detail: "Pi RPC client is not started." });
        }
        const requestId = `req_${yield* Ref.getAndUpdate(requestCounterRef, (value) => value + 1)}`;
        const payload = { ...command, id: requestId };
        const deferred = yield* Deferred.make<PiRpcResponse, PiRpcClientError>();
        yield* Ref.update(pendingRef, (pending) => {
          const next = new Map(pending);
          next.set(requestId, { deferred });
          return next;
        });
        yield* writePayload(child, serializeJsonLine(payload));
        return yield* Deferred.await(deferred);
      });

    const writeLine = (value: unknown) =>
      Effect.gen(function* () {
        if (!child) {
          return yield* new PiRpcClientError({ detail: "Pi RPC client is not started." });
        }
        yield* writePayload(child, serializeJsonLine(value));
      });

    return {
      start,
      stop,
      sendCommand,
      writeLine,
      events: Stream.fromPubSub(eventsPubSub),
      getStderr: () => stderr,
    } satisfies PiRpcClient;
  });

/**
 * Start a short-lived Pi RPC client, run `runner`, then stop the process.
 */
export const withPiRpcClient = <A, E>(
  options: PiSpawnOptions,
  runner: (client: PiRpcClient) => Effect.Effect<A, E>,
): Effect.Effect<A, E | PiRpcClientError, ChildProcessSpawner.ChildProcessSpawner> =>
  Effect.gen(function* () {
    const client = yield* makePiRpcClient(options);
    yield* client.start();
    return yield* runner(client).pipe(Effect.ensuring(client.stop()));
  });
