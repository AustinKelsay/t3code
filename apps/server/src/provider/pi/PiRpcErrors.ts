/**
 * Map Pi RPC client failures into typed provider adapter request errors.
 *
 * @module provider/pi/PiRpcErrors
 */
import type { ProviderDriverKind } from "@t3tools/contracts";
import * as Schema from "effect/Schema";

import { ProviderAdapterRequestError } from "../Errors.ts";
import { PiRpcClientError } from "./PiRpcClient.ts";

const isPiRpcClientError = Schema.is(PiRpcClientError);

/**
 * Convert a Pi RPC client error into a `ProviderAdapterRequestError`, or return the original cause.
 */
export function mapPiRpcClientError(
  provider: ProviderDriverKind,
  method: string,
  cause: unknown,
): ProviderAdapterRequestError | unknown {
  return isPiRpcClientError(cause)
    ? new ProviderAdapterRequestError({
        provider,
        method,
        detail: cause.detail,
        cause,
      })
    : cause;
}

/**
 * Build an `Effect.mapError` handler that maps Pi RPC client errors for a given method.
 */
export function mapPiRpcClientErrorForMethod(
  provider: ProviderDriverKind,
  method: string,
): (cause: PiRpcClientError) => ProviderAdapterRequestError {
  return (cause) =>
    new ProviderAdapterRequestError({
      provider,
      method,
      detail: cause.detail,
      cause,
    });
}
