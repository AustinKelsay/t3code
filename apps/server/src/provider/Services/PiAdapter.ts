/**
 * PiAdapter — shape type for the Pi provider adapter.
 *
 * @module PiAdapter
 */
import type { ProviderAdapterError } from "../Errors.ts";
import type { ProviderAdapterShape } from "./ProviderAdapter.ts";

/**
 * PiAdapterShape — per-instance Pi adapter contract.
 */
export interface PiAdapterShape extends ProviderAdapterShape<ProviderAdapterError> {}
