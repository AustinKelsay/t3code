/**
 * Pi RPC protocol types and Effect schemas.
 *
 * Reference: `@earendil-works/pi-coding-agent/dist/modes/rpc/`
 *
 * @module provider/pi/PiRpcProtocol
 */
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";

export const PiRpcCommandType = Schema.Literals([
  "prompt",
  "steer",
  "follow_up",
  "abort",
  "new_session",
  "get_state",
  "set_model",
  "get_available_models",
  "switch_session",
  "get_messages",
]);
export type PiRpcCommandType = typeof PiRpcCommandType.Type;

export const PiImageContent = Schema.Struct({
  type: Schema.Literal("image"),
  data: Schema.String,
  mimeType: Schema.String,
});
export type PiImageContent = typeof PiImageContent.Type;

export const PiRpcCommand = Schema.Struct({
  id: Schema.optional(Schema.String),
  type: PiRpcCommandType,
  message: Schema.optional(Schema.String),
  images: Schema.optional(Schema.Array(PiImageContent)),
  streamingBehavior: Schema.optional(Schema.Literals(["steer", "followUp"])),
  provider: Schema.optional(Schema.String),
  modelId: Schema.optional(Schema.String),
  sessionPath: Schema.optional(Schema.String),
});
export type PiRpcCommand = typeof PiRpcCommand.Type;

export const PiRpcSessionState = Schema.Struct({
  sessionId: Schema.String,
  sessionFile: Schema.optional(Schema.String),
  sessionName: Schema.optional(Schema.String),
  isStreaming: Schema.optional(Schema.Boolean),
  model: Schema.optional(
    Schema.Struct({
      provider: Schema.String,
      id: Schema.String,
      name: Schema.optional(Schema.String),
    }),
  ),
});
export type PiRpcSessionState = typeof PiRpcSessionState.Type;

export const PiRpcModelInfo = Schema.Struct({
  provider: Schema.String,
  id: Schema.String,
  name: Schema.optional(Schema.String),
});
export type PiRpcModelInfo = typeof PiRpcModelInfo.Type;

export const PiRpcResponse = Schema.Struct({
  type: Schema.Literal("response"),
  id: Schema.optional(Schema.String),
  command: Schema.String,
  success: Schema.Boolean,
  data: Schema.optional(Schema.Unknown),
  error: Schema.optional(Schema.String),
});
export type PiRpcResponse = typeof PiRpcResponse.Type;

export const PiRpcExtensionUiDialogMethod = Schema.Literals([
  "confirm",
  "select",
  "input",
  "editor",
]);
export type PiRpcExtensionUiDialogMethod = typeof PiRpcExtensionUiDialogMethod.Type;

export const PiRpcExtensionUiFireAndForgetMethod = Schema.Literals([
  "notify",
  "setStatus",
  "setWidget",
  "setTitle",
  "set_editor_text",
]);
export type PiRpcExtensionUiFireAndForgetMethod = typeof PiRpcExtensionUiFireAndForgetMethod.Type;

export const PiRpcExtensionUiRequest = Schema.Struct({
  type: Schema.Literal("extension_ui_request"),
  id: Schema.String,
  method: Schema.String,
  title: Schema.optional(Schema.String),
  message: Schema.optional(Schema.String),
  placeholder: Schema.optional(Schema.String),
  prefill: Schema.optional(Schema.String),
  options: Schema.optional(Schema.Array(Schema.String)),
  notifyType: Schema.optional(Schema.Literals(["info", "warning", "error"])),
  statusKey: Schema.optional(Schema.String),
  statusText: Schema.optional(Schema.String),
  widgetKey: Schema.optional(Schema.String),
  widgetLines: Schema.optional(Schema.Array(Schema.String)),
  text: Schema.optional(Schema.String),
});
export type PiRpcExtensionUiRequest = typeof PiRpcExtensionUiRequest.Type;

export const PiRpcExtensionUiResponse = Schema.Union([
  Schema.Struct({
    type: Schema.Literal("extension_ui_response"),
    id: Schema.String,
    cancelled: Schema.Literal(true),
  }),
  Schema.Struct({
    type: Schema.Literal("extension_ui_response"),
    id: Schema.String,
    value: Schema.String,
  }),
  Schema.Struct({
    type: Schema.Literal("extension_ui_response"),
    id: Schema.String,
    confirmed: Schema.Boolean,
  }),
]);
export type PiRpcExtensionUiResponse = typeof PiRpcExtensionUiResponse.Type;

export const PiAgentSessionEvent = Schema.Struct({
  type: Schema.String,
});
export type PiAgentSessionEvent = typeof PiAgentSessionEvent.Type;

export const PI_RESUME_VERSION = 1 as const;

export const PiResumeCursor = Schema.Struct({
  schemaVersion: Schema.Literal(PI_RESUME_VERSION),
  sessionId: Schema.String,
  sessionFile: Schema.optional(Schema.String),
});
export type PiResumeCursor = typeof PiResumeCursor.Type;

const decodePiResumeCursor = Schema.decodeUnknownOption(PiResumeCursor);

/**
 * Decode a persisted resume cursor. Invalid shapes return `undefined`.
 */
export function parsePiResumeCursor(raw: unknown): PiResumeCursor | undefined {
  return Option.getOrUndefined(decodePiResumeCursor(raw));
}

/**
 * Build a durable resume cursor from Pi `get_state` data.
 */
export function buildPiResumeCursor(
  state: PiRpcSessionState | undefined,
): PiResumeCursor | undefined {
  if (!state?.sessionId) {
    return undefined;
  }
  return {
    schemaVersion: PI_RESUME_VERSION,
    sessionId: state.sessionId,
    ...(state.sessionFile ? { sessionFile: state.sessionFile } : {}),
  };
}

/**
 * Validate resume input before starting a Pi session.
 */
export function validatePiResumeCursor(resume: PiResumeCursor | undefined): string | undefined {
  if (!resume) {
    return undefined;
  }
  if (!resume.sessionFile?.trim()) {
    return "Pi resume requires sessionFile in resumeCursor.";
  }
  return undefined;
}

/**
 * Parse `provider/model` or a bare model id into Pi RPC fields.
 */
export function parsePiModelSelection(
  model: string | undefined,
  fallbackProvider: string | undefined,
  fallbackModel: string | undefined,
): { readonly provider: string; readonly modelId: string } | undefined {
  const trimmed = model?.trim();
  if (trimmed && trimmed.includes("/")) {
    const slashIndex = trimmed.indexOf("/");
    const provider = trimmed.slice(0, slashIndex).trim();
    const modelId = trimmed.slice(slashIndex + 1).trim();
    if (provider.length > 0 && modelId.length > 0) {
      return { provider, modelId };
    }
  }

  const resolvedModel = (trimmed && trimmed.length > 0 ? trimmed : fallbackModel?.trim()) ?? "";
  const resolvedProvider = fallbackProvider?.trim() ?? "";
  if (resolvedProvider.length === 0 || resolvedModel.length === 0) {
    return undefined;
  }
  return { provider: resolvedProvider, modelId: resolvedModel };
}
