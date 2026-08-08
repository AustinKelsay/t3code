/**
 * PiExtensionUiMapping — parse and map Pi extension UI requests into T3 payloads.
 *
 * @module provider/pi/PiExtensionUiMapping
 */
import type { RuntimeWarningPayload, UserInputQuestion } from "@t3tools/contracts";

import type { PiAgentSessionEvent, PiRpcExtensionUiRequest } from "./PiRpcProtocol.ts";

function readString(record: Record<string, unknown>, key: string): string | undefined {
  const value = record[key];
  return typeof value === "string" && value.trim().length > 0 ? value : undefined;
}

const PI_EXTENSION_UI_DIALOG_METHODS = new Set(["confirm", "select", "input", "editor"]);
const PI_EXTENSION_UI_FIRE_AND_FORGET_METHODS = new Set([
  "notify",
  "setStatus",
  "setWidget",
  "setTitle",
  "set_editor_text",
]);

/**
 * Whether a Pi extension UI method blocks for a client response.
 */
export function isPiExtensionUiDialogMethod(method: string): boolean {
  return PI_EXTENSION_UI_DIALOG_METHODS.has(method);
}

/**
 * Whether a Pi extension UI method is fire-and-forget.
 */
export function isPiExtensionUiFireAndForgetMethod(method: string): boolean {
  return PI_EXTENSION_UI_FIRE_AND_FORGET_METHODS.has(method);
}

/**
 * Parse a Pi extension UI request event.
 */
export function parsePiExtensionUiRequest(
  event: PiAgentSessionEvent,
): PiRpcExtensionUiRequest | undefined {
  const record = event as Record<string, unknown>;
  if (record.type !== "extension_ui_request") {
    return undefined;
  }
  const id = readString(record, "id");
  const method = readString(record, "method");
  if (!id || !method) {
    return undefined;
  }
  const options = Array.isArray(record.options)
    ? record.options.filter((entry): entry is string => typeof entry === "string")
    : undefined;
  const widgetLines = Array.isArray(record.widgetLines)
    ? record.widgetLines.filter((entry): entry is string => typeof entry === "string")
    : undefined;
  const notifyType = record.notifyType;
  return {
    type: "extension_ui_request",
    id,
    method,
    ...(readString(record, "title") ? { title: readString(record, "title") } : {}),
    ...(readString(record, "message") ? { message: readString(record, "message") } : {}),
    ...(readString(record, "placeholder")
      ? { placeholder: readString(record, "placeholder") }
      : {}),
    ...(readString(record, "prefill") ? { prefill: readString(record, "prefill") } : {}),
    ...(options && options.length > 0 ? { options } : {}),
    ...(notifyType === "info" || notifyType === "warning" || notifyType === "error"
      ? { notifyType }
      : {}),
    ...(readString(record, "statusKey") ? { statusKey: readString(record, "statusKey") } : {}),
    ...(readString(record, "statusText") ? { statusText: readString(record, "statusText") } : {}),
    ...(readString(record, "widgetKey") ? { widgetKey: readString(record, "widgetKey") } : {}),
    ...(widgetLines && widgetLines.length > 0 ? { widgetLines } : {}),
    ...(readString(record, "text") ? { text: readString(record, "text") } : {}),
  };
}

/**
 * Build a cancellation response for Pi extension UI dialog methods.
 */
export function buildPiExtensionUiCancelResponse(requestId: string): {
  readonly type: "extension_ui_response";
  readonly id: string;
  readonly cancelled: true;
} {
  return {
    type: "extension_ui_response",
    id: requestId,
    cancelled: true,
  };
}

/**
 * Build a confirmation response for Pi `confirm` dialogs.
 */
export function buildPiExtensionUiConfirmedResponse(
  requestId: string,
  confirmed: boolean,
): {
  readonly type: "extension_ui_response";
  readonly id: string;
  readonly confirmed: boolean;
} {
  return {
    type: "extension_ui_response",
    id: requestId,
    confirmed,
  };
}

/**
 * Build a value response for Pi `select`, `input`, and `editor` dialogs.
 */
export function buildPiExtensionUiValueResponse(
  requestId: string,
  value: string,
): {
  readonly type: "extension_ui_response";
  readonly id: string;
  readonly value: string;
} {
  return {
    type: "extension_ui_response",
    id: requestId,
    value,
  };
}

/**
 * Map a Pi `confirm` request into a T3 approval request payload.
 */
export function mapPiConfirmToRequestOpenedPayload(request: PiRpcExtensionUiRequest): {
  readonly requestType: "dynamic_tool_call";
  readonly detail: string;
  readonly args: PiRpcExtensionUiRequest;
} {
  const title = request.title ?? "Confirm";
  const detail = request.message ? `${title}: ${request.message}` : title;
  return {
    requestType: "dynamic_tool_call",
    detail,
    args: request,
  };
}

/**
 * Map a Pi `select` request into T3 user-input questions.
 */
export function mapPiSelectToUserInputQuestions(request: PiRpcExtensionUiRequest): {
  readonly questions: Array<UserInputQuestion>;
  readonly questionId: string;
} {
  const questionId = request.title ?? "select";
  return {
    questionId,
    questions: [
      {
        id: questionId,
        header: request.title ?? "Select",
        question: request.title ?? "Select an option",
        options: (request.options ?? []).map((option) => ({
          label: option,
          description: option,
        })),
        multiSelect: false,
      },
    ],
  };
}

/**
 * Map a Pi `input` request into T3 user-input questions.
 */
export function mapPiInputToUserInputQuestions(request: PiRpcExtensionUiRequest): {
  readonly questions: Array<UserInputQuestion>;
  readonly questionId: string;
} {
  const questionId = request.title ?? "input";
  const prompt = request.placeholder
    ? `${request.title ?? "Input"} (${request.placeholder})`
    : (request.title ?? "Input");
  return {
    questionId,
    questions: [
      {
        id: questionId,
        header: request.title ?? "Input",
        question: prompt,
        options: [],
        multiSelect: false,
      },
    ],
  };
}

/**
 * Map a Pi `editor` request into T3 user-input questions.
 */
export function mapPiEditorToUserInputQuestions(request: PiRpcExtensionUiRequest): {
  readonly questions: Array<UserInputQuestion>;
  readonly questionId: string;
} {
  const questionId = request.title ?? "editor";
  return {
    questionId,
    questions: [
      {
        id: questionId,
        header: request.title ?? "Editor",
        question: request.title ?? "Edit text",
        options: [],
        multiSelect: false,
      },
    ],
  };
}

/**
 * Map a Pi `notify` request into a T3 runtime warning event payload.
 */
export function mapPiNotifyToRuntimeWarning(
  request: PiRpcExtensionUiRequest,
): RuntimeWarningPayload | undefined {
  const message = request.message ?? request.title;
  if (!message) {
    return undefined;
  }
  const prefix =
    request.notifyType === "error"
      ? "Error"
      : request.notifyType === "warning"
        ? "Warning"
        : "Info";
  return {
    message: `${prefix}: ${message}`,
    detail: request,
  };
}

/**
 * Whether an inbound Pi line is an extension UI request.
 */
export function isPiExtensionUiRequest(event: PiAgentSessionEvent): string | undefined {
  return parsePiExtensionUiRequest(event)?.id;
}
