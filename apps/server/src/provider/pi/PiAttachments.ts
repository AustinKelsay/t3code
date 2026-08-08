/**
 * Resolve T3 chat attachments into Pi RPC `ImageContent` payloads.
 *
 * @module provider/pi/PiAttachments
 */
import type { ChatAttachment } from "@t3tools/contracts";
import { ProviderDriverKind } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import type * as FileSystem from "effect/FileSystem";

import { resolveAttachmentPath } from "../../attachmentStore.ts";
import { ProviderAdapterRequestError, ProviderAdapterValidationError } from "../Errors.ts";
import type { PiImageContent } from "./PiRpcProtocol.ts";

const PROVIDER = ProviderDriverKind.make("piAgent");

function isPiImageAttachment(attachment: ChatAttachment): boolean {
  return attachment.type === "image" && attachment.mimeType.toLowerCase().startsWith("image/");
}

/**
 * Read image attachments from disk and encode them for Pi RPC commands.
 * Rejects non-image attachments with a validation error.
 */
export function resolvePiTurnImages(
  fileSystem: FileSystem.FileSystem,
  input: {
    readonly attachmentsDir: string;
    readonly attachments: ReadonlyArray<ChatAttachment> | undefined;
    readonly operation?: string;
  },
): Effect.Effect<
  Array<PiImageContent>,
  ProviderAdapterRequestError | ProviderAdapterValidationError
> {
  return Effect.gen(function* () {
    const operation = input.operation ?? "sendTurn";
    const images: Array<PiImageContent> = [];

    for (const attachment of input.attachments ?? []) {
      if (!isPiImageAttachment(attachment)) {
        return yield* new ProviderAdapterValidationError({
          provider: PROVIDER,
          operation,
          issue: `Pi only supports image attachments; got '${attachment.name}' (${attachment.mimeType}).`,
        });
      }
      const attachmentPath = resolveAttachmentPath({
        attachmentsDir: input.attachmentsDir,
        attachment,
      });
      if (!attachmentPath) {
        return yield* new ProviderAdapterRequestError({
          provider: PROVIDER,
          method: "prompt",
          detail: `Invalid attachment id '${attachment.id}'.`,
        });
      }
      const bytes = yield* fileSystem.readFile(attachmentPath).pipe(
        Effect.mapError(
          (cause) =>
            new ProviderAdapterRequestError({
              provider: PROVIDER,
              method: "prompt",
              detail: cause.message,
              cause,
            }),
        ),
      );
      images.push({
        type: "image",
        data: Buffer.from(bytes).toString("base64"),
        mimeType: attachment.mimeType,
      });
    }

    return images;
  });
}
