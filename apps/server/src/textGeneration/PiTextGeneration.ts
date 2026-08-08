/**
 * PiTextGeneration — one-shot text generation via `pi -p --mode json`.
 *
 * @module PiTextGeneration
 */
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import { ChildProcessSpawner } from "effect/unstable/process";

import { type ModelSelection, type PiSettings, TextGenerationError } from "@t3tools/contracts";
import { sanitizeBranchFragment, sanitizeFeatureBranchName } from "@t3tools/shared/git";
import { extractJsonObject } from "@t3tools/shared/schemaJson";

import * as TextGeneration from "./TextGeneration.ts";
import {
  buildBranchNamePrompt,
  buildCommitMessagePrompt,
  buildPrContentPrompt,
  buildThreadTitlePrompt,
} from "./TextGenerationPrompts.ts";
import {
  normalizeCliError,
  sanitizeCommitSubject,
  sanitizePrTitle,
  sanitizeThreadTitle,
} from "./TextGenerationUtils.ts";
import { parsePiModelSelection } from "../provider/pi/PiRpcProtocol.ts";
import { resolvePiPrintCommand } from "../provider/pi/PiSpawn.ts";
import { createJsonlLineReader } from "../provider/pi/PiJsonl.ts";

const PI_TIMEOUT_MS = 180_000;
const isTextGenerationError = Schema.is(TextGenerationError);

type PiJsonlExtraction =
  | { readonly _tag: "ok"; readonly text: string }
  | { readonly _tag: "unparseable"; readonly lineCount: number }
  | { readonly _tag: "empty"; readonly lineCount: number };

function extractAssistantTextFromPiJsonLines(stdout: string): PiJsonlExtraction {
  const reader = createJsonlLineReader();
  const lines = [...reader.push(stdout)];
  const trailing = reader.flush();
  if (trailing) {
    lines.push(trailing);
  }
  const chunks: Array<string> = [];
  let parseFailures = 0;
  for (const line of lines) {
    try {
      const event = JSON.parse(line) as Record<string, unknown>;
      if (event.type !== "message_end" || !event.message || typeof event.message !== "object") {
        continue;
      }
      const message = event.message as Record<string, unknown>;
      if (message.role !== "assistant" || !Array.isArray(message.content)) {
        continue;
      }
      for (const part of message.content) {
        if (
          part &&
          typeof part === "object" &&
          "type" in part &&
          part.type === "text" &&
          "text" in part &&
          typeof part.text === "string"
        ) {
          chunks.push(part.text);
        }
      }
    } catch {
      parseFailures += 1;
    }
  }
  const text = chunks.join("").trim();
  if (text.length > 0) {
    return { _tag: "ok", text };
  }
  if (lines.length > 0 && parseFailures === lines.length) {
    return { _tag: "unparseable", lineCount: lines.length };
  }
  return { _tag: "empty", lineCount: lines.length };
}

export const makePiTextGeneration = Effect.fn("makePiTextGeneration")(function* (
  piSettings: PiSettings,
  environment: NodeJS.ProcessEnv = process.env,
) {
  const commandSpawner = yield* ChildProcessSpawner.ChildProcessSpawner;

  const runPiJson = <S extends Schema.Top>({
    operation,
    cwd,
    prompt,
    outputSchemaJson,
    modelSelection,
  }: {
    operation:
      | "generateCommitMessage"
      | "generatePrContent"
      | "generateBranchName"
      | "generateThreadTitle";
    cwd: string;
    prompt: string;
    outputSchemaJson: S;
    modelSelection: ModelSelection;
  }): Effect.Effect<S["Type"], TextGenerationError, S["DecodingServices"]> =>
    Effect.gen(function* () {
      const selectedModel = parsePiModelSelection(
        modelSelection.model,
        piSettings.defaultProvider,
        piSettings.defaultModel,
      );
      const resolved = yield* resolvePiPrintCommand({
        settings: piSettings,
        cwd,
        environment,
        ...(selectedModel
          ? { provider: selectedModel.provider, model: selectedModel.modelId }
          : {}),
        prompt,
      });
      const child = yield* commandSpawner.spawn(resolved.command);
      const stdout = yield* child.stdout.pipe(
        Stream.decodeText(),
        Stream.runFold(
          () => "",
          (acc, chunk) => acc + chunk,
        ),
      );
      const exitCode = yield* child.exitCode.pipe(Effect.map(Number));
      if (exitCode !== 0) {
        return yield* new TextGenerationError({
          operation,
          detail: `Pi print mode exited with code ${exitCode}.`,
        });
      }

      const extracted = extractAssistantTextFromPiJsonLines(stdout);
      if (extracted._tag === "unparseable") {
        return yield* new TextGenerationError({
          operation,
          detail: `Pi returned unparseable JSONL output (${extracted.lineCount} line(s)).`,
        });
      }
      if (extracted._tag === "empty") {
        return yield* new TextGenerationError({
          operation,
          detail:
            extracted.lineCount > 0
              ? "Pi returned JSONL output without assistant text."
              : "Pi returned empty output.",
        });
      }

      const decodeOutput = Schema.decodeEffect(Schema.fromJsonString(outputSchemaJson));
      return yield* decodeOutput(extractJsonObject(extracted.text)).pipe(
        Effect.catchTags({
          SchemaError: (cause) =>
            Effect.fail(
              new TextGenerationError({
                operation,
                detail: "Pi returned invalid structured output.",
                cause,
              }),
            ),
        }),
      );
    }).pipe(
      Effect.timeoutOption(PI_TIMEOUT_MS),
      Effect.flatMap(
        Option.match({
          onNone: () =>
            Effect.fail(
              new TextGenerationError({ operation, detail: "Pi text generation timed out." }),
            ),
          onSome: (value) => Effect.succeed(value),
        }),
      ),
      Effect.mapError((cause) =>
        isTextGenerationError(cause)
          ? cause
          : normalizeCliError("pi", operation, cause, "Pi text generation failed."),
      ),
      Effect.scoped,
    );

  const generateCommitMessage: TextGeneration.TextGeneration["Service"]["generateCommitMessage"] =
    Effect.fn("PiTextGeneration.generateCommitMessage")(function* (input) {
      const { prompt, outputSchema } = buildCommitMessagePrompt({
        branch: input.branch,
        stagedSummary: input.stagedSummary,
        stagedPatch: input.stagedPatch,
        includeBranch: input.includeBranch === true,
        policy: input.policy,
      });
      const generated = yield* runPiJson({
        operation: "generateCommitMessage",
        cwd: input.cwd,
        prompt,
        outputSchemaJson: outputSchema,
        modelSelection: input.modelSelection,
      });
      return {
        subject: sanitizeCommitSubject(generated.subject),
        body: generated.body.trim(),
        ...("branch" in generated && typeof generated.branch === "string"
          ? { branch: sanitizeFeatureBranchName(generated.branch) }
          : {}),
      };
    });

  const generatePrContent: TextGeneration.TextGeneration["Service"]["generatePrContent"] =
    Effect.fn("PiTextGeneration.generatePrContent")(function* (input) {
      const { prompt, outputSchema } = buildPrContentPrompt({
        baseBranch: input.baseBranch,
        headBranch: input.headBranch,
        commitSummary: input.commitSummary,
        diffSummary: input.diffSummary,
        diffPatch: input.diffPatch,
        policy: input.policy,
        changeRequestTemplate: input.changeRequestTemplate,
      });
      const generated = yield* runPiJson({
        operation: "generatePrContent",
        cwd: input.cwd,
        prompt,
        outputSchemaJson: outputSchema,
        modelSelection: input.modelSelection,
      });
      return {
        title: sanitizePrTitle(generated.title),
        body: generated.body.trim(),
      };
    });

  const generateBranchName: TextGeneration.TextGeneration["Service"]["generateBranchName"] =
    Effect.fn("PiTextGeneration.generateBranchName")(function* (input) {
      const { prompt, outputSchema } = buildBranchNamePrompt({
        message: input.message,
        attachments: input.attachments,
      });
      const generated = yield* runPiJson({
        operation: "generateBranchName",
        cwd: input.cwd,
        prompt,
        outputSchemaJson: outputSchema,
        modelSelection: input.modelSelection,
      });
      return { branch: sanitizeBranchFragment(generated.branch) };
    });

  const generateThreadTitle: TextGeneration.TextGeneration["Service"]["generateThreadTitle"] =
    Effect.fn("PiTextGeneration.generateThreadTitle")(function* (input) {
      const { prompt, outputSchema } = buildThreadTitlePrompt({
        message: input.message,
        previousTitle: input.previousTitle,
        attachments: input.attachments,
      });
      const generated = yield* runPiJson({
        operation: "generateThreadTitle",
        cwd: input.cwd,
        prompt,
        outputSchemaJson: outputSchema,
        modelSelection: input.modelSelection,
      });
      return {
        title: sanitizeThreadTitle(generated.title),
      } satisfies TextGeneration.ThreadTitleGenerationResult;
    });

  return {
    generateCommitMessage,
    generatePrContent,
    generateBranchName,
    generateThreadTitle,
  } satisfies TextGeneration.TextGeneration["Service"];
});
