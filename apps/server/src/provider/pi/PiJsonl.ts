/**
 * Strict LF-only JSONL framing for the Pi RPC protocol.
 *
 * Mirrors `@earendil-works/pi-coding-agent` jsonl helpers: records are split on
 * `\n` only (never Node readline) so U+2028/U+2029 inside JSON strings stay intact.
 *
 * @module provider/pi/PiJsonl
 */

/**
 * Serialize a value as one JSONL record terminated by a single LF.
 */
export function serializeJsonLine(value: unknown): string {
  return `${JSON.stringify(value)}\n`;
}

/**
 * Incremental LF-only line splitter for string chunks.
 */
export interface JsonlLineReader {
  readonly push: (chunk: string) => ReadonlyArray<string>;
  readonly flush: () => string | undefined;
}

/**
 * Create a stateful JSONL line reader that splits on `\n` only.
 */
export function createJsonlLineReader(): JsonlLineReader {
  let buffer = "";

  const emitLine = (line: string): string => (line.endsWith("\r") ? line.slice(0, -1) : line);

  return {
    push(chunk: string): ReadonlyArray<string> {
      buffer += chunk;
      const lines: Array<string> = [];
      while (true) {
        const newlineIndex = buffer.indexOf("\n");
        if (newlineIndex === -1) {
          break;
        }
        lines.push(emitLine(buffer.slice(0, newlineIndex)));
        buffer = buffer.slice(newlineIndex + 1);
      }
      return lines;
    },
    flush(): string | undefined {
      if (buffer.length === 0) {
        return undefined;
      }
      const line = emitLine(buffer);
      buffer = "";
      return line;
    },
  };
}
