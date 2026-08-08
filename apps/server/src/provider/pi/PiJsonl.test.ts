import * as NodeAssert from "node:assert/strict";
import { describe, it } from "vite-plus/test";

import { createJsonlLineReader, serializeJsonLine } from "./PiJsonl.ts";

describe("PiJsonl", () => {
  it("serializes values with a trailing LF", () => {
    NodeAssert.equal(
      serializeJsonLine({ type: "prompt", message: "hi" }),
      '{"type":"prompt","message":"hi"}\n',
    );
  });

  it("splits records on LF only and preserves Unicode line separators inside JSON strings", () => {
    const reader = createJsonlLineReader();
    const payload = JSON.stringify({ text: "a\u2028b" });
    const lines = reader.push(`${payload}\n{"type":"done"}\n`);
    NodeAssert.deepEqual(lines, [payload, '{"type":"done"}']);
  });

  it("buffers partial records across chunks", () => {
    const reader = createJsonlLineReader();
    NodeAssert.deepEqual(reader.push('{"type":'), []);
    NodeAssert.deepEqual(reader.push('"ping"}\n'), ['{"type":"ping"}']);
  });

  it("flushes a trailing record without a newline", () => {
    const reader = createJsonlLineReader();
    reader.push('{"type":"tail"}');
    NodeAssert.equal(reader.flush(), '{"type":"tail"}');
    NodeAssert.equal(reader.flush(), undefined);
  });

  it("strips a trailing carriage return from CRLF-delimited input", () => {
    const reader = createJsonlLineReader();
    NodeAssert.deepEqual(reader.push('{"ok":true}\r\n'), ['{"ok":true}']);
  });
});
