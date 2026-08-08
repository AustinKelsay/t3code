# Pi

This guide is for using the [Pi coding agent](https://github.com/earendil-works/pi-mono) in T3 Code.

## Install Pi

Install the Pi CLI with npm, then authenticate with Pi's interactive login flow:

```bash
npm install -g @earendil-works/pi-coding-agent
pi
/login
```

Run `pi` on the machine hosting the T3 Code server, then enter `/login` in the Pi REPL to sign in with your model providers. For printing credentials for external clients, see `pi auth --help`.

## Add a Pi provider in T3 Code

1. Open **Settings → Providers**.
2. Choose **Pi** and create an instance.
3. Configure the instance:

| Setting                   | Description                                                                                             |
| ------------------------- | ------------------------------------------------------------------------------------------------------- |
| Binary path               | Path to the `pi` executable (default: `pi`).                                                            |
| Agent directory           | Optional `PI_CODING_AGENT_DIR` override. Leave empty to use `~/.pi/agent`.                              |
| Default provider          | Optional provider name passed when a session starts (for example `anthropic`).                          |
| Default model             | Optional model id passed when a session starts (for example `claude-sonnet-4-5`).                       |
| Approve project resources | When enabled, T3 Code passes `--approve` so project-local `.pi` extensions and skills load in RPC mode. |

## How T3 Code talks to Pi

T3 Code starts Pi in RPC mode (`pi --mode rpc`) and exchanges strict newline-delimited JSON over stdin/stdout. Each thread gets its own Pi process. Turns complete when Pi emits `agent_settled`.

Model changes on an existing thread are applied in-session via Pi's `set_model` RPC command.

## Extension UI prompts

Pi extensions can request user interaction through the RPC extension UI protocol. T3 Code surfaces blocking prompts in the composer and replies with `extension_ui_response` when you respond.

| Pi method | T3 surface                                 | Response mapping                                                                    |
| --------- | ------------------------------------------ | ----------------------------------------------------------------------------------- |
| `confirm` | Approval prompt (`request.opened`)         | Accept → `confirmed: true`, Reject → `confirmed: false`, Cancel → `cancelled: true` |
| `select`  | User-input prompt (`user-input.requested`) | Selected option → `value`                                                           |
| `input`   | User-input prompt (`user-input.requested`) | Entered text → `value`                                                              |
| `editor`  | User-input prompt (`user-input.requested`) | Edited text → `value`                                                               |

Fire-and-forget methods (`notify`, `setStatus`, `setWidget`, `setTitle`, `set_editor_text`) do not block the session. `notify` may appear as a runtime warning.

Pending extension UI requests are cancelled when a turn is interrupted or the session stops.

## Limitations

- Thread rollback is not supported for Pi sessions.

For architecture details, see [internals/providers.md](../internals/providers.md).
