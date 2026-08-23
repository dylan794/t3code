import * as NodeReadline from "node:readline";

const output = (message) => process.stdout.write(`${JSON.stringify(message)}\n`);
const input = NodeReadline.createInterface({ input: process.stdin });

input.on("line", (line) => {
  const command = JSON.parse(line);
  if (command.type === "get_state") {
    output({
      id: command.id,
      type: "response",
      command: "get_state",
      success: true,
      data: { sessionId: "mock-session", sessionFile: "mock-session.jsonl" },
    });
    return;
  }
  if (command.type === "get_commands") {
    output({
      id: command.id,
      type: "response",
      command: "get_commands",
      success: true,
      data: { commands: [{ name: "jarvis", description: "Jarvis status" }] },
    });
    return;
  }
  if (command.type === "get_available_models") {
    output({
      id: command.id,
      type: "response",
      command: "get_available_models",
      success: true,
      data: { models: [{ provider: "openai-codex", id: "gpt-test", name: "GPT Test" }] },
    });
    return;
  }
  if (command.type === "prompt") {
    output({ id: command.id, type: "response", command: "prompt", success: true });
    output({ type: "agent_start" });
    if (command.message === "wait") return;
    if (command.message === "exit") {
      setTimeout(() => process.exit(7), 10);
      return;
    }
    output({ type: "message_start", message: { role: "assistant" } });
    output({
      type: "message_update",
      assistantMessageEvent: { type: "text_delta", contentIndex: 0, delta: "mock reply" },
    });
    output({ type: "message_end", message: { role: "assistant", stopReason: "stop" } });
    output({ type: "agent_end", messages: [], willRetry: false });
    output({ type: "agent_settled" });
    return;
  }
  if (command.type === "abort") {
    output({ id: command.id, type: "response", command: "abort", success: true });
    output({ type: "message_end", message: { role: "assistant", stopReason: "aborted" } });
    output({ type: "agent_end", messages: [], willRetry: false });
    output({ type: "agent_settled" });
    return;
  }
  if (command.type === "set_model" || command.type === "set_thinking_level") {
    output({ id: command.id, type: "response", command: command.type, success: true });
    return;
  }
  if (command.type === "set_session_name") {
    output({
      id: command.id,
      type: "response",
      command: "set_session_name",
      success: command.name !== "rejected",
      ...(command.name === "rejected" ? { error: "mock rejected session name" } : {}),
    });
  }
});
