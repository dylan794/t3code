import * as NodeReadline from "node:readline";

const output = (message) => process.stdout.write(`${JSON.stringify(message)}\n`);
const input = NodeReadline.createInterface({ input: process.stdin });
const pendingDialogs = new Map();
let currentSessionFile = "mock-session.jsonl";
let currentSessionId = "mock-session";
let forkSequence = 0;
let forkMessages = [];
let isStreaming = false;

const finishRun = (text) => {
  output({ type: "message_start", message: { role: "assistant" } });
  output({
    type: "message_update",
    assistantMessageEvent: { type: "text_delta", contentIndex: 0, delta: text },
  });
  output({ type: "message_end", message: { role: "assistant", stopReason: "stop" } });
  output({ type: "agent_end", messages: [], willRetry: false });
  isStreaming = false;
  output({ type: "agent_settled" });
};

input.on("line", (line) => {
  const command = JSON.parse(line);
  if (command.type === "extension_ui_response") {
    const dialog = pendingDialogs.get(command.id);
    if (!dialog) return;
    pendingDialogs.delete(command.id);
    if (dialog === "confirm") {
      finishRun(
        command.cancelled === true
          ? "cancelled"
          : command.confirmed === true
            ? "confirmed"
            : "declined",
      );
    } else {
      finishRun(command.cancelled === true ? "cancelled" : `${dialog}:${String(command.value)}`);
    }
    return;
  }
  if (command.type === "get_state") {
    output({
      id: command.id,
      type: "response",
      command: "get_state",
      success: true,
      data: { sessionId: currentSessionId, sessionFile: currentSessionFile, isStreaming },
    });
    return;
  }
  if (command.type === "get_fork_messages") {
    output({
      id: command.id,
      type: "response",
      command: "get_fork_messages",
      success: true,
      data: { messages: forkMessages },
    });
    return;
  }
  if (command.type === "fork") {
    const targetIndex = forkMessages.findIndex((message) => message.entryId === command.entryId);
    if (targetIndex < 0) {
      output({
        id: command.id,
        type: "response",
        command: "fork",
        success: false,
        error: "unknown fork entry",
      });
      return;
    }
    const target = forkMessages[targetIndex];
    forkMessages = forkMessages.slice(0, targetIndex);
    forkSequence += 1;
    currentSessionId = `mock-session-fork-${forkSequence}`;
    currentSessionFile = `${currentSessionId}.jsonl`;
    output({
      id: command.id,
      type: "response",
      command: "fork",
      success: true,
      data: { text: target.text, cancelled: false },
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
    forkMessages.push({
      entryId: `mock-entry-${forkMessages.length + 1}`,
      text: command.message,
    });
    if (command.message === "handled-without-run") {
      output({
        type: "extension_ui_request",
        id: "mock-auth-warning",
        method: "notify",
        message: "T3 cannot unlock Jarvis in this release. Secret input is not implemented yet.",
        notifyType: "warning",
      });
      output({ id: command.id, type: "response", command: "prompt", success: true });
      return;
    }
    if (command.message === "handled-without-run-info") {
      output({
        type: "extension_ui_request",
        id: "mock-auth-info",
        method: "notify",
        message: "Jarvis authentication is already active.",
        notifyType: "info",
      });
      output({ id: command.id, type: "response", command: "prompt", success: true });
      return;
    }
    if (command.message === "handled-without-run-slow") {
      output({
        type: "extension_ui_request",
        id: "mock-auth-warning-slow",
        method: "notify",
        message: "T3 cannot unlock Jarvis in this release. Secret input is not implemented yet.",
        notifyType: "warning",
      });
      setTimeout(
        () => output({ id: command.id, type: "response", command: "prompt", success: true }),
        50,
      );
      return;
    }
    if (command.message === "handled-then-delayed-run") {
      output({ id: command.id, type: "response", command: "prompt", success: true });
      setTimeout(() => {
        isStreaming = true;
        output({ type: "agent_start" });
        setTimeout(() => {
          if (isStreaming) finishRun("delayed reply");
        }, 10);
      }, 25);
      return;
    }
    if (command.message === "handled-then-notified-delayed-run") {
      output({ id: command.id, type: "response", command: "prompt", success: true });
      setTimeout(() => {
        output({
          type: "extension_ui_request",
          id: "mock-delayed-info",
          method: "notify",
          message: "Jarvis queued the nested run.",
          notifyType: "info",
        });
      }, 5);
      setTimeout(() => {
        isStreaming = true;
        output({ type: "agent_start" });
        setTimeout(() => {
          if (isStreaming) finishRun("delayed reply after notification");
        }, 10);
      }, 25);
      return;
    }
    if (command.message === "reject-prompt") {
      output({
        id: command.id,
        type: "response",
        command: "prompt",
        success: false,
        error: "mock prompt failed",
      });
      return;
    }
    isStreaming = true;
    output({ id: command.id, type: "response", command: "prompt", success: true });
    output({ type: "agent_start" });
    if (command.message === "wait") return;
    if (command.message === "exit") {
      setTimeout(() => process.exit(7), 10);
      return;
    }
    if (command.message === "exit-with-input") {
      pendingDialogs.set("mock-exit-input", "input");
      output({
        type: "extension_ui_request",
        id: "mock-exit-input",
        method: "input",
        title: "Input before exit",
      });
      setTimeout(() => process.exit(7), 30);
      return;
    }
    if (command.message === "confirm") {
      pendingDialogs.set("mock-confirm", "confirm");
      output({
        type: "extension_ui_request",
        id: "mock-confirm",
        method: "confirm",
        title: "Run command?",
        message: "npm test",
        timeout: 10_000,
      });
      return;
    }
    if (command.message === "timeout-zero") {
      pendingDialogs.set("mock-timeout-zero", "confirm");
      output({
        type: "extension_ui_request",
        id: "mock-timeout-zero",
        method: "confirm",
        title: "No timeout",
        message: "Zero disables Pi's timeout",
        timeout: 0,
      });
      return;
    }
    if (command.message === "input") {
      pendingDialogs.set("mock-input", "input");
      output({
        type: "extension_ui_request",
        id: "mock-input",
        method: "input",
        title: "Your name",
        placeholder: "Type a name",
      });
      return;
    }
    if (command.message === "invalid-dialog") {
      pendingDialogs.set("mock-invalid", "confirm");
      output({
        type: "extension_ui_request",
        id: "mock-invalid",
        method: "confirm",
        message: "Missing title",
      });
      return;
    }
    if (command.message === "select") {
      pendingDialogs.set("mock-select", "select");
      output({
        type: "extension_ui_request",
        id: "mock-select",
        method: "select",
        title: "Choose a suit",
        options: ["Iron Man", " War Machine "],
      });
      return;
    }
    if (command.message === "editor") {
      pendingDialogs.set("mock-editor", "editor");
      output({
        type: "extension_ui_request",
        id: "mock-editor",
        method: "editor",
        title: "Edit briefing",
        prefill: "Keep the whitespace\n",
      });
      return;
    }
    if (command.message === "set-editor-text") {
      output({
        type: "extension_ui_request",
        id: "mock-editor-text",
        method: "set_editor_text",
        text: "Voice transcript for correction",
      });
      finishRun("editor text requested");
      return;
    }
    if (command.message === "timeout") {
      pendingDialogs.set("mock-timeout", "confirm");
      output({
        type: "extension_ui_request",
        id: "mock-timeout",
        method: "confirm",
        title: "Timed request",
        message: "Wait for timeout",
        timeout: 30,
      });
      setTimeout(() => {
        if (!pendingDialogs.has("mock-timeout")) return;
        pendingDialogs.delete("mock-timeout");
        finishRun("declined");
      }, 30);
      return;
    }
    finishRun("mock reply");
    return;
  }
  if (command.type === "abort") {
    output({ id: command.id, type: "response", command: "abort", success: true });
    if (!isStreaming) return;
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
