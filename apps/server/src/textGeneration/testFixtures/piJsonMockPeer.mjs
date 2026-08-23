const chunks = [];
for await (const chunk of process.stdin) chunks.push(chunk);

const modelIndex = process.argv.indexOf("--model");
const model = modelIndex >= 0 ? process.argv[modelIndex + 1] : undefined;
const requiredFlags = [
  "--mode",
  "json",
  "--no-session",
  "--no-extensions",
  "--no-skills",
  "--no-prompt-templates",
  "--no-context-files",
  "--no-tools",
  "--no-approve",
];
if (requiredFlags.some((flag) => !process.argv.includes(flag)) || !model) {
  process.stderr.write("unsafe or incomplete Pi text-generation arguments\n");
  process.exit(9);
}

if (model === "mock/exit") {
  process.stderr.write("mock authentication failed\n");
  process.exit(7);
}
if (model === "mock/hang") {
  setInterval(() => {}, 1_000);
} else if (model === "mock/no-message") {
  process.stdout.write(`${JSON.stringify({ type: "agent_settled" })}\n`);
} else {
  const value =
    model === "mock/malformed"
      ? "not json"
      : JSON.stringify({
          subject: "Fix Pi metadata.",
          body: "  Explain the generated metadata.  ",
          branch: "Feature/Pi Metadata",
          title: "Generate Pi metadata",
        });
  process.stdout.write(
    `${JSON.stringify({
      type: "message_end",
      message:
        model === "mock/provider-error"
          ? { role: "assistant", content: [], errorMessage: "mock provider unavailable" }
          : { role: "assistant", content: [{ type: "text", text: value }] },
    })}\n`,
  );
}
