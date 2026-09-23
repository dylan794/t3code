import { type ApprovalRequestId } from "@t3tools/contracts";
import { useState } from "react";

import { type PendingSecretInput } from "../../session-logic";

/**
 * Collects one secret in component state and clears it before the submit callback.
 * The value is not written into draft storage or the thread transcript.
 */
export function OwnerSecretPrompt({
  prompt,
  onSubmit,
  onCancel,
}: {
  prompt: PendingSecretInput;
  onSubmit: (requestId: ApprovalRequestId, secret: string) => Promise<void> | void;
  onCancel: (requestId: ApprovalRequestId) => Promise<void> | void;
}) {
  const [value, setValue] = useState("");
  const [busy, setBusy] = useState(false);

  const submit = async () => {
    if (busy) return;
    const secret = value;
    setValue("");
    setBusy(true);
    try {
      await onSubmit(prompt.requestId, secret);
    } finally {
      setBusy(false);
    }
  };

  return (
    <form
      className="mx-3 mb-3 rounded-xl border border-border bg-background p-3"
      data-owner-secret-prompt="true"
      onSubmit={(event) => {
        event.preventDefault();
        void submit();
      }}
    >
      <label className="block text-sm font-medium" htmlFor={`owner-secret-${prompt.requestId}`}>
        {prompt.title}
      </label>
      <p className="mt-1 text-xs text-muted-foreground">
        The Owner thread stays hidden. This password is not saved in the conversation.
      </p>
      <input
        id={`owner-secret-${prompt.requestId}`}
        className="mt-3 w-full rounded-md border border-input bg-background px-3 py-2"
        type="password"
        name="owner-secret"
        autoComplete="off"
        autoCorrect="off"
        spellCheck={false}
        value={value}
        disabled={busy}
        onChange={(event) => setValue(event.target.value)}
      />
      <div className="mt-3 flex gap-2">
        <button className="rounded-md bg-primary px-3 py-1.5 text-sm text-primary-foreground" type="submit" disabled={busy}>
          Unlock
        </button>
        <button
          className="rounded-md border border-border px-3 py-1.5 text-sm"
          type="button"
          disabled={busy}
          onClick={() => {
            setValue("");
            void onCancel(prompt.requestId);
          }}
        >
          Cancel
        </button>
      </div>
    </form>
  );
}
