import { clearComposerDraftsEnvironment } from "../composerDraftStore";
import { useAtomValue } from "@effect/atom-react";
import {
  createOwnerPrivateAtoms,
  ownerPrivatePresentation,
} from "@t3tools/client-runtime/state/owner-private";
import type { EnvironmentId } from "@t3tools/contracts";
import { useLayoutEffect, useState, useSyncExternalStore, type PropsWithChildren } from "react";
import { connectionAtomRuntime } from "../connection/runtime";
import { environmentCatalog } from "../connection/catalog";
import { appAtomRegistry } from "../rpc/atomRegistry";

const privateAtoms = createOwnerPrivateAtoms(connectionAtomRuntime);
function PrivateSubscription({ environmentId }: { environmentId: EnvironmentId }) {
  useAtomValue(privateAtoms.frames({ environmentId, input: { environmentId } }));
  return null;
}

/** Unmount ordinary content before any private-input acknowledgement. */
export function OwnerPrivateBoundary({ children }: PropsWithChildren) {
  const catalog = useAtomValue(environmentCatalog.catalogValueAtom);
  const frame = useSyncExternalStore(
    ownerPrivatePresentation.subscribe,
    ownerPrivatePresentation.getSnapshot,
    () => null,
  );
  const [value, setValue] = useState("");
  const [sending, setSending] = useState(false);
  useLayoutEffect(() => {
    setValue("");
    if (frame?.kind !== "conceal") return;
    document.title = "T3 Code";
    clearComposerDraftsEnvironment(frame.environmentId);
    // This layout effect runs after the ordinary subtree was removed. The
    // server cannot release the Pi prompt until durable caches are cleared.
    void privateAtoms.respond.run(appAtomRegistry, {
      environmentId: frame.environmentId,
      input: {
        threadId: frame.threadId,
        requestId: frame.requestId,
        epoch: frame.epoch,
        kind: "ack",
      },
    });
  }, [frame]);
  const respond = async (kind: "secret" | "cancel") => {
    if (!frame || sending) return;
    const secret = value;
    setValue("");
    setSending(true);
    try {
      await privateAtoms.respond.run(appAtomRegistry, {
        environmentId: frame.environmentId,
        input: {
          threadId: frame.threadId,
          requestId: frame.requestId,
          epoch: frame.epoch,
          kind,
          ...(kind === "secret" ? { value: secret } : {}),
        },
      });
    } finally {
      setSending(false);
    }
  };
  return (
    <>
      {Array.from(catalog.entries.keys(), (environmentId) => (
        <PrivateSubscription key={environmentId} environmentId={environmentId} />
      ))}
      {frame ? (
        <main
          role="dialog"
          aria-modal="true"
          aria-label="Private Owner input"
          className="fixed inset-0 z-[9999] flex flex-col items-center justify-center gap-4 bg-background p-8 text-foreground"
        >
          <h1>Private Owner input</h1>
          {frame.kind === "secret" ? (
            <form
              onSubmit={(event) => {
                event.preventDefault();
                void respond("secret");
              }}
              className="flex flex-col gap-4"
            >
              <label>
                Secret
                <input
                  type="password"
                  aria-label="Secret"
                  autoComplete="off"
                  spellCheck={false}
                  maxLength={256}
                  value={value}
                  onChange={(event) => setValue(event.target.value)}
                  disabled={sending}
                  autoFocus
                />
              </label>
              <button type="submit" disabled={sending}>
                Submit privately
              </button>
              <button type="button" disabled={sending} onClick={() => void respond("cancel")}>
                Cancel
              </button>
            </form>
          ) : (
            <p>The workspace is concealed for this private request.</p>
          )}
        </main>
      ) : (
        children
      )}
    </>
  );
}
