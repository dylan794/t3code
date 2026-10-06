import { clearThreadOutboxEnvironment } from "../../state/thread-outbox";
import { clearComposerDraftsEnvironment } from "../../state/use-composer-drafts";
import { useAtomValue } from "@effect/atom-react";
import {
  createOwnerPrivateAtoms,
  ownerPrivatePresentation,
  dismissOwnerPrivatePresentation,
} from "@t3tools/client-runtime/state/owner-private";
import type { EnvironmentId } from "@t3tools/contracts";
import { useLayoutEffect, useState, useSyncExternalStore, type PropsWithChildren } from "react";
import { View, Text, TextInput, Pressable } from "react-native";
import { connectionAtomRuntime } from "../../connection/runtime";
import { environmentCatalog } from "../../connection/catalog";
import { appAtomRegistry } from "../../state/atom-registry";

const privateAtoms = createOwnerPrivateAtoms(connectionAtomRuntime);
function PrivateSubscription({ environmentId }: { environmentId: EnvironmentId }) {
  useAtomValue(privateAtoms.frames({ environmentId, input: { environmentId } }));
  return null;
}

export function OwnerPrivateBoundary({ children }: PropsWithChildren) {
  const catalog = useAtomValue(environmentCatalog.catalogValueAtom);
  const frame = useSyncExternalStore(
    ownerPrivatePresentation.subscribe,
    ownerPrivatePresentation.getSnapshot,
    () => null,
  );
  const [value, setValue] = useState("");
  const [sending, setSending] = useState(false);
  const [error, setError] = useState<string | null>(null);
  useLayoutEffect(() => {
    setValue("");
    setError(null);
    if (frame?.kind !== "conceal") return;
    void Promise.all([
      clearComposerDraftsEnvironment(frame.environmentId),
      clearThreadOutboxEnvironment(frame.environmentId),
    ]).then(() =>
      privateAtoms.respond.run(appAtomRegistry, {
        environmentId: frame.environmentId,
        input: {
          threadId: frame.threadId,
          requestId: frame.requestId,
          epoch: frame.epoch,
          kind: "ack",
        },
      }),
    );
  }, [frame]);
  const respond = async (kind: "secret" | "cancel" | "leave") => {
    if (!frame || sending) return;
    if (kind === "leave" && frame.requestId === "disconnected") {
      dismissOwnerPrivatePresentation(frame);
      return;
    }
    const secret = value;
    setValue("");
    setSending(true);
    try {
      const result = await privateAtoms.respond.run(appAtomRegistry, {
        environmentId: frame.environmentId,
        input: {
          threadId: frame.threadId,
          requestId: frame.requestId,
          epoch: frame.epoch,
          kind,
          ...(kind === "secret" ? { value: secret } : {}),
        },
      });
      if (result._tag !== "Success" || !result.value.accepted)
        throw new Error("Private channel unavailable");
      if (kind === "leave") dismissOwnerPrivatePresentation(frame);
    } catch {
      setError("Could not complete the private request. Retry after the connection is available.");
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
        <View
          accessibilityViewIsModal
          style={{
            flex: 1,
            alignItems: "center",
            justifyContent: "center",
            padding: 32,
            gap: 20,
            backgroundColor: "#ffffff",
          }}
        >
          <Text>Private Owner input</Text>
          {error && <Text accessibilityRole="alert">{error}</Text>}
          {frame.kind === "secret" ? (
            <>
              <TextInput
                accessibilityLabel="Secret"
                secureTextEntry
                autoComplete="off"
                autoCorrect={false}
                textContentType="none"
                maxLength={256}
                value={value}
                onChangeText={setValue}
                editable={!sending}
                style={{ borderWidth: 1, padding: 12, width: "100%" }}
              />
              <Pressable disabled={sending} onPress={() => void respond("secret")}>
                <Text>Submit privately</Text>
              </Pressable>
              <Pressable disabled={sending} onPress={() => void respond("cancel")}>
                <Text>Cancel</Text>
              </Pressable>
            </>
          ) : (
            <>
              <Text>The workspace is concealed for this private request.</Text>
              <Text>Return to the workspace to start a fresh Pi thread and run /owner unlock.</Text>
              <Pressable disabled={sending} onPress={() => void respond("leave")}>
                <Text>Return to workspace</Text>
              </Pressable>
            </>
          )}
        </View>
      ) : (
        children
      )}
    </>
  );
}
