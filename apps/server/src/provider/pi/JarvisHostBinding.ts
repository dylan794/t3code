// @effect-diagnostics nodeBuiltinImport:off
import * as NodeCrypto from "node:crypto";

/** Identity only: Jarvis still requires current Owner authorization and destination verification. */
export function jarvisHostEnvironment(
  environmentId: string | undefined,
  instanceId: string,
  threadId: string,
): Record<string, string> {
  const unavailable = {
    JARVIS_T3_ENVIRONMENT_ID: "",
    JARVIS_T3_HOST_CONNECTION_ID: "",
    JARVIS_T3_THREAD_ID: "",
  };
  if (
    !environmentId ||
    !/^[\w:.-]{1,180}$/.test(environmentId) ||
    !/^[\w:.-]{1,180}$/.test(threadId)
  ) {
    return unavailable;
  }
  return {
    JARVIS_T3_ENVIRONMENT_ID: environmentId,
    JARVIS_T3_HOST_CONNECTION_ID: NodeCrypto.createHash("sha256")
      .update(JSON.stringify([environmentId, instanceId, threadId]))
      .digest("hex"),
    JARVIS_T3_THREAD_ID: threadId,
  };
}
