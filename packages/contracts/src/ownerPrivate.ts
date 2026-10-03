import * as Schema from "effect/Schema";
import { ThreadId } from "./baseSchemas.ts";

export const OwnerPrivateFrame = Schema.Struct({
  kind: Schema.Literals(["conceal", "secret", "cancel", "restore"]),
  threadId: ThreadId,
  requestId: Schema.String,
  epoch: Schema.Int,
});
export const OwnerPrivateResponse = Schema.Struct({
  threadId: ThreadId,
  requestId: Schema.String,
  epoch: Schema.Int,
  kind: Schema.Literals(["ack", "secret", "cancel"]),
  value: Schema.optional(Schema.String),
});
