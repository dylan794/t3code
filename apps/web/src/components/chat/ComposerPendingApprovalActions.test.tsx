import { ApprovalRequestId } from "@t3tools/contracts";
import { renderToStaticMarkup } from "react-dom/server";
import { describe, expect, it } from "vite-plus/test";

import { ComposerPendingApprovalActions } from "./ComposerPendingApprovalActions";

describe("ComposerPendingApprovalActions", () => {
  it("states that the persistent approval lasts for this session", () => {
    const markup = renderToStaticMarkup(
      <ComposerPendingApprovalActions
        requestId={ApprovalRequestId.make("approval-1")}
        isResponding={false}
        onRespondToApproval={async () => undefined}
      />,
    );

    expect(markup).toContain(">Cancel<");
    expect(markup).toContain("Always allow this session");
    expect(markup).not.toContain(">Always allow<");
    expect(markup).toContain("h-5");
    expect(markup).toContain("sm:text-[11px]");
    expect(markup).not.toContain("sm:h-6");
  });

  it("hides session approval when the provider cannot persist it", () => {
    const markup = renderToStaticMarkup(
      <ComposerPendingApprovalActions
        requestId={ApprovalRequestId.make("pi-confirm-1")}
        isResponding={false}
        supportsAcceptForSession={false}
        onRespondToApproval={async () => undefined}
      />,
    );

    expect(markup).toContain(">Approve<");
    expect(markup).toContain(">Decline<");
    expect(markup).not.toContain("Always allow this session");
  });
});
