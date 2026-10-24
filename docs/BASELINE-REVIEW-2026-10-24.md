# Phantom SDK baseline review — 2026-10-24

Reviewed main: `a18c7e812031686cb9fb8b9bc7ee7689dc2a29f1`.

- All 289 root SDK tests passed on this revision.
- npm audit reported zero known dependency advisories for the root lockfile.
- No open pull requests. The three remaining non-main branch heads exactly match merged PR heads: `codex/canonical-share-demo` (#16), `codex/demo-showcase` (#17), and `codex/retire-diagnostic-demos` (#18). No additional merge is needed.
- Demo presentation now contains Phantom Share and Shared canvas. Older connection and Breach harnesses live under tools. Relative imports and current dependency paths were updated; historical paths in the comparison builder deliberately refer to its preserved reference commit.
- All relative Markdown link targets in the checked tracked tree resolve.
- The tracked tree contains no APKs, signing-key containers, local.properties, dependency directories or generated dist/report directories. This is a current-tree check, not a historical secret scan or security certification.
- All 66 package files match Phantom Share's vendored copy byte-for-byte, using Git blob identities. The source pin remains valid; no SDK snapshot sync is needed for these documentation/demo changes.
- The SDK Pages workflow completed successfully for the latest diagnostic-page change. Demo documentation changes do not require a new application build.

## Working agreement

SDK networking APIs belong here; Phantom Share owns its interface, native host and saving behavior. Keep the Share product link canonical at https://phantomshare.app/. Add Canvas's branded showcase when the application is ready.

Keep application updates in the established application checkout, with generated web assets and the existing Android Studio project. CI artifacts are for validation rather than routine user updates.

## Remaining housekeeping

The GitHub connector has no branch-deletion operation. The three verified merged branches above may be deleted after rechecking that their heads have not advanced. Preserve main and any branch with new work.
