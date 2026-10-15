v17 normalizes recovery packets through JSON before signing. Signaling already
uses JSON on the wire; platform descriptions/candidates must be converted to
their wire fields rather than passed through structuredClone.

A regression using non-cloneable JSON-serializable description and candidate
objects produces DataCloneError on v16 and passes with this conversion. This
reproduces a concrete bridge-path defect consistent with the native zero-send
failure. The uploaded native run did not retain its exception message, so it
does not independently prove that this was its only failure.

Encoding/signing errors now report a bounded SYS_SIGNAL_BRIDGE failure and
return false to the caller. They do not throw into SYS connection setup.
The harness also retains sys_birth_exception_detail separately from throttled
snapshots. No relay recovery or routing-order changes are included.

Run the same sparse scenario and upload summary.json, events.jsonl, final.json:

    bash tools/swarm/run.sh --nostr --scenario peer-bridge --peers 5 --bytes 4194304 --timeout 90000

Native recovery through the intermediary remains to be validated.
