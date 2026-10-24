# Retained differential oracles

These implementations are test-only. The owned-string XML walker, worksheet scanner and retained cell/value types preserve the algorithms and observations that preceded borrowed events and compact source marks. Their intentional duplicate types prevent representation changes from silently weakening the comparison.

Do not simplify them into calls to the production walker/scanner they verify, deduplicate their parsing decisions with that implementation, or change refusal order while moving files. The normalization adapter projects compact marks into the retained shape and checks scalar proofs; it is not a replacement reference parser. The legacy ingestion path independently exercises Calamine and exact formula replay, including its test-only temporary XML view.

`worksheet_intermediate.rs` retains the intermediate projected scan loop. It intentionally shares source-mark helpers and borrowed XML primitives, as before; the owned-string walker/scanner remains the independent end-to-end reference. This file has a test-only lexical mount under `worksheet` to preserve access to private mark helpers without broadening their production visibility. The `oracles` namespace re-exports that mount for differential callers. No production reader can enter it.

The differential harness shadows test walks/scans, preserves counters and error observations, and compares retained output. Its thread-local guard prevents recursive shadowing. Generated cases, corpus runners and literal/formula assertions remain in behavior-oriented test modules outside this directory.
