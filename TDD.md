# TDD evidence

## v1 pairing

- Red: `mix test test/relay_protocol_test.exs` failed with `WebSockex.RequestError{code: 404}` because the bootstrap router had no `/ws` upgrade route.
- Green: the same real Bandit/WebSocket test passed after adding query validation, the WebSock upgrade, and the single-node session registry.

## v2 control and buffering

- Red: the first control assertion failed because it compared serialized JSON rather than the control message it represents.
- Green: the test now decodes the real received control frame and verifies the `sync` and `connected` messages, then verifies ordered buffered text and binary delivery after the daemon data socket connects.

## duplicate daemon data

- Red: `mix test test/relay_protocol_test.exs:102` timed out waiting for the replacement daemon data socket after the displaced socket's termination deleted the new route.
- Green: the registry now deletes a v2 data route only when its current owner disconnects; the focused real-WebSocket test passes.
