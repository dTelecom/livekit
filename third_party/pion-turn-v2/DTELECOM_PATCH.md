# Patched copy of github.com/pion/turn/v2 v2.1.0

Wired in via `replace` in the root `go.mod`. Source: the v2.1.0 module zip,
minus `examples/` and upstream CI/lint config.

## Changes vs v2.1.0 (`stun_conn.go`)

Backported from upstream `pion/turn` master (`internal/proto/stun_conn.go`):

1. `STUNConn.ReadFrom` rejects a frame larger than the caller's buffer with
   `errTURNFrameTooLarge` (and consumes it). Before, it returned the declared
   frame length while copying only `len(p)` bytes; `Server.readLoop` then
   sliced `buf[:n]` with its 1600-byte buffer and panicked, crashing the
   node. Any client reaching TURN over TCP/TLS could trigger it. Seen in
   production: `slice bounds out of range [:21336] with capacity 1600`.
2. `consumeSingleTURNFrame` computes the frame size in `uint32`. In `uint16`
   a STUN length >= 0xFFEC or ChannelData length >= 0xFFFC wrapped to 0, so
   `ReadFrom` returned an empty frame forever and the read loop spun.

Regression tests: `stun_conn_test.go`.

## Removing this copy

Drop the `replace` once the node moves to a pion/turn release that carries
both fixes (v5+/v6 have a different API, so that is a separate upgrade).
