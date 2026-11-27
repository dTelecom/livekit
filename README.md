# dTelecom WebRTC server

The dTelecom network uses this Go selective forwarding unit (SFU) for WebRTC calls and live streams. It is a fork of LiveKit built on Pion WebRTC, with dTelecom's node discovery, mesh signaling, and cross-node media relays. The maintained branch is `dtelecom-db`. Each running SFU must join the dTelecom network. You can run your app and token backend locally and connect them to dTelecom Cloud. [Connection guide](https://docs.dtelecom.org/rooms/connect/)

## Features

- Scalable, distributed WebRTC SFU (Selective Forwarding Unit)
- JWT authentication
- Robust networking and connectivity, UDP/TCP/TURN
- Room features
    - speaker detection
    - simulcast
    - end-to-end optimizations
    - selective subscription
    - moderation APIs
    - participant and track webhooks
    - distributed and multi-region

## Build an application

Create a dTelecom Cloud project and keep its API key and secret on your backend. The backend requests a participant token and the corresponding node URL. Pass both to the client when it joins the room. [dTelecom Cloud](https://cloud.dtelecom.org/) · [Connect to a room](https://docs.dtelecom.org/rooms/connect/)

| Developer task | dTelecom packages |
| --- | --- |
| Join a room from a browser | [JavaScript / TypeScript client](https://github.com/dTelecom/client-sdk-js) and optional [React components](https://github.com/dTelecom/components-js) |
| Join a room from iOS or Android | [React Native SDK](https://github.com/dTelecom/client-sdk-react-native) and its [Meet and Live invite example](https://github.com/dTelecom/client-sdk-react-native/tree/dtelecom-m150/example) |
| Issue tokens and administer rooms | [JavaScript / TypeScript backend SDK](https://github.com/dTelecom/server-sdk-js) or the [Go SDK](https://github.com/dTelecom/server-sdk-go) using its `cloud` package |
| Join from a Go service | [Go participant SDK](https://github.com/dTelecom/server-sdk-go) |
| Build a voice agent | [JavaScript agents framework](https://github.com/dTelecom/agents-js) |

For runnable app examples, see [Meet](https://github.com/dTelecom/dtelecom-meet) and [Live streaming with OBS/WHIP](https://github.com/dTelecom/dtelecom-live). Find package details and additional examples in the [SDK catalog](https://docs.dtelecom.org/reference/sdks/) and [example index](https://docs.dtelecom.org/reference/examples/).

## Building from source

Use Go 1.21 or newer to select the repository's pinned Go 1.23.8 toolchain. Put the Go binary installation directory (`GOBIN`, or `$(go env GOPATH)/bin` by default) on your PATH. From the repository root, install the pinned version of Mage and build for your host.

```sh
export GOTOOLCHAIN="go$(cat .go-version)"
./bootstrap.sh
mage
go version -m bin/livekit-server
```

Use `mage buildLinux` for a static Linux/amd64 binary at `bin/livekit-server-amd64` and `mage test` for the short suite. Both build targets use the checked-in generated sources. Run `mage generate` after changing generator inputs. End-to-end node tests use joined nodes. Record the compiler version, VCS revision, and modified-tree flag from `go version -m` alongside test results.

### Running the full server suite

Run `mage test` for local development. Operators can run `mage testAll` with two directly peered, registered **test nodes**. The full suite sends a chat message, runs the local tests, creates disposable rooms, exercises media and administration on one and two nodes, and finishes by draining node A.

| Environment variable | Required input |
| --- | --- |
| `DTELECOM_SMOKE_NODE_A`, `DTELECOM_SMOKE_NODE_B` | Distinct direct node origins, including scheme and port. Run Mage on node A's host so its chat fallback can reach the loopback listener. Both nodes must support chat. |
| `DTELECOM_SMOKE_TOKEN_PROVIDER` | Executable that takes one private directory path and writes room grants for a fresh room on each invocation. The included Cloud provider is `$PWD/test/fixtures/cloud-room.cjs`. |
| `DTELECOM_CHAT_TOKEN_A_FILE`, `DTELECOM_CHAT_TOKEN_B_FILE` | Files containing fresh signed chat JWTs for two distinct test users and devices from the same registered chat tenant. A must permit sending, B receiving. Both grants must use `http://127.0.0.1:<port>/chat-fixture-fallback` as their callback. Choose a free port for A. |

To use the included provider, export your test project's `DTELECOM_CLOUD_URL`, `DTELECOM_API_KEY`, and `DTELECOM_API_SECRET`. Set `DTELECOM_FIXTURE_BACKEND_DIR` to a backend directory with the Cloud-capable `@dtelecom/server-sdk-js` installed. This provider issues RTC and admin grants. Obtain the two chat grants separately from the registered chat issuer.

```sh
export DTELECOM_SMOKE_TOKEN_PROVIDER="$PWD/test/fixtures/cloud-room.cjs"
mage testAll
```

When the final test prints `DTELECOM_DRAIN_READY` with the two serving node IDs, send **SIGTERM to the test process for node A** and keep B running. The suite verifies that the participant can rejoin B and exchange media and data. To substitute a different room-token provider, have it write `a.jwt`, `b.jwt`, and `c.jwt` for three distinct regular participants, `restricted.jwt` for a fourth participant with subscription disabled, and `admin.jwt` with room administration and creation permissions. All five grants must cover the same new room. Use owner-only file permissions. Mage removes each temporary room-grant directory after its case.

### macOS node configuration

Use the following command to set the UDP receive buffer on a macOS node.

```sh
sudo sysctl -w net.inet.udp.recvspace=2500000
```

## Contributing

When porting upstream fixes, identify the source revision, preserve the applicable licenses and notices, and test the adapted change against the dTelecom network behavior. We welcome your contributions toward improving dTelecom! Please [join us](http://dtelecom.org) to discuss your ideas and/or PRs.

## License

dTelecom server is licensed under Apache License v2.0.
