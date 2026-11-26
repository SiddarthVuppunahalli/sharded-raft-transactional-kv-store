# MVP checklist

- [x] Health endpoint.
- [x] Durable replicated metadata: list, plain-text key lookup and version-checked updates.
- [x] Monotonic timestamps allocated through metadata Raft.
- [x] Plain-text GET/PUT routing with bounded retries.
- [x] Separate plain frontend and backend; public /fe/ and /be/ routes.
- [x] API calls and request bodies documented in URLs.txt.
- [ ] Verify GET/PUT with Mukesh's real range receiver using the plain-text contract in HandOff.txt.

No transaction implementation, range storage engine, automatic splits or balancing.
Checks: `py -B -m unittest discover -s tests -v` (routing uses a test receiver).
