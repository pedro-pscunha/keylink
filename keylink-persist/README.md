# keylink-persist

State that outlives its process, for [keylink](https://github.com/pedro-pscunha/keylink) actors and anything else that needs it.

The crate is a port and one adapter. The port says what durable state needs: one state per id, loaded once, replaced atomically, written by one process at a time, plus small keyed records. The adapter in the `file` feature puts that on a local disk. Any other store (a bucket, a database) is a second adapter of the same port.

The crate does not depend on keylink. keylink depends on it through its `persist` feature.

A snapshot stored through `Versioned` carries a version frame, and it migrates one version at a time on load. A load never writes. The keyed records are opaque bytes; whoever writes them owns their format.

**Status: early.** Nothing here has run in production, and the API will move. Read keylink's README before you build on it.

## License

MIT or Apache-2.0, at your option.
