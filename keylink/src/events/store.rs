//! The one raw Claim owner for events. The snapshot is an identity frame, not
//! actor state: `klevents` magic, layout 1 (u16 LE), then the 16-byte log id.
//! Event batches use the Claim adapter's checksummed journal frames. Neither
//! format is a Versioned snapshot; the framing gate names this file explicitly.

use std::future::Future;
use std::pin::Pin;

use keylink_persist::{Claim, Snapshots, StateId};
use serde::{de::DeserializeOwned, Deserialize, Serialize};

use super::{LogId, OpenError, Seq};

type StoreFut<'a> = Pin<Box<dyn Future<Output = keylink_persist::Result<()>> + Send + 'a>>;
type Loaded = (LogId, Box<dyn Records>, Vec<(u64, Vec<u8>)>);

pub(super) trait Records: Send {
    fn append<'a>(&'a mut self, seq: u64, record: &'a [u8]) -> StoreFut<'a>;
    fn truncate_before(&mut self, seq: u64) -> StoreFut<'_>;
}

struct Store<C>(C);
impl<C: Claim> Records for Store<C> {
    fn append<'a>(&'a mut self, seq: u64, record: &'a [u8]) -> StoreFut<'a> {
        Box::pin(self.0.append(keylink_persist::Seq::new(seq), record))
    }

    fn truncate_before(&mut self, seq: u64) -> StoreFut<'_> {
        Box::pin(self.0.truncate_before(keylink_persist::Seq::new(seq)))
    }
}

const MAGIC: &[u8; 8] = b"klevents";
const LAYOUT: [u8; 2] = 1_u16.to_le_bytes();

pub(super) async fn open<S: Snapshots>(snapshots: &S, id: &StateId) -> Result<Loaded, OpenError> {
    let claim = snapshots.claim(id).await?;
    // Creation and torn-tail repair write too. Cancellation must not release the
    // exclusive claim while the adapter's detached filesystem work still runs.
    n0_future::task::spawn(async move { initialize(claim).await })
        .await
        .map_err(|error| keylink_persist::Error::Io(std::io::Error::other(error)))?
}

async fn initialize<C: Claim>(mut claim: C) -> Result<Loaded, OpenError> {
    let identity = match claim.load().await? {
        Some(frame) => {
            if frame.len() != 26 || &frame[..8] != MAGIC || frame[8..10] != LAYOUT {
                return Err(OpenError::Identity);
            }
            let mut id = [0; 16];
            id.copy_from_slice(&frame[10..]);
            LogId(id)
        }
        None => {
            // Never attach a newly minted identity to orphaned records.
            let tail = claim.recover(keylink_persist::Seq::new(0)).await?;
            if let Some(bytes) = tail.torn {
                return Err(OpenError::Torn { bytes });
            }
            if !tail.records.is_empty() {
                return Err(OpenError::Identity);
            }
            let identity = LogId::mint();
            let mut frame = Vec::with_capacity(26);
            frame.extend_from_slice(MAGIC);
            frame.extend_from_slice(&LAYOUT);
            frame.extend_from_slice(&identity.0);
            claim.store(&frame).await?;
            return Ok((identity, Box::new(Store(claim)), Vec::new()));
        }
    };
    let tail = claim.recover(keylink_persist::Seq::new(0)).await?;
    if let Some(bytes) = tail.torn {
        return Err(OpenError::Torn { bytes });
    }
    let records = tail
        .records
        .into_iter()
        .map(|(seq, bytes)| (seq.get(), bytes))
        .collect();
    Ok((identity, Box::new(Store(claim)), records))
}

pub(super) fn decode<T: DeserializeOwned>(seq: Seq, bytes: &[u8]) -> Result<T, OpenError> {
    let (value, remaining) =
        postcard::take_from_bytes(bytes).map_err(|error| OpenError::Decode { seq, error })?;
    if !remaining.is_empty() {
        return Err(OpenError::Invalid { seq });
    }
    Ok(value)
}

#[derive(Deserialize)]
pub(super) struct Batch {
    pub(super) at_ms: u64,
    pub(super) events: Vec<Vec<u8>>,
}

pub(super) fn encode_batch<'a>(
    at_ms: u64,
    events: impl Iterator<Item = &'a [u8]>,
) -> Result<Vec<u8>, postcard::Error> {
    #[derive(Serialize)]
    struct Batch<'a> {
        at_ms: u64,
        events: Vec<&'a [u8]>,
    }
    postcard::to_allocvec(&Batch {
        at_ms,
        events: events.collect(),
    })
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used, clippy::unwrap_used)]
    use super::*;
    use std::sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    };
    use tokio::sync::Notify;

    #[derive(Default)]
    struct State {
        held: AtomicBool,
        bytes: Mutex<Option<Vec<u8>>>,
        entered: Notify,
        release: Notify,
        dropped: Notify,
    }
    struct Port(Arc<State>);
    struct Held(Arc<State>);
    impl Snapshots for Port {
        type Claim = Held;
        async fn claim(&self, id: &StateId) -> keylink_persist::Result<Held> {
            if self.0.held.swap(true, Ordering::SeqCst) {
                return Err(keylink_persist::Error::Busy { id: id.clone() });
            }
            Ok(Held(self.0.clone()))
        }
    }
    impl Drop for Held {
        fn drop(&mut self) {
            self.0.held.store(false, Ordering::SeqCst);
            self.0.dropped.notify_one();
        }
    }
    impl Claim for Held {
        async fn load(&self) -> keylink_persist::Result<Option<Vec<u8>>> {
            Ok(self.0.bytes.lock().unwrap().clone())
        }
        async fn store(&mut self, bytes: &[u8]) -> keylink_persist::Result<()> {
            self.0.entered.notify_one();
            self.0.release.notified().await;
            *self.0.bytes.lock().unwrap() = Some(bytes.to_vec());
            Ok(())
        }
        async fn append(
            &mut self,
            _seq: keylink_persist::Seq,
            _record: &[u8],
        ) -> keylink_persist::Result<()> {
            unreachable!("opening an empty log must not append events")
        }
        async fn recover(
            &mut self,
            _from: keylink_persist::Seq,
        ) -> keylink_persist::Result<keylink_persist::Tail> {
            Ok(keylink_persist::Tail::default())
        }
        async fn truncate_before(
            &mut self,
            _seq: keylink_persist::Seq,
        ) -> keylink_persist::Result<()> {
            unreachable!("opening an empty log must not truncate events")
        }
    }

    #[tokio::test]
    async fn cancelling_open_does_not_release_a_claim_while_identity_creation_is_writing() {
        let state = Arc::new(State::default());
        let port = Port(state.clone());
        let id = StateId::new("events").unwrap();
        let opening = tokio::spawn(async move { open(&port, &id).await });
        state.entered.notified().await;
        opening.abort();
        assert!(matches!(opening.await, Err(error) if error.is_cancelled()));
        assert!(
            state.held.load(Ordering::SeqCst),
            "the identity writer must retain exclusivity"
        );
        state.release.notify_one();
        state.dropped.notified().await;
        assert!(!state.held.load(Ordering::SeqCst));
        assert_eq!(&state.bytes.lock().unwrap().as_ref().unwrap()[..8], MAGIC);
    }
}
