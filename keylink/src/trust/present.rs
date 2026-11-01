use iroh::EndpointAddr;
use serde::{Deserialize, Serialize};

use super::{Chain, ChainError, Timestamp, Trust};
use crate::{Allowlist, Context, Node, ServiceBuilder};

// The generated surface stays private; only the builder's wire types escape,
// hidden below because callers need no second way to build this service.
mod protocol {
    use super::Presented;
    crate::service! {
        service Present;
        rpc Offer(chain: Vec<u8>) -> Presented;
    }
}
pub(super) use protocol::present as wire;

/// The result of offering a chain for the presenting peer's own key.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, postcard_schema::Schema)]
pub enum Presented {
    /// The chain verified and is now held by the server.
    Held {
        /// Exclusive end of its validity.
        not_after: Timestamp,
    },
    /// The chain did not establish authority for this peer.
    Refused(ChainError),
}
impl wire::Handler for Trust {
    async fn offer(&self, cx: &Context, bytes: Vec<u8>) -> Presented {
        let result = (|| {
            let chain = Chain::from_bytes(&bytes)?;
            if chain.holder() != Some(cx.peer()) {
                return Err(ChainError::NotTheHolder);
            }
            let credential = self.verify(&chain, Timestamp::now())?;
            let not_after = credential.caveats().validity.not_after();
            self.insert(chain)?;
            Ok(not_after)
        })();
        match result {
            Ok(not_after) => Presented::Held { not_after },
            Err(error) => Presented::Refused(error),
        }
    }
}
pub(super) fn service(trust: Trust) -> ServiceBuilder<wire::Request, wire::Response> {
    wire::service(trust, Allowlist::anyone())
}

/// Offer the chain making `node`'s own key welcome at `to`, before dialing a
/// guarded service. The presentation protocol itself admits anyone.
/// Returns transport errors normally; a refused chain is `Presented::Refused`.
pub async fn present(
    node: &Node,
    to: impl Into<EndpointAddr>,
    chain: &Chain,
) -> crate::Result<Presented> {
    let client = wire::Client::connect(node, to).await?;
    client.offer(chain.to_bytes()).await
}
