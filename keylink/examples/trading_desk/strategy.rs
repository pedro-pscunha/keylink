//! One trading robot: the state, and the one call it makes out of a turn.

use std::sync::Arc;

use keylink::compose::Lagged;
use keylink::Caller;
use tokio::sync::Semaphore;

use crate::venue::Desk;
use crate::{desk, robot, Position, Print, Quote};

/// A robot that acts on quotes and sends its orders to one desk.
pub struct Robot {
    name: String,
    /// The desk, captured when the robot is built. A handle is how one actor
    /// reaches another, and nothing is injected.
    desk: desk::Handle<Desk>,
    /// Held to stall this robot deliberately, so the market feed overflows for
    /// it and not for its sibling.
    gate: Arc<Semaphore>,
    seen: u64,
    skipped: u64,
    prints: u64,
    filled: u64,
    halted: bool,
}

impl Robot {
    /// A robot that has traded nothing yet.
    pub fn new(name: &str, desk: desk::Handle<Desk>, gate: Arc<Semaphore>) -> Self {
        Self {
            name: name.to_string(),
            desk,
            gate,
            seen: 0,
            skipped: 0,
            prints: 0,
            filled: 0,
            halted: false,
        }
    }
}

impl robot::Actor for Robot {
    async fn on_quotes(&mut self, quote: Lagged<Quote>) {
        // Both arms, always. Ignoring `Skipped` is what makes stale state
        // indistinguishable from complete state.
        let quote: Quote = match quote {
            Lagged::Item(quote) => quote,
            Lagged::Skipped(missed) => {
                self.skipped += missed.get();
                return;
            }
        };
        let _permit = self.gate.acquire().await;
        self.seen += 1;
        if self.halted {
            return;
        }
        // The one call out of a turn. One edge, one direction. The desk answers
        // in its reply rather than calling back, because a call back would
        // close the ring and be refused as reentrant.
        if let Ok(fill) = self
            .desk
            .execute(self.name.clone(), quote.cents / 100)
            .await
        {
            self.filled += fill.qty;
        }
    }

    async fn on_prints(&mut self, _print: Print) {
        self.prints += 1;
    }

    async fn position(&mut self, _cx: Caller<'_>) -> Position {
        Position {
            seen: self.seen,
            skipped: self.skipped,
            prints: self.prints,
            filled: self.filled,
            halted: self.halted,
        }
    }

    async fn halt(&mut self, _cx: Caller<'_>) {
        self.halted = true;
    }
}
