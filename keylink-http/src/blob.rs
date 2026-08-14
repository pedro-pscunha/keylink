//! Byte-resource tickets: which principal may fetch which content.

use std::collections::{HashMap, VecDeque};
use std::sync::{Mutex, MutexGuard, PoisonError};

use crate::Principal;

/// A content address as it appears in a path.
///
/// **The gateway never turns one back into a [`keylink::BlobId`].** It is a
/// lookup key and nothing else: the handle it finds came from an invocation
/// this principal actually made, so a syntactically perfect address nobody was
/// granted finds nothing at all. Keeping it a string is what makes "never
/// construct a handle from a path ID" true by construction rather than by
/// discipline — there is no constructor here to reach for.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct Address(String);

impl Address {
    /// Exactly the 64 lowercase hex characters [`keylink::BlobId`] renders.
    ///
    /// Uppercase and short forms are refused rather than canonicalised, so one
    /// resource has one path and a caller cannot mint variants of it.
    pub(crate) fn parse(raw: &str) -> Option<Self> {
        let canonical = raw.len() == 64
            && raw
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte));
        canonical.then(|| Self(raw.to_owned()))
    }

    /// The address of content a handler actually granted.
    pub(crate) fn of(id: keylink::BlobId) -> Self {
        Self(id.to_string())
    }

    pub(crate) fn as_str(&self) -> &str {
        &self.0
    }
}

/// The entity tag of a byte resource.
///
/// Strong, and free: the identity IS the hash of the content, so two responses
/// carrying one tag are byte-identical by construction rather than by a
/// convention somebody has to keep.
pub(crate) fn etag(address: &Address) -> String {
    format!("\"{}\"", address.as_str())
}

/// What a caller asked for, once the ask is known to be well formed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Asked {
    /// The whole representation. No `Range` header at all.
    Whole,
    /// One satisfiable half-open byte range.
    Part(std::ops::Range<u64>),
}

/// Why a `Range` header cannot be served, and they are different statuses.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RangeError {
    /// Unparseable, wrong unit, more than one range, or arithmetic that
    /// overflows. `400`, because the request is wrong whatever the content is.
    Malformed,
    /// Well formed, and outside this representation. `416`, because the same
    /// request against different content could succeed.
    Unsatisfiable,
}

/// One decimal numeral from a range spec.
///
/// RFC 9110 §14.1.2 requires a recipient to handle an arbitrarily large
/// numeral without an integer-conversion failure, so an overflow is a very
/// large number rather than a syntax error. `bytes=18446744073709551616-`
/// asks for a first byte past every representation, which is unsatisfiable —
/// not malformed.
fn numeral(text: &str) -> Option<Bound> {
    if text.is_empty() || !text.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    Some(text.parse::<u64>().map_or(Bound::Huge, Bound::Exact))
}

enum Bound {
    Exact(u64),
    /// Larger than `u64` can hold, and therefore larger than any content.
    Huge,
}

/// Parse at most one `bytes=` range against a known length.
///
/// **Ignoring a range is always allowed; refusing one is not.** RFC 9110 §14.2
/// lets an origin ignore a `Range` it does not serve, and requires it to
/// ignore a range unit it does not understand. So everything this gateway
/// declines to serve — an unknown unit, a set of more than one range, a suffix
/// against empty content — becomes the full representation rather than a
/// refusal. `400` is kept for grammar that is genuinely wrong, where no
/// representation could make the ask meaningful.
///
/// **One range or none.** A multipart/byteranges response is a second body
/// format, a second content type and a boundary generator, for a case no
/// caller of this gateway has. Repeated `Range` field lines are one
/// comma-separated list under §5.3, so two headers are a range SET and are
/// ignored for the same reason rather than treated as a contradiction.
pub(crate) fn range(headers: &http::HeaderMap, len: u64) -> Result<Asked, RangeError> {
    let mut values = headers.get_all(http::header::RANGE).iter();
    let Some(value) = values.next() else {
        return Ok(Asked::Whole);
    };
    if values.next().is_some() {
        return Ok(Asked::Whole);
    }
    let Ok(value) = value.to_str() else {
        return Err(RangeError::Malformed);
    };
    let Some((unit, spec)) = value.split_once('=') else {
        return Err(RangeError::Malformed);
    };
    // Range units are case-insensitive (§14.1), and one this gateway does not
    // implement is ignored (§14.2).
    if !unit.trim().eq_ignore_ascii_case("bytes") {
        return Ok(Asked::Whole);
    }
    if spec.contains(',') {
        return Ok(Asked::Whole);
    }
    // Trimmed at the ends only. Whitespace is allowed around list commas, not
    // inside the byte-range grammar, so `bytes=0 - 1` stays malformed.
    let spec = spec.trim();
    let Some((first, last)) = spec.split_once('-') else {
        return Err(RangeError::Malformed);
    };

    // A suffix length, not a negative offset. It is the one form whose meaning
    // depends on the length, which is why it cannot be read without one.
    if first.is_empty() {
        return match numeral(last).ok_or(RangeError::Malformed)? {
            // Zero bytes from the end names nothing at all (§14.1.2).
            Bound::Exact(0) => Err(RangeError::Unsatisfiable),
            // Nothing to take a suffix of. A range of empty content cannot be
            // expressed as a `Content-Range`, so the range is ignored rather
            // than refused.
            _ if len == 0 => Ok(Asked::Whole),
            Bound::Huge => Ok(Asked::Part(0..len)),
            Bound::Exact(suffix) => Ok(Asked::Part(len.saturating_sub(suffix)..len)),
        };
    }

    let start = match numeral(first).ok_or(RangeError::Malformed)? {
        // Past every representation there is.
        Bound::Huge => return Err(RangeError::Unsatisfiable),
        Bound::Exact(start) => start,
    };
    if start >= len {
        // Includes every first-position range against empty content.
        return Err(RangeError::Unsatisfiable);
    }
    if last.is_empty() {
        return Ok(Asked::Part(start..len));
    }
    let end = match numeral(last).ok_or(RangeError::Malformed)? {
        Bound::Huge => len,
        Bound::Exact(last) if last < start => return Err(RangeError::Malformed),
        // Inclusive on the wire, half-open here, and clamped: a last byte past
        // the end is satisfiable as long as the first one is not.
        Bound::Exact(last) => last.saturating_add(1).min(len),
    };
    Ok(Asked::Part(start..end))
}

/// Whether a combined `If-None-Match` list matches this representation.
///
/// Weak comparison, which for a content-addressed tag is the same answer as
/// strong: `W/"x"` and `"x"` name identical bytes here.
///
/// **The whole field is parsed before anything is decided.** Returning on the
/// first match would let `"<current>", unquoted` answer `304` on a field the
/// caller got wrong, and would let `*, "other"` pass when `*` is a complete
/// field on its own. Grammar the caller got wrong is `400` rather than a
/// silent miss: treating an unparseable condition as "no match" turns a typo
/// into the full download it asked not to receive.
pub(crate) fn none_match(headers: &http::HeaderMap, tag: &str) -> Result<bool, ()> {
    let mut wildcard = false;
    let mut matched = false;
    let mut entries = 0usize;
    for value in headers.get_all(http::header::IF_NONE_MATCH) {
        let value = value.to_str().map_err(|_| ())?;
        for candidate in value.split(',') {
            let candidate = candidate.trim();
            // RFC 9110 §5.6.1.2: "a recipient MUST parse and ignore a
            // reasonable number of empty list elements". Merging repeated
            // fields into one comma list is exactly what a proxy in front of
            // this gateway does, and refusing the trailing comma it leaves
            // would cost a legal conditional request its `304`.
            if candidate.is_empty() {
                continue;
            }
            entries += 1;
            if candidate == "*" {
                wildcard = true;
                continue;
            }
            let unweak = candidate.strip_prefix("W/").unwrap_or(candidate);
            let inner = unweak
                .strip_prefix('"')
                .and_then(|rest| rest.strip_suffix('"'))
                .ok_or(())?;
            // `etagc` admits no bare DQUOTE, so `"a"b"` is not one tag.
            if inner.contains('"') {
                return Err(());
            }
            if unweak == tag {
                matched = true;
            }
        }
    }
    if wildcard && entries > 1 {
        return Err(());
    }
    Ok(wildcard || matched)
}

/// Whether an `If-Range` permits the range that was already parsed.
///
/// Only ONE exact strong entity tag does. A weak tag, a date, a list, a second
/// header and anything unparseable all produce the full representation, which
/// is what RFC 9110 requires and is also the safe direction: sending more than
/// the caller asked for costs bandwidth, sending a range of the wrong version
/// corrupts the file it is being reassembled into.
pub(crate) fn if_range_allows(headers: &http::HeaderMap, tag: &str) -> bool {
    let mut values = headers.get_all(http::header::IF_RANGE).iter();
    let Some(value) = values.next() else {
        return true;
    };
    if values.next().is_some() {
        return false;
    }
    value.to_str().is_ok_and(|value| value.trim() == tag)
}

/// What a byte resource is keyed on.
///
/// The content address is one part of four and never the key on its own. An
/// HTTP path carries the address and nothing else, so a table keyed on it
/// would hand the bytes to any caller who learned one — under any method, for
/// any principal, and for any actor. Each of the other three answers a
/// question the path cannot.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct Ticket {
    pub(crate) principal: Principal,
    pub(crate) method: &'static str,
    pub(crate) actor: Option<keylink::ActorId>,
    pub(crate) id: Address,
}

/// Which insertion a held value came from.
///
/// A refusal arrives after an `await`, and between the read and the removal
/// another request can have refreshed the same ticket. Removing by key alone
/// would delete that fresh grant on the strength of a stale answer, so every
/// removal names the generation it is removing.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Generation(u64);

/// A finite, FIFO table of granted byte resources.
///
/// **The bound is global, not per principal.** One caller that issues
/// `max_blob_tickets` lookups evicts every other caller's outstanding grants,
/// and each victim's next request answers `404` — which is by design
/// indistinguishable from "you never had this", so a resumable download cannot
/// tell that a fresh lookup would fix it. Sizing the limit is therefore a
/// tenancy decision and not only a memory one.
///
/// Generic over what it holds, and that is not speculation: nothing in here
/// reads the value, and a test that had to stand up a QUIC endpoint to check
/// an eviction order would be testing the wrong module. The one runtime
/// instantiation holds a [`keylink::BlobHandle`].
#[derive(Debug)]
pub(crate) struct Tickets<T> {
    inner: Mutex<Book<T>>,
    max: usize,
}

#[derive(Debug)]
struct Book<T> {
    live: HashMap<Ticket, (Generation, T)>,
    /// Oldest first, and always exactly the keys of `live`. A replacement
    /// removes the old place before pushing the new one, so the queue is
    /// bounded by tickets rather than by writes — a caller refreshing one
    /// resource in a loop must not be able to grow it.
    order: VecDeque<Ticket>,
    next: u64,
}

impl<T: Clone> Tickets<T> {
    pub(crate) fn new(max: usize) -> Self {
        Self {
            inner: Mutex::new(Book {
                live: HashMap::new(),
                order: VecDeque::new(),
                next: 0,
            }),
            max,
        }
    }

    /// Poison recovery rather than propagation, the same call keylink's own
    /// ledgers make: one panicking handler must not wedge every later caller.
    fn lock(&self) -> MutexGuard<'_, Book<T>> {
        self.inner.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Record a grant, replacing any earlier one for the same resource.
    ///
    /// Returns nothing. The generation exists so a REFUSAL can name the entry
    /// it saw, and the only caller that needs one reads it from
    /// [`get`](Self::get); handing it back here gave every call site a value
    /// to discard.
    pub(crate) fn insert(&self, ticket: Ticket, value: T) {
        let mut book = self.lock();
        let generation = Generation(book.next);
        book.next += 1;
        if book
            .live
            .insert(ticket.clone(), (generation, value))
            .is_some()
        {
            if let Some(at) = book.order.iter().position(|held| held == &ticket) {
                book.order.remove(at);
            }
        }
        book.order.push_back(ticket);
        while book.order.len() > self.max {
            let Some(oldest) = book.order.pop_front() else {
                break;
            };
            book.live.remove(&oldest);
        }
    }

    /// The grant for this exact resource, cloned so no lock is held across an
    /// await.
    pub(crate) fn get(&self, ticket: &Ticket) -> Option<(Generation, T)> {
        self.lock().live.get(ticket).cloned()
    }

    /// Remove one grant, and only if it is still the generation named.
    pub(crate) fn remove(&self, ticket: &Ticket, generation: Generation) -> bool {
        let mut book = self.lock();
        if book
            .live
            .get(ticket)
            .is_none_or(|(live, _)| *live != generation)
        {
            return false;
        }
        book.live.remove(ticket);
        if let Some(at) = book.order.iter().position(|held| held == ticket) {
            book.order.remove(at);
        }
        true
    }

    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.lock().live.len()
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used)]

    use super::{Ticket, Tickets};
    use crate::Principal;

    fn principal() -> Principal {
        Principal::new(b"tenant-a").expect("valid principal")
    }

    fn address(seed: u8) -> super::Address {
        super::Address::parse(&format!("{seed:02x}").repeat(32)).expect("64 hex characters")
    }

    fn ticket(seed: u8) -> Ticket {
        Ticket {
            principal: principal(),
            method: "Fetch",
            actor: None,
            id: address(seed),
        }
    }

    fn headers(name: http::HeaderName, values: &[&str]) -> http::HeaderMap {
        let mut headers = http::HeaderMap::new();
        for value in values {
            headers.append(
                name.clone(),
                http::HeaderValue::from_str(value).expect("valid header value"),
            );
        }
        headers
    }

    fn asked(value: &str, len: u64) -> Result<super::Asked, super::RangeError> {
        super::range(&headers(http::header::RANGE, &[value]), len)
    }

    /// One range or none, and the two refusals are different statuses.
    ///
    /// `400` says the request is wrong whatever the content is; `416` says the
    /// same request against different content could have worked. Collapsing
    /// them tells a client to stop retrying when it should resume, or to keep
    /// retrying a request that will never work.
    ///
    /// Watched red with `scripts/break-once.sh` (row `HK13`) by replacing
    /// `if start >= len {`
    /// with `if false {`.
    #[test]
    fn blob_range_parser_classifies_single_malformed_and_unsatisfiable_ranges() {
        use super::{Asked, RangeError};

        assert_eq!(super::range(&http::HeaderMap::new(), 100), Ok(Asked::Whole));
        assert_eq!(asked("bytes=0-499", 1000), Ok(Asked::Part(0..500)));
        assert_eq!(asked("bytes=500-", 1000), Ok(Asked::Part(500..1000)));
        assert_eq!(
            asked("bytes=0-0", 1000),
            Ok(Asked::Part(0..1)),
            "inclusive on the wire, half-open here"
        );
        assert_eq!(
            asked("bytes=900-5000", 1000),
            Ok(Asked::Part(900..1000)),
            "a last byte past the end clamps rather than failing"
        );
        assert_eq!(
            asked("bytes=-200", 1000),
            Ok(Asked::Part(800..1000)),
            "a suffix length, not a negative offset"
        );
        assert_eq!(
            asked("bytes=-5000", 1000),
            Ok(Asked::Part(0..1000)),
            "and a suffix longer than the content is the whole of it"
        );

        assert_eq!(
            asked("Bytes=0-9", 1000),
            Ok(Asked::Part(0..10)),
            "range units are case-insensitive (RFC 9110 §14.1)"
        );

        for wrong in [
            "0-499",
            "bytes=",
            "bytes=abc-1",
            "bytes=1-abc",
            "bytes=500-100",
            "bytes=0 - 1",
            "bytes=0- 1",
        ] {
            assert_eq!(asked(wrong, 1000), Err(RangeError::Malformed), "{wrong}");
        }

        // Ignored, not refused. RFC 9110 §14.2 lets an origin ignore a `Range`
        // it does not serve and REQUIRES it to ignore a unit it does not
        // understand, so each of these is the full representation.
        for ignored in ["items=0-499", "bytes=0-1,5-6"] {
            assert_eq!(asked(ignored, 1000), Ok(Asked::Whole), "{ignored}");
        }
        assert_eq!(
            super::range(
                &headers(http::header::RANGE, &["bytes=0-1", "bytes=2-3"]),
                1000
            ),
            Ok(Asked::Whole),
            "repeated field lines are one list (§5.3), so this is a range SET"
        );

        // §14.1.2: a recipient must handle an arbitrarily large numeral
        // without an integer-conversion failure. Past the end is
        // unsatisfiable; a huge LAST byte clamps like any other.
        assert_eq!(
            asked("bytes=18446744073709551616-", 1000),
            Err(RangeError::Unsatisfiable)
        );
        assert_eq!(
            asked("bytes=-18446744073709551616", 1000),
            Ok(Asked::Part(0..1000))
        );
        assert_eq!(
            asked("bytes=0-18446744073709551616", 1000),
            Ok(Asked::Part(0..1000))
        );

        for outside in ["bytes=1000-", "bytes=1000-2000", "bytes=-0"] {
            assert_eq!(
                asked(outside, 1000),
                Err(RangeError::Unsatisfiable),
                "{outside}"
            );
        }
        for outside in ["bytes=0-0", "bytes=0-"] {
            assert_eq!(
                asked(outside, 0),
                Err(RangeError::Unsatisfiable),
                "{outside}: a first position past empty content"
            );
        }
        assert_eq!(
            asked("bytes=-1", 0),
            Ok(Asked::Whole),
            "a suffix of empty content cannot be expressed as a Content-Range,              so the range is ignored rather than called unsatisfiable"
        );
    }

    /// A condition list is parsed whole, and grammar the caller got wrong is a
    /// refusal rather than a miss.
    ///
    /// Treating an unparseable `If-None-Match` as "no match" turns a typo into
    /// the full download the caller asked not to receive.
    ///
    /// Three guards, watched separately, because the first version of this
    /// parser had one and codex found two more holes behind it.
    ///
    /// Grammar, row `HK14`: replacing
    /// `.and_then(|rest| rest.strip_suffix('"'))`
    /// with `.map(|rest| rest.trim_end_matches('"'))` accepts an unquoted tag.
    ///
    /// Wildcard, row `HK21`: replacing
    /// `if wildcard && entries > 1 {`
    /// with `if false {` lets `*, "other"` answer `304` on a field that is
    /// invalid — `*` is a complete field on its own.
    ///
    /// Embedded quotes, row `HK22`: replacing
    /// `if inner.contains('"') {`
    /// with `if false {` accepts `"a"b"` as one entity tag.
    #[test]
    fn blob_if_none_match_parses_lists_and_precedes_range() {
        let tag = super::etag(&address(7));
        let check = |values: &[&str]| {
            super::none_match(&headers(http::header::IF_NONE_MATCH, values), &tag)
        };

        assert_eq!(check(&[]), Ok(false), "no condition is no match");
        assert_eq!(check(&["*"]), Ok(true));
        assert_eq!(check(&[&tag]), Ok(true));
        assert_eq!(
            check(&[&format!("W/{tag}")]),
            Ok(true),
            "weak comparison, and for a content address it is the same answer"
        );
        assert_eq!(
            check(&[&format!("\"other\", {tag} , \"third\"")]),
            Ok(true),
            "one member of a list is a match, whitespace included"
        );
        assert_eq!(
            check(&["\"other\"", &format!("W/{tag}")]),
            Ok(true),
            "and the fields combine across repeated headers"
        );
        assert_eq!(check(&["\"other\""]), Ok(false));

        for wrong in [
            "unquoted",
            "\"open",
            "W/unquoted",
            "\"a\", unquoted",
            "\"a\"b\"",
        ] {
            assert_eq!(check(&[wrong]), Err(()), "{wrong}");
        }
        assert_eq!(
            check(&[&format!("{tag}, unquoted")]),
            Err(()),
            "the whole field is parsed before anything is decided, so a match \
             in front of bad grammar does not answer 304"
        );
        assert_eq!(
            check(&["*, \"other\""]),
            Err(()),
            "`*` is a complete field on its own"
        );
    }

    /// Only one exact strong entity tag lets a range through.
    ///
    /// Everything else produces the full representation. Sending more than was
    /// asked for costs bandwidth; sending a range of the wrong version corrupts
    /// the file it is being reassembled into.
    ///
    /// Watched red with `scripts/break-once.sh` (row `HK15`) by replacing
    /// `value.to_str().is_ok_and(|value| value.trim() == tag)`
    /// with `true`.
    #[test]
    fn blob_if_range_requires_one_matching_strong_etag() {
        let tag = super::etag(&address(7));
        let allows = |values: &[&str]| {
            super::if_range_allows(&headers(http::header::IF_RANGE, values), &tag)
        };

        assert!(allows(&[]), "no condition permits the range");
        assert!(allows(&[&tag]));

        assert!(!allows(&[&format!("W/{tag}")]), "a weak tag does not");
        assert!(!allows(&["\"other\""]));
        assert!(
            !allows(&["Wed, 21 Oct 2015 07:28:00 GMT"]),
            "a date does not, because this gateway has no modification time"
        );
        assert!(!allows(&[&format!("{tag}, \"other\"")]), "a list does not");
        assert!(
            !allows(&[&tag, &tag]),
            "and two headers do not, even equal ones"
        );
    }

    /// A second grant for the same resource replaces the first and becomes the
    /// newest, rather than leaving two entries or keeping its old place.
    ///
    /// Two entries would mean one caller can fill a finite table by asking
    /// twice. Keeping the old place would mean a resource somebody is actively
    /// refreshing falls off the end while an idle one survives.
    ///
    /// Watched red with `scripts/break-once.sh` (row `HK11`) by replacing
    /// `if let Some(at) = book.order.iter().position(|held| held == &ticket) {`
    /// with `if let Some(at) = None::<usize> {`, which leaves two queue entries
    /// for one live ticket and reports `left: 1, right: 2` — the bound then
    /// evicts the entry that is still live.
    #[test]
    fn blob_ticket_fifo_replacement_moves_entry_to_newest() {
        let tickets = Tickets::new(2);
        tickets.insert(ticket(1), 1u8);
        tickets.insert(ticket(2), 2u8);
        tickets.insert(ticket(1), 11u8);

        assert_eq!(tickets.len(), 2, "the replacement left one live entry");

        tickets.insert(ticket(3), 3u8);
        assert!(
            tickets.get(&ticket(1)).is_some(),
            "the refreshed entry moved to newest and survived the overflow"
        );
        assert!(
            tickets.get(&ticket(2)).is_none(),
            "and the entry that had become oldest is the one that left"
        );
        assert!(tickets.get(&ticket(3)).is_some());
    }

    /// The table is bounded, and it evicts by age rather than by luck.
    ///
    /// Watched red with `scripts/break-once.sh` (row `HK12`) by replacing
    /// `while book.order.len() > self.max {`
    /// with `while false {`, which reports `left: 3, right: 2`.
    #[test]
    fn blob_ticket_overflow_evicts_oldest_live_generation() {
        let tickets = Tickets::new(2);
        tickets.insert(ticket(1), 1u8);
        tickets.insert(ticket(2), 2u8);
        tickets.insert(ticket(3), 3u8);

        assert_eq!(tickets.len(), 2, "the bound holds");
        assert!(tickets.get(&ticket(1)).is_none(), "oldest first");
        assert!(tickets.get(&ticket(2)).is_some());
        assert!(tickets.get(&ticket(3)).is_some());
    }
}
