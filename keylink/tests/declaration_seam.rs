//! The one seam a documentation generator reads a `service!` block through.
//!
//! This file is a SEPARATE CRATE that depends on `keylink`, and it writes its
//! own `service!` blocks — which is the whole visibility story. The callback is
//! `pub(crate)` inside the module the macro generates, so it is reachable from
//! the crate that wrote the declaration and from no other. The refusal for a
//! downstream crate is proven where it belongs, in `fixtures/imported-ledger`.

#![allow(clippy::expect_used)]

keylink::service! {
    service Catalogue;
    rpc Count(value: u64) -> u64;
    recv Fetch(sku: String) -> u32;
    send Upload(name: String) -> u64;
    tell Ignored(value: u64) -> ();
}

macro_rules! shout {
    (
        service = $svc:expr,
        singleton = [ $( { cfg = [ $( #[$cfg:meta] )* ], kind = $kind:ident,
                           method = $method:expr, args = [ $( $arg:ident : $ty:ty ),* ],
                           ret = $ret:ty, } )* ],
        actor = [ $( { cfg = [ $( #[$acfg:meta] )* ], kind = $akind:ident,
                       method = $amethod:expr, args = [ $( $aarg:ident : $aty:ty ),* ],
                       ret = $aret:ty, } )* ],
    ) => {
        fn seam_report() -> Vec<String> {
            let mut out = vec![format!("service={}", $svc)];
            $( out.push(format!(
                "singleton {} {} -> {}",
                ::core::stringify!($kind), $method, ::core::stringify!($ret)
            )); )*
            $( out.push(format!(
                "actor {} {} -> {}",
                ::core::stringify!($akind), $amethod, ::core::stringify!($aret)
            )); )*
            out
        }
    };
}

catalogue::__keylink_declarations!(shout);

#[test]
fn the_declaration_seam_reports_every_external_method_and_no_tell() {
    let report = seam_report();
    assert_eq!(report[0], "service=Catalogue");
    assert!(report.contains(&"singleton rpc Count -> u64".to_owned()));
    assert!(report.contains(&"singleton recv Fetch -> u32".to_owned()));
    assert!(report.contains(&"singleton send Upload -> u64".to_owned()));
    assert!(report.contains(&"actor rpc Count -> u64".to_owned()));
    assert!(report.contains(&"actor recv Fetch -> u32".to_owned()));
    assert!(!report.iter().any(|line| line.contains("Ignored")));
    assert!(!report.iter().any(|line| line.starts_with("actor send")));
}

// Two services in one crate, each with its own module-scoped callback. The
// generated macro has the same NAME in both modules; what keeps them apart is
// that each is an item of its own module and each module re-exports its own.
keylink::service! {
    service Ledger;
    rpc Whoami() -> String;
    blob Take() -> u64;
}

macro_rules! only_names {
    (
        $label:tt,
        service = $svc:expr,
        singleton = [ $( { cfg = [ $( #[$cfg:meta] )* ], kind = $kind:ident,
                           method = $method:expr, args = [ $( $arg:ident : $ty:ty ),* ],
                           ret = $ret:ty, } )* ],
        actor = [ $( { cfg = [ $( #[$acfg:meta] )* ], kind = $akind:ident,
                       method = $amethod:expr, args = [ $( $aarg:ident : $aty:ty ),* ],
                       ret = $aret:ty, } )* ],
    ) => {
        fn $label() -> (&'static str, Vec<&'static str>) {
            ($svc, vec![ $( $method ),* ])
        }
    };
}

catalogue::__keylink_declarations!(only_names, first);
ledger::__keylink_declarations!(only_names, second);

/// Two documented services coexist, and each callback reports its own.
///
/// The generated macro has one name, so two services in one crate is exactly
/// where a crate-root export would collide. Module scoping is what makes this
/// compile at all, and the assertion is that neither service reads the other's
/// declarations.
#[test]
fn two_services_in_one_crate_each_report_their_own_declarations() {
    assert_eq!(first(), ("Catalogue", vec!["Count", "Fetch", "Upload"]));
    assert_eq!(second(), ("Ledger", vec!["Whoami", "Take"]));
}
