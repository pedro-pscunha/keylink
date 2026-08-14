//! The one macro a service-defining crate invokes.

/// Implement [`Documented`](crate::Documented) for one generated service.
///
/// Invoke it once, in the crate that wrote the `service!` block:
///
/// ```
/// keylink::service! { service Inventory; rpc Count(value: u64) -> u64; }
/// keylink_openapi::document_service!(inventory);
/// ```
///
/// It cannot be invoked anywhere else. The declaration callback it reads is
/// `pub(crate)` in the generated module, so a downstream crate cannot reach it
/// — which is what stops one service being documented twice, differently, by
/// two crates that both depend on it.
///
/// # A type without `JsonSchema` is refused, by name
///
/// `JsonSchema` is required only where documents are generated. A service can
/// use any type it likes until someone asks for a document, and then the
/// missing bound is a compile error naming the type — not a fallback schema
/// that says `{}` and describes nothing:
///
/// ```compile_fail
/// fn main() {}
/// #[derive(serde::Serialize, serde::Deserialize, postcard_schema::Schema, Clone, Debug, PartialEq)]
/// pub struct Weight { pub grams: u64 }
///
/// keylink::service! { service Scales; rpc Weigh(item: Weight) -> u64; }
/// keylink_openapi::document_service!(scales);
/// ```
///
/// The control is what makes that refusal a measurement. It is the SAME
/// fixture with one derive added, so the only thing the two blocks disagree
/// about is the bound — and a `compile_fail` that failed for a typo or a
/// scoping mistake would take this one down with it. (The error-code
/// annotation is not a check: rustdoc enforces it only on nightly, measured on
/// `rustc 1.97.1`.)
///
/// ```
/// fn main() {}
/// #[derive(serde::Serialize, serde::Deserialize, postcard_schema::Schema,
///          schemars::JsonSchema, Clone, Debug, PartialEq)]
/// pub struct Weight { pub grams: u64 }
///
/// keylink::service! { service Scales; rpc Weigh(item: Weight) -> u64; }
/// keylink_openapi::document_service!(scales);
/// ```
#[macro_export]
macro_rules! document_service {
    ($module:ident) => {
        $module::__keylink_declarations!($crate::__keylink_document_impl, $module);
    };
}

/// The callback [`document_service!`] hands to the generated declaration list.
#[doc(hidden)]
#[macro_export]
macro_rules! __keylink_document_impl {
    (
        $module:ident,
        service = $svc:expr,
        singleton = [ $(
            {
                cfg = [ $( #[$cfg:meta] )* ],
                kind = $kind:ident,
                method = $method:expr,
                args = [ $( $arg:ident : $ty:ty ),* ],
                ret = $ret:ty,
            }
        )* ],
        actor = [ $(
            {
                cfg = [ $( #[$acfg:meta] )* ],
                kind = $akind:ident,
                method = $amethod:expr,
                args = [ $( $aarg:ident : $aty:ty ),* ],
                ret = $aret:ty,
            }
        )* ],
    ) => {
        impl $crate::Documented for $module::External {
            const SERVICE: &'static str = $svc;

            fn operations() -> ::std::vec::Vec<$crate::Operation> {
                // Which declarations the KEYED surface also serves. Read off the
                // actor list rather than assumed from the kind, because that
                // list is what the runtime routes from.
                let keyed: &[&str] = &[ $( $( #[$acfg] )* $amethod, )* ];
                let mut out = ::std::vec::Vec::new();
                $(
                    $( #[$cfg] )*
                    {
                        // The request contract, with the SAME serde attributes
                        // the runtime decodes through. A schema that permitted
                        // unknown fields would describe a request the gateway
                        // refuses.
                        #[derive(::schemars::JsonSchema)]
                        #[serde(deny_unknown_fields)]
                        #[allow(dead_code, non_camel_case_types)]
                        struct Request { $( $arg: $ty, )* }

                        out.push($crate::Operation {
                            method: $method,
                            kind: $crate::__keylink_document_kind!($kind),
                            keyed: keyed.contains(&$method),
                            request: ::serde_json::to_value(
                                ::schemars::schema_for!(Request)
                            ).unwrap_or_default(),
                            response: ::serde_json::to_value(
                                ::schemars::schema_for!($ret)
                            ).unwrap_or_default(),
                        });
                    }
                )*
                out
            }
        }
    };
}

/// One declaration keyword to one route family.
#[doc(hidden)]
#[macro_export]
macro_rules! __keylink_document_kind {
    (rpc) => {
        $crate::__contract::RouteKind::Rpc
    };
    (send) => {
        $crate::__contract::RouteKind::Send
    };
    (recv) => {
        $crate::__contract::RouteKind::Recv
    };
    (blob) => {
        $crate::__contract::RouteKind::BlobLookup
    };
}
