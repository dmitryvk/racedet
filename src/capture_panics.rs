use std::{any::Any, panic::AssertUnwindSafe};

use futures_util::FutureExt;

#[derive(Debug, Clone)]
pub(crate) struct CapturedPanic {
    pub(crate) message: String,
}

/// Captures panics during async execution of `inner` and (asynchronously) returns `Result<T, CapturedPanic>`
pub(crate) async fn capture_panic<T, Fut>(inner: Fut) -> Result<Fut::Output, CapturedPanic>
where
    Fut: Future<Output = T>,
{
    AssertUnwindSafe(inner)
        .catch_unwind()
        .await
        .map_err(CapturedPanic::from_panic_payload)
}

impl CapturedPanic {
    fn from_panic_payload(panic: Box<dyn Any + Send + 'static>) -> Self {
        CapturedPanic {
            message: if let Ok(s) = panic.downcast::<String>() {
                *s
            } else {
                "(non-string panic payload)".to_owned()
            },
        }
    }
}
