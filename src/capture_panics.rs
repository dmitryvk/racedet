use std::{panic::AssertUnwindSafe, task::Poll};

use pin_project::pin_project;

#[derive(Debug, Clone)]
pub struct CapturedPanic {
    pub message: String,
    pub location: String,
    pub backtrace: String,
}

/// Captures panics during async execution of `inner` and (asynchronously) returns `Result<T, CapturedPanic>`
pub fn capture_panic<T, Fut>(inner: Fut) -> CapturePanicFut<Fut>
where
    Fut: Future<Output = T>,
{
    CapturePanicFut { inner }
}

#[pin_project]
pub struct CapturePanicFut<Fut> {
    #[pin]
    inner: Fut,
}

impl<Fut: Future> Future for CapturePanicFut<Fut> {
    type Output = Result<Fut::Output, CapturedPanic>;

    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let this = self.project();
        let res = std::panic::catch_unwind(AssertUnwindSafe(|| this.inner.poll(cx)));
        match res {
            Ok(Poll::Pending) => Poll::Pending,
            Ok(Poll::Ready(res)) => Poll::Ready(Ok(res)),
            Err(panic) => {
                let msg = if let Some(s) = panic.downcast_ref::<String>() {
                    s.clone()
                } else {
                    "unknown panic".to_string()
                };
                let info = CapturedPanic {
                    message: msg,
                    location: "".to_string(),
                    backtrace: "".to_string(),
                };
                Poll::Ready(Err(info))
            }
        }
    }
}
