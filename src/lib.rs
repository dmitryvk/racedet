use std::{panic::AssertUnwindSafe, task::Poll};

use futures::pin_mut;

use pin_project::pin_project;
use tokio::task::yield_now;

pub async fn execution_point(name: &str) {
    println!("{name}");
    yield_now().await;
}

pub async fn task<T>(name: &str, inner: impl Future<Output = T>) -> T {
    println!("task {name}");
    inner.await
}

pub async fn run_with_schedule<T, Fut>(inner: Fut) -> (Trace, RunResult<T>)
where
    Fut: Future<Output = T> + Sized,
{
    let panic_fut = PanicFut { inner };
    pin_mut!(panic_fut);
    let res = panic_fut.await;
    let trace = Trace { trace: Vec::new() };
    match res {
        Ok(res) => (trace, RunResult::Ok(res)),
        Err(panic) => (trace, RunResult::Panic(panic)),
    }
}

#[derive(Debug, Clone)]
pub enum RunResult<T> {
    Ok(T),
    Panic(PanicInfo),
}

#[derive(Debug, Clone)]
pub struct Trace {
    pub trace: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct PanicInfo {
    pub message: String,
    pub location: String,
    pub backtrace: String,
}

#[pin_project]
struct PanicFut<T> {
    #[pin]
    inner: T,
}

impl<T: Future> Future for PanicFut<T> {
    type Output = Result<T::Output, PanicInfo>;

    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let this = self.project();
        match std::panic::catch_unwind(AssertUnwindSafe(|| this.inner.poll(cx))) {
            Ok(Poll::Pending) => Poll::Pending,
            Ok(Poll::Ready(res)) => Poll::Ready(Ok(res)),
            Err(panic) => {
                let msg = if let Some(s) = panic.downcast_ref::<String>() {
                    s.clone()
                } else {
                    "unknown panic".to_string()
                };
                let info = PanicInfo {
                    message: msg,
                    location: "".to_string(),
                    backtrace: "".to_string(),
                };
                Poll::Ready(Err(info))
            }
        }
    }
}
