use std::{
    collections::HashMap,
    str::FromStr,
    sync::{Arc, Mutex},
};

use axum::{
    extract::{Path, Request, State},
    http::StatusCode,
    response::Response,
};
use futures::{FutureExt, future::BoxFuture};
use racedet::{
    ReplayTrace,
    scheduler::{SchedulerHandle, new_scheduler, with_scheduler_blocking},
    task::{StartBarrier, Task, task},
};
use tokio::select;
use tokio_util::sync::CancellationToken;
use tower::{Layer, Service};

use super::AppState;

// basic handler that responds with a static string
pub(crate) async fn retrieve_racedet_trace(
    State(state): State<Arc<AppState>>,
    Path(scheduler_id): Path<String>,
) -> Result<String, (StatusCode, String)> {
    if let Some((scheduler, cancellation_token)) =
        state.scheduler_registry.take_scheduler(&scheduler_id)
    {
        cancellation_token.cancel();
        Ok(format!(
            "{}\n-H 'x-racedet-replay: {}'\n",
            scheduler.get_trace(),
            scheduler.get_replay()
        ))
    } else {
        Err((
            StatusCode::NOT_FOUND,
            format!("scheduler {scheduler_id} not found"),
        ))
    }
}

#[derive(Clone)]
pub(crate) struct RacedetLayer {
    schedulers: Arc<SchedulerRegistry>,
}

impl RacedetLayer {
    pub(crate) fn new(scheduler_registry: Arc<SchedulerRegistry>) -> Self {
        RacedetLayer {
            schedulers: scheduler_registry,
        }
    }
}
pub(crate) struct SchedulerRegistry {
    schedulers: Mutex<HashMap<String, (SchedulerHandle, StartBarrier, CancellationToken)>>,
}

impl SchedulerRegistry {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(SchedulerRegistry {
            schedulers: Mutex::new(HashMap::new()),
        })
    }
}

impl<S> Layer<S> for RacedetLayer {
    type Service = RacedetService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        RacedetService {
            schedulers: self.schedulers.clone(),
            inner,
        }
    }
}

#[derive(Clone)]
pub(crate) struct RacedetService<S> {
    schedulers: Arc<SchedulerRegistry>,
    inner: S,
}

impl<S> Service<Request> for RacedetService<S>
where
    S: Service<Request, Response = Response> + Send + 'static,
    S::Future: Send + 'static,
{
    type Response = S::Response;

    type Error = S::Error;

    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: Request) -> Self::Future {
        let future = if let Some(header) = req
            .headers()
            .get("x-racedet")
            .and_then(|h| h.to_str().ok())
            .and_then(|h| RequestRacedetHeader::from_str(h).ok())
        {
            let replay = req
                .headers()
                .get("x-racedet-replay")
                .and_then(|h| h.to_str().ok());
            let (scheduler, barrier, _) = self.schedulers.get_or_insert(
                header.scheduler_id,
                header.concurrent_task_count,
                replay,
            );
            task(
                Task::new(header.task_id)
                    .with_start_barrier(barrier)
                    .with_scheduler(Some(scheduler)),
                self.inner.call(req),
            )
            .boxed()
        } else {
            self.inner.call(req).boxed()
        };
        Box::pin(async move {
            let response = future.await?;
            Ok(response)
        })
    }
}

struct RequestRacedetHeader {
    concurrent_task_count: usize,
    scheduler_id: String,
    task_id: String,
}

impl FromStr for RequestRacedetHeader {
    type Err = RequestRacedetHeaderParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let (n, s) = s.split_once('-').ok_or(RequestRacedetHeaderParseError)?;
        let n: usize = n.parse().map_err(|_| RequestRacedetHeaderParseError)?;
        let (scheduler_id, task_id) = s.split_once('-').ok_or(RequestRacedetHeaderParseError)?;
        if scheduler_id.is_empty() || !scheduler_id.chars().all(char::is_alphanumeric) {
            return Err(RequestRacedetHeaderParseError);
        }
        if task_id.is_empty() || !task_id.chars().all(char::is_alphanumeric) {
            return Err(RequestRacedetHeaderParseError);
        }
        Ok(Self {
            concurrent_task_count: n,
            scheduler_id: scheduler_id.to_string(),
            task_id: task_id.to_string(),
        })
    }
}

#[derive(Debug, thiserror::Error)]
#[error("error parsing racedet header")]
struct RequestRacedetHeaderParseError;

impl SchedulerRegistry {
    fn take_scheduler(&self, id: &str) -> Option<(SchedulerHandle, CancellationToken)> {
        let mut schedulers = self.schedulers.lock().unwrap();
        let (scheduler, _, cancellation_token) = schedulers.remove(id)?;
        Some((scheduler, cancellation_token))
    }

    fn get_or_insert(
        self: &Arc<Self>,
        id: String,
        task_count: usize,
        replay: Option<&str>,
    ) -> (SchedulerHandle, StartBarrier, CancellationToken) {
        use std::collections::hash_map::Entry;
        let mut schedulers = self.schedulers.lock().unwrap();
        match schedulers.entry(id) {
            Entry::Occupied(entry) => {
                let (scheduler, barrier, cancellation_token) = entry.get();
                (
                    scheduler.clone(),
                    barrier.clone(),
                    cancellation_token.clone(),
                )
            }
            Entry::Vacant(entry) => {
                let replay = replay.map(|s| ReplayTrace::from_str(s).unwrap());
                let (scheduler, scheduler_fut) = new_scheduler(replay.as_ref());
                let barrier = with_scheduler_blocking(&scheduler, || StartBarrier::new(task_count));
                let cancellation_token = CancellationToken::new();
                let id = entry.key().clone();
                tokio::spawn({
                    let scheduler = scheduler.clone();
                    let cancellation_token = cancellation_token.clone();
                    async move {
                        select! {
                            _ = cancellation_token.cancelled() => {},
                            _ = scheduler_fut => {}
                        }

                        tracing::info!(
                            "racedet scheduler {id} complete. trace:\n{}\nreplay:\n-H \
                             'x-racedet-replay: {}'\n",
                            scheduler.get_trace(),
                            scheduler.get_replay(),
                        );
                    }
                });
                let (scheduler, barrier, cancellation_token) =
                    entry.insert((scheduler, barrier, cancellation_token));
                (
                    scheduler.clone(),
                    barrier.clone(),
                    cancellation_token.clone(),
                )
            }
        }
    }
}
