use std::{
    collections::HashMap,
    str::FromStr,
    sync::{Arc, Mutex, atomic::AtomicU64},
};

use axum::{
    Json, Router,
    extract::{Path, Request, State},
    http::StatusCode,
    response::Response,
    routing::{get, post},
};
use futures::{FutureExt, future::BoxFuture};
use racedet::{
    ReplayTrace, SchedulerHandle, new_scheduler,
    task::{StartBarrier, execution_point, new_start_barrier, task, with_start_barrier},
    with_scheduler_blocking,
};
use serde::{Deserialize, Serialize};
use tokio::select;
use tokio_util::sync::CancellationToken;
use tower::{Layer, Service};

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    // use the following script to execute the example:
    // curl http://localhost:3000/reset -X POST && curl http://localhost:3000/increment -H 'x-concchecker: 2-foo-r1' & curl http://localhost:3000/increment -H 'x-concchecker: 2-foo-r2' & wait; curl http://localhost:3000/retrieve_concchecker_trace/foo -X POST
    // add -H 'x-conchecker-replay: <...>'

    let scheduler_registry = SchedulerRegistry::new();
    // build our application with a route
    let app = Router::new()
        // `GET /` goes to `root`
        .route("/", get(root))
        .route("/reset", post(reset_counter))
        .route(
            "/retrieve_concchecker_trace/{trace_id}",
            post(retrieve_concchecker_trace),
        )
        .route("/increment", get(increment))
        // `POST /users` goes to `create_user`
        .route("/users", post(create_user))
        .layer(ConcCheckerLayer::new(scheduler_registry.clone()))
        .with_state(Arc::new(AppState {
            var: AtomicU64::new(0),
            scheduler_registry,
        }));

    // run our app with hyper, listening globally on port 3000
    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await.unwrap();
    axum::serve(listener, app).await.unwrap();
}

struct AppState {
    var: AtomicU64,
    scheduler_registry: Arc<SchedulerRegistry>,
}

// basic handler that responds with a static string
async fn reset_counter(State(state): State<Arc<AppState>>) {
    state.var.store(0, std::sync::atomic::Ordering::Relaxed);
}

// basic handler that responds with a static string
async fn retrieve_concchecker_trace(
    State(state): State<Arc<AppState>>,
    Path(scheduler_id): Path<String>,
) -> Result<String, (StatusCode, String)> {
    if let Some((scheduler, cancellation_token)) =
        state.scheduler_registry.take_scheduler(&scheduler_id)
    {
        cancellation_token.cancel();
        Ok(format!(
            "{}\n{}",
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

// basic handler that responds with a static string
// #[axum::debug_handler]
async fn root() -> &'static str {
    "Hello, World!"
}

// basic handler that responds with a static string
async fn increment(State(state): State<Arc<AppState>>) -> String {
    execution_point("load").await;
    let x = state.var.load(std::sync::atomic::Ordering::Relaxed);
    execution_point("store").await;
    state.var.store(x + 1, std::sync::atomic::Ordering::Relaxed);
    format!("new value: {}\n", x + 1)
}

async fn create_user(
    // this argument tells axum to parse the request body
    // as JSON into a `CreateUser` type
    Json(payload): Json<CreateUser>,
) -> (StatusCode, Json<User>) {
    // insert your application logic here
    let user = User {
        id: 1337,
        username: payload.username,
    };

    // this will be converted into a JSON response
    // with a status code of `201 Created`
    (StatusCode::CREATED, Json(user))
}

// the input to our `create_user` handler
#[derive(Deserialize)]
struct CreateUser {
    username: String,
}

// the output to our `create_user` handler
#[derive(Serialize)]
struct User {
    id: u64,
    username: String,
}

#[derive(Clone)]
struct ConcCheckerLayer {
    schedulers: Arc<SchedulerRegistry>,
}

impl ConcCheckerLayer {
    fn new(scheduler_registry: Arc<SchedulerRegistry>) -> Self {
        ConcCheckerLayer {
            schedulers: scheduler_registry,
        }
    }
}

struct SchedulerRegistry {
    schedulers: Mutex<HashMap<String, (SchedulerHandle, StartBarrier, CancellationToken)>>,
}

impl SchedulerRegistry {
    fn new() -> Arc<Self> {
        Arc::new(SchedulerRegistry {
            schedulers: Mutex::new(HashMap::new()),
        })
    }
}

impl<S> Layer<S> for ConcCheckerLayer {
    type Service = ConcCheckerService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        ConcCheckerService {
            schedulers: self.schedulers.clone(),
            inner,
        }
    }
}

#[derive(Clone)]
struct ConcCheckerService<S> {
    schedulers: Arc<SchedulerRegistry>,
    inner: S,
}

impl<S> Service<Request> for ConcCheckerService<S>
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
            .get("x-concchecker")
            .and_then(|h| h.to_str().ok())
            .and_then(|h| RequestConccheckerHeader::from_str(h).ok())
        {
            let replay = req
                .headers()
                .get("x-concchecker-replay")
                .and_then(|h| h.to_str().ok());
            let (scheduler, barrier, _) = self.schedulers.get_or_insert(
                header.scheduler_id,
                header.concurrent_task_count,
                replay,
            );
            racedet::with_scheduler(
                scheduler,
                task(
                    header.task_id,
                    with_start_barrier(barrier, self.inner.call(req)),
                ),
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

struct RequestConccheckerHeader {
    concurrent_task_count: usize,
    scheduler_id: String,
    task_id: String,
}

impl FromStr for RequestConccheckerHeader {
    type Err = RequestConccheckerHeaderParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let (n, s) = s
            .split_once('-')
            .ok_or(RequestConccheckerHeaderParseError)?;
        let n: usize = n.parse().map_err(|_| RequestConccheckerHeaderParseError)?;
        let (scheduler_id, task_id) = s
            .split_once('-')
            .ok_or(RequestConccheckerHeaderParseError)?;
        if scheduler_id.is_empty() || !scheduler_id.chars().all(char::is_alphanumeric) {
            return Err(RequestConccheckerHeaderParseError);
        }
        if task_id.is_empty() || !task_id.chars().all(char::is_alphanumeric) {
            return Err(RequestConccheckerHeaderParseError);
        }
        Ok(Self {
            concurrent_task_count: n,
            scheduler_id: scheduler_id.to_string(),
            task_id: task_id.to_string(),
        })
    }
}

#[derive(Debug, thiserror::Error)]
#[error("error parsing concchecker header")]
struct RequestConccheckerHeaderParseError;

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
                let barrier = with_scheduler_blocking(&scheduler, || new_start_barrier(task_count));
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
                            "conchecker scheduler {id} complete. trace:\n{}\nreplay:\n{}",
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
