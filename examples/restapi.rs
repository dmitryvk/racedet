use std::{
    collections::HashMap,
    str::FromStr,
    sync::{Arc, Mutex, atomic::AtomicU64},
};

use axum::{
    Json, Router,
    extract::{Path, Request, State},
    response::Response,
    routing::{get, post},
};
use conc_checker::{SchedulerHandle, TaskStartBarrierId, execution_point, new_scheduler, task};
use futures::{FutureExt, future::BoxFuture};
use hyper::StatusCode;
use serde::{Deserialize, Serialize};
use tower::{Layer, Service};

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    // use the following script to execute the example:
    // curl http://localhost:3000/reset -X POST && curl http://localhost:3000/increment -H 'x-concchecker: 2-foo-r1' & curl http://localhost:3000/increment -H 'x-concchecker: 2-foo-r2' & wait; curl http://localhost:3000/retrieve_concchecker_trace/foo -X POST

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
    if let Some(scheduler) = state.scheduler_registry.take_scheduler(&scheduler_id) {
        Ok(format!("{}", scheduler.get_trace()))
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
    schedulers: Mutex<HashMap<String, (SchedulerHandle, TaskStartBarrierId)>>,
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
            let (scheduler, barrier) = self
                .schedulers
                .get_or_insert(header.scheduler_id, header.concurrent_task_count);
            let task_id = scheduler.register_task(&header.task_id, Some(barrier));
            conc_checker::with_scheduler(scheduler, task(task_id, self.inner.call(req))).boxed()
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
    fn take_scheduler(&self, id: &str) -> Option<SchedulerHandle> {
        let mut schedulers = self.schedulers.lock().unwrap();
        let (scheduler, _) = schedulers.remove(id)?;
        Some(scheduler)
    }

    fn get_or_insert(
        self: &Arc<Self>,
        id: String,
        task_count: usize,
    ) -> (SchedulerHandle, TaskStartBarrierId) {
        use std::collections::hash_map::Entry;
        let mut schedulers = self.schedulers.lock().unwrap();
        match schedulers.entry(id) {
            Entry::Occupied(entry) => {
                let (scheduler, barrier) = entry.get();
                (scheduler.clone(), *barrier)
            }
            Entry::Vacant(entry) => {
                let (scheduler, _scheduler_fut) = new_scheduler();
                // TODO: use scheduler_fut
                let barrier = scheduler.register_task_start_barrier("http requests", task_count);
                let id = entry.key().clone();
                tokio::spawn({
                    let scheduler = scheduler.clone();
                    async move {
                        // TODO: scheduler stop conditions
                        scheduler.clone().run_control_loop(Some(barrier)).await;

                        tracing::info!(
                            "conchecker scheduler {id} complete. trace:\n{}",
                            scheduler.get_trace()
                        );
                    }
                });
                let (scheduler, barrier) = entry.insert((scheduler, barrier));
                (scheduler.clone(), *barrier)
            }
        }
    }
}
