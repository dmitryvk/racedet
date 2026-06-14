use std::sync::{Arc, atomic::AtomicU64};

use axum::{
    Json, Router,
    extract::State,
    http::StatusCode,
    routing::{get, post},
};
use racedet::task::execution_point;
use serde::{Deserialize, Serialize};

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    // use the following script to execute the example:
    // curl http://localhost:3000/reset -X POST && curl http://localhost:3000/increment -H 'x-racedet: 2-foo-r1' & curl http://localhost:3000/increment -H 'x-racedet: 2-foo-r2' & wait; curl http://localhost:3000/retrieve_racedet_trace/foo -X POST
    // add -H 'x-racedet-replay: <...>'

    #[cfg(feature = "racedet_active")]
    let scheduler_registry = racedet_support::SchedulerRegistry::new();
    // build our application with a route
    let app = Router::new()
        // `GET /` goes to `root`
        .route("/", get(root))
        .route("/reset", post(reset_counter));

    #[cfg(feature = "racedet_active")]
    let app = app.route(
        "/retrieve_racedet_trace/{trace_id}",
        post(racedet_support::retrieve_racedet_trace),
    );

    let app = app
        .route("/increment", get(increment))
        // `POST /users` goes to `create_user`
        .route("/users", post(create_user));

    #[cfg(feature = "racedet_active")]
    let app = app.layer(racedet_support::RacedetLayer::new(
        scheduler_registry.clone(),
    ));

    let app = app.with_state(Arc::new(AppState {
        var: AtomicU64::new(0),
        #[cfg(feature = "racedet_active")]
        scheduler_registry,
    }));

    // run our app with hyper, listening globally on port 3000
    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await.unwrap();
    println!(
        "
    Started http server at http://0.0.0.0:3000."
    );
    #[cfg(feature = "racedet_active")]
    println!(
        "
    Use the following command for testing:
      curl http://localhost:3000/reset -X POST && \
         \\
      curl http://localhost:3000/increment -H 'x-racedet: 2-foo-r1' & \\
      curl http://localhost:3000/increment \
         -H 'x-racedet: 2-foo-r2' & \\
      wait; \\
      curl http://localhost:3000/retrieve_racedet_trace/foo \
         -X POST

    To replay execution, add the following arguments to curl invocations:
      add -H 'x-racedet-replay: <...>'
    "
    );
    axum::serve(listener, app).await.unwrap();
}

struct AppState {
    var: AtomicU64,
    #[cfg(feature = "racedet_active")]
    scheduler_registry: Arc<racedet_support::SchedulerRegistry>,
}

// basic handler that responds with a static string
async fn reset_counter(State(state): State<Arc<AppState>>) {
    state.var.store(0, std::sync::atomic::Ordering::Relaxed);
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

#[cfg(feature = "racedet_active")]
mod racedet_support;
