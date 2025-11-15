use conc_checker::{execute, execution_point, new_scheduler, register_task, task};
use tokio::join;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[tokio::main]
async fn main() {
    tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer())
        .with(tracing_subscriber::EnvFilter::from_default_env())
        .init();
    let (trace, res) = execute(new_scheduler(), foo()).await;
    println!("{res:?}");
    println!("{trace}");
}

async fn foo() {
    task(register_task("bar", None), bar()).await;
}

async fn bar() {
    execution_point("before").await;
    join!(execution_point("a"), execution_point("b"));
}
