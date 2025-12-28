use conc_checker::{execute, execution_point, new_scheduler, register_task, task};
use tokio::join;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();
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
