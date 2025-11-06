use conc_checker::{execution_point, register_task, run_with_schedule, task};
use tokio::join;

#[tokio::main]
async fn main() {
    let (trace, res) = run_with_schedule(foo()).await;
    println!("{res:?}");
    println!("{trace}");
}

async fn foo() {
    task(register_task("bar"), bar()).await;
}

async fn bar() {
    execution_point("before").await;
    join!(execution_point("a"), execution_point("b"));
}
