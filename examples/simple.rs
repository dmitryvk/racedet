use std::sync::{Arc, atomic::AtomicI64};

use conc_checker::{
    execute, execution_point, new_scheduler, register_task, register_task_start_barrier, task,
};
use tokio::join;

#[tokio::main]
async fn main() {
    let (trace, res) = execute(new_scheduler(), foo()).await;
    println!("{res:?}");
    println!("{trace}");
}

async fn foo() {
    let var = Arc::new(AtomicI64::new(0));
    let start_barrier = register_task_start_barrier("tasks", 2);
    let bar1 = register_task("bar", start_barrier);
    let bar2 = register_task("bar", start_barrier);
    join!(task(bar1, bar(var.clone())), task(bar2, bar(var.clone())));
    assert_eq!(2, var.load(std::sync::atomic::Ordering::Relaxed));
}

async fn bar(var: Arc<AtomicI64>) {
    execution_point("before load").await;
    let x = var.load(std::sync::atomic::Ordering::Relaxed);
    execution_point("after load").await;
    var.store(x + 1, std::sync::atomic::Ordering::Relaxed);
}
