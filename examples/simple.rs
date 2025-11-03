use std::sync::{Arc, atomic::AtomicI64};

use conc_checker::{execution_point, register_task, run_with_schedule, task};
use tokio::join;

#[tokio::main]
async fn main() {
    let (trace, res) = run_with_schedule(foo()).await;
    println!("{res:?}");
    println!("{trace}");
}

async fn foo() {
    let var = Arc::new(AtomicI64::new(0));
    let bar1 = register_task("bar");
    let bar2 = register_task("bar");
    join!(task(bar1, bar(var.clone())), task(bar2, bar(var.clone())));
    assert_eq!(2, var.load(std::sync::atomic::Ordering::Relaxed));
}

async fn bar(var: Arc<AtomicI64>) {
    execution_point("before load").await;
    let x = var.load(std::sync::atomic::Ordering::Relaxed);
    execution_point("after load").await;
    var.store(x + 1, std::sync::atomic::Ordering::Relaxed);
}
