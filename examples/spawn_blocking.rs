use std::time::Duration;

use conc_checker::{
    capture_panics::capture_panic,
    current_scheduler, execution_point, execution_point_blocking, maybe_with_scheduler_blocking,
    new_scheduler, new_start_barrier, sync_event,
    sync_model::task_wait::{TaskGroup, TaskWaitAnyN, TaskWaitCompleted},
    task, task_blocking, with_scheduler, with_start_barrier_blocking, with_task_group_blocking,
};
use timeout_tracing::{CaptureSpanAndStackTrace, timeout};
use tokio::{task::spawn_blocking, time::sleep};

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();
    let replay = std::env::var("CONC_CHECKER_REPLAY").ok();
    let (scheduler, control_fut) = new_scheduler(replay.as_deref());
    tokio::spawn(control_fut);
    let res = timeout(
        Duration::from_secs(10),
        CaptureSpanAndStackTrace,
        with_scheduler(scheduler.clone(), capture_panic(foo())),
    )
    .await;
    match res {
        Ok(Ok(res)) => {
            println!("ok {res:?}");
        }
        Ok(Err(panic)) => {
            println!(
                "panic {} at {}\n{}",
                panic.message, panic.location, panic.backtrace
            );
        }
        Err(timeout) => {
            println!("timeout {}", timeout.active_traces[0].stack_trace());
        }
    }
    println!("{}", scheduler.get_trace());
    println!("{}", scheduler.get_replay());
}

async fn foo() {
    task("bar", bar()).await;
}

async fn bar() {
    execution_point("before spawn").await;

    // task_join means that the current task is waiting for nested tasks and should not be scheduled in of itself (but other tasks should be scheduled instead)
    let barrier = new_start_barrier(2);
    let task_group = TaskGroup::new();
    let task_a: tokio::task::JoinHandle<_> = spawn_blocking({
        let barrier = barrier.clone();
        let scheduler = current_scheduler();
        move || {
            tracing::debug!("in spawn_blocking 1");
            maybe_with_scheduler_blocking(scheduler, || {
                task_blocking("spawn a", || {
                    with_task_group_blocking(task_group, 0, || {
                        with_start_barrier_blocking(barrier.clone(), || {
                            execution_point_blocking("a1");
                            execution_point_blocking("a2");
                        })
                    })
                })
            })
        }
    });
    let task_b = spawn_blocking({
        let barrier = barrier.clone();
        let scheduler = current_scheduler();
        move || {
            tracing::debug!("in spawn_blocking 2");
            maybe_with_scheduler_blocking(scheduler, || {
                task_blocking("spawn b", || {
                    with_task_group_blocking(task_group, 1, || {
                        with_start_barrier_blocking(barrier.clone(), || {
                            execution_point_blocking("b1");
                            execution_point_blocking("b2");
                        })
                    })
                })
            })
        }
    });
    // TODO: sleep is a hack to ensure that spawn happens before the task becomes blocked
    sleep(Duration::from_millis(1)).await;
    tracing::debug!("sync_event starting join");
    sync_event(TaskWaitAnyN(task_group, 2));
    tracing::debug!("starting join");
    task_a.await.unwrap();
    task_b.await.unwrap();
    tracing::debug!("joined");
    sync_event(TaskWaitCompleted);
    tracing::info!("ok");
    execution_point("joined").await;
}
