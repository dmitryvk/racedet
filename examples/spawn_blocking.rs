use std::time::Duration;

use conc_checker::{
    current_scheduler,
    driver::Driver,
    execution_point, execution_point_blocking, maybe_with_scheduler_blocking, new_start_barrier,
    sync_event,
    sync_model::task_wait::{NewTaskGroup, TaskGroup, TaskWaitAnyN, TaskWaitCompleted},
    task, task_blocking, with_start_barrier_blocking, with_task_group_blocking,
};
use tokio::{task::spawn_blocking, time::sleep};

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    Driver::new()
        .with_replay_many_env_var("RACE_DET_REPLAY")
        .max_iterations_env_var("RACE_DET_ITERS", None)
        .max_total_duration_env_var("RACE_DET_DURATION_SEC", Duration::from_secs(10))
        .test_timeout(Duration::from_secs(1))
        .run_async(async || foo().await)
        .await;
}

async fn foo() {
    task("bar", bar()).await;
}

async fn bar() {
    execution_point("before spawn").await;

    // task_join means that the current task is waiting for nested tasks and should not be scheduled in of itself (but other tasks should be scheduled instead)
    let barrier = new_start_barrier(2);
    let task_group = TaskGroup::new();
    sync_event(NewTaskGroup(task_group));
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
