use std::time::Duration;

use racedet::{
    driver::Driver,
    sync::task_wait::{NewTaskGroup, TaskGroup, TaskSpawned, TaskWaitAnyN, TaskWaitCompleted},
    task::{StartBarrier, Task, execution_point, sync_event, task},
};
use tokio::spawn;

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
    task(Task::new("root"), root()).await;
}

async fn root() {
    execution_point("before spawn").await;

    // task_join means that the current task is waiting for nested tasks and should not be scheduled in of itself (but other tasks should be scheduled instead)
    let barrier = StartBarrier::new(2);
    let task_group = TaskGroup::new();
    sync_event(NewTaskGroup(task_group));
    let task_a = spawn(task(
        Task::new("child a")
            .with_task_group(task_group, 0)
            .with_start_barrier(barrier.clone()),
        child("a"),
    ));
    let task_b = spawn(task(
        Task::new("child b")
            .with_task_group(task_group, 1)
            .with_start_barrier(barrier.clone()),
        child("b"),
    ));
    sync_event(TaskSpawned(task_group, 0));
    sync_event(TaskSpawned(task_group, 1));
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

async fn child(name: &'static str) {
    execution_point("a1").await;
    let task_group = TaskGroup::new();
    sync_event(NewTaskGroup(task_group));
    let r = spawn(task(
        Task::new(format!("leaf {name}")).with_task_group(task_group, 0),
        execution_point("done"),
    ));
    sync_event(TaskSpawned(task_group, 0));
    sync_event(TaskWaitAnyN(task_group, 1));
    r.await.unwrap();
    sync_event(TaskWaitCompleted);
    execution_point("a2").await;
}
