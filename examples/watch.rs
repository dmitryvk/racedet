use std::time::Duration;

use racedet::{
    driver::Driver,
    execution_point, new_start_barrier, sync_event,
    sync_model::watch::{WaitingForWatchUpdate, WatchId, WatchNotified},
    task, with_start_barrier,
};
use tokio::{
    join,
    sync::watch::{Receiver, Sender},
};

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
    let barrier = new_start_barrier(3);
    let (tx, rx) = tokio::sync::watch::channel(1);
    join!(
        task(
            "producer",
            with_start_barrier(barrier.clone(), producer(tx))
        ),
        task(
            "consumer1",
            with_start_barrier(barrier.clone(), consumer(rx.clone()))
        ),
        task(
            "consumer2",
            with_start_barrier(barrier.clone(), consumer(rx.clone()))
        )
    );
}

async fn producer(watch: Sender<i32>) {
    let watch_id = WatchId::from_sender(&watch);
    for i in 0..10 {
        execution_point("update").await;
        tracing::info!("sending {i}");
        watch.send(i).unwrap();
        tracing::info!("sent {i}");
        tracing::debug!("sync_event WatchNotified");
        sync_event(WatchNotified(watch_id));
    }
    drop(watch);
    sync_event(WatchNotified(watch_id));
}

async fn consumer(mut watch: Receiver<i32>) {
    let watch_id = WatchId::from_receiver(&watch);
    loop {
        execution_point("wait for").await;
        let Ok(v) = watch
            .wait_for(|v| {
                if *v % 2 == 0 {
                    tracing::debug!("predicate is true, v={}", *v);
                    true
                } else {
                    tracing::debug!("sync_event WaitingForWatchUpdate, v={}", *v);
                    // TODO: at EOF, we will observe the last value once more which results in running 2 tasks at the same time:
                    // 62. finish 1 producer
                    // 63. resumed [3 consumer2] (run: [], suspended: [2 consumer1 at wait for, 3 consumer2 at wait for], options: [[2 consumer1], [3 consumer2]])
                    // 64. resumed [2 consumer1] (run: [3 consumer2 at wait for], suspended: [2 consumer1 at wait for], options: [[2 consumer1]])
                    // 65. finish 3 consumer2
                    // 66. finish 2 consumer1
                    sync_event(WaitingForWatchUpdate(watch_id));
                    false
                }
            })
            .await
        else {
            tracing::info!("received EOF");
            break;
        };
        tracing::info!("received {v}", v = *v);
        drop(v);
        execution_point("received update").await;
    }
}
