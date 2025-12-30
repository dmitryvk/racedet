use std::{sync::Arc, time::Duration};

use conc_checker::{
    capture_panics::capture_panic,
    execution_point, new_scheduler, new_start_barrier, sync_event,
    sync_model::watch::{WaitingForWatchUpdate, WatchId, WatchNotified},
    task, with_scheduler, with_start_barrier,
};
use timeout_tracing::{CaptureSpanAndStackTrace, timeout};
use tokio::{join, sync::watch::Sender};

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();
    let (scheduler, control_fut) = new_scheduler();
    tokio::spawn(control_fut);
    let res = timeout(
        Duration::from_secs(10),
        CaptureSpanAndStackTrace,
        with_scheduler(scheduler.clone(), capture_panic(foo())),
    )
    .await;
    let trace = scheduler.get_trace();
    println!("{trace}");
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
}

async fn foo() {
    let barrier = new_start_barrier(3);
    let (tx, _) = tokio::sync::watch::channel(1);
    let watch = Arc::new(tx);
    join!(
        task(
            "producer",
            with_start_barrier(barrier.clone(), producer(watch.clone()))
        ),
        task(
            "consumer1",
            with_start_barrier(barrier.clone(), consumer(watch.clone()))
        ),
        task(
            "consumer2",
            with_start_barrier(barrier.clone(), consumer(watch.clone()))
        )
    );
}

async fn producer(watch: Arc<Sender<i32>>) {
    let watch_id = WatchId::from_sender(&watch);
    for i in 0..10 {
        execution_point("update").await;
        watch.send(i).unwrap();
        tracing::info!("sent {i}");
        tracing::debug!("sync_event WatchNotified");
        sync_event(WatchNotified(watch_id));
    }
}

async fn consumer(watch: Arc<Sender<i32>>) {
    let mut watch = watch.subscribe();
    let watch_id = WatchId::from_receiver(&watch);
    loop {
        execution_point("wait for").await;
        let Ok(v) = watch
            .wait_for(|v| {
                if *v % 2 == 0 {
                    tracing::debug!("wait for completed");
                    true
                } else {
                    tracing::debug!("sync_event WaitingForWatchUpdate");
                    sync_event(WaitingForWatchUpdate(watch_id));
                    false
                }
            })
            .await
        else {
            break;
        };
        tracing::info!("received {v}", v = *v);
    }
}
