use std::{
    str::FromStr,
    time::{Duration, Instant},
};

use tokio::time::timeout;

use crate::{ReplayTrace, capture_panics::capture_panic, new_scheduler, with_scheduler};

pub struct Driver {
    mode: RunMode,
    replay_env_var_name: Option<String>,
    max_iterations: Option<u32>,
    max_total_duration: Duration,
    test_timeout: Option<Duration>,
}

enum RunMode {
    ReplayOnce(ReplayTrace),
    ReplayMany(ReplayTrace),
    Random,
}

impl Driver {
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        Self {
            mode: RunMode::Random,
            replay_env_var_name: None,
            max_iterations: None,
            max_total_duration: Duration::from_secs(10),
            test_timeout: None,
        }
    }

    pub fn with_replay_once_env_var(mut self, env_var_name: &str) -> Self {
        let replay = std::env::var(env_var_name)
            .ok()
            .map(|s| ReplayTrace::from_str(&s).unwrap_or_else(|err| panic!("unable to parse trace for replay from the \"{env_var_name}\" environment variable: {err}")));
        self.mode = if let Some(replay) = replay {
            RunMode::ReplayOnce(replay)
        } else {
            RunMode::Random
        };
        self.replay_env_var_name = Some(env_var_name.to_owned());
        self
    }

    pub fn with_replay_many_env_var(mut self, env_var_name: &str) -> Self {
        let replay = std::env::var(env_var_name)
            .ok()
            .map(|s| ReplayTrace::from_str(&s).unwrap_or_else(|err| panic!("unable to parse trace for replay from the \"{env_var_name}\" environment variable: {err}")));
        self.mode = if let Some(replay) = replay {
            RunMode::ReplayMany(replay)
        } else {
            RunMode::Random
        };
        self.replay_env_var_name = Some(env_var_name.to_owned());
        self
    }

    pub fn max_iterations(mut self, num_iterations: u32) -> Self {
        self.max_iterations = Some(num_iterations);
        self
    }

    pub fn max_iterations_env_var(
        mut self,
        env_var_name: &str,
        default_num_iterations: Option<u32>,
    ) -> Self {
        let num_iterations = std::env::var(env_var_name)
            .ok()
            .map(|s| u32::from_str(&s).unwrap_or_else(|err| panic!("unable to parse max number of iterations from the \"{env_var_name}\" environment variable: {err}")))
            .or(default_num_iterations);
        self.max_iterations = num_iterations;
        self
    }

    pub fn max_total_duration(mut self, duration: Duration) -> Self {
        self.max_total_duration = duration;
        self
    }

    pub fn max_total_duration_env_var(
        mut self,
        env_var_name: &str,
        default_duration: Duration,
    ) -> Self {
        let duration = std::env::var(env_var_name)
            .ok()
            .map(|s| f64::from_str(&s).unwrap_or_else(|err| panic!("unable to parse max number of iterations from the \"{env_var_name}\" environment variable: {err}")))
            .map(Duration::from_secs_f64)
            .unwrap_or(default_duration);
        self.max_total_duration = duration;
        self
    }

    pub fn test_timeout(mut self, timeout: Duration) -> Self {
        self.test_timeout = Some(timeout);
        self
    }

    pub async fn run_async<F, Fut>(&self, mut f: F)
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = ()>,
    {
        match &self.mode {
            RunMode::ReplayOnce(replay) => {
                self.run_once_async(Some(replay), &mut f).await;
            }
            RunMode::ReplayMany(replay) => {
                self.run_many_async(Some(replay), f).await;
            }
            RunMode::Random => {
                self.run_many_async(None, f).await;
            }
        };
    }

    async fn run_many_async<F, Fut>(&self, replay: Option<&ReplayTrace>, mut f: F)
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = ()>,
    {
        let started_at = Instant::now();
        let mut iter = 0;
        while self.max_iterations.is_none_or(|max| iter < max)
            && started_at.elapsed() < self.max_total_duration
        {
            self.run_once_async(replay, &mut f).await;
            iter = iter
                .checked_add(1)
                .expect("iter is bounded by self.max_iterations");
        }
        println!("executed {iter} tests in {:?}", started_at.elapsed());
    }

    async fn run_once_async<F, Fut>(&self, replay: Option<&ReplayTrace>, f: &mut F)
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = ()>,
    {
        let (scheduler, control_fut) = new_scheduler(replay);
        let control_handle = tokio::spawn(control_fut);
        let run = async {
            let scheduler = scheduler.clone();
            if let Some(test_timeout) = self.test_timeout {
                timeout(test_timeout, capture_panic(with_scheduler(scheduler, f()))).await
            } else {
                Ok(capture_panic(with_scheduler(scheduler, f())).await)
            }
        };
        let test_res = match run.await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(panic)) => Err(format!(
                "test panicked:\n{message}",
                message = panic.message
            )),
            Err(_) => Err(format!(
                "test timed out after {:?}",
                self.test_timeout
                    .expect("this is reachable only if test_timeout is set")
            )),
        };
        control_handle.abort();
        if let Err(join_error) = control_handle.await
            && join_error.is_panic()
        {
            let replay_message = if let Some(env_var) = &self.replay_env_var_name {
                format!(
                    "To replay this test, set the following environment variable:\n{env_var}=\"{}\"\n",
                    scheduler.get_replay()
                )
            } else {
                "".to_owned()
            };
            panic!(
                "Internal error in test scheduler: {join_error}.\n{replay_message}Execution trace:\n{}",
                scheduler.get_trace()
            );
        }
        if let Err(message) = test_res {
            let replay_message = if let Some(env_var) = &self.replay_env_var_name {
                format!(
                    "To replay this test, set the following environment variable:\n{env_var}=\"{}\"\n",
                    scheduler.get_replay()
                )
            } else {
                "".to_owned()
            };
            panic!(
                "{message}\n\n{replay_message}\nExecution trace:\n{}",
                scheduler.get_trace()
            );
        }
    }
}
