# Idea

`run_with_schedule` is an entrypoint, which schedules the future within itself.
It uses `task` and `point` to control the interleaving of the future's execution.

It keeps a list of `tasks` that are ready to run; each time it chooses one task and drives it until it reaches a `point`. Then it choose another task and so on - until the inner future completes (by either returning a value or panicking).

A sequence of `task` choices (and their current `point`s) forms a trace.
The choices are controlled by a `schedule` which can either be random or specified manually.

# Implementation

- Scheduler coordinates everything. It is available as a thread-local.
- Panic hook can be installed to collect panic backtrace
- Track lock acquires and releases, since not every task is always ready to execute
