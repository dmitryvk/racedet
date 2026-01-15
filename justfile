check:
    cargo +nightly fmt
    cargo clippy
    cargo clippy --all-targets