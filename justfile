check:
    cargo +nightly fmt
    cargo clippy
    cargo clippy --all-features
    cargo clippy --workspace --all-targets
    cargo clippy --workspace --all-targets --all-features