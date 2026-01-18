check:
    cargo +nightly fmt
    cargo clippy
    cargo clippy --all-features
    cargo clippy --all-targets
    cargo clippy --all-targets --all-features