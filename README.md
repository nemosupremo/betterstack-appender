# betterstack-appender

This crate provides a buffered writer that is meant to be used as a sink for
[`tracing`](https://docs.rs/tracing/latest/tracing/) spans and events.

## Usage

This crate is best paired with [`json-subscriber`](https://docs.rs/json-subscriber/latest/json_subscriber/).

`json-subscriber` will handle serializing your events and spans to JSON and then this crate can ship
those JSON events to betterstack.

```rust
let (betterstack, _guard) = betterstack::BetterStackWriter::builder(
    BETTERSTACK_SOURCE_TOKEN,
    BETTERSTACK_INGEST_URL,
)
.finish();

json_subscriber::fmt()
    .flatten_event(true)
    .with_writer(betterstack);
```

To rename the `timestamp` field to `dt` you will need to access the Json Layer.

```rust
let json = {
    let mut layer = json_subscriber::layer().without_time().flatten_event(true);
    layer.inner_layer_mut()
        .with_timer("dt", tracing_subscriber::fmt::time::SystemTime);
    layer
};
tracing_subscriber::registry()
    .with(json)
    .init();
```
