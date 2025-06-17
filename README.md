# TurboStore

A concurrent, in-memory, in-process, redis-like storage for Rust.

TurboStore supports multiple data structures, including KV pairs, maps, deques, and sets. It's fast
and async-ready thanks to `scc` and `bitcode`, and does not limit the types for each data
structure.

```rs
use turbostore::{TurboStore, Duration};

// This example uses tokio, but the library is runtime-agnostic
async fn main() {
    let store: TurboStore<String> = TurboStore::new();

    // Duration is the TTL. All keys have TTLs since this store is mostly intended to store
    // ephemeral data. Support for non-expiring keys will be added in the future.
    store.set("my_key".to_string(), 1, Duration::minutes(1)).await;

    let value = store.get::<i32>("my_key").await.unwrap();
}
```

Typed keys, structs, hash maps, sets, deques, and more are supported.

## Documentation

Go to [docs.rs/turbostore](https://docs.rs/turbostore) for the full documentation of the crate.
