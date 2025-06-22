//! A concurrent, in-memory, in-process, Redis-like storage for Rust.
//!
//! Any [bitcode]-encodable and [bitcode]-decodable type can be stored using this store, without
//! being locked to a single type.
//!
//! Turbostore uses [`scc::HashMap`] under the hood, so the performance stays consistent under
//! load. This means, though, that data structures are locked on access (e.g. accessing two keys of
//! a single hashmap will lock). Locks are released as soon as possible for maximum performance.
//!
//! # Installation
//!
//! To install it, just run `cargo add turbostore bitcode`. The crate has no features and is
//! async-only. You need to install bitcode because of the [`Encode`] and [`Decode`] derive macros.
//!
//! A sync API may be added in the future since the underlying APIs used by this crate provide
//! both sync and async APIs in their majority.
//!
//! # Usage
//!
//! If you prefer to jump to the reference section, go to [`TurboStore`]. The API is simple and the
//! operation names will be familiar if you've used Redis in the past.
//!
//! ## Creating Keys
//!
//! Keys are of a single hashable type. [`String`] works by default, but it's prone to typos,
//! introduces storage overhead, and it requires you to implement your own key formatting, so if
//! you want more type safety in your keys (and less storage requirements, therefore), create a key
//! enum as follows:
//!
//! ```
//! use std::hash::Hash;
//!
//! // All derives are required to be able to use this type as a key.
//! #[derive(Hash, PartialEq, Eq)]
//! enum Key {
//!     // An example of using the store for user data store.
//!     UserCache(i32),
//! }
//! ```
//!
//! ## Creating the TurboStore
//!
//! To create a store using the key you just created, create a new instance of [`TurboStore<K>`]
//! using [`TurboStore::new`].
//!
//! ```
//! use turbostore::TurboStore;
//!
//! let store: TurboStore<String> = TurboStore::new();
//! ```
//!
//! *Note: This example uses [`String`] as a key type for brevity.*
//!
//! ### Preallocating Memory
//!
//! If you already know you'll have a certain amount of usage, you can preallocate space with
//! [`TurboStore::with_capacity`].
//!
//! ```
//! # use pollster::FutureExt as _;
//! #
//! # async {
//! use turbostore::{TurboStore, Duration};
//!
//! let store: TurboStore<String> = TurboStore::with_capacity(3);
//!
//! store.set("key1".into(), &"value1".to_string(), Duration::minutes(1)).await;
//! store.set("key2".into(), &"value2".to_string(), Duration::minutes(1)).await;
//! store.set("key3".into(), &"value3".to_string(), Duration::minutes(1)).await;
//! # }.block_on();
//! ```
//!
//! Each structure (KV, map, set, deque) gets its own independent capacity — so setting 3 gives 3
//! slots per type. This means that if you set the capacity to 3, you'll have preallocated space
//! for 3 KV pairs, 3 hash maps, 3 hash sets, and 3 deques.
//!
//! ## Creating Storable Data Structures
//!
//! Storable data structures are easy to implement. They must derive [`bitcode::Encode`] to be set,
//! and [`bitcode::Decode`] to be retrieved.
//!
//! ```
//! use turbostore::{Encode, Decode};
//!
//! #[derive(Encode, Decode)]
//! struct UserCache {
//!     id: i32,
//!     name: String,
//! }
//! ```
//!
//! ## TTL
//!
//! This store was mainly intended to be used as a temporary, fast-access cache. It's not limited
//! to doing only that, but many of the design choices are made based on that purpose. This library
//! may be generalized later.
//!
//! TTLs are all set based on [`chrono::Duration`] and relative to the time of the execution of the
//! function. Eviction is done both lazily and with manual calls to [`TurboStore::evict`]. This can be
//! called in loop in a background task to periodically clean up expired keys. For example:
//!
//! ```ignore
//! use std::sync::Arc;
//! use tokio::time::Duration;
//! use turbostore::TurboStore;
//!
//! let store: Arc<TurboStore<()>> = Arc::new(TurboStore::new());
//! let thread_store = store.clone();
//!
//! tokio::spawn(async move {
//!     loop {
//!         tokio::time::sleep(Duration::from_secs(2)).await;
//!         thread_store.evict().await;
//!     }
//! });
//! ```
//!
//! Turbostore is runtime-agnostic, so this can also be implemented using async-std.
//!
//! Not calling [`TurboStore::evict`] periodically will result in expired data staying in memory
//! until it's next attempted to be accessed. When the data is next attempted to be accessed after
//! being expired, TurboStore will automatically clean that value up.
//! 
//! *Note: Calling [`TurboStore::evict`] is quite inefficient at the moment, as it scans through
//! all keys and items, expired or not. This'll be worked on in a future version to emulate Redis's
//! garbage collection.*
//!
//! ## Data Structures
//!
//! Turbostore currently supports 3 different data structures, plus a normal KV pair storage.
//!
//! The supported data structures are:
//! - [KV Pairs](#kv-pairs) - Normal key-value pairs.
//! - [Hash Maps](#hash-maps) - Namespaced key-value pairs.
//! - [Hash Sets](#hash-sets) - Lists of unique items.
//! - [Deques](#deques) - Lists with O(1) operations on the first and last items.
//!
//! ### KV Pairs
//!
//! Turbostore supports the basic get, set, remove, pop, and other common operations on key-value
//! pairs.
//!
//! The available operations on key-value pairs are:
//! - [`TurboStore::set`] - Set a new KV pair.
//! - [`TurboStore::get`] - Get a KV pair's value.
//! - [`TurboStore::rem`] - Remove a KV pair.
//! - [`TurboStore::pop`] - Remove a KV pair and return its value.
//! - [`TurboStore::incr`] - Increment a value by x.
//! - [`TurboStore::decr`] - Decrement a value by x.
//! - [`TurboStore::expire`] - Update the TTL of a KV pair.
//! - [`TurboStore::exists`] - Checks whether a key is set.
//!
//! ### Hash Maps
//!
//! Turbostore supports storing sub-hash-maps inside the store.
//!
//! The available operations on hash maps are:
//! - [`TurboStore::hset`] - Add a KV pair to the hash map.
//! - [`TurboStore::hget`] - Get a value from a KV pair in the hash map.
//! - [`TurboStore::hrem`] - Remove a KV pair from the hash map.
//! - [`TurboStore::hpop`] - Remove and return a KV pair from the hash map.
//! - [`TurboStore::hexists`] - Check whether a key is set in the hash map.
//! - [`TurboStore::hexpire`] - Set the TTL of a KV pair in the hash map.
//! - [`TurboStore::hlen`] - Count the amount of KV pairs in the hash map.
//! - [`TurboStore::hclear`] - Empty the hash map.
//!
//! TTLs are per KV pair in a hash map, and KV pairs are expired individually.
//!
//! ### Hash Sets
//!
//! Hash sets are lists of unique items.
//!
//! The available operations on hash sets are:
//! - [`TurboStore::sadd`] - Add an item to the set.
//! - [`TurboStore::srem`] - Remove an item from the set.
//! - [`TurboStore::slen`] - Count the amount of items in the set.
//! - [`TurboStore::sclear`] - Empty the set.
//! - [`TurboStore::sttl`] - Get the expiry date and time of an item in the set.
//! - [`TurboStore::scontains`] - Check whether an item is in the set.
//! - [`TurboStore::sall`] - Get all items in the set.
//!
//! As with hash maps, hash set items have per-item TTLs and expire individually.
//!
//! ### Deques
//!
//! Deques are lists optimized for O(1) operations at both ends.
//!
//! The available operations on deques are:
//! - [`TurboStore::dappend`] - Append an item to the deque.
//! - [`TurboStore::dprepend`] - Prepend an item to the deque.
//! - [`TurboStore::dfpop`] - Remove and return the first item of the deque.
//! - [`TurboStore::dbpop`] - Remove and return the last item of the deque.
//! - [`TurboStore::dpop`] - Remove and return an item of the deque by index.
//! - [`TurboStore::dfrem`] - Remove the first item of the deque.
//! - [`TurboStore::dbrem`] - Remove the last item of the deque.
//! - [`TurboStore::drem`] - Remove an item of the deque by index.
//! - [`TurboStore::dfpeek`] - Get the first item of the deque.
//! - [`TurboStore::dbpeek`] - Get the last item of the deque.
//! - [`TurboStore::dpeek`] - Get an item of the deque by index.
//! - [`TurboStore::dlen`] - Count the amount of items in the deque.
//! - [`TurboStore::expire`] - Update the expiration time of an item in the deque.
//! - [`TurboStore::dclear`] - Empty the deque.
//! - [`TurboStore::dmap`] - Overwrite each item of the deque.
//! - [`TurboStore::dall`] - Get all items in the deque.
//! - [`TurboStore::dftruncate`] - Keep only the first x items in the deque.
//! - [`TurboStore::dbtruncate`] - Keep only the last x items in the deque.
//!
//! Note that `rem` operations are faster than `pop` operations due to the lack of need to decode
//! the removed value.
//!
//! Items in deques have each their individual TTLs and expire individually.
//!
//! ## The [`Value`] Wrapper
//!
//! Value is a wrapper that holds your data **plus metadata like the expiry time**. Currently, that
//! metadata is the expiry time of the value, which can be accessed with [`Value::expires_at`].
//!
//! ## Example
//!
//! This example shows how to set and get a value from the store. Operations with other data
//! structures and operations follow a similar encoding/decoding style, so it's easy to work with
//! them. If you've worked with Redis in the past, you'll find this crate familiar.
//!
//! ```
//! # use pollster::FutureExt as _;
//! #
//! # async {
//! use std::hash::Hash;
//! use turbostore::{TurboStore, Value, Duration, Encode, Decode};
//!
//! #[derive(Hash, PartialEq, Eq)]
//! enum Key {
//!     User(i32),
//! }
//!
//! #[derive(Debug, Encode, Decode, PartialEq, Eq, Clone)]
//! struct User {
//!     id: i32,
//!     name: String,
//!     email: String,
//! }
//!
//! let store: TurboStore<Key> = TurboStore::new();
//!
//! let user = User {
//!     id: 1,
//!     name: "John Doe".into(),
//!     email: "john@example.com".into()
//! };
//!
//! store.set(
//!     Key::User(1),
//!     &user,
//!     Duration::minutes(15)
//! ).await;
//!
//! // The first `Option` returns whether the key existed.
//! // The `Result` is the result of decoding the value. Since types are dynamic-ish, setting the
//! //   wrong value will fail.
//! // The `Value` is TurboStore's internal wrapper over values, which stores metadata (e.g. expiry
//! //   time).
//! let stored_user: Value<User> = store.get::<User>(&Key::User(1)).await.unwrap().unwrap();
//!
//! assert_eq!(*stored_user, user);
//! # }.block_on();
//! ```
//!
//! # Roadmap
//!
//! Distribution both sharded and replicated is planned to be supported. Pipelines are also
//! planned, since running multiple operations is currently neither atomic across ops nor as
//! efficient as it could be (e.g. setting two items in a set fetches the set twice, once per op).
//!
//! Contributions are welcome, whether it's feedback, bug reports, or pull requests. Check out
//! [our GitHub repository](https://github.com/Nekidev/turbostore) for the source code and issue
//! tracker.
//! 
//! [bitcode]: https://docs.rs/bitcode

use std::{
    collections::VecDeque,
    hash::Hash,
    ops::{Deref, Neg},
};

pub use bitcode::{Decode, DecodeOwned, Encode};
pub use chrono::{DateTime, Duration, Utc};
use scc::HashMap;

use crate::math::{SaturatingAdd, SaturatingSub};

mod math;

/// A TurboStore value.
#[derive(Clone, Debug)]
pub struct Value<T> {
    /// The date and time this value expires at.
    pub expires_at: DateTime<Utc>,

    /// The inner value of the item.
    pub value: T,
}

impl<T> Deref for Value<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.value
    }
}

impl<T> Value<T> {
    /// Returns whether the value is already expired.
    ///
    /// Arguments:
    /// * `now` - The current datetime. Passed as an argument so that `Utc::now()` doesn't have to
    ///   be called on multiple iterations.
    ///
    /// Returns:
    /// [`bool`] - Whether the value has already expired.
    fn is_expired(&self, now: &DateTime<Utc>) -> bool {
        self.expires_at <= *now
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Error {
    /// The value could not be decoded to the specified type.
    Decoding,
}

/// A concurrent, in-memory, in-process, Redis-like storage for Rust.
///
/// Check the [`crate`]'s documentation to learn how to use it.
pub struct TurboStore<K: Hash + Eq> {
    pairs: HashMap<K, Value<Vec<u8>>>,
    maps: HashMap<K, HashMap<K, Value<Vec<u8>>>>,
    sets: HashMap<K, HashMap<Vec<u8>, Value<()>>>,
    deques: HashMap<K, VecDeque<Value<Vec<u8>>>>,
}

impl<K> TurboStore<K>
where
    K: Hash + Eq,
{
    /// Creates a new store with capacity.
    ///
    /// Returns:
    /// [`TurboStore`] - The new store instance.
    pub fn new() -> Self {
        Self {
            pairs: HashMap::new(),
            maps: HashMap::new(),
            sets: HashMap::new(),
            deques: HashMap::new(),
        }
    }

    /// Creates a new store with capacity.
    ///
    /// Arguments:
    /// * `capacity` - The amount of key-value pairs for whose space should be preallocated.
    ///
    /// Returns:
    /// [`TurboStore`] - The new store instance.
    pub fn with_capacity(capacity: usize) -> Self {
        // TODO: Allow fine-grained capacity setting.

        Self {
            pairs: HashMap::with_capacity(capacity),
            maps: HashMap::with_capacity(capacity),
            sets: HashMap::with_capacity(capacity),
            deques: HashMap::with_capacity(capacity),
        }
    }

    /// Cleans up all expired keys.
    pub async fn evict(&self) {
        let now = Utc::now();

        self.pairs.retain_async(|_, v| !v.is_expired(&now)).await;

        self.maps
            .retain_async(|_, map| {
                map.retain(|_, v| !v.is_expired(&now));

                !map.is_empty()
            })
            .await;

        self.sets
            .retain_async(|_, set| {
                set.retain(|_, v| !v.is_expired(&now));

                !set.is_empty()
            })
            .await;

        self.deques
            .retain_async(|_, deque| {
                deque.retain(|i| !i.is_expired(&now));

                !deque.is_empty()
            })
            .await;
    }

    /// Sets a new KV pair.
    ///
    /// If the key already exists, the value is overwritten with the new value and TTL.
    ///
    /// Arguments:
    /// * `key` - The key of the KV pair.
    /// * `value` - The value of the KV pair.
    /// * `ttl` - The TTL of the KV pair.
    pub async fn set<V>(&self, key: K, value: &V, ttl: Duration)
    where
        V: Encode,
    {
        let expires_at = Utc::now() + ttl;

        let encoded: Vec<u8> = bitcode::encode(value);

        self.pairs
            .upsert_async(
                key,
                Value {
                    value: encoded,
                    expires_at,
                },
            )
            .await;
    }

    /// Increments a KV pair's value.
    ///
    /// The TTL is overwritten on every increment.
    ///
    /// Integer overflows are saturated, they cannot go past `type::MAX` nor under `type::MIN`.
    ///
    /// Arguments:
    /// * `key` - The key of the KV pair.
    /// * `by` - The amount to increment by.
    /// * `ttl` - The TTL of the KV pair.
    pub async fn incr<V>(&self, key: K, by: V, ttl: Duration) -> Result<V, Error>
    where
        V: Encode + DecodeOwned + SaturatingAdd<Output = V>,
    {
        let expires_at = Utc::now() + ttl;

        let value = self.pairs.get_async(&key).await;

        if let Some(mut value) = value {
            let decoded = bitcode::decode::<V>(&value).map_err(|_| Error::Decoding)?;

            let new_value = decoded.saturating_add(&by);

            *value = Value {
                value: bitcode::encode(&new_value),
                expires_at,
            };

            Ok(new_value)
        } else {
            self.pairs
                .upsert_async(
                    key,
                    Value {
                        value: bitcode::encode(&by),
                        expires_at,
                    },
                )
                .await;

            Ok(by)
        }
    }

    /// Decrements a KV pair's value.
    ///
    /// The TTL is overwritten on every decrement.
    ///
    /// Integer overflows are saturated, they cannot go past `type::MAX` nor under `type::MIN`.
    ///
    /// Arguments:
    /// * `key` - The key of the KV pair.
    /// * `by` - The amount to decrement by.
    /// * `ttl` - The TTL of the KV pair.
    pub async fn decr<V>(&self, key: K, by: V, ttl: Duration) -> Result<V, Error>
    where
        V: Encode + DecodeOwned + SaturatingSub<Output = V> + Neg<Output = V>,
    {
        let expires_at = Utc::now() + ttl;

        let value = self.pairs.get_async(&key).await;

        if let Some(mut value) = value {
            let decoded = bitcode::decode::<V>(&value).map_err(|_| Error::Decoding)?;

            let new_value = decoded.saturating_sub(&by);

            *value = Value {
                value: bitcode::encode(&new_value),
                expires_at,
            };

            Ok(new_value)
        } else {
            let new_value = -by;

            self.pairs
                .upsert_async(
                    key,
                    Value {
                        value: bitcode::encode(&new_value),
                        expires_at,
                    },
                )
                .await;

            Ok(new_value)
        }
    }

    /// Gets the value of a KV pair by key.
    ///
    /// Arguments:
    /// * `key` - The key whose value to retrieve.
    ///
    /// Returns:
    /// [`Option<Result<Value<V>, Error>>`] - `Some(Ok(V))` if there is a non-expired value,
    /// `None` if the value is non-existent or expired, or `Some(Err(Error::Decoding))` if
    /// the key exists and is not expired but the value could not be decoded to the specified type.
    pub async fn get<V>(&self, key: &K) -> Option<Result<Value<V>, Error>>
    where
        V: DecodeOwned,
    {
        let value = self.pairs.read_async(key, |_, v| v.clone()).await?;

        if value.is_expired(&Utc::now()) {
            self.pairs.remove_async(key).await;
            return None;
        }

        Some(
            bitcode::decode(&value)
                .map(|decoded| Value {
                    value: decoded,
                    expires_at: value.expires_at,
                })
                .map_err(|_| Error::Decoding),
        )
    }

    /// Removes a KV pair by key.
    ///
    /// Arguments:
    /// * `key` - The key of the KV pair to delete.
    ///
    /// Returns:
    /// [`bool`] - Whether a KV pair got deleted.
    pub async fn rem(&self, key: &K) -> bool {
        self.pairs.remove_async(key).await.is_some()
    }

    /// Removes a KV pair by key and returns the pair (pop).
    ///
    /// Arguments:
    /// * `key` - The key of the KV pair to pop.
    ///
    /// Returns:
    /// [`Option<Result<Value<V>, Error>>`] - `Some(Ok(V))` if there is a non-expired value,
    /// `None` if the value is non-existent or expired, or `Some(Err(Error::Decoding))` if
    /// the key exists and is not expired but the value could not be decoded to the specified type.
    pub async fn pop<V>(&self, key: &K) -> Option<Result<Value<V>, Error>>
    where
        V: DecodeOwned,
    {
        let (_key, value) = self.pairs.remove_async(key).await?;

        if value.is_expired(&Utc::now()) {
            return None;
        }

        Some(
            bitcode::decode(&value)
                .map(|decoded| Value {
                    value: decoded,
                    expires_at: value.expires_at,
                })
                .map_err(|_| Error::Decoding),
        )
    }

    /// Update the expiration datetime of a KV pair.
    ///
    /// Arguments:
    /// * `key` - The key of the KV to update the expiration datetime for.
    /// * `ttl` - The TTL of the KV pair since the moment this function is called.
    ///
    /// Returns:
    /// [`bool`] - `true` if a key's expiration datetime was updated, `false` if no KV for the
    /// provided key existed.
    pub async fn expire(&self, key: K, ttl: Duration) -> bool {
        let expires_at = Utc::now() + ttl;

        let maybe_new_value = self
            .pairs
            .read_async(&key, |_, v| v.clone())
            .await
            .map(|value| Value {
                value: value.value,
                expires_at,
            });

        match maybe_new_value {
            None => false,
            Some(new_value) => {
                self.pairs.upsert_async(key, new_value).await;
                true
            }
        }
    }

    /// Returns whether a KV pair for a key exists.
    ///
    /// Arguments:
    /// * `key` - The key to find.
    ///
    /// Returns:
    /// [`bool`] - Whether a KV pair for that key exists.
    pub async fn exists(&self, key: &K) -> bool {
        let expired = match self.pairs.read_async(key, |_, v| v.clone()).await {
            None => return false,
            Some(inner) => inner.is_expired(&Utc::now()),
        };

        if expired {
            self.pairs.remove_async(key).await;
            return false;
        }

        true
    }

    /// Inserts a value into a set by the set's key.
    ///
    /// If no set exists yet, a new set with a preallocated `capacity` is automatically created.
    ///
    /// If the item is already in the set, the TTL is updated to match the new `ttl`.
    ///
    /// Arguments:
    /// * `skey` - The key of the set to add the value to.
    /// * `item` - The value to add to the set.
    /// * `ttl` - The amount of time this item should exist for.
    /// * `capacity` - In case the set does not yet exist, a new set with this capacity will be
    ///   created. This is the minimum number of items that should fit in the preallocated space.
    pub async fn sadd<V>(&self, skey: K, item: V, ttl: Duration, capacity: usize)
    where
        V: Encode,
    {
        let expires_at = Utc::now() + ttl;

        let set = self.sets.get_async(&skey).await;

        let encoded = bitcode::encode(&item);
        let value = Value {
            value: (),
            expires_at,
        };

        if let Some(set) = set {
            set.upsert_async(encoded, value).await;
        } else {
            let set = HashMap::with_capacity(capacity.max(1));

            set.upsert_async(encoded, value).await;
            self.sets.upsert_async(skey, set).await;
        }
    }

    /// Removes an item from a set.
    ///
    /// Arguments:
    /// * `skey` - The key of the set to remove the item from.
    /// * `item` - The item to remove from the set.
    pub async fn srem<V>(&self, skey: &K, item: &V)
    where
        V: Encode,
    {
        let set = match self.sets.get_async(skey).await {
            None => return,
            Some(v) => v,
        };

        let encoded = bitcode::encode(item);

        let result = set.remove_async(&encoded).await;

        if result.is_some() && set.is_empty() {
            // Removing with a reference still alive causes a deadlock.
            drop(set);
            self.sets.remove_async(skey).await;
        }
    }

    /// Returns the amount of items in a set.
    ///
    /// Internally, this iterates through all the items in the set to make sure no item being
    /// counted has expired, so the call gets more expensive the more items in the set.
    ///
    /// Arguments:
    /// * `skey` - The key of the set.
    ///
    /// Returns:
    /// [`usize`] - The amount of items in the set, or 0 if the set doesn't exist.
    pub async fn slen(&self, skey: &K) -> usize {
        let set = match self.sets.get_async(skey).await {
            None => return 0,
            Some(v) => v,
        };

        let now = Utc::now();

        // Returning `.len()` without this check first would cause expired keys to be counted too.
        set.retain_async(|_k, v| !v.is_expired(&now)).await;

        set.len()
    }

    /// Empties a set.
    ///
    /// Arguments:
    /// * `skey` - The key of the set to empty.
    pub async fn sclear(&self, skey: &K) {
        self.sets.remove_async(skey).await;
    }

    /// Returns the expiry datetime of an item in a set.
    ///
    /// Arguments:
    /// * `skey` - The key of the set to find the item in.
    /// * `item` - The item whose expiry datetime to find in the set.
    ///
    /// Returns:
    /// [`Option<DateTime<Utc>>`] - `Some(DateTime<Utc>)` if the item is in the set and hasn't
    /// expired, or [`None`] otherwise.
    pub async fn sttl<V>(&self, skey: &K, item: &V) -> Option<DateTime<Utc>>
    where
        V: Encode,
    {
        let set = self.sets.get_async(skey).await?;

        let encoded = bitcode::encode(item);

        let item = set.read_async(&encoded, |_, v| v.clone()).await?;

        if item.is_expired(&Utc::now()) {
            set.remove_async(&encoded).await;

            if set.is_empty() {
                // Drop everything before removing, as keeping references while removing causes a
                // deadlock.
                drop(set);
                self.sets.remove_async(skey).await;
            }

            return None;
        }

        Some(item.expires_at)
    }

    /// Updates the TTL of an item in a set.
    ///
    /// If the item does not exist in the set, nothing happens.
    ///
    /// Arguments:
    /// * `skey` - The key of the set whose item the TTL to update.
    /// * `item` - The item whose TTL to update.
    /// * `ttl` - The new TTL of the item since this function is called.
    pub async fn sexpire<V>(&self, skey: &K, item: &V, ttl: Duration)
    where
        V: Encode,
    {
        let set = match self.sets.get_async(skey).await {
            None => return,
            Some(v) => v,
        };

        let encoded = bitcode::encode(item);

        let mut item = match set.get_async(&encoded).await {
            None => return,
            Some(v) => v,
        };

        let now = Utc::now();

        if item.is_expired(&now) {
            set.remove_async(&encoded).await;

            if set.is_empty() {
                // Drop everything before removing, as keeping references while removing causes a
                // deadlock.
                drop(item);
                drop(set);
                self.sets.remove_async(skey).await;
            }

            return;
        }

        let expires_at = now + ttl;

        *item = Value {
            value: (),
            expires_at,
        };
    }

    /// Checks whether an item is in a set.
    ///
    /// Arguments:
    /// * `skey` - The key of the set in which to check for the item's existance.
    /// * `item` - The item to check for its existance in the set.
    ///
    /// Returns:
    /// [`bool`] - Whether the item is in the set.
    pub async fn scontains<V>(&self, skey: &K, item: &V) -> bool
    where
        V: Encode,
    {
        let set = match self.sets.get_async(skey).await {
            None => return false,
            Some(v) => v,
        };

        let encoded = bitcode::encode(item);

        let value = match set.read_async(&encoded, |_, v| v.clone()).await {
            None => return false,
            Some(v) => v,
        };

        if value.is_expired(&Utc::now()) {
            set.remove_async(&encoded).await;

            if set.is_empty() {
                // Drop everything before removing, as keeping references while removing causes a
                // deadlock.
                drop(set);
                self.sets.remove_async(skey).await;
            }

            return false;
        }

        true
    }

    /// Returns all members in a set.
    ///
    /// Arguments:
    /// * `skey` - The set's key.
    ///
    /// Returns:
    /// [`Vec<Result<Value<V>, Error>>`] - An array with all the items in the set, each of them
    /// containing the result of their decoding individually.
    pub async fn sall<V>(&self, skey: &K) -> Vec<Result<Value<V>, Error>>
    where
        V: DecodeOwned,
    {
        let set = match self.sets.get_async(skey).await {
            None => return Vec::default(),
            Some(v) => v,
        };

        let mut values = Vec::with_capacity(set.len());

        let now = Utc::now();

        set.retain_async(|k, v| {
            if v.is_expired(&now) {
                return false;
            }

            let value = bitcode::decode::<V>(k)
                .map(|inner| Value {
                    value: inner,
                    expires_at: v.expires_at,
                })
                .map_err(|_| Error::Decoding);

            values.push(value);

            true
        })
        .await;

        values
    }

    /// Inserts a new value to a map.
    ///
    /// If there's already a value with this key in the map, the original value and its TTL are
    /// overwritten.
    ///
    /// Arguments:
    /// * `hkey` - The key of the hashmap to which insert the new value.
    /// * `key` - The key of the KV pair being inserted to the hashmap.
    /// * `value` - The value of the KV pair to insert to the hashmap.
    /// * `ttl` - The amount of time this key will live from the moment the function is run.
    /// * `capacity` - If a new hashmap is created, this value is the amount of KV pairs that will
    ///   fit in the preallocated space.
    pub async fn hset<V>(&self, hkey: K, key: K, value: V, ttl: Duration, capacity: usize)
    where
        V: Encode,
    {
        let map = self.maps.get_async(&hkey).await;

        let expires_at = Utc::now() + ttl;

        let encoded = bitcode::encode(&value);
        let value = Value {
            value: encoded,
            expires_at,
        };

        if let Some(map) = map {
            map.upsert_async(key, value).await;
        } else {
            let map = HashMap::with_capacity(std::cmp::max(capacity, 1));

            map.upsert_async(key, value).await;
            self.maps.upsert_async(hkey, map).await;
        }
    }

    /// Returns the value for a key in a map.
    ///
    /// Arguments:
    /// * `hkey` - The key of the map to get the value from.
    /// * `key` - The key whose value to get in the map.
    ///
    /// Returns:
    /// [`Option<Result<Value<V>, Error>>`] - `Some(Ok(V))` if there is a non-expired value,
    /// `None` if the value is non-existent or expired, or `Some(Err(Error::Decoding))` if
    /// the key exists and is not expired but the value could not be decoded to the specified type.
    pub async fn hget<V>(&self, hkey: &K, key: &K) -> Option<Result<Value<V>, Error>>
    where
        V: DecodeOwned,
    {
        let map = self.maps.get_async(hkey).await?;

        let value = map.read_async(key, |_, v| v.clone()).await?;

        if value.is_expired(&Utc::now()) {
            map.remove_async(key).await;

            if map.is_empty() {
                // Drop everything before removing, as keeping references while removing causes a
                // deadlock.
                drop(value);
                drop(map);
                self.maps.remove_async(hkey).await;
            }

            return None;
        }

        Some(
            bitcode::decode(&value)
                .map(|decoded| Value {
                    value: decoded,
                    expires_at: value.expires_at,
                })
                .map_err(|_| Error::Decoding),
        )
    }

    /// Removes a KV pair from a map.
    ///
    /// Arguments:
    /// * `hkey` - The key of the map in which to remove the pair.
    /// * `key` - The key of the KV pair to remove.
    pub async fn hrem(&self, hkey: &K, key: &K) {
        let map = match self.maps.get_async(hkey).await {
            None => return,
            Some(v) => v,
        };

        map.remove_async(key).await;
    }

    /// Removes a KV pair from a map and returns the value.
    ///
    /// Arguments:
    /// * `hkey` - The key of the map to remove the KV pair from.
    /// * `key` - The key of the KV pair to remove.
    ///
    /// Returns:
    /// [`Option<Result<Value<V>, Error>>`] - `Some(Ok(V))` if there is a non-expired value,
    /// `None` if the value is non-existent or expired, or `Some(Err(Error::Decoding))` if
    /// the key exists and is not expired but the value could not be decoded to the specified type.
    pub async fn hpop<V>(&self, hkey: &K, key: &K) -> Option<Result<Value<V>, Error>>
    where
        V: DecodeOwned,
    {
        let map = self.maps.get_async(hkey).await?;

        let (_key, value) = map.remove_async(key).await?;

        if map.is_empty() {
            // No dropping here since `_key` and `value` aren't locks (they're removed from the
            // map).
            drop(map);
            self.maps.remove_async(hkey).await;
        } else {
            // Dropping early to release the lock.
            drop(map);
        }

        if value.is_expired(&Utc::now()) {
            return None;
        }

        Some(
            bitcode::decode(&value)
                .map(|decoded| Value {
                    value: decoded,
                    expires_at: value.expires_at,
                })
                .map_err(|_| Error::Decoding),
        )
    }

    /// Checks whether a key exists in a map.
    ///
    /// Arguments:
    /// * `hkey` - The key of the map in which to check for the key's existance.
    ///
    /// Returns:
    /// [`bool`] - Whether the key exists or not.
    pub async fn hexists(&self, hkey: &K, key: &K) -> bool {
        let map = match self.maps.get_async(hkey).await {
            None => return false,
            Some(v) => v,
        };

        let value = match map.read_async(key, |_, v| v.clone()).await {
            None => return false,
            Some(v) => v,
        };

        if value.is_expired(&Utc::now()) {
            map.remove_async(key).await;

            if map.is_empty() {
                // Drop everything before removing, as keeping references while removing causes a
                // deadlock.
                drop(value);
                drop(map);
                self.maps.remove_async(hkey).await;
            }

            return false;
        }

        true
    }

    /// Overwrites the TTL of a key in a map.
    ///
    /// If no KV pair for such key exists in the map, nothing happens.
    ///
    /// Arguments:
    /// * `hkey` - The key of the map in which to update the key's TTL.
    /// * `key` - The key for which to overwrite the TTL.
    /// * `ttl` - The remaining time the key has before it expires.
    pub async fn hexpire(&self, hkey: &K, key: K, ttl: Duration) {
        let map = match self.maps.get_async(hkey).await {
            None => return,
            Some(v) => v,
        };

        let mut value = match map.get_async(&key).await {
            None => return,
            Some(v) => v,
        };

        let now = Utc::now();

        if value.is_expired(&now) {
            map.remove_async(&key).await;

            if map.is_empty() {
                // Drop everything before removing, as keeping references while removing causes a
                // deadlock.
                drop(value);
                drop(map);
                self.maps.remove_async(hkey).await;
            }

            return;
        }

        value.expires_at = now + ttl;
    }

    /// Returns the amount of keys in a map.
    ///
    /// Arguments:
    /// * `hkey` - The map whose length to return.
    ///
    /// Returns:
    /// [`usize`] - The amount of keys in the map.
    pub async fn hlen(&self, hkey: &K) -> usize {
        let map = match self.maps.get_async(hkey).await {
            None => return 0,
            Some(v) => v,
        };

        let now = Utc::now();

        // Returning `.len()` without this check first would cause expired keys to be counted too.
        // Consider caching the length of the map using atomic ops (which has to be updated every
        // time an item is added or removed).
        map.retain_async(|_k, v| !v.is_expired(&now)).await;

        if map.is_empty() {
            // Drop before removing as not doing so causes a deadlock.
            drop(map);
            self.maps.remove_async(hkey).await;
            return 0;
        }

        map.len()
    }

    /// Empties all keys in a map.
    ///
    /// Arguments:
    /// * `hkey` - The key of the map to empty.
    pub async fn hclear(&self, hkey: &K) {
        self.maps.remove_async(hkey).await;
    }

    /// Appends (adds to the end) an item to the deque.
    ///
    /// Arguments:
    /// * `dkey` - The key of the deque.
    /// * `value` - The value to append to the deque.
    /// * `ttl` - The time this value will be valid for since the call of this function.
    /// * `capacity` - If a new deque is created, this is the amount of items that will fit
    ///   (at least) in the preallocated space.
    pub async fn dappend<V>(&self, dkey: K, value: V, ttl: Duration, capacity: usize)
    where
        V: Encode,
    {
        let deque = self.deques.get_async(&dkey).await;

        let encoded = bitcode::encode(&value);
        let value = Value {
            value: encoded,
            expires_at: Utc::now() + ttl,
        };

        if let Some(mut deque) = deque {
            deque.push_back(value);
        } else {
            let mut deque = VecDeque::with_capacity(capacity.max(1));

            deque.push_back(value);
            self.deques.upsert_async(dkey, deque).await;
        }
    }

    /// Prepends (adds to the front) an item to the deque.
    ///
    /// Arguments:
    /// * `dkey` - The key of the deque.
    /// * `value` - The value to prepend to the deque.
    /// * `ttl` - The time this value will be valid for since the call of this function.
    /// * `capacity` - If a new deque is created, this is the amount of items that will fit
    ///   (at least) in the preallocated space.
    pub async fn dprepend<V>(&self, dkey: K, value: V, ttl: Duration, capacity: usize)
    where
        V: Encode,
    {
        let deque = self.deques.get_async(&dkey).await;

        let encoded = bitcode::encode(&value);
        let value = Value {
            value: encoded,
            expires_at: Utc::now() + ttl,
        };

        if let Some(mut deque) = deque {
            deque.push_front(value);
        } else {
            let mut deque = VecDeque::with_capacity(capacity.max(1));

            deque.push_front(value);
            self.deques.upsert_async(dkey, deque).await;
        }
    }

    /// Removes and returns the first item of the deque.
    ///
    /// Arguments:
    /// * `dkey` - The key of the deque.
    ///
    /// Returns:
    /// [`Option<Result<Value<V>, Error>>`] - `Some(Ok(V))` if there is a non-expired value,
    /// `None` if the deque is empty, or `Some(Err(Error::Decoding))` if the key exists and
    /// is not expired but the value could not be decoded to the specified type.
    pub async fn dfpop<V>(&self, dkey: &K) -> Option<Result<Value<V>, Error>>
    where
        V: DecodeOwned,
    {
        let mut deque = self.deques.get_async(dkey).await?;

        let now = Utc::now();

        let value = loop {
            let value = deque.pop_front()?;

            if value.is_expired(&now) {
                continue;
            }

            break Some(value);
        }?;

        if deque.is_empty() {
            // Cannot call remove_async while having a reference to the item, as it causes a
            // deadlock. Dropping the reference also allows other threads to start using the deque,
            // which is better for concurrency. (Waiting would cause every other thread trying to
            // access the deque to have to wait for the decoding to finish).
            drop(deque);
            self.deques.remove_async(dkey).await;
        } else {
            // Cannot drop earlier because of this if's condition.
            drop(deque);
        }

        Some(
            bitcode::decode(&value)
                .map(|decoded| Value {
                    value: decoded,
                    expires_at: value.expires_at,
                })
                .map_err(|_| Error::Decoding),
        )
    }

    /// Removes and returns the last item of the deque.
    ///
    /// Arguments:
    /// * `dkey` - The key of the deque.
    ///
    /// Returns:
    /// [`Option<Result<Value<V>, Error>>`] - `Some(Ok(V))` if there is a non-expired value,
    /// `None` if the deque is empty, or `Some(Err(Error::Decoding))` if the key exists and
    /// is not expired but the value could not be decoded to the specified type.
    pub async fn dbpop<V>(&self, dkey: &K) -> Option<Result<Value<V>, Error>>
    where
        V: DecodeOwned,
    {
        let mut deque = self.deques.get_async(dkey).await?;

        let now = Utc::now();

        let value = loop {
            let value = deque.pop_back()?;

            if value.is_expired(&now) {
                continue;
            }

            break Some(value);
        }?;

        if deque.is_empty() {
            // Cannot call remove_async while having a reference to the item, as it causes a
            // deadlock. Dropping the reference also allows other threads to start using the deque,
            // which is better for concurrency. (Waiting would cause every other thread trying to
            // access the deque to have to wait for the decoding to finish).
            drop(deque);
            self.deques.remove_async(dkey).await;
        } else {
            // Cannot drop earlier because of this if's condition.
            drop(deque);
        }

        Some(
            bitcode::decode(&value)
                .map(|decoded| Value {
                    value: decoded,
                    expires_at: value.expires_at,
                })
                .map_err(|_| Error::Decoding),
        )
    }

    /// Removes and returns an item of the deque.
    ///
    /// In case of a non-existent index, this function does nothing (and returns [`None`]).
    ///
    /// Arguments:
    /// * `dkey` - The key of the deque.
    /// * `index` - The index of the item to pop in the deque.
    ///
    /// Returns:
    /// [`Option<Result<Value<V>, Error>>`] - `Some(Ok(V))` if there is a non-expired value,
    /// `None` if the deque is empty, or `Some(Err(Error::Decoding))` if the key exists and
    /// is not expired but the value could not be decoded to the specified type.
    pub async fn dpop<V>(&self, dkey: &K, index: usize) -> Option<Result<Value<V>, Error>>
    where
        V: DecodeOwned,
    {
        let mut deque = self.deques.get_async(dkey).await?;

        // Not removing all expired items before draining would cause issues with indexes being
        // misplaced because of having counted expired items too.
        let now = Utc::now();
        deque.retain(|i| !i.is_expired(&now));

        let value = deque.drain(index..=index).next()?;

        if deque.is_empty() {
            // Drop before removing as not doing so causes a deadlock.
            drop(deque);
            self.deques.remove_async(dkey).await;
        } else {
            // Early dropping to allow other threads to start using the deque early.
            drop(deque);
        }

        Some(
            bitcode::decode(&value)
                .map(|decoded| Value {
                    value: decoded,
                    expires_at: value.expires_at,
                })
                .map_err(|_| Error::Decoding),
        )
    }

    /// Removes the first item of the deque.
    ///
    /// Arguments:
    /// * `dkey` - The key of the deque.
    pub async fn dfrem(&self, dkey: &K) {
        let mut deque = match self.deques.get_async(dkey).await {
            Some(v) => v,
            None => return,
        };

        let now = Utc::now();

        loop {
            let value = match deque.pop_front() {
                Some(v) => v,
                None => {
                    // Drop before removing as not doing so causes a deadlock.
                    drop(deque);
                    self.deques.remove_async(dkey).await;

                    break;
                }
            };

            if !value.is_expired(&now) {
                break;
            }
        }
    }

    /// Removes the last item of the deque.
    ///
    /// Arguments:
    /// * `dkey` - The key of the deque.
    pub async fn dbrem(&self, dkey: &K) {
        let mut deque = match self.deques.get_async(dkey).await {
            Some(v) => v,
            None => return,
        };

        let now = Utc::now();

        loop {
            let value = match deque.pop_back() {
                Some(v) => v,
                None => {
                    // Drop before removing as not doing so causes a deadlock.
                    drop(deque);
                    self.deques.remove_async(dkey).await;

                    break;
                }
            };

            if !value.is_expired(&now) {
                break;
            }
        }
    }

    /// Removes an item of the deque.
    ///
    /// In case of a non-existent index, this function does nothing.
    ///
    /// Arguments:
    /// * `dkey` - The key of the deque.
    /// * `index` - The index of the item in the deque.
    pub async fn drem(&self, dkey: &K, index: usize) {
        let mut deque = match self.deques.get_async(dkey).await {
            Some(v) => v,
            None => return,
        };

        // Not removing all expired items before draining would cause issues with indexes being
        // misplaced because of having counted expired items too.
        let now = Utc::now();
        deque.retain(|i| !i.is_expired(&now));

        deque.drain(index..=index);

        if deque.is_empty() {
            // Drop before removing, as not doing so causes a deadlock.
            drop(deque);
            self.deques.remove_async(dkey).await;
        }
    }

    /// Returns the first item of the deque.
    ///
    /// Arguments:
    /// * `dkey` - The key of the deque.
    ///
    /// Returns:
    /// [`Option<Result<Value<V>, Error>>`] - `Some(Ok(V))` if there is a non-expired value,
    /// `None` if the deque is empty, or `Some(Err(Error::Decoding))` if the key exists and
    /// is not expired but the value could not be decoded to the specified type.
    pub async fn dfpeek<V>(&self, dkey: &K) -> Option<Result<Value<V>, Error>>
    where
        V: DecodeOwned,
    {
        let mut deque = self.deques.get_async(dkey).await?;

        let now = Utc::now();

        let value = loop {
            let value = match deque.front() {
                Some(v) => v,
                None => {
                    drop(deque);
                    self.deques.remove_async(dkey).await;
                    return None;
                }
            };

            if value.is_expired(&now) {
                deque.pop_front();
                continue;
            }

            break Some(value);
        }?;

        Some(
            bitcode::decode(value)
                .map(|decoded| Value {
                    value: decoded,
                    expires_at: value.expires_at,
                })
                .map_err(|_| Error::Decoding),
        )
    }

    /// Returns the last item of the deque.
    ///
    /// Arguments:
    /// * `dkey` - The key of the deque.
    ///
    /// Returns:
    /// [`Option<Result<Value<V>, Error>>`] - `Some(Ok(V))` if there is a non-expired value,
    /// `None` if the deque is empty, or `Some(Err(Error::Decoding))` if the key exists and
    /// is not expired but the value could not be decoded to the specified type.
    pub async fn dbpeek<V>(&self, dkey: &K) -> Option<Result<Value<V>, Error>>
    where
        V: DecodeOwned,
    {
        let mut deque = self.deques.get_async(dkey).await?;

        let now = Utc::now();

        let value = loop {
            let value = match deque.back() {
                Some(v) => v,
                None => {
                    drop(deque);
                    self.deques.remove_async(dkey).await;
                    return None;
                }
            };

            if value.is_expired(&now) {
                deque.pop_back();
                continue;
            }

            break Some(value);
        }?;

        Some(
            bitcode::decode(value)
                .map(|decoded| Value {
                    value: decoded,
                    expires_at: value.expires_at,
                })
                .map_err(|_| Error::Decoding),
        )
    }

    /// Returns an item of the deque.
    ///
    /// Arguments:
    /// * `dkey` - The key of the deque.
    /// * `index` - The index of the item in the deque to return.
    ///
    /// Returns:
    /// [`Option<Result<Value<V>, Error>>`] - `Some(Ok(V))` if there is a non-expired value,
    /// `None` if the deque is empty, or `Some(Err(Error::Decoding))` if the key exists and
    /// is not expired but the value could not be decoded to the specified type.
    pub async fn dpeek<V>(&self, dkey: &K, index: usize) -> Option<Result<Value<V>, Error>>
    where
        V: DecodeOwned,
    {
        let mut deque = self.deques.get_async(dkey).await?;

        // Not removing all expired items before draining would cause issues with indexes being
        // misplaced because of having counted expired items too.
        let now = Utc::now();
        deque.retain(|i| !i.is_expired(&now));

        if deque.is_empty() {
            // Drop before removing not to cause a deadlock.
            drop(deque);
            self.deques.remove_async(dkey).await;
            return None;
        }

        let item = (*deque).get(index)?;

        Some(
            bitcode::decode(item)
                .map(|decoded| Value {
                    value: decoded,
                    expires_at: item.expires_at,
                })
                .map_err(|_| Error::Decoding),
        )
    }

    /// Returns the amount of items in a deque.
    ///
    /// Arguments:
    /// * `dkey` - The key of the deque.
    ///
    /// Returns:
    /// [`usize`] - The amount of items in the deque.
    pub async fn dlen(&self, dkey: &K) -> usize {
        let mut deque = match self.deques.get_async(dkey).await {
            Some(v) => v,
            None => return 0,
        };

        // Not removing all expired items before draining would cause issues with indexes being
        // misplaced because of having counted expired items too.
        let now = Utc::now();
        deque.retain(|i| !i.is_expired(&now));

        if deque.is_empty() {
            // Drop before removing not to cause a deadlock.
            drop(deque);
            self.deques.remove_async(dkey).await;
            return 0;
        }

        deque.len()
    }

    /// Updates the expiration time of an item in a deque.
    ///
    /// If the item is non-existent (invalid index/empty deque) this function does nothing.
    ///
    /// Arguments:
    /// * `dkey` - The key of the deque.
    /// * `index` - The index of the item in the deque to update the TTL for.
    /// * `ttl` - The remaining time left to live of the item in the deque.
    pub async fn dexpire(&self, dkey: &K, index: usize, ttl: Duration) {
        let mut deque = match self.deques.get_async(dkey).await {
            Some(v) => v,
            None => return,
        };

        // Not removing all expired items before draining would cause issues with indexes being
        // misplaced because of having counted expired items too.
        let now = Utc::now();
        deque.retain(|i| !i.is_expired(&now));

        if deque.is_empty() {
            // Drop before removing not to cause a deadlock.
            drop(deque);
            self.deques.remove_async(dkey).await;
            return;
        }

        let item = match (*deque).get_mut(index) {
            Some(v) => v,
            None => return,
        };

        item.expires_at = now + ttl;
    }

    /// Removes all values in a deque.
    ///
    /// Arguments:
    /// * `dkey` - The key of the deque.
    pub async fn dclear(&self, dkey: &K) {
        self.deques.remove_async(dkey).await;
    }

    /// Runs a function over each of the items in a deque and overrides their values with the
    /// returned value of the function.
    ///
    /// The function takes one `item` argument, it being [`Value<Result<V, Error>>`]. The function
    /// returns [`Option<Value<V>>`] (which is not the same as the `item` argument's type). The
    /// [Result] type of the [Value]'s inner value may be an error if the inner value cannot be
    /// decoded into the provided data type.
    ///
    /// The function returns either [None], meaning that the value stays as-is, or
    /// [`Some<Value<Result<T, Error>>>`] which replaces the old value with the function's returned
    /// value. You may update the value's expiry time even if the inner value failed to decode by
    /// setting the original value to the returned [Value]'s inner value.
    ///
    /// Arguments:
    /// * `dkey` - The key of the deque.
    /// * `f` - The function to be called for each item in the deque.
    pub async fn dmap<V>(
        &self,
        dkey: &K,
        f: impl Fn(Value<Result<V, Error>>) -> Option<Value<Result<V, Error>>>,
    ) where
        V: Encode + DecodeOwned,
    {
        let mut deque = match self.deques.get_async(dkey).await {
            Some(v) => v,
            None => return,
        };

        let now = Utc::now();

        deque.retain_mut(|v| {
            if v.is_expired(&now) {
                return false;
            }

            let decoded = bitcode::decode::<V>(v).map_err(|_| Error::Decoding);

            let new_value = f(Value {
                value: decoded,
                expires_at: v.expires_at,
            });

            if let Some(new_value) = new_value {
                if let Ok(inner) = new_value.value {
                    let encoded = bitcode::encode(&inner);

                    *v = Value {
                        value: encoded,
                        expires_at: new_value.expires_at,
                    };
                } else {
                    v.expires_at = new_value.expires_at;
                }
            }

            true
        });

        if deque.is_empty() {
            // Drop before removing not to cause a deadlock.
            drop(deque);
            self.deques.remove_async(dkey).await;
        }
    }

    /// Returns all items in a deque.
    ///
    /// This function returns a cloned version of the deque.
    ///
    /// Arguments:
    /// * `dkey` - The key of the deque.
    ///
    /// Returns:
    /// [`VecDeque<Result<Value<V>, Error>>`] - The deque with its values, each with its own inner
    /// decoding result. This will be an empty deque if the deque does not exist.
    pub async fn dall<V>(&self, dkey: &K) -> VecDeque<Result<Value<V>, Error>>
    where
        V: DecodeOwned,
    {
        let mut deque = match self.deques.get_async(dkey).await {
            Some(v) => v,
            None => return VecDeque::new(),
        };

        // Not removing all expired items before draining would cause issues with indexes being
        // misplaced because of having counted expired items too.
        let now = Utc::now();
        deque.retain(|i| !i.is_expired(&now));

        if deque.is_empty() {
            // Drop before removing not to cause a deadlock.
            drop(deque);
            self.deques.remove_async(dkey).await;
            return VecDeque::new();
        }

        (*deque)
            .iter()
            .map(|i| {
                bitcode::decode(i)
                    .map(|decoded| Value {
                        value: decoded,
                        expires_at: i.expires_at,
                    })
                    .map_err(|_| Error::Decoding)
            })
            .collect()
    }

    /// Retains only the first `len` items of the deque.
    ///
    /// Arguments:
    /// * `dkey` - The key of the deque.
    /// * `len` - The amount of items to retain.
    pub async fn dftruncate(&self, dkey: &K, len: usize) {
        let mut deque = match self.deques.get_async(dkey).await {
            Some(v) => v,
            None => return,
        };

        // Not removing all expired items before draining would cause issues with indexes being
        // misplaced because of having counted expired items too.
        let now = Utc::now();
        deque.retain(|i| !i.is_expired(&now));

        if deque.is_empty() {
            // Drop before removing not to cause a deadlock.
            drop(deque);
            self.deques.remove_async(dkey).await;
            return;
        }

        deque.truncate(len);
    }

    /// Retains only the last `len` items of the deque.
    ///
    /// Arguments:
    /// * `dkey` - The key of the deque.
    /// * `len` - The amount of items to retain.
    pub async fn dbtruncate(&self, dkey: &K, len: usize) {
        let mut deque = match self.deques.get_async(dkey).await {
            Some(v) => v,
            None => return,
        };

        // Not removing all expired items before draining would cause issues with indexes being
        // misplaced because of having counted expired items too.
        let now = Utc::now();
        deque.retain(|i| !i.is_expired(&now));

        if deque.is_empty() {
            // Drop before removing not to cause a deadlock.
            drop(deque);
            self.deques.remove_async(dkey).await;
            return;
        }

        let idx = (deque.len().saturating_sub(len)).max(0);
        deque.drain(..idx);
    }
}

impl<K> Default for TurboStore<K>
where
    K: Hash + Eq,
{
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, Encode, Decode, PartialEq, Eq, Clone)]
    struct TestValue {
        number: i32,
        string: String,
    }

    #[pollster::test]
    async fn test_kv_operations() {
        // Tested:
        // - set
        // - get
        // - rem
        // - pop
        // - incr
        // - decr
        // - expire
        // - exists

        let store: TurboStore<i32> = TurboStore::with_capacity(2);

        let value_1 = TestValue {
            number: 123,
            string: "Hello world!".into(),
        };
        let value_2 = TestValue {
            number: 456,
            string: "Bye bye world!".into(),
        };

        store.set(1, &value_1, Duration::minutes(1)).await;

        // Test the insert happening twice and being properly overwritten.
        store.set(2, &value_1, Duration::minutes(1)).await;
        store.set(2, &value_2, Duration::minutes(1)).await;

        let retrieved_1 = store
            .get::<TestValue>(&1)
            .await
            .expect("There was no KV pair with key 1")
            .expect("The retrieved KV 1 failed to be decoded");

        let now = Utc::now();
        // Leave a threshold for the code up to have time to execute.
        assert!(
            retrieved_1.expires_at > now + Duration::seconds(59)
                && retrieved_1.expires_at <= now + Duration::minutes(1),
            "The expiry time for key 1 was not between 1 minute and 59 seconds in the future."
        );

        assert_eq!(
            *retrieved_1, value_1,
            "The retrieved value for key 1 was not the same value than the one set previously."
        );

        store.expire(1, Duration::seconds(30)).await;

        let popped_1 = store
            .pop::<TestValue>(&1)
            .await
            .expect("There was no value to pop for key 1")
            .expect("The popped KV 1 failed to be decoded");

        let now = Utc::now();
        // Leave a threshold for the code up to have time to execute.
        assert!(
            popped_1.expires_at > now + Duration::seconds(29)
                && popped_1.expires_at <= now + Duration::seconds(30),
            "The expiry time for key 1 was not between 29 and 30 seconds in the future, even after its TTL was updated."
        );

        assert_eq!(
            *popped_1, value_1,
            "The retrieved value for key 1 was not the same value than the one set previously."
        );

        let retrieved_after_popped_1 = store.get::<TestValue>(&1).await;

        assert!(
            retrieved_after_popped_1.is_none(),
            "The value for key 1 was there even after popped."
        );

        assert!(
            store.exists(&2).await,
            "The key 2 did not exist even after it was added."
        );

        store.rem(&2).await;

        assert!(
            !store.exists(&2).await,
            "The key 2 existed even after it was removed."
        );

        let retrieved_after_removed_2 = store.get::<TestValue>(&2).await;

        assert!(
            retrieved_after_removed_2.is_none(),
            "The value for key 2 was there even after removed."
        );

        store.set(1, &1, Duration::minutes(1)).await;

        let incr_1 = store
            .get::<i32>(&1)
            .await
            .expect("There was no value for key 1 after setting to a previously-removed key")
            .expect(
                "The value could not be decoded into i32 after setting to a previously-removed key",
            );

        let now = Utc::now();
        // Leave a threshold for the code up to have time to execute.
        assert!(
            incr_1.expires_at > now + Duration::seconds(59)
                && popped_1.expires_at <= now + Duration::minutes(1),
            "The expiry time for key 1 was not between 59 seconds and a minute in the future after setting to a previously-removed key."
        );

        assert_eq!(
            *incr_1, 1,
            "The value of key 1 was not 1 after setting to a previously-removed key"
        );

        store
            .incr(1, 2, Duration::minutes(5))
            .await
            .expect("INCR failed to run because the old value could not be decoded");

        let incr_1 = store
            .get::<i32>(&1)
            .await
            .expect("There was no value for key 1 after incrementing")
            .expect("The value could not be decoded into i32 after incrementing");

        assert_eq!(
            *incr_1, 3,
            "The value of key 1 was not properly incremented by INCR"
        );

        assert!(
            incr_1.expires_at > now + Duration::seconds(60 * 4 + 59)
                && popped_1.expires_at <= now + Duration::minutes(5),
            "The expiry time for key 1 was not between 4:59 min and 5:00 min in the future after setting to a previously-removed key."
        );

        store
            .decr(1, 1, Duration::minutes(1))
            .await
            .expect("Could not decode the old value to i32 before decrementing");

        let incr_1 = store
            .get::<i32>(&1)
            .await
            .expect("There was no value for key 1 after decrementing after incrementing")
            .expect(
                "The value could not be decoded into i32 after decrementing after incrementing",
            );

        let now = Utc::now();
        // Leave a threshold for the code up to have time to execute.
        assert!(
            incr_1.expires_at > now + Duration::seconds(59)
                && popped_1.expires_at <= now + Duration::minutes(1),
            "The expiry time for key 1 was not between 59 seconds and a minute in the future after decrementing after incrementing."
        );

        assert_eq!(
            *incr_1, 2,
            "The value of key 1 was not 1 after decrementing after incrementing"
        );

        store.set(1, &0, Duration::minutes(1)).await;

        store
            .decr(1, i32::MAX, Duration::minutes(1))
            .await
            .expect("Could not decode the old value when decrementing by i32::MAX");
        store.decr(1, i32::MAX, Duration::minutes(1)).await.expect(
            "Could not decode the old value when decrementing by i32::MAX for the second time",
        );

        let decr_1 = store
            .get::<i32>(&1)
            .await
            .expect("There was no value for key 1 after decrementing after incrementing")
            .expect("The value could not be decoded into i32 after decrementing by i32::MAX");

        assert_eq!(
            *decr_1,
            i32::MIN,
            "The value of key 1 was not saturated to i32::MIN after decrementing by i32::MAX"
        );

        store.set(1, &0, Duration::minutes(1)).await;

        store
            .incr(1, i32::MAX, Duration::minutes(1))
            .await
            .expect("Could not decode the old value when incrementing by i32::MAX");
        store.incr(1, i32::MAX, Duration::minutes(1)).await.expect(
            "Could not decode the old value when incrementing by i32::MAX for the second time",
        );

        let incr_1 = store
            .get::<i32>(&1)
            .await
            .expect("There was no value for key 1 after incrementing after resetting")
            .expect("The value could not be decoded into i32 after incrementing by i32::MAX");

        assert_eq!(
            *incr_1,
            i32::MAX,
            "The value of key 1 was not saturated to i32::MAX after incrementing by i32::MAX"
        );
    }

    #[pollster::test]
    async fn test_set_operations() {
        // Tested:
        // - sadd
        // - srem
        // - scontains
        // - slen
        // - sttl
        // - sclear
        // - sall

        let store: TurboStore<i32> = TurboStore::with_capacity(4);

        // Double 1 and 3 is added to test deduplication, no 4 to test unexistent keys.
        store.sadd(1, 1, Duration::minutes(1), 5).await;
        store.sadd(1, 1, Duration::minutes(1), 5).await;
        store.sadd(1, 2, Duration::minutes(1), 5).await;
        store.sadd(1, 3, Duration::minutes(1), 5).await;
        store.sadd(1, 3, Duration::minutes(1), 5).await;
        store.sadd(1, 5, Duration::minutes(1), 5).await;

        assert!(store.scontains(&1, &1).await, "1 was not in the set");
        assert!(store.scontains(&1, &2).await, "2 was not in the set");
        assert!(store.scontains(&1, &3).await, "3 was not in the set");
        assert!(!store.scontains(&1, &4).await, "4 was in the set");
        assert!(store.scontains(&1, &5).await, "5 was not in the set");
        assert_eq!(store.slen(&1).await, 4, "The set's length was not 4");

        store.srem(&1, &1).await;

        assert!(!store.scontains(&1, &1).await, "1 was in the set");
        assert_eq!(store.slen(&1).await, 3, "The set's length was not 3");

        // Run for a second time to test for any weird deadlocks.
        store.srem(&1, &1).await;

        store.sclear(&1).await;
        assert_eq!(store.slen(&1).await, 0, "The set's length was not 0");

        store.sadd(1, 1, Duration::minutes(1), 2).await;
        store.sadd(1, 2, Duration::minutes(1), 2).await;

        let item_ttl = store
            .sttl(&1, &1)
            .await
            .expect("There was no TTL for item 1 in set");

        let now = Utc::now();
        assert!(
            item_ttl > now + Duration::seconds(59) && item_ttl <= now + Duration::minutes(1),
            "The expiry time for item 1 was not between 59 seconds and a minute in the future"
        );

        store.sexpire(&1, &1, Duration::seconds(30)).await;

        let item_ttl = store
            .sttl(&1, &1)
            .await
            .expect("There was no TTL for item 1 in set after updating its expiry time");

        let now = Utc::now();
        assert!(
            item_ttl > now + Duration::seconds(29) && item_ttl <= now + Duration::seconds(30),
            "The expiry time for item 1 was not between 29 and 30 seconds in the future"
        );

        let items = store.sall::<i32>(&1).await;

        let nums: Vec<_> = items.into_iter().map(|i| *i.unwrap()).collect();

        assert!(
            nums.iter().all(|i| [1, 2].contains(i)),
            "Not all items were either 1 or 2"
        );
        assert!(
            [1, 2].iter().all(|i| nums.contains(i)),
            "Not both 1 and 2 were in the set"
        );
        assert_eq!(
            nums.len(),
            2,
            "The set didn't have the expected amount of items"
        );
    }

    #[pollster::test]
    async fn test_deque_operations() {
        // Tested:
        // - dappend
        // - dprepend
        // - dbpop
        // - dfpop
        // - dpop
        // - dfrem
        // - dbrem
        // - drem
        // - dfpeek
        // - dbpeek
        // - dpeek
        // - dall
        // - dlen
        // - dexpire
        // - dclear
        // - dmap
        // - dftruncate
        // - dbtruncate

        let store: TurboStore<i32> = TurboStore::new();

        store.dappend(1, 1, Duration::minutes(1), 3).await;
        store.dappend(1, 2, Duration::minutes(1), 3).await;
        store.dappend(1, 3, Duration::minutes(1), 3).await;

        assert_eq!(
            store.dlen(&1).await,
            3,
            "The deque's length was not of the expected size"
        );

        let nums: Vec<i32> = store
            .dall::<i32>(&1)
            .await
            .into_iter()
            .map(|i| *i.unwrap())
            .collect();

        assert_eq!(nums, [1, 2, 3], "The deque's items are not 1, 2, and 3");

        assert_eq!(
            *store.dbpop::<i32>(&1).await.unwrap().unwrap(),
            3,
            "The bpopped item was not 3"
        );
        assert_eq!(
            *store.dbpop::<i32>(&1).await.unwrap().unwrap(),
            2,
            "The bpopped item was not 2"
        );
        assert_eq!(
            *store.dbpop::<i32>(&1).await.unwrap().unwrap(),
            1,
            "The bpopped item was not 1"
        );
        assert!(
            store.dbpop::<i32>(&1).await.is_none(),
            "The bpopped item was not None"
        );

        store.dprepend(1, 1, Duration::minutes(1), 3).await;
        store.dprepend(1, 2, Duration::minutes(1), 3).await;
        store.dprepend(1, 3, Duration::minutes(1), 3).await;

        assert_eq!(
            store.dlen(&1).await,
            3,
            "The deque's length was not of the expected size"
        );

        let nums: Vec<i32> = store
            .dall(&1)
            .await
            .into_iter()
            .map(|i| *i.unwrap())
            .collect();

        assert_eq!(nums, [3, 2, 1], "The deque's items are not 3, 2, and 1");

        assert_eq!(
            *store.dfpop::<i32>(&1).await.unwrap().unwrap(),
            3,
            "The fpopped item was not 3"
        );
        assert_eq!(
            *store.dfpop::<i32>(&1).await.unwrap().unwrap(),
            2,
            "The fpopped item was not 2"
        );
        assert_eq!(
            *store.dfpop::<i32>(&1).await.unwrap().unwrap(),
            1,
            "The fpopped item was not 1"
        );
        assert!(
            store.dfpop::<i32>(&1).await.is_none(),
            "The fpopped item was not None"
        );

        assert_eq!(
            store.dlen(&1).await,
            0,
            "The deque was not empty even after fully cleaning it up"
        );

        store.dappend(1, 1, Duration::minutes(1), 3).await;
        store.dappend(1, 2, Duration::minutes(1), 3).await;
        store.dappend(1, 3, Duration::minutes(1), 3).await;

        assert_eq!(
            *store.dpop::<i32>(&1, 1).await.unwrap().unwrap(),
            2,
            "The dpopped item was not 2"
        );
        assert_eq!(
            store.dlen(&1).await,
            2,
            "The deque's length was not what was expected"
        );

        store.dclear(&1).await;

        assert_eq!(
            store.dlen(&1).await,
            0,
            "The deque was not empty even after clearing it up"
        );

        store.dappend(1, 1, Duration::minutes(1), 3).await;
        store.dappend(1, 2, Duration::minutes(1), 3).await;
        store.dappend(1, 3, Duration::minutes(1), 3).await;

        store.dfrem(&1).await;

        assert_eq!(
            *store.dfpeek::<i32>(&1).await.unwrap().unwrap(),
            2,
            "The fpeeked item was not 2"
        );
        assert_eq!(
            store.dlen(&1).await,
            2,
            "The deque's length was not what was expected after removing an item with frem"
        );

        store.dfrem(&1).await;

        assert_eq!(
            *store.dfpeek::<i32>(&1).await.unwrap().unwrap(),
            3,
            "The fpeeked item was not 2"
        );
        assert_eq!(
            store.dlen(&1).await,
            1,
            "The deque's length was not what was expected after removing an item with frem"
        );

        store.dfrem(&1).await;

        assert!(
            store.dfpeek::<i32>(&1).await.is_none(),
            "The fpeeked item was not 2"
        );
        assert_eq!(
            store.dlen(&1).await,
            0,
            "The deque's length was not what was expected after removing an item with frem"
        );

        // Remove on an empty deque, this should cause nothing.
        store.dfrem(&1).await;

        for i in 1..=3 {
            store.dappend(1, i, Duration::minutes(1), 5).await;
        }

        store.dbrem(&1).await;

        assert_eq!(
            *store.dbpeek::<i32>(&1).await.unwrap().unwrap(),
            2,
            "The bpeeked item was not 2"
        );
        assert_eq!(
            store.dlen(&1).await,
            2,
            "The deque's length was not what was expected after removing an item with brem"
        );

        store.dbrem(&1).await;

        assert_eq!(
            *store.dbpeek::<i32>(&1).await.unwrap().unwrap(),
            1,
            "The bpeeked item was not 1"
        );
        assert_eq!(
            store.dlen(&1).await,
            1,
            "The deque's length was not what was expected after removing an item with brem"
        );

        store.dbrem(&1).await;

        assert!(
            store.dbpeek::<i32>(&1).await.is_none(),
            "The bpeeked item was not 2"
        );
        assert_eq!(
            store.dlen(&1).await,
            0,
            "The deque's length was not what was expected after removing an item with brem"
        );

        for i in 1..=5 {
            store.dappend(1, i, Duration::minutes(1), 5).await;
        }

        store.drem(&1, 2).await;

        let nums: Vec<i32> = store
            .dall(&1)
            .await
            .into_iter()
            .map(|i| *i.unwrap())
            .collect();

        assert_eq!(
            nums,
            [1, 2, 4, 5],
            "The deque's items are not 1, 2, 4, and 5"
        );

        store.drem(&1, 2).await;

        let nums: Vec<i32> = store
            .dall(&1)
            .await
            .into_iter()
            .map(|i| *i.unwrap())
            .collect();

        assert_eq!(nums, [1, 2, 5], "The deque's items are not 1, 2, and 5");
        assert_eq!(
            *store.dpeek::<i32>(&1, 0).await.unwrap().unwrap(),
            1,
            "dpeek did not return 1"
        );
        assert_eq!(
            *store.dpeek::<i32>(&1, 1).await.unwrap().unwrap(),
            2,
            "dpeek did not return 2"
        );
        assert_eq!(
            *store.dpeek::<i32>(&1, 2).await.unwrap().unwrap(),
            5,
            "dpeek did not return 5"
        );
        assert!(
            store.dpeek::<i32>(&1, 3).await.is_none(),
            "dpeek did not return None on an index out of bounds"
        );

        store.dappend(1, 1, Duration::minutes(1), 1).await;

        let item_ttl = store.dfpeek::<i32>(&1).await.unwrap().unwrap().expires_at;

        let now = Utc::now();
        assert!(
            item_ttl > now + Duration::seconds(59) && item_ttl <= now + Duration::minutes(1),
            "The expiry time for item was not between 59 seconds and a minute in the future"
        );

        store.dexpire(&1, 0, Duration::seconds(30)).await;

        let item_ttl = store.dfpeek::<i32>(&1).await.unwrap().unwrap().expires_at;

        let now = Utc::now();
        assert!(
            item_ttl > now + Duration::seconds(29) && item_ttl <= now + Duration::seconds(30),
            "The expiry time for item was not between 29 and 30 seconds in the future"
        );

        store.dclear(&1).await;

        // Expire a non-existent item.
        store.dexpire(&1, 0, Duration::seconds(30)).await;

        for i in 1..=20 {
            store.dappend(1, i, Duration::minutes(1), 20).await;
        }

        assert_eq!(
            store.dlen(&1).await,
            20,
            "The deque's length was not what was expected after adding 20 items"
        );

        store.dftruncate(&1, 15).await;

        assert_eq!(
            store.dlen(&1).await,
            15,
            "The deque's length was not what was expected after ftruncating it"
        );

        let nums: Vec<i32> = store
            .dall(&1)
            .await
            .into_iter()
            .map(|i| *i.unwrap())
            .collect();

        assert_eq!(
            nums,
            (1..=15).collect::<Vec<_>>(),
            "The deque's items are not 1..=15 after ftruncating it"
        );

        store.dbtruncate(&1, 10).await;

        assert_eq!(
            store.dlen(&1).await,
            10,
            "The deque's length was not what was expected after btruncating it"
        );

        let nums: Vec<i32> = store
            .dall(&1)
            .await
            .into_iter()
            .map(|i| *i.unwrap())
            .collect();

        assert_eq!(
            nums,
            (6..=15).collect::<Vec<_>>(),
            "The deque's items are not 5..=15 after btruncating it"
        );

        store.dftruncate(&1, 100).await;
        store.dbtruncate(&1, 100).await;

        assert_eq!(
            store.dlen(&1).await,
            10,
            "The deque's length was not 10 after btruncating and ftruncating to sizes bigger than it"
        );

        store.dclear(&1).await;

        for i in 1..=20 {
            store.dappend(1, i, Duration::minutes(1), 20).await;
        }

        store
            .dmap::<i32>(&1, |v| {
                let new = v.as_ref().unwrap() + 100;

                Some(Value {
                    value: Ok(new),
                    expires_at: v.expires_at,
                })
            })
            .await;

        assert_eq!(
            store.dlen(&1).await,
            20,
            "The deque's length was not what was expected after mapping it"
        );

        let nums: Vec<i32> = store
            .dall(&1)
            .await
            .into_iter()
            .map(|i| *i.unwrap())
            .collect();

        assert_eq!(
            nums,
            (101..=120).collect::<Vec<_>>(),
            "The deque's items are not 101..=120 after mapping it"
        );
    }

    #[pollster::test]
    async fn test_map_operations() {
        // Tested:
        // - hset
        // - hget
        // - hpop
        // - hrem
        // - hexists
        // - hexpire
        // - hlen
        // - hclear

        let store: TurboStore<i32> = TurboStore::new();

        for i in 1..=3 {
            for j in 1..=5 {
                store
                    .hset(i, j * i, j * i + 5, Duration::minutes(1), 5)
                    .await;

                assert_eq!(
                    store.hlen(&i).await,
                    j as usize,
                    "Map {i}'s length did not match the amount of items inserted"
                );
            }
        }

        for i in 1..=3 {
            for j in 1..=5 {
                let value = store.hget::<i32>(&i, &(j * i)).await.unwrap_or_else(|| {
                    panic!(
                        "There was no value in map {i} for key {} when getting",
                        j * i
                    )
                });

                assert_eq!(
                    j * i + 5,
                    *value.unwrap_or_else(|_| {
                        panic!(
                            "Could not decode value in map {i} for key {} when getting",
                            j * i
                        )
                    }),
                    "The value set was not the value retrieved for key {} in map {i} when getting",
                    i * j
                );
            }
        }

        for i in 1..=3 {
            for j in 1..=5 {
                let value = store.hpop::<i32>(&i, &(j * i)).await.unwrap_or_else(|| {
                    panic!(
                        "There was no value in map {i} for key {} when popping",
                        j * i
                    )
                });

                assert_eq!(
                    j * i + 5,
                    *value.unwrap_or_else(|_| {
                        panic!(
                            "Could not decode value in map {i} for key {} when popping",
                            j * i
                        )
                    }),
                    "The value set was not the value retrieved for key {} in map {i} when popping",
                    i * j
                );
            }
        }

        for i in 1..=3 {
            for j in 1..=5 {
                let value = store.hget::<i32>(&i, &(j * i)).await;

                assert!(
                    value.is_none(),
                    "The value was not None after popped for key {} in map {i}",
                    j * i
                );
            }
        }

        for i in 1..=3 {
            for j in 1..=5 {
                store
                    .hset(i, j * i, j * i + 5, Duration::minutes(1), 5)
                    .await;

                assert_eq!(
                    store.hlen(&i).await,
                    j as usize,
                    "Map {i}'s length did not match the amount of items inserted"
                );
            }
        }

        for i in 1..=3 {
            for j in 1..=5 {
                store.hrem(&i, &(j * i)).await;
                let value = store.hget::<i32>(&i, &(j * i)).await;

                assert!(
                    value.is_none(),
                    "The value was not None after removed for key {} in map {i}",
                    j * i
                );
            }
        }

        for i in 1..=3 {
            for j in 1..=5 {
                assert!(
                    !store.hexists(&i, &(i * j)).await,
                    "The key {} existed in map {i} before inserting it",
                    i * j
                );

                store
                    .hset(i, j * i, j * i + 5, Duration::minutes(1), 5)
                    .await;

                assert!(
                    store.hexists(&i, &(i * j)).await,
                    "The key {} did not exist in map {i} after inserting it",
                    i * j
                );

                let value_ttl = store
                    .hget::<i32>(&i, &(i * j))
                    .await
                    .unwrap_or_else(|| panic!("Item {} in map {i} did not return a value", i * j))
                    .unwrap()
                    .expires_at;

                let now = Utc::now();
                assert!(
                    value_ttl > now + Duration::seconds(59)
                        && value_ttl <= now + Duration::minutes(1),
                    "The expiry time for item {} in map {i} was not between 59 seconds and a minute in the future",
                    j * i
                );
            }
        }

        for i in 1..=3 {
            assert_eq!(
                store.hlen(&i).await,
                5,
                "The map {i} did not have the expected amount of items before clearing it"
            );

            store.hclear(&i).await;

            assert_eq!(
                store.hlen(&i).await,
                0,
                "The map {i} did not have the expected amount of items after clearing it"
            );
        }
    }
}
