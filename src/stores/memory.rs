//! In memory store for rate limiting
use actix::prelude::*;
use dashmap::DashMap;
use futures::future::{self};
use log::*;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::errors::ARError;
use crate::{ActorMessage, ActorResponse, QuotaResponse};

/// Type used to create a concurrent hashmap store
#[derive(Clone)]
pub struct MemoryStore {
    inner: Arc<DashMap<String, (i32, Duration)>>,
}

impl MemoryStore {
    /// Create a new hashmap
    ///
    /// # Example
    /// ```rust
    /// use actix_ratelimit::MemoryStore;
    ///
    /// let store = MemoryStore::new();
    /// ```
    pub fn new() -> Self {
        debug!("Creating new MemoryStore");
        MemoryStore {
            inner: Arc::new(DashMap::<String, (i32, Duration)>::new()),
        }
    }

    /// Create a new hashmap with the provided capacity
    pub fn with_capacity(capacity: usize) -> Self {
        debug!("Creating new MemoryStore");
        MemoryStore {
            inner: Arc::new(DashMap::<String, (i32, Duration)>::with_capacity(capacity)),
        }
    }
}

/// Actor for memory store
pub struct MemoryStoreActor {
    inner: Arc<DashMap<String, (i32, Duration)>>,
}

impl From<MemoryStore> for MemoryStoreActor {
    fn from(store: MemoryStore) -> Self {
        MemoryStoreActor { inner: store.inner }
    }
}

impl MemoryStoreActor {
    /// Starts the memory actor and returns it's address
    pub fn start(self) -> Addr<Self> {
        debug!("Started memory store");
        Supervisor::start(|_| self)
    }
}

impl Actor for MemoryStoreActor {
    type Context = Context<Self>;
}

impl Supervised for MemoryStoreActor {
    fn restarting(&mut self, _: &mut Self::Context) {
        debug!("Restarting memory store");
    }
}

impl Handler<ActorMessage> for MemoryStoreActor {
    type Result = ActorResponse;
    fn handle(&mut self, msg: ActorMessage, ctx: &mut Self::Context) -> Self::Result {
        match msg {
            ActorMessage::Set { key, value, expiry } => {
                debug!("Inserting key {} with expiry {}", &key, &expiry.as_secs());
                let future_key = String::from(&key);
                let now = SystemTime::now();
                let now = now.duration_since(UNIX_EPOCH).unwrap();
                self.inner.insert(key, (value, now + expiry));
                ctx.notify_later(ActorMessage::Remove(future_key), expiry);
                ActorResponse::Set(Box::pin(future::ready(Ok(()))))
            }
            ActorMessage::Update { key, value } => match self.inner.get_mut(&key) {
                Some(mut c) => {
                    let val_mut: &mut (i32, Duration) = c.value_mut();
                    if val_mut.0 > value {
                        val_mut.0 -= value;
                    } else {
                        val_mut.0 = 0;
                    }
                    let new_val = val_mut.0;
                    ActorResponse::Update(Box::pin(future::ready(Ok(new_val))))
                }
                None => {
                    return ActorResponse::Update(Box::pin(future::ready(Err(
                        ARError::ReadWriteError("memory store: read failed!".to_string()),
                    ))))
                }
            },
            ActorMessage::Get(key) => {
                if self.inner.contains_key(&key) {
                    let val = match self.inner.get(&key) {
                        Some(c) => c,
                        None => {
                            return ActorResponse::Get(Box::pin(future::ready(Err(
                                ARError::ReadWriteError("memory store: read failed!".to_string()),
                            ))))
                        }
                    };
                    let val = val.value().0;
                    ActorResponse::Get(Box::pin(future::ready(Ok(Some(QuotaResponse {
                        key: key,
                        quota_remaining: val as u64,
                        expiry: 0, //TODO(mhala) fetch value of the key expiry
                    })))))
                } else {
                    ActorResponse::Get(Box::pin(future::ready(Ok(None))))
                }
            }
            ActorMessage::Remove(key) => {
                debug!("Removing key: {}", &key);
                let val = match self.inner.remove::<String>(&key) {
                    Some(c) => c,
                    None => {
                        return ActorResponse::Remove(Box::pin(future::ready(Err(
                            ARError::ReadWriteError("memory store: remove failed!".to_string()),
                        ))))
                    }
                };
                let val = val.1;
                ActorResponse::Remove(Box::pin(future::ready(Ok(val.0))))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[actix_rt::test]
    async fn test_set() {
        let store = MemoryStore::new();
        let addr = MemoryStoreActor::from(store.clone()).start();
        let res = addr
            .send(ActorMessage::Set {
                key: "hello".to_string(),
                value: 30i32,
                expiry: Duration::from_secs(5),
            })
            .await;
        let res = res.expect("Failed to send msg");
        match res {
            ActorResponse::Set(c) => match c.await {
                Ok(()) => {}
                Err(e) => panic!("Shouldn't happen {}", &e),
            },
            _ => panic!("Shouldn't happen!"),
        }
    }

    #[actix_rt::test]
    async fn test_get() {
        let store = MemoryStore::new();
        let addr = MemoryStoreActor::from(store.clone()).start();
        let expiry = Duration::from_secs(5);
        let res = addr
            .send(ActorMessage::Set {
                key: "hello".to_string(),
                value: 30i32,
                expiry: expiry,
            })
            .await;
        let res = res.expect("Failed to send msg");
        match res {
            ActorResponse::Set(c) => match c.await {
                Ok(()) => {}
                Err(e) => panic!("Shouldn't happen {}", &e),
            },
            _ => panic!("Shouldn't happen!"),
        }
        let res2 = addr.send(ActorMessage::Get("hello".to_string())).await;
        let res2 = res2.expect("Failed to send msg");
        match res2 {
            ActorResponse::Get(c) => match c.await {
                Ok(d) => {
                    let d = d.unwrap();
                    assert_eq!(d.quota_remaining, 30u64);
                }
                Err(e) => panic!("Shouldn't happen {}", &e),
            },
            _ => panic!("Shouldn't happen!"),
        };
    }
}
