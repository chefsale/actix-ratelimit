//! Redis store for rate limiting
use actix::prelude::*;
use backoff::backoff::Backoff;
use backoff::ExponentialBackoff;
use log::*;
use redis_rs::aio::ConnectionManager;
use redis_rs::{self as redis};
use std::time::Duration;

use crate::errors::ARError;
use crate::{ActorMessage, ActorResponse};

struct GetAddr;
impl Message for GetAddr {
    type Result = Result<ConnectionManager, ARError>;
}

/// Type used to connect to a running redis instance
pub struct RedisStore {
    client: ConnectionManager,
}

impl RedisStore {
    /// Accepts a valid connection string to connect to redis
    ///
    /// # Example
    /// ```rust
    /// use actix_ratelimit::RedisStore;
    ///
    /// #[actix_rt::main]
    /// async fn main() -> std::io::Result<()>{
    ///     let store = RedisStore::connect("redis://127.0.0.1");
    ///     Ok(())
    /// }
    /// ```
    pub fn connect(client: ConnectionManager) -> Addr<Self> {
        let mut backoff = ExponentialBackoff::default();
        backoff.max_elapsed_time = None;
        Supervisor::start(|_| RedisStore {
            client,
        })
    }
    
}
impl Actor for RedisStore {
    type Context = Context<Self>;

    fn started(&mut self, _ctx: &mut Context<Self>) {
        info!("Started main redis store");
    }
}

impl Supervised for RedisStore {
    fn restarting(&mut self, _: &mut Self::Context) {
        debug!("restarting redis store");
    }
}

impl Handler<GetAddr> for RedisStore {
    type Result = Result<ConnectionManager, ARError>;
    fn handle(&mut self, _: GetAddr, _ctx: &mut Self::Context) -> Self::Result {
        Ok(self.client.clone())
    }
}

/// Actor for redis store
pub struct RedisStoreActor {
    addr: Addr<RedisStore>,
    backoff: ExponentialBackoff,
    inner: Option<ConnectionManager>,
}

impl Actor for RedisStoreActor {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Context<Self>) {
        let addr = self.addr.clone();
        async move { addr.send(GetAddr).await }
            .into_actor(self)
            .map(|res, act, context| match res {
                Ok(c) => {
                    if let Ok(conn) = c {
                        act.inner = Some(conn);
                    } else {
                        error!("could not get redis store address");
                        if let Some(timeout) = act.backoff.next_backoff() {
                            context.run_later(timeout, |_, ctx| ctx.stop());
                        }
                    }
                }
                Err(_) => {
                    error!("mailboxerror: could not get redis store address");
                    if let Some(timeout) = act.backoff.next_backoff() {
                        context.run_later(timeout, |_, ctx| ctx.stop());
                    }
                }
            })
            .wait(ctx);
    }
}

impl From<Addr<RedisStore>> for RedisStoreActor {
    fn from(addr: Addr<RedisStore>) -> Self {
        let mut backoff = ExponentialBackoff::default();
        backoff.max_interval = Duration::from_secs(3);
        RedisStoreActor {
            addr,
            backoff,
            inner: None,
        }
    }
}

impl RedisStoreActor {
    /// Starts the redis actor and returns it's address
    pub fn start(self) -> Addr<Self> {
        debug!("started redis actor");
        Supervisor::start(|_| self)
    }
}

impl Supervised for RedisStoreActor {
    fn restarting(&mut self, _: &mut Self::Context) {
        debug!("restarting redis actor!");
        self.inner.take();
    }
}

impl Handler<ActorMessage> for RedisStoreActor {
    type Result = ActorResponse;

    fn handle(&mut self, msg: ActorMessage, ctx: &mut Self::Context) -> Self::Result {
        let connection = self.inner.clone();
        if let Some(mut con) = connection {
            match msg {
                ActorMessage::Set { key, value, expiry } => {
                    ActorResponse::Set(Box::pin(async move {
                        let mut cmd = redis::Cmd::new();
                        cmd.arg("SET")
                           .arg(key)
                           .arg(value)
                           .arg("EX")
                           .arg(expiry.as_secs());

                        let result = cmd.query_async::<ConnectionManager, ()>(&mut con).await;
                        match result {
                            Ok(_) => Ok(()),
                            Err(e) => Err(ARError::ReadWriteError(format!("Set failed, {:?}", &e))),
                        }
                    }))
                }
                ActorMessage::Update { key, value: _ } => {
                    ActorResponse::Update(Box::pin(async move {
                        let mut cmd = redis::Cmd::new();

                         cmd.arg("DECR")
                           .arg(key);
                        
                        let result = cmd.query_async::<ConnectionManager, Option<i32>>(&mut con).await;
                        match result {
                            Ok(c) => {
                                Ok(c.unwrap())
                            },
                            Err(e) => {
                                Err(ARError::ReadWriteError(format!("Update failed, {:?}", &e)))
                            }
                        }
                    }))
                }
                ActorMessage::Get(key) => ActorResponse::Get(Box::pin(async move {
                    let mut cmd = redis::Cmd::new();
                    cmd.arg("GET").arg(key);
                    let result = cmd
                        .query_async::<ConnectionManager, Option<i32>>(&mut con)
                        .await;

                    match result {
                        Ok(c) => {
                            Ok(c)
                        },
                        Err(e) => Err(ARError::ReadWriteError(format!("Get failed, {:?}", &e))),
                    }
                })),
                ActorMessage::Expire(key) => ActorResponse::Expire(Box::pin(async move {
                    let mut cmd = redis::Cmd::new();
                    cmd.arg("TTL").arg(key);
                    let result = cmd.query_async::<ConnectionManager, isize>(&mut con).await;
                    match result {
                        Ok(c) => {
                            if c >= 0 {
                                Ok(Duration::new(c as u64, 0))
                            } else {
                                error!("Expire failed, redis error: key does not exists or does not has a associated ttl. Return value {}", c);
                                Err(ARError::ReadWriteError(format!("Expire failed, return value: {}", c)))
                            }
                        }
                        Err(e) => Err(ARError::ReadWriteError(format!("Expire failed, {:?}", &e))),
                    }
                })),
                ActorMessage::Remove(key) => ActorResponse::Remove(Box::pin(async move {
                    error!("Remove called, key => {}", key);
                    let mut cmd = redis::Cmd::new();
                    cmd.arg("DEL").arg(key);
                    let result = cmd.query_async::<ConnectionManager, i32>(&mut con).await;
                    match result {
                        Ok(c) => Ok(c),
                        Err(e) => Err(ARError::ReadWriteError(format!("Remove failed, {:?}", &e))),
                    }
                })),
            }
        } else {
            ctx.stop();
            ActorResponse::Set(Box::pin(async move { Err(ARError::Disconnected) }))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn init() -> Result<Addr<RedisStore>, ()> {
        let _ = env_logger::builder().is_test(true).try_init();
        let redis_connection = redis::Client::open("redis://localhost").unwrap().get_tokio_connection_manager().await;
        match redis_connection {
            Ok(c) => Ok(RedisStore::connect(c)),
            Err(e) => {
                panic!("{}", e);
            }
        }
    }

    #[actix_rt::test]
    async fn test_set() {
        let store = init().await.unwrap();
        let addr = RedisStoreActor::from(store.clone()).start();
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
                Err(e) => panic!("Shouldn't happen: {}", &e),
            },
            _ => panic!("Shouldn't happen!"),
        }
    }

    #[actix_rt::test]
    async fn test_get() {
        let store = init().await.unwrap();
        let addr = RedisStoreActor::from(store.clone()).start();
        let expiry = Duration::from_secs(5);
        let res = addr
            .send(ActorMessage::Set {
                key: "hello".to_string(),
                value: 30i32,
                expiry,
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
                    assert_eq!(d, 30i32);
                }
                Err(e) => panic!("Shouldn't happen {}", &e),
            },
            _ => panic!("Shouldn't happen!"),
        };
    }

    #[actix_rt::test]
    async fn test_expiry() {
        let store = init().await.unwrap();
        let addr = RedisStoreActor::from(store.clone()).start();
        let expiry = Duration::from_secs(3);
        let res = addr
            .send(ActorMessage::Set {
                key: "hello_test".to_string(),
                value: 30i32,
                expiry,
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
        assert_eq!(addr.connected(), true);

        let res3 = addr
            .send(ActorMessage::Expire("hello_test".to_string()))
            .await;
        let res3 = res3.expect("Failed to send msg");
        match res3 {
            ActorResponse::Expire(c) => match c.await {
                Ok(dur) => {
                    let now = Duration::from_secs(3);
                    if dur > now {
                        panic!("Shouldn't happen: {}, {}", &dur.as_secs(), &now.as_secs())
                    }
                }
                Err(e) => {
                    panic!("Shouldn't happen: {}", &e);
                }
            },
            _ => panic!("Shouldn't happen!"),
        };
    }
    
}
