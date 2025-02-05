//! Redis store for rate limiting
use actix::prelude::*;
use backoff::backoff::Backoff;
use backoff::ExponentialBackoff;
use log::*;
use redis_rs::aio::ConnectionManager;
use redis_rs::{self as redis};
use std::time::Duration;

use crate::errors::ARError;
use crate::{ActorMessage, ActorResponse, QuotaResponse};

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
        Supervisor::start(|_| RedisStore { client })
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
                        let cmd = redis::Cmd::set_ex(key, value, expiry.as_secs() as usize);

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

                        cmd.arg("DECR").arg(key);

                        let result = cmd
                            .query_async::<ConnectionManager, Option<i32>>(&mut con)
                            .await;
                        match result {
                            Ok(c) => Ok(c.unwrap()),
                            Err(e) => {
                                Err(ARError::ReadWriteError(format!("Update failed, {:?}", &e)))
                            }
                        }
                    }))
                }
                ActorMessage::Get(key) => ActorResponse::Get(Box::pin(async move {
                    let mut pipeline = redis::Pipeline::new();
                    pipeline
                        .atomic()
                        .cmd("GET")
                        .arg(key.clone())
                        .cmd("TTL")
                        .arg(key.clone());
                    let result = pipeline
                        .query_async::<ConnectionManager, Option<(u64, usize)>>(&mut con)
                        .await;

                    match result {
                        Ok(c) => Ok(Some(QuotaResponse {
                            key: key,
                            quota_remaining: c.unwrap().0,
                            expiry: c.unwrap().1,
                        })),
                        Err(e) => {
                            debug!("Get failed, {:?}", &e);
                            Ok(None)
                        }
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
        let redis_connection = redis::Client::open("redis://localhost")
            .unwrap()
            .get_tokio_connection_manager()
            .await;
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
    async fn test_quota_exhaustion() {
        let store = init().await.unwrap();
        let addr = RedisStoreActor::from(store.clone()).start();
        let res = addr
            .send(ActorMessage::Set {
                key: "boo".to_string(),
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
        for i in 1..40 {
            let res = addr
                .send(ActorMessage::Update {
                    key: "boo".to_string(),
                    value: 1i32,
                })
                .await;
            let res = res.expect("Failed to send msg");
            match res {
                ActorResponse::Update(c) => match c.await {
                    Ok(n) => {
                        assert_eq!(n, 30 - i);
                    }
                    Err(e) => panic!("Shouldn't happen: {}", &e),
                },
                _ => panic!("Shouldn't happen!"),
            }
            //   ActorResponse::Get(c) => match c.await {
            //         Ok(d) => {
            //             let d = d.unwrap();
            //             assert_eq!(d, 30i32);
            //         }
            //         Err(e) => panic!("Shouldn't happen {}", &e),
            //     }
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
                    assert_eq!(d.quota_remaining, 30u64);
                }
                Err(e) => panic!("Shouldn't happen {}", &e),
            },
            _ => panic!("Shouldn't happen!"),
        };
    }
}
