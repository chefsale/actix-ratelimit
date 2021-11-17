//! RateLimiter middleware for actix application
use actix::dev::*;
use actix_web::{
    dev::{Service, ServiceRequest, ServiceResponse, Transform},
    error::Error as AWError,
    http::{HeaderName, HeaderValue},
    HttpResponse,
};
use futures::future::{ok, Ready};
use log::*;
use std::{
    cell::RefCell,
    future::Future,
    ops::Fn,
    pin::Pin,
    rc::Rc,
    task::{Context, Poll},
    time::Duration,
};

use crate::{errors::ARError, ActorMessage, ActorResponse};

/// Type that implements the ratelimit middleware.
///
/// This accepts _interval_ which specifies the
/// window size, _max_requests_ which specifies the maximum number of requests in that window, and
/// _store_ which is essentially a data store used to store client access information. Entry is removed from
/// the store after _interval_.
///
/// # Example
/// ```rust
/// # use std::time::Duration;
/// use actix_ratelimit::{MemoryStore, MemoryStoreActor};
/// use actix_ratelimit::RateLimiter;
///
/// #[actix_rt::main]
/// async fn main() {
///     let store = MemoryStore::new();
///     let ratelimiter = RateLimiter::new(
///                             MemoryStoreActor::from(store.clone()).start())
///                         .with_interval(Duration::from_secs(60))
///                         .with_max_requests(100);
/// }
/// ```
pub struct RateLimiter<T, A>
where
    T: Handler<ActorMessage> + Send + Sync + 'static,
    T::Context: ToEnvelope<T, ActorMessage>,
{
    interval: Duration,
    store: Addr<T>,
    identifier: Rc<Box<dyn Fn(&ServiceRequest) -> Result<String, ARError>>>,
    auth_quota: A,
}

pub trait QuotaChecker {
    fn check_quota(&self, req: &ServiceRequest, identifier: String) -> Option<usize>;
    fn whitelisted(&self, req: &ServiceRequest) -> bool;
}

impl<T,A:QuotaChecker> RateLimiter<T, A>
where
    T: Handler<ActorMessage> + Send + Sync + 'static,
    <T as Actor>::Context: ToEnvelope<T, ActorMessage>,
{
    /// Creates a new instance of `RateLimiter` with the provided address of `StoreActor`.
    pub fn new(store: Addr<T>, auth_quota: A) -> Self {
        let identifier = |req: &ServiceRequest| {
            let connection_info = req.connection_info();
            let ip = connection_info
                .remote_addr()
                .ok_or(ARError::IdentificationError)?;
            Ok(String::from(ip))
        };
        RateLimiter {
            interval: Duration::from_secs(0),
            store,
            identifier: Rc::new(Box::new(identifier)),
            auth_quota,
        }
    }

    /// Specify the interval. The counter for a client is reset after this interval
    pub fn with_interval(mut self, interval: Duration) -> Self {
        self.interval = interval;
        self
    }

    /// Function to get the identifier for the client request
    pub fn with_identifier<F: Fn(&ServiceRequest) -> Result<String, ARError> + 'static>(
        mut self,
        identifier: F,
    ) -> Self {
        self.identifier = Rc::new(Box::new(identifier));
        self
    }
}

impl<T, S, B, A: QuotaChecker + Clone > Transform<S> for RateLimiter<T, A>
where
    T: Handler<ActorMessage> + Send + Sync + 'static,
    T::Context: ToEnvelope<T, ActorMessage>,
    S: Service<Request = ServiceRequest, Response = ServiceResponse<B>, Error = AWError> + 'static,
    S::Future: 'static,
    B: 'static,
{
    type Request = ServiceRequest;
    type Response = ServiceResponse<B>;
    type Error = S::Error;
    type InitError = ();
    type Transform = RateLimitMiddleware<S, T, A>;
    type Future = Ready<Result<Self::Transform, Self::InitError>>;

    fn new_transform(&self, service: S) -> Self::Future {
        ok(RateLimitMiddleware {
            service: Rc::new(RefCell::new(service)),
            store: self.store.clone(),
            interval: self.interval.as_secs(),
            identifier: self.identifier.clone(),
            auth_quota: self.auth_quota.clone(),
        })
    }
}

/// Service factory for RateLimiter
pub struct RateLimitMiddleware<S, T, A>
where
    S: 'static,
    T: Handler<ActorMessage> + 'static,
{
    service: Rc<RefCell<S>>,
    store: Addr<T>,
    // Exists here for the sole purpose of knowing the max_requests and interval from RateLimiter
    interval: u64,
    identifier: Rc<Box<dyn Fn(&ServiceRequest) -> Result<String, ARError> + 'static>>,
    auth_quota: A,
}

impl<T, S, B, A:QuotaChecker> Service for RateLimitMiddleware<S, T, A>
where
    T: Handler<ActorMessage> + 'static,
    S: Service<Request = ServiceRequest, Response = ServiceResponse<B>, Error = AWError> + 'static,
    S::Future: 'static,
    B: 'static,
    T::Context: ToEnvelope<T, ActorMessage>,
{
    type Request = ServiceRequest;
    type Response = ServiceResponse<B>;
    type Error = S::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>>>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.service.borrow_mut().poll_ready(cx)
    }

    fn call(&mut self, req: ServiceRequest) -> Self::Future {
        let mut srv_1 = self.service.clone();
        if self.auth_quota.whitelisted(&req) {
            return Box::pin(async move {
                let fut = srv_1.call(req);
                let mut res = fut.await?;
                let headers = res.headers_mut();
                headers.insert(HeaderName::from_static("x-ratelimit-whitelisted"),HeaderValue::from_static( "true"));
                Ok::<Self::Response, Self::Error>(res)
            });
        }
        let mut srv = self.service.clone();
        let store = self.store.clone();

        let interval = Duration::from_secs(self.interval);
        let identifier = self.identifier.clone();
        let identifier: String = (identifier)(&req).unwrap();
        let quota  =  self.auth_quota.check_quota(&req , identifier.clone());

        //TODO(mhala) handle errors gracefully
        

        Box::pin(async move {
            if quota.is_none() {
                let response = HttpResponse::BadRequest();
                // let mut response = (error_callback)(&mut response);
                return Err(response.into())
            }
            let max_requests = quota.unwrap();
            info!("Max request for identifier: {} {}", identifier, max_requests);
            let remaining: ActorResponse = store
                .send(ActorMessage::Get(String::from(&identifier)))
                .await?;
            match remaining {
                ActorResponse::Get(opt) => {
                    let opt = opt.await?;
                    if let Some(c) = opt {
                        // Existing entry in store
                        let expiry = store
                            .send(ActorMessage::Expire(String::from(&identifier)))
                            .await?;
                        let reset: Duration = match expiry {
                            ActorResponse::Expire(dur) => dur.await?,
                            _ => unreachable!(),
                        };
                        if c == 0 {
                            info!("Limit exceeded for client: {}", &identifier);
                            let mut response = HttpResponse::TooManyRequests();
                            // let mut response = (error_callback)(&mut response);
                            response.set_header("x-ratelimit-limit", max_requests.to_string());
                            response.set_header("x-ratelimit-remaining", c.to_string());
                            response.set_header("x-ratelimit-reset", reset.as_secs().to_string());
                            Err(response.into())
                        } else {
                            // Decrement value
                            let res: ActorResponse = store
                                .send(ActorMessage::Update {
                                    key: identifier,
                                    value: 1,
                                })
                                .await?;
                            let updated_value: usize = match res {
                                ActorResponse::Update(c) => c.await?,
                                _ => unreachable!(),
                            };
                            // Execute the request
                            let fut = srv.call(req);
                            let mut res = fut.await?;
                            let headers = res.headers_mut();
                            // Safe unwraps, since usize is always convertible to string
                            headers.insert(
                                HeaderName::from_static("x-ratelimit-limit"),
                                HeaderValue::from_str(max_requests.to_string().as_str())?,
                            );
                            headers.insert(
                                HeaderName::from_static("x-ratelimit-remaining"),
                                HeaderValue::from_str(updated_value.to_string().as_str())?,
                            );
                            headers.insert(
                                HeaderName::from_static("x-ratelimit-reset"),
                                HeaderValue::from_str(reset.as_secs().to_string().as_str())?,
                            );
                            Ok(res)
                        }
                    } else {
                        // New client, create entry in store
                        let current_value = max_requests - 1;
                        let res = store
                            .send(ActorMessage::Set {
                                key: String::from(&identifier),
                                value: current_value,
                                expiry: interval,
                            })
                            .await?;
                        match res {
                            ActorResponse::Set(c) => c.await?,
                            _ => unreachable!(),
                        }
                        let fut = srv.call(req);
                        let mut res = fut.await?;
                        let headers = res.headers_mut();
                        // Safe unwraps, since usize is always convertible to string
                        headers.insert(
                            HeaderName::from_static("x-ratelimit-limit"),
                            HeaderValue::from_str(max_requests.to_string().as_str()).unwrap(),
                        );
                        headers.insert(
                            HeaderName::from_static("x-ratelimit-remaining"),
                            HeaderValue::from_str(current_value.to_string().as_str()).unwrap(),
                        );
                        headers.insert(
                            HeaderName::from_static("x-ratelimit-reset"),
                            HeaderValue::from_str(interval.as_secs().to_string().as_str()).unwrap(),
                        );
                        Ok(res)
                    }
                }
                _ => {
                    unreachable!();
                }
            }
        })
    }
}
