//! A simple Client for the Lucid KV

#[macro_use]
extern crate failure;

#[macro_use]
extern crate fehler;

pub use futures_retry::{ErrorHandler, RetryPolicy};

use bytes::Bytes;
use futures::future;
use futures::{Stream, TryStreamExt};
use futures_retry::StreamRetryExt;
use jsonwebtoken::EncodingKey;
use reqwest::header::{self, HeaderMap, HeaderValue};
use reqwest::{Body, Client, ClientBuilder, StatusCode, Url};
use reqwest_eventsource::{Error as EventsourceError, RequestBuilderExt};
use serde::{de::DeserializeOwned, ser::Serializer, Serialize};
use std::fmt;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

cfg_if::cfg_if! {
    if #[cfg(feature = "flexbuffers")] {
        use flexbuffers as serde_mod;
    } else {
        use serde_json as serde_mod;
    }
}

#[cfg(feature = "rustls-tls")]
pub use reqwest::Certificate;

/// Errors when doing Client operations
#[derive(Fail, Debug)]
pub enum Error {
    #[fail(display = "invalid url")]
    InvalidUrl,
    #[fail(display = "invalid client")]
    InvalidClient(reqwest::Error),
    #[fail(display = "invalid response")]
    InvalidResponse,
    #[fail(display = "invalid request: {}", _0)]
    InvalidRequest(reqwest::Error),
    #[fail(display = "unauthorized")]
    Unauthorized,
    #[fail(display = "conflict")]
    Conflict,
    #[fail(display = "not found")]
    NotFound,
    #[fail(display = "non numeric value")]
    NonNumericValue,
    #[fail(display = "bad request")]
    BadRequest,
    #[fail(display = "serialize error")]
    SerializeError,
    #[fail(display = "deserialize error")]
    DeserializeError,
    #[fail(display = "invalid JWT key")]
    InvalidJWTKey,
}

/// Errors when retrieving Notifications
#[derive(Fail, Debug)]
pub enum NotificationError<E>
where
    E: fmt::Display + fmt::Debug + Send + Sync + 'static,
{
    #[fail(display = "transport error after {} attempts: {}", _1, _0)]
    Other(E, usize),
    #[fail(display = "deserialize error")]
    DeserializeError(Bytes),
}

/// Whether a Key was created or not
#[repr(u16)]
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum PutStatus {
    Ok,
    Created,
}

#[derive(Debug, Clone, Copy)]
enum Operation {
    Lock,
    Unlock,
    Increment,
    Decrement,
    TTL,
}

#[derive(Debug, Clone, Default, Serialize)]
struct Claims {
    sub: String,
    iss: String,
    iat: i64,
    exp: i64,
}

#[derive(Debug, Clone, Serialize)]
struct PatchValue {
    operation: Operation,
    value: Option<String>,
}

struct NotificationsErrorHandler<F>
where
    F: ErrorHandler<reqwest::Error>,
{
    inner: F,
}

/// A notification sent when a key value pair is changed
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct Notification<T> {
    pub key: String,
    pub data: T,
}

/// The main Client
#[derive(Clone, Debug)]
pub struct LucidClient {
    client: Client,
    url: Url,
    jwt_key: Option<EncodingKey>,
}

/// A builder for adding custom options to the LucidClient
#[derive(Debug)]
pub struct Builder<'a> {
    client: ClientBuilder,
    url: &'a str,
    jwt_key: Option<EncodingKey>,
}

impl<'a> Builder<'a> {
    /// Create a new Client Builder with a base URL (e.g. `http://localhost:7020`)
    pub fn new<U: AsRef<str> + ?Sized>(base_url: &'a U) -> Self {
        Self {
            client: ClientBuilder::new(),
            url: base_url.as_ref(),
            jwt_key: None,
        }
    }

    /// Add a JWT secret to authenticate against
    pub fn add_jwt_key<T: AsRef<[u8]> + ?Sized>(mut self, key: &T) -> Self {
        self.jwt_key = Some(EncodingKey::from_secret(key.as_ref()));
        self
    }

    #[cfg(feature = "rustls-tls")]
    /// Add a custom root certificate
    pub fn add_root_certificate(mut self, cert: Certificate) -> Self {
        self.client = self.client.add_root_certificate(cert);
        self
    }

    /// Build the LucidClient itself
    #[throws]
    pub fn build(self) -> LucidClient {
        LucidClient {
            client: self.client.build().map_err(Error::InvalidClient)?,
            url: Url::parse(self.url).map_err(|_| Error::InvalidUrl)?,
            jwt_key: self.jwt_key,
        }
    }
}

impl LucidClient {
    /// Build a basic Client. This is equivalent to `Builder::new(url).build()`
    #[throws]
    pub fn new<U: AsRef<str> + ?Sized>(base_url: &U) -> Self {
        Builder::new(base_url).build()?
    }

    /// Configure a Client with the Builder
    pub fn builder<'a, U: AsRef<str> + ?Sized>(base_url: &'a U) -> Builder<'a> {
        Builder::new(base_url)
    }

    /// Health check
    #[throws]
    pub async fn health_check(&self) {
        let url = self.url.join("health").map_err(|_| Error::InvalidUrl)?;
        let res = self
            .client
            .head(url)
            .send()
            .await
            .map_err(Error::InvalidRequest)?;
        match res.status() {
            StatusCode::OK => (),
            _ => throw!(Error::InvalidResponse),
        }
    }

    /// Store a string or bytes as a value for a key. Creates a new key if it does not exist
    #[throws]
    pub async fn put_raw<K: AsRef<str> + ?Sized, V: Into<Body>>(
        &self,
        key: &K,
        value: V,
    ) -> PutStatus {
        let res = self
            .client
            .put(self.key_url(key)?)
            .headers(self.authorization()?)
            .body(value)
            .send()
            .await
            .map_err(Error::InvalidRequest)?;
        match res.status() {
            StatusCode::OK => PutStatus::Ok,
            StatusCode::CREATED => PutStatus::Created,
            StatusCode::UNAUTHORIZED => throw!(Error::Unauthorized),
            StatusCode::CONFLICT => throw!(Error::Conflict),
            _ => throw!(Error::InvalidResponse),
        }
    }

    /// Gets raw bytes from a key's value
    #[throws]
    pub async fn get_raw<K: AsRef<str> + ?Sized>(&self, key: &K) -> Option<Bytes> {
        let res = self
            .client
            .get(self.key_url(key)?)
            .headers(self.authorization()?)
            .send()
            .await
            .map_err(Error::InvalidRequest)?;
        match res.status() {
            StatusCode::OK => Some(res.bytes().await.map_err(|_| Error::InvalidResponse)?),
            StatusCode::NOT_FOUND => None,
            _ => throw!(Error::InvalidResponse),
        }
    }

    /// Delete a key's value. Returns `true` if the key existed and was actually deleted
    #[throws]
    pub async fn delete<K: AsRef<str> + ?Sized>(&self, key: &K) -> bool {
        let res = self
            .client
            .delete(self.key_url(key)?)
            .headers(self.authorization()?)
            .send()
            .await
            .map_err(Error::InvalidRequest)?;
        match res.status() {
            StatusCode::OK | StatusCode::NO_CONTENT => true,
            StatusCode::NOT_FOUND => false,
            StatusCode::UNAUTHORIZED => throw!(Error::Unauthorized),
            _ => throw!(Error::InvalidResponse),
        }
    }

    /// Check if a key exists
    #[throws]
    pub async fn exists<K: AsRef<str> + ?Sized>(&self, key: &K) -> bool {
        let res = self
            .client
            .head(self.key_url(key)?)
            .headers(self.authorization()?)
            .send()
            .await
            .map_err(Error::InvalidRequest)?;
        match res.status() {
            StatusCode::OK | StatusCode::NO_CONTENT => true,
            StatusCode::NOT_FOUND => false,
            StatusCode::UNAUTHORIZED => throw!(Error::Unauthorized),
            _ => throw!(Error::InvalidResponse),
        }
    }

    /// Serialize a rust object and store as the value for a key
    #[throws]
    pub async fn put<K: AsRef<str> + ?Sized, V: Serialize>(&self, key: &K, value: &V) -> PutStatus {
        self.put_raw(
            key,
            serde_mod::to_vec(value).map_err(|_| Error::SerializeError)?,
        )
        .await?
    }

    /// Get the value for a key and deserialize it into a rust object
    #[throws]
    pub async fn get<K: AsRef<str> + ?Sized, V: DeserializeOwned>(&self, key: &K) -> Option<V> {
        let bytes = self.get_raw(key).await?;
        match bytes {
            None => None,
            Some(bytes) => {
                Some(serde_mod::from_slice(bytes.as_ref()).map_err(|_| Error::DeserializeError)?)
            }
        }
    }

    /// Lock a key. Returns `false` if the key is already locked and `true` otherwise
    #[throws]
    pub async fn lock<K: AsRef<str> + ?Sized>(&self, key: &K) -> bool {
        match self
            .patch(key, &PatchValue::new(Operation::Lock, None))
            .await
        {
            Ok(_) => true,
            Err(err) => match err {
                Error::Conflict => false,
                err => throw!(err),
            },
        }
    }

    /// Unlock a key. Returns `false` if the key is already unlocked and `true` otherwise
    #[throws]
    pub async fn unlock<K: AsRef<str> + ?Sized>(&self, key: &K) -> bool {
        match self
            .patch(key, &PatchValue::new(Operation::Unlock, None))
            .await
        {
            Ok(_) => true,
            Err(err) => match err {
                Error::Conflict => false,
                err => throw!(err),
            },
        }
    }

    /// Increment a key. Note the key's value must be like `b"0"` otherwise this will throw an
    /// [Error::NonNumericValue]
    #[throws]
    pub async fn increment<K: AsRef<str> + ?Sized>(&self, key: &K) {
        self.patch(key, &PatchValue::new(Operation::Increment, None))
            .await
            .map_err(|err| match err {
                Error::BadRequest => Error::NonNumericValue,
                err => err,
            })?
    }

    /// Decrement a key. See [LucidClient::increment] for more info.
    #[throws]
    pub async fn decrement<K: AsRef<str> + ?Sized>(&self, key: &K) {
        self.patch(key, &PatchValue::new(Operation::Decrement, None))
            .await
            .map_err(|err| match err {
                Error::BadRequest => Error::NonNumericValue,
                err => err,
            })?
    }

    /// Add a "time to live" constraint to a key
    #[throws]
    pub async fn ttl<K: AsRef<str> + ?Sized>(&self, key: &K, duration: Duration) {
        self.patch(
            key,
            &PatchValue::new(Operation::TTL, Some(duration.as_secs().to_string())),
        )
        .await?
    }

    /// Get raw notification blobs
    #[throws]
    pub async fn notifications_raw<F, E>(
        &self,
        handler: F,
    ) -> impl Stream<Item = Result<Notification<Bytes>, NotificationError<E>>>
    where
        F: ErrorHandler<reqwest::Error, OutError = E>,
        E: fmt::Display + fmt::Debug + Send + Sync + 'static,
    {
        let url = self
            .url
            .join("notifications")
            .map_err(|_| Error::InvalidUrl)?;
        self.client
            .get(url)
            .headers(self.authorization()?)
            .eventsource()
            .unwrap()
            .retry(NotificationsErrorHandler::new(handler))
            .map_ok(|(event, _attempt)| Notification {
                key: percent_encoding::percent_decode_str(&event.event.unwrap())
                    .decode_utf8_lossy()
                    .to_string(),
                data: event.data.into(),
            })
            .map_err(|(err, usize)| NotificationError::Other(err, usize))
    }

    /// Get notifications and deserialize them into objects
    #[throws]
    pub async fn notifications<F, T, E>(
        &self,
        handler: F,
    ) -> impl Stream<Item = Result<Notification<T>, NotificationError<E>>>
    where
        F: ErrorHandler<reqwest::Error, OutError = E>,
        E: fmt::Display + fmt::Debug + Send + Sync + 'static,
        T: DeserializeOwned,
    {
        self.notifications_raw(handler)
            .await?
            .and_then(|notification| {
                future::ready(
                    serde_mod::from_slice(&notification.data)
                        .map_err(|_| NotificationError::DeserializeError(notification.data.clone()))
                        .and_then(|data| {
                            Ok(Notification {
                                key: notification.key,
                                data,
                            })
                        }),
                )
            })
    }

    #[throws]
    async fn patch<K: AsRef<str> + ?Sized>(&self, key: &K, value: &PatchValue) {
        let res = self
            .client
            .patch(self.key_url(key)?)
            .headers(self.authorization()?)
            .body(serde_json::to_string(&value).map_err(|_| Error::SerializeError)?)
            .send()
            .await
            .map_err(Error::InvalidRequest)?;
        match res.status() {
            StatusCode::OK | StatusCode::NO_CONTENT => (),
            StatusCode::NOT_FOUND => throw!(Error::NotFound),
            StatusCode::CONFLICT => throw!(Error::Conflict),
            StatusCode::BAD_REQUEST => throw!(Error::BadRequest),
            StatusCode::UNAUTHORIZED => throw!(Error::Unauthorized),
            _ => throw!(Error::InvalidResponse),
        }
    }

    #[inline]
    #[throws]
    fn key_url<K: AsRef<str>>(&self, key: K) -> Url {
        let encoded =
            percent_encoding::utf8_percent_encode(key.as_ref(), percent_encoding::NON_ALPHANUMERIC)
                .to_string();
        self.url
            .join(&format!("api/kv/{}", encoded))
            .map_err(|_| Error::InvalidUrl)?
    }

    #[inline]
    #[throws]
    fn authorization(&self) -> HeaderMap<HeaderValue> {
        let mut headers = HeaderMap::default();
        let key = if let Some(ref key) = self.jwt_key {
            key
        } else {
            return headers;
        };

        let iat = match SystemTime::now().duration_since(UNIX_EPOCH) {
            Ok(n) => n.as_secs() as i64,
            Err(_) => panic!("SystemTime before UNIX EPOCH!"),
        };
        let claims = Claims {
            iat,
            exp: iat + 60,
            ..Default::default()
        };
        let token = jsonwebtoken::encode(&jsonwebtoken::Header::default(), &claims, &key)
            .map_err(|_| Error::InvalidJWTKey)?;

        headers.append(
            header::AUTHORIZATION,
            format!("Bearer {}", token)
                .parse()
                .map_err(|_| Error::InvalidJWTKey)?,
        );
        headers
    }
}

impl Operation {
    #[inline]
    fn as_str(self) -> &'static str {
        match self {
            Operation::Lock => "lock",
            Operation::Unlock => "unlock",
            Operation::Increment => "increment",
            Operation::Decrement => "decrement",
            Operation::TTL => "ttl",
        }
    }
}

impl PatchValue {
    #[inline]
    fn new(operation: Operation, value: Option<String>) -> Self {
        Self { operation, value }
    }
}

impl Serialize for Operation {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

impl<F> NotificationsErrorHandler<F>
where
    F: ErrorHandler<reqwest::Error>,
{
    fn new(inner: F) -> Self {
        Self { inner }
    }
}

impl<F> ErrorHandler<EventsourceError<reqwest::Error>> for NotificationsErrorHandler<F>
where
    F: ErrorHandler<reqwest::Error>,
{
    type OutError = F::OutError;

    fn handle(
        &mut self,
        attempt: usize,
        err: EventsourceError<reqwest::Error>,
    ) -> RetryPolicy<Self::OutError> {
        match err {
            EventsourceError::Parse(_) => {
                // ignore all parsing errors
                RetryPolicy::Repeat
            }
            EventsourceError::Transport(err) => self.inner.handle(attempt, err),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use futures::stream::StreamExt;
    use serde::Deserialize;

    #[derive(Debug, Serialize, Deserialize, Eq, PartialEq)]
    struct TestStruct {
        a: u32,
        b: String,
        c: Vec<u8>,
    }

    #[throws]
    fn client() -> LucidClient {
        #[allow(unused_mut, unused_assignments)]
        let mut builder = LucidClient::builder("http://localhost:7020");
        #[cfg(feature = "rustls-tls")]
        {
            builder = LucidClient::builder("https://localhost:7021");
            let ca_cert = Certificate::from_pem(
                std::fs::read("test_assets/ssl/ca-cert.pem")
                    .unwrap()
                    .as_ref(),
            )
            .unwrap();
            builder = builder.add_root_certificate(ca_cert);
        }
        builder.add_jwt_key("secret").build()?
    }

    #[test]
    #[throws]
    fn build() {
        LucidClient::new("http://localhost:7020")?;
        client()?;
    }

    #[tokio::test]
    async fn put_raw() -> Result<(), Error> {
        let client = client()?;
        client.put_raw("put_raw", "value1").await?;
        Ok(())
    }

    #[tokio::test]
    async fn put_raw_bytes() -> Result<(), Error> {
        let client = client()?;
        client
            .put_raw::<_, &[u8]>("put_raw_bytes", &[0, 1, 2, 3, 4])
            .await?;
        Ok(())
    }

    #[tokio::test]
    async fn get_raw() -> Result<(), Error> {
        let client = client()?;
        let test_value = "value1";
        client.put_raw("get_raw", test_value).await?;
        let db_value = client.get_raw("get_raw").await?;
        assert_eq!(
            test_value,
            String::from_utf8_lossy(db_value.unwrap().as_ref())
        );
        Ok(())
    }

    #[tokio::test]
    async fn update_raw() -> Result<(), Error> {
        let client = client()?;
        let key = "update_raw";

        let test_value1 = "value1";
        client.put_raw(key, test_value1).await?;
        let db_value = client.get_raw(key).await?;
        assert_eq!(
            test_value1,
            String::from_utf8_lossy(db_value.unwrap().as_ref())
        );

        let test_value2 = "value2";
        client.put_raw(key, test_value2).await?;
        let db_value = client.get_raw(key).await?;
        assert_eq!(
            test_value2,
            String::from_utf8_lossy(db_value.unwrap().as_ref())
        );

        Ok(())
    }

    #[tokio::test]
    async fn delete_missing() -> Result<(), Error> {
        let client = client()?;
        assert!(!client.delete("delete_missing").await?);
        Ok(())
    }

    #[tokio::test]
    async fn delete() -> Result<(), Error> {
        let client = client()?;
        let key = "delete";

        let test_value = "value";
        client.put_raw(key, test_value).await?;
        let db_value = client.get_raw(key).await?;
        assert_eq!(
            test_value,
            String::from_utf8_lossy(db_value.unwrap().as_ref())
        );

        assert!(client.delete(key).await?);
        let db_value = client.get_raw(key).await?;
        assert!(db_value.is_none());

        Ok(())
    }

    #[tokio::test]
    async fn exists_false() -> Result<(), Error> {
        let client = client()?;
        assert!(!client.exists("exists_false").await?);
        Ok(())
    }

    #[tokio::test]
    async fn exists_true() -> Result<(), Error> {
        let client = client()?;
        client.put_raw("exists_true", "value").await?;
        assert!(client.exists("exists_true").await?);
        Ok(())
    }

    #[tokio::test]
    async fn lock_unlock() -> Result<(), Error> {
        let client = client()?;
        let key = "lock_unlock";

        client.put_raw(key, "value").await?;
        assert!(!client.unlock(key).await?);
        assert!(client.lock(key).await?);
        assert!(!client.lock(key).await?);
        assert!(client.unlock(key).await?);
        assert!(!client.unlock(key).await?);

        Ok(())
    }

    #[tokio::test]
    async fn missing_lock() -> Result<(), Error> {
        let client = client()?;
        let key = "missing_lock";

        assert!(matches!(client.unlock(key).await, Err(Error::NotFound)));
        assert!(matches!(client.lock(key).await, Err(Error::NotFound)));

        Ok(())
    }

    #[tokio::test]
    async fn increment_decrement() -> Result<(), Error> {
        let client = client()?;
        let key = "increment_decrement";

        client.put_raw(key, "0").await?;
        assert_eq!(
            "0",
            String::from_utf8_lossy(client.get_raw(key).await?.unwrap().as_ref())
        );
        client.increment(key).await?;
        assert_eq!(
            "1",
            String::from_utf8_lossy(client.get_raw(key).await?.unwrap().as_ref())
        );
        client.decrement(key).await?;
        assert_eq!(
            "0",
            String::from_utf8_lossy(client.get_raw(key).await?.unwrap().as_ref())
        );
        client.decrement(key).await?;
        assert_eq!(
            "-1",
            String::from_utf8_lossy(client.get_raw(key).await?.unwrap().as_ref())
        );
        client.increment(key).await?;
        assert_eq!(
            "0",
            String::from_utf8_lossy(client.get_raw(key).await?.unwrap().as_ref())
        );

        Ok(())
    }

    #[tokio::test]
    async fn non_numeric_increment_decrement() -> Result<(), Error> {
        let client = client()?;
        let key = "non_numeric_increment_decrement";

        client.put_raw(key, "cool").await?;
        println!("{:?}", client.increment(key).await);
        assert!(matches!(
            client.increment(key).await,
            Err(Error::NonNumericValue)
        ));
        assert!(matches!(
            client.decrement(key).await,
            Err(Error::NonNumericValue)
        ));

        Ok(())
    }

    #[tokio::test]
    async fn ttl() -> Result<(), Error> {
        let client = client()?;
        let key = "ttl";

        client.put_raw(key, "cool").await?;
        client.ttl(key, Duration::from_secs(3)).await?;
        assert_eq!(
            "cool",
            String::from_utf8_lossy(client.get_raw(key).await?.unwrap().as_ref())
        );

        Ok(())
    }

    #[tokio::test]
    async fn put() -> Result<(), Error> {
        let client = client()?;
        let value = TestStruct {
            a: 1,
            b: "cool".to_string(),
            c: vec![1, 2, 3],
        };
        client.put("put", &value).await?;
        Ok(())
    }

    #[tokio::test]
    async fn get() -> Result<(), Error> {
        let client = client()?;
        let test_value = TestStruct {
            a: 1,
            b: "cool".to_string(),
            c: vec![1, 2, 3],
        };
        client.put("get", &test_value).await?;
        let db_value = client.get("get").await?;
        assert_eq!(Some(test_value), db_value);
        Ok(())
    }

    #[tokio::test]
    async fn health_check() -> Result<(), Error> {
        let client = client()?;
        client.health_check().await?;
        let client = LucidClient::new("http://localhost:9999")?;
        assert!(client.health_check().await.is_err());
        Ok(())
    }

    #[tokio::test]
    async fn notifications_raw() -> Result<(), Error> {
        let client = client()?;
        let key = "notifications_raw";
        client.put_raw(key, "value1").await?;
        let mut stream = client
            .clone()
            .notifications_raw(|err| RetryPolicy::ForwardError(err))
            .await?;
        let (next, _) = tokio::join!(stream.next(), client.put_raw(key, "value2"));
        assert_eq!(
            next.unwrap().unwrap(),
            Notification {
                key: key.to_string(),
                data: "value2".into()
            }
        );
        Ok(())
    }

    #[tokio::test]
    async fn notifications() -> Result<(), Error> {
        let client = client()?;
        let key = "notifications";
        let test_value1 = TestStruct {
            a: 1,
            b: "value1".to_string(),
            c: vec![1, 2, 3],
        };
        let test_value2 = TestStruct {
            a: 2,
            b: "value2".to_string(),
            c: vec![4, 5, 6],
        };
        client.put(key, &test_value1).await?;
        let mut stream = client
            .clone()
            .notifications(|err| RetryPolicy::ForwardError(err))
            .await?;
        let (next, _) = tokio::join!(stream.next(), client.put(key, &test_value2));
        assert_eq!(
            next.unwrap().unwrap(),
            Notification {
                key: key.to_string(),
                data: test_value2
            }
        );
        Ok(())
    }
}
