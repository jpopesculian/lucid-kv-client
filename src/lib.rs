//! A simple Client for the Lucid KV
//!
//! Currently supports operations for `get`, `put`, `delete` and `exists`.
//!
//! `lock`, `unlock`, `increment`, `decrement` and `ttl` are still being implemented.
//!
//! Notifications currently unsupported

#[macro_use]
extern crate failure;

#[macro_use]
extern crate fehler;

use bytes::Bytes;
use jsonwebtoken::EncodingKey;
use reqwest::header::{self, HeaderMap, HeaderValue};
use reqwest::{Body, Client, ClientBuilder, StatusCode, Url};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::time::{SystemTime, UNIX_EPOCH};

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
    #[fail(display = "serialize error")]
    SerializeError,
    #[fail(display = "deserialize error")]
    DeserializeError,
    #[fail(display = "invalid JWT key")]
    InvalidJWTKey,
}

/// Whether a Key was created or not
#[repr(u16)]
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum PutStatus {
    Ok,
    Created,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct Claims {
    pub sub: String,
    pub iss: String,
    pub iat: i64,
    pub exp: i64,
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

    /// Store a string or bytes as a value for a key. Creates a new key if it does not exist
    #[throws]
    pub async fn put_raw<K: AsRef<str>, V: Into<Body>>(&self, key: K, value: V) -> PutStatus {
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
    pub async fn get_raw<K: AsRef<str>>(&self, key: K) -> Option<Bytes> {
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
    pub async fn delete<K: AsRef<str>>(&self, key: K) -> bool {
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
    pub async fn exists<K: AsRef<str>>(&self, key: K) -> bool {
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
    pub async fn put<K: AsRef<str>, V: Serialize>(&self, key: K, value: &V) -> PutStatus {
        self.put_raw(
            key,
            serde_mod::to_vec(value).map_err(|_| Error::SerializeError)?,
        )
        .await?
    }

    /// Get the value for a key and deserialize it into a rust object
    #[throws]
    pub async fn get<K: AsRef<str>, V: DeserializeOwned>(&self, key: K) -> Option<V> {
        let bytes = self.get_raw(key).await?;
        match bytes {
            None => None,
            Some(bytes) => {
                Some(serde_mod::from_slice(bytes.as_ref()).map_err(|_| Error::DeserializeError)?)
            }
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

#[cfg(test)]
mod tests {
    use super::*;

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

    #[cfg(feature = "serde")]
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

    #[cfg(feature = "serde")]
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
}
