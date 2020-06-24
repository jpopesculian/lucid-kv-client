//! A simple Client for the Lucid KV

#[macro_use]
extern crate failure;
#[macro_use]
extern crate fehler;

use bytes::Bytes;
use reqwest::{Body, Client, Url};

#[cfg(feature = "serde")]
use serde::{de::DeserializeOwned, Serialize};

/// Errors when doing Client operations
#[derive(Fail, Debug)]
pub enum Error {
    #[fail(display = "invalid url")]
    InvalidUrl,
    #[fail(display = "invalid response")]
    InvalidResponse,
    #[fail(display = "invalid request: {}", _0)]
    InvalidRequest(reqwest::Error),
    #[cfg(feature = "serde")]
    #[fail(display = "serialize error")]
    SerializeError,
    #[cfg(feature = "serde")]
    #[fail(display = "deserialize error")]
    DeserializeError,
}

/// The main Client
#[derive(Clone, Debug)]
pub struct LucidClient {
    client: Client,
    url: Url,
}

cfg_if::cfg_if! {
    if #[cfg(all(feature = "serde-json", feature = "serde-flexbuffers"))] {
        compile_error!("Cannot use both `serde-json` and `serde-flexbuffers`");
    } else if #[cfg(feature = "serde-json")] {
        use serde_json as serde_mod;
    } else if #[cfg(feature = "serde-flexbuffers")] {
        use flexbuffers as serde_mod;
    }
}

impl LucidClient {
    /// Build a Client from a base url (e.g. `"http://localhost:7020"`)
    #[throws]
    pub fn build<U: AsRef<str>>(base_url: U) -> Self {
        let client = Client::new();
        let url = Url::parse(base_url.as_ref())
            .and_then(|url| url.join("api/kv/"))
            .map_err(|_| Error::InvalidUrl)?;
        Self { client, url }
    }

    /// Store a string or bytes as a value for a key. Creates a new key if it does not exist
    #[throws]
    pub async fn put_raw<K: AsRef<str>, V: Into<Body>>(&self, key: K, value: V) {
        self.client
            .put(self.key_url(key)?)
            .body(value)
            .send()
            .await
            .map_err(Error::InvalidRequest)?;
    }

    /// Gets raw bytes from a key's value
    #[throws]
    pub async fn get_raw<K: AsRef<str>>(&self, key: K) -> Bytes {
        self.client
            .get(self.key_url(key)?)
            .send()
            .await
            .map_err(Error::InvalidRequest)?
            .bytes()
            .await
            .map_err(Error::InvalidRequest)?
    }

    /// Serialize a rust object and store as the value for a key
    #[cfg(feature = "serde")]
    #[throws]
    pub async fn put<K: AsRef<str>, V: Serialize>(&self, key: K, value: &V) {
        self.put_raw(
            key,
            serde_mod::to_vec(value).map_err(|_| Error::SerializeError)?,
        )
        .await?
    }

    /// Get the value for a key and deserialize it into a rust object
    #[cfg(feature = "serde")]
    #[throws]
    pub async fn get<K: AsRef<str>, V: DeserializeOwned>(&self, key: K) -> V {
        serde_mod::from_slice(self.get_raw(key).await?.as_ref())
            .map_err(|_| Error::DeserializeError)?
    }

    #[inline]
    #[throws]
    fn key_url<K: AsRef<str>>(&self, key: K) -> Url {
        let encoded =
            percent_encoding::utf8_percent_encode(key.as_ref(), percent_encoding::NON_ALPHANUMERIC)
                .to_string();
        self.url.join(&encoded).map_err(|_| Error::InvalidUrl)?
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(feature = "serde")]
    use serde::Deserialize;

    #[cfg(feature = "serde")]
    #[derive(Debug, Serialize, Deserialize, Eq, PartialEq)]
    struct TestStruct {
        a: u32,
        b: String,
        c: Vec<u8>,
    }

    #[throws]
    fn client() -> LucidClient {
        LucidClient::build("http://localhost:7020")?
    }

    #[test]
    #[throws]
    fn build() {
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
        assert_eq!(test_value, String::from_utf8_lossy(db_value.as_ref()));
        Ok(())
    }

    #[tokio::test]
    async fn update_raw() -> Result<(), Error> {
        let client = client()?;
        let key = "update_raw";

        let test_value1 = "value1";
        client.put_raw(key, test_value1).await?;
        let db_value = client.get_raw(key).await?;
        assert_eq!(test_value1, String::from_utf8_lossy(db_value.as_ref()));

        let test_value2 = "value2";
        client.put_raw(key, test_value2).await?;
        let db_value = client.get_raw(key).await?;
        assert_eq!(test_value2, String::from_utf8_lossy(db_value.as_ref()));

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
        assert_eq!(test_value, db_value);
        Ok(())
    }
}
