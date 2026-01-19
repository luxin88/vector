use std::time::Duration;

use async_compression::tokio::bufread;
use bytes::Bytes;
use futures::{future, ready, Future, FutureExt, Sink, Stream, StreamExt, TryFutureExt, TryStreamExt};
use http::StatusCode;
use hyper::{Body, Client, Method, Request, Uri};
use hyper_tls::HttpsConnector;
use serde::Serialize;
use snafu::Snafu;
use tokio::io::AsyncRead;
use tokio::pin;
use tokio_util::io::StreamReader;
use tower::Service;
use tracing::Instrument;
use vector_core::{
    config::LogNamespace,
    event::{Event, EventFinalizer, Finalizable, Metric, MetricKind, MetricValue},
    internal_event::{
        error_kind, ByteSize, BytesSent, Count, CountByteSize, EventSent, InternalEventHandle as _,
        MaybeEnabled, Registered,
    },
    stream::{BatcherSettings, DriverResponse, StreamExt as _, StreamResult},
    sink::VectorSink,
};
use vector_lib::{
    config::log_schema,
    event::{BatchNotifier, BatchStatus, EstimatedJsonEncodedSizeOf},
};

use super::{config::HuaweiObsConfig, sink::HuaweiObsSinkError};

#[derive(Debug, Snafu)]
pub enum HuaweiObsServiceError {
    #[snafu(display("Failed to send request: {}", source))]
    RequestFailed { source: hyper::Error },

    #[snafu(display("Unexpected response status: {}", status))]
    UnexpectedStatus { status: http::StatusCode },

    #[snafu(display("Failed to serialize payload: {}", source))]
    SerializationFailed { source: serde_json::Error },

    #[snafu(display("Failed to compress payload: {}", source))]
    CompressionFailed { source: std::io::Error },
}

pub struct HuaweiObsService {
    config: HuaweiObsConfig,
    http_client: Client<HttpsConnector<hyper::client::HttpConnector>>,
}

impl HuaweiObsService {
    pub fn new(config: HuaweiObsConfig) -> Self {
        let https = HttpsConnector::new();
        let client = Client::builder().build::<_, hyper::Body>(https);

        Self {
            config,
            http_client: client,
        }
    }

    async fn get_auth_header(&self, method: &str, path: &str, content_type: Option<&str>) -> Result<String, hyper::Error> {
        let date = chrono::Utc::now().to_rfc2822();
        let content_type = content_type.unwrap_or("");
        
        let string_to_sign = format!("{}\n\n{}\n{}\n/{}/{}", method, content_type, date, self.config.bucket, path);
        
        let signature = hmac_sha1::HmacSha1::from_key(self.config.secret_key.as_bytes())
            .update(string_to_sign.as_bytes())
            .digest();
        
        let signature_base64 = base64::encode(signature.as_ref());
        
        Ok(format!("OBS {}:{}", self.config.access_key, signature_base64))
    }

    async fn put_object(&self, key: &str, body: Body, content_type: &str) -> Result<(), HuaweiObsServiceError> {
        let url = format!("{}/{}/{}", self.config.endpoint, self.config.bucket, key);

        let auth_header = self.get_auth_header("PUT", key, Some(content_type)).await
            .map_err(|e| HuaweiObsServiceError::RequestFailed { source: e })?;

        let req = Request::builder()
            .method(Method::PUT)
            .uri(url)
            .header("Authorization", auth_header)
            .header("Content-Type", content_type)
            .header("Date", chrono::Utc::now().to_rfc2822())
            .body(body)
            .map_err(|e| HuaweiObsServiceError::RequestFailed { source: e })?;

        let resp = self.http_client.request(req).await
            .map_err(|e| HuaweiObsServiceError::RequestFailed { source: e })?;

        if resp.status() != StatusCode::OK && resp.status() != StatusCode::CREATED {
            return Err(HuaweiObsServiceError::UnexpectedStatus { status: resp.status() });
        }

        Ok(())
    }
}

impl Service<Bytes> for HuaweiObsService {
    type Response = ();
    type Error = HuaweiObsServiceError;
    type Future = future::BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, _cx: &mut std::task::Context<'_>) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }

    fn call(&mut self, bytes: Bytes) -> Self::Future {
        let config = self.config.clone();
        let client = self.http_client.clone();

        async move {
            let key = format!("{}{}.json", config.key_prefix, chrono::Utc::now().format("%Y%m%d%H%M%S%f"));
            let body = Body::from(bytes);

            let service = HuaweiObsService {
                config,
                http_client: client,
            };

            service.put_object(&key, body, "application/json").await
        }.boxed()
    }
}