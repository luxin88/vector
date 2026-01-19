use std::collections::HashMap;
use std::time::Duration;

use async_compression::tokio::bufread;
use bytes::Bytes;
use chrono::{DateTime, TimeZone, Utc};
use futures::{FutureExt, Stream, StreamExt, TryFutureExt, TryStreamExt};
use http::StatusCode;
use hyper::{Body, Client, Method, Request, Uri};
use hyper_tls::HttpsConnector;
use serde::{Deserialize, Serialize};
use snafu::Snafu;
use tokio::io::AsyncRead;
use tokio::pin;
use tokio_util::codec::FramedRead;
use tokio_util::io::StreamReader;
use tracing::Instrument;
use vector_lib::{
    codecs::{
        NewlineDelimitedDecoderConfig,
        decoding::{DeserializerConfig, FramingConfig, NewlineDelimitedDecoderOptions, FramingError},
    },
    config::{LegacyKey, LogNamespace, log_schema},
    configurable::configurable_component,
    event::{BatchNotifier, BatchStatus, EstimatedJsonEncodedSizeOf, MaybeAsLogMut, Event, LogEvent},
    internal_event::{
        ByteSize, BytesReceived, CountByteSize, InternalEventHandle as _, Protocol, Registered,
        StreamClosedError,
    },
    lookup::{PathPrefix, metadata_path, path, owned_value_path},
    source_sender::SendError,
};

use super::util::MultilineConfig;
use crate::{
    codecs::{Decoder, DecodingConfig},
    common::backoff::ExponentialBackoff,
    config::{
        ProxyConfig, SourceAcknowledgementsConfig, SourceConfig, SourceContext, SourceOutput,
    },
    line_agg,
    serde::{bool_or_struct, default_decoding},
    tls::TlsConfig,
};

/// Compression scheme for objects retrieved from OBS.
#[configurable_component]
#[configurable(metadata(docs::advanced))]
#[derive(Clone, Copy, Debug, Derivative, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
#[derivative(Default)]
pub enum Compression {
    /// Automatically attempt to determine the compression scheme.
    ///
    /// The compression scheme of the object is determined from its `Content-Encoding` and
    /// `Content-Type` metadata, as well as the key suffix (for example, `.gz`).
    ///
    /// It is set to `none` if the compression scheme cannot be determined.
    #[derivative(Default)]
    Auto,

    /// Uncompressed.
    None,

    /// GZIP.
    Gzip,

    /// ZSTD.
    Zstd,
}

/// DIS configuration for receiving OBS notifications.
#[configurable_component]
#[derive(Clone, Debug, Derivative)]
#[derivative(Default)]
#[serde(deny_unknown_fields)]
pub struct DisConfig {
    /// The DIS stream name.
    #[configurable(metadata(docs::examples = "my-stream"))]
    pub stream_name: String,

    /// The DIS access key.
    #[configurable(metadata(docs::examples = "AKIAIOSFODNN7EXAMPLE"))]
    pub access_key: String,

    /// The DIS secret key.
    #[configurable(metadata(docs::examples = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"))]
    pub secret_key: String,

    /// The DIS endpoint URL.
    #[configurable(metadata(docs::examples = "https://dis.cn-north-1.myhuaweicloud.com"))]
    pub endpoint: String,

    /// The DIS region.
    #[configurable(metadata(docs::examples = "cn-north-1"))]
    pub region: String,

    /// How long to wait while polling the stream for new messages, in seconds.
    ///
    /// Generally, this should not be changed unless instructed to do so, as if messages are available,
    /// they are always consumed, regardless of the value of `poll_secs`.
    #[serde(default = "default_poll_secs")]
    #[derivative(Default(value = "default_poll_secs()"))]
    #[configurable(metadata(docs::type_unit = "seconds"))]
    pub poll_secs: u32,

    /// Whether to delete the message once it is processed.
    ///
    /// It can be useful to set this to `false` for debugging or during the initial setup.
    #[serde(default = "default_true")]
    #[derivative(Default(value = "default_true()"))]
    pub delete_message: bool,

    /// Whether to delete non-retryable messages.
    ///
    /// If a message is rejected by the sink and not retryable, it is deleted from the stream.
    #[serde(default = "default_true")]
    #[derivative(Default(value = "default_true()"))]
    pub delete_failed_message: bool,
}

const fn default_poll_secs() -> u32 {
    15
}

const fn default_true() -> bool {
    true
}

/// Huawei Cloud OBS source configuration.
#[configurable_component(source(
    "huawei_obs",
    "Collect logs from Huawei Cloud OBS object storage system with DIS notifications."
))]
#[derive(Clone, Debug, Derivative)]
#[serde(default, deny_unknown_fields)]
pub struct HuaweiObsConfig {
    /// The OBS bucket name.
    #[configurable(metadata(docs::examples = "my-bucket"))]
    pub bucket: String,

    /// The OBS access key.
    #[configurable(metadata(docs::examples = "AKIAIOSFODNN7EXAMPLE"))]
    pub access_key: String,

    /// The OBS secret key.
    #[configurable(metadata(docs::examples = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"))]
    pub secret_key: String,

    /// The OBS endpoint URL.
    #[configurable(metadata(docs::examples = "https://obs.cn-north-1.myhuaweicloud.com"))]
    pub endpoint: String,

    /// The compression scheme used for decompressing objects retrieved from OBS.
    pub compression: Compression,

    /// DIS configuration for receiving OBS notifications.
    #[configurable(derived)]
    pub dis: DisConfig,

    /// Multiline aggregation configuration.
    ///
    /// If not specified, multiline aggregation is disabled.
    #[configurable(derived)]
    multiline: Option<MultilineConfig>,

    #[configurable(derived)]
    #[serde(default, deserialize_with = "bool_or_struct")]
    acknowledgements: SourceAcknowledgementsConfig,

    #[configurable(derived)]
    tls_options: Option<TlsConfig>,

    /// The namespace to use for logs. This overrides the global setting.
    #[configurable(metadata(docs::hidden))]
    #[serde(default)]
    log_namespace: Option<bool>,

    #[configurable(derived)]
    #[serde(default = "default_framing")]
    #[derivative(Default(value = "default_framing()"))]
    pub framing: FramingConfig,

    #[configurable(derived)]
    #[serde(default = "default_decoding")]
    #[derivative(Default(value = "default_decoding()"))]
    pub decoding: DeserializerConfig,
}

const fn default_framing() -> FramingConfig {
    FramingConfig::NewlineDelimited(NewlineDelimitedDecoderConfig {
        newline_delimited: NewlineDelimitedDecoderOptions { max_length: None },
    })
}

impl_generate_config_from_default!(HuaweiObsConfig);

#[async_trait::async_trait]
#[typetag::serde(name = "huawei_obs")]
impl SourceConfig for HuaweiObsConfig {
    async fn build(&self, cx: SourceContext) -> crate::Result<super::Source> {
        let log_namespace = cx.log_namespace(self.log_namespace);

        let multiline_config: Option<line_agg::Config> = self
            .multiline
            .as_ref()
            .map(|config| config.try_into())
            .transpose()?;

        Ok(Box::pin(
            self.create_obs_ingestor(multiline_config, &cx.proxy, log_namespace)
                .await?
                .run(cx, self.acknowledgements, log_namespace),
        ))
    }

    fn outputs(&self, global_log_namespace: LogNamespace) -> Vec<SourceOutput> {
        let log_namespace = global_log_namespace.merge(self.log_namespace);
        let mut schema_definition = self
            .decoding
            .schema_definition(log_namespace)
            .with_source_metadata(
                HuaweiObsConfig::NAME,
                Some(LegacyKey::Overwrite(path!("bucket"))),
                path!("bucket"),
                Kind::bytes(),
                None,
            )
            .with_source_metadata(
                HuaweiObsConfig::NAME,
                Some(LegacyKey::Overwrite(path!("object"))),
                path!("object"),
                Kind::bytes(),
                None,
            )
            .with_source_metadata(
                HuaweiObsConfig::NAME,
                Some(LegacyKey::Overwrite(path!("region"))),
                path!("region"),
                Kind::bytes(),
                None,
            )
            .with_source_metadata(
                HuaweiObsConfig::NAME,
                None,
                path!("timestamp"),
                Kind::timestamp(),
                Some("timestamp"),
            )
            .with_standard_vector_source_metadata()
            .with_source_metadata(
                HuaweiObsConfig::NAME,
                None,
                path!("metadata"),
                Kind::object(Collection::empty().with_unknown(Kind::bytes())).or_undefined(),
                None,
            );

        if log_namespace == LogNamespace::Legacy {
            schema_definition = schema_definition.unknown_fields(Kind::bytes());
        }

        vec![SourceOutput::new_maybe_logs(
            self.decoding.output_type(),
            schema_definition,
        )]
    }

    fn can_acknowledge(&self) -> bool {
        true
    }
}

impl HuaweiObsConfig {
    async fn create_obs_ingestor(
        &self,
        multiline: Option<line_agg::Config>,
        proxy: &ProxyConfig,
        log_namespace: LogNamespace,
    ) -> crate::Result<Ingestor> {
        let decoder =
            DecodingConfig::new(self.framing.clone(), self.decoding.clone(), log_namespace)
                .build()?;

        let ingestor = Ingestor::new(
            self.bucket.clone(),
            self.access_key.clone(),
            self.secret_key.clone(),
            self.endpoint.clone(),
            self.dis.clone(),
            self.compression,
            multiline,
            decoder,
        )
        .await?;

        Ok(ingestor)
    }
}

#[derive(Debug, Snafu)]
enum CreateObsIngestorError {
    #[snafu(display("Failed to create HTTP client: {}", source))]
    CreateHttpClient { source: hyper::Error },
}

#[derive(Debug, Snafu)]
pub enum ProcessingError {
    #[snafu(display("Failed to fetch obs://{}/{}: {}", bucket, key, source))]
    GetObject {
        source: hyper::Error,
        bucket: String,
        key: String,
    },
    #[snafu(display("Failed to read all of obs://{}/{}: {}", bucket, key, source))]
    ReadObject {
        source: Box<dyn FramingError>,
        bucket: String,
        key: String,
    },
    #[snafu(display("Failed to flush all of obs://{}/{}: {}", bucket, key, source))]
    PipelineSend {
        source: SendError,
        bucket: String,
        key: String,
    },
    #[snafu(display("Failed to read from DIS stream: {}", source))]
    ReadDisStream { source: hyper::Error },
    #[snafu(display(
        "Sink reported an error sending events for an obs object: obs://{}/{}",
        bucket,
        key
    ))]
    ErrorAcknowledgement {
        bucket: String,
        key: String,
    },
}

pub struct Ingestor {
    bucket: String,
    access_key: String,
    secret_key: String,
    endpoint: String,
    dis_config: DisConfig,
    compression: Compression,
    multiline: Option<line_agg::Config>,
    decoder: Decoder,
    http_client: Client<HttpsConnector<hyper::client::HttpConnector>>,
}

impl Ingestor {
    pub async fn new(
        bucket: String,
        access_key: String,
        secret_key: String,
        endpoint: String,
        dis_config: DisConfig,
        compression: Compression,
        multiline: Option<line_agg::Config>,
        decoder: Decoder,
    ) -> Result<Ingestor, CreateObsIngestorError> {
        let https = HttpsConnector::new();
        let client = Client::builder().build::<_, hyper::Body>(https);

        Ok(Ingestor {
            bucket,
            access_key,
            secret_key,
            endpoint,
            dis_config,
            compression,
            multiline,
            decoder,
            http_client: client,
        })
    }

    pub async fn run(
        self,
        cx: SourceContext,
        acknowledgements: SourceAcknowledgementsConfig,
        log_namespace: LogNamespace,
    ) -> Result<(), ()> {
        let acknowledgements = cx.do_acknowledgements(acknowledgements);
        let process = IngestorProcess::new(
            self,
            cx.out,
            cx.shutdown,
            log_namespace,
            acknowledgements,
        );
        process.run().await
    }
}

pub struct IngestorProcess {
    ingestor: Ingestor,
    out: SourceSender,
    shutdown: ShutdownSignal,
    log_namespace: LogNamespace,
    acknowledgements: bool,
    bytes_received: Registered<BytesReceived>,
    events_received: Registered<EventsReceived>,
    backoff: ExponentialBackoff,
}

impl IngestorProcess {
    pub fn new(
        ingestor: Ingestor,
        out: SourceSender,
        shutdown: ShutdownSignal,
        log_namespace: LogNamespace,
        acknowledgements: bool,
    ) -> Self {
        Self {
            ingestor,
            out,
            shutdown,
            log_namespace,
            acknowledgements,
            bytes_received: register!(BytesReceived::from(Protocol::HTTP)),
            events_received: register!(EventsReceived),
            backoff: ExponentialBackoff::default().max_delay(Duration::from_secs(30)),
        }
    }

    async fn run(mut self) {
        let shutdown = self.shutdown.clone().fuse();
        pin!(shutdown);

        loop {
            select! {
                _ = &mut shutdown => break,
                result = self.run_once() => {
                    match result {
                        Ok(()) => {
                            self.backoff.reset();
                        }
                        Err(_) => {
                            let delay = self.backoff.next().expect("backoff never ends");
                            trace!(
                                delay_ms = delay.as_millis(),
                                "`run_once` failed, will retry after delay.",
                            );
                            tokio::time::sleep(delay).await;
                        }
                    }
                },
            }
        }
    }

    async fn run_once(&mut self) -> Result<(), ()> {
        match self.read_dis_records().await {
            Ok(records) => {
                debug!(
                    message = "Read records from DIS stream",
                    stream = self.ingestor.dis_config.stream_name,
                    count = records.len(),
                );

                for record in records {
                    match self.handle_dis_record(record).await {
                        Ok(()) => {
                            debug!(
                                message = "Processed DIS record",
                                stream = self.ingestor.dis_config.stream_name,
                            );
                        }
                        Err(err) => {
                            error!(
                                message = "Failed to process DIS record",
                                stream = self.ingestor.dis_config.stream_name,
                                error = %err,
                            );
                        }
                    }
                }
            }
            Err(err) => {
                error!(
                    message = "Failed to read records from DIS stream",
                    stream = self.ingestor.dis_config.stream_name,
                    error = %err,
                );
                return Err(());
            }
        }

        Ok(())
    }

    async fn read_dis_records(&mut self) -> Result<Vec<DisRecord>, hyper::Error> {
        let url = format!(
            "{}/v2/{}/streams/{}/records",
            self.ingestor.dis_config.endpoint,
            self.ingestor.dis_config.region,
            self.ingestor.dis_config.stream_name
        );

        let req = Request::builder()
            .method(Method::GET)
            .uri(url)
            .header("Content-Type", "application/json")
            .header("X-Auth-Token", self.get_auth_token().await?)
            .body(Body::empty())?;

        let resp = self.ingestor.http_client.request(req).await?;

        if resp.status() != StatusCode::OK {
            return Err(hyper::Error::new(
                hyper::error::Kind::Status,
                format!("Unexpected status: {}", resp.status()),
            ));
        }

        let body = hyper::body::to_bytes(resp.into_body()).await?;
        let response: DisReadRecordsResponse = serde_json::from_slice(&body)
            .map_err(|e| hyper::Error::new(hyper::error::Kind::Parse, e))?;

        Ok(response.records)
    }

    async fn get_auth_token(&mut self) -> Result<String, hyper::Error> {
        let url = "https://iam.cn-north-1.myhuaweicloud.com/v3/auth/tokens";

        let request = IamAuthRequest {
            auth: IamAuth {
                identity: IamIdentity {
                    methods: vec!["password"],
                    password: IamPassword {
                        user: IamUser {
                            name: self.ingestor.dis_config.access_key.clone(),
                            password: self.ingestor.dis_config.secret_key.clone(),
                            domain: IamDomain {
                                name: "default",
                            },
                        },
                    },
                },
                scope: IamScope {
                    project: IamProject {
                        name: self.ingestor.dis_config.region.clone(),
                    },
                },
            },
        };

        let body = serde_json::to_vec(&request)
            .map_err(|e| hyper::Error::new(hyper::error::Kind::Serialize, e))?;

        let req = Request::builder()
            .method(Method::POST)
            .uri(url)
            .header("Content-Type", "application/json")
            .body(Body::from(body))?;

        let resp = self.ingestor.http_client.request(req).await?;

        if resp.status() != StatusCode::CREATED {
            return Err(hyper::Error::new(
                hyper::error::Kind::Status,
                format!("Unexpected status: {}", resp.status()),
            ));
        }

        resp.headers()
            .get("X-Subject-Token")
            .ok_or_else(|| hyper::Error::new(hyper::error::Kind::Header, "Missing X-Subject-Token"))?
            .to_str()
            .map_err(|e| hyper::Error::new(hyper::error::Kind::Header, e))
            .map(|s| s.to_string())
    }

    async fn handle_dis_record(&mut self, record: DisRecord) -> Result<(), ProcessingError> {
        let obs_notification: ObsNotification = serde_json::from_slice(&record.data)
            .map_err(|e| ProcessingError::ReadDisStream { source: e.into() })?;

        for object in obs_notification.objects {
            match self.process_object(&object.key).await {
                Ok(()) => {
                    debug!(
                        message = "Processed OBS object from DIS notification",
                        bucket = self.ingestor.bucket,
                        key = object.key,
                    );
                }
                Err(err) => {
                    error!(
                        message = "Failed to process OBS object from DIS notification",
                        bucket = self.ingestor.bucket,
                        key = object.key,
                        error = %err,
                    );
                    return Err(err);
                }
            }
        }

        Ok(())
    }

    async fn process_object(&mut self, key: &str) -> Result<(), ProcessingError> {
        let object_result = self.get_obs_object(key).await;

        let (body, metadata, last_modified) = object_result.context(GetObjectSnafu {
            bucket: self.ingestor.bucket.clone(),
            key: key.to_string(),
        })?;

        debug!(
            message = "Got OBS object from DIS notification",
            bucket = self.ingestor.bucket,
            key = key,
        );

        let timestamp = last_modified.map(|ts| {
            Utc.timestamp_opt(ts.secs(), ts.subsec_nanos())
                .single()
                .expect("invalid timestamp")
        });

        let (batch, receiver) = BatchNotifier::maybe_new_with_receiver(self.acknowledgements);
        let object_reader = obs_object_decoder(
            self.ingestor.compression,
            key,
            metadata.get("content-encoding").map(|v| v.as_str()),
            metadata.get("content-type").map(|v| v.as_str()),
            body,
        )
        .await;

        let mut read_error = None;
        let bytes_received = self.bytes_received.clone();
        let events_received = self.events_received.clone();
        let lines: Box<dyn Stream<Item = Bytes> + Send + Unpin> = Box::new(
            FramedRead::new(object_reader, self.ingestor.decoder.framer.clone())
                .map(|res| {
                    res.inspect(|bytes| {
                        bytes_received.emit(ByteSize(bytes.len()));
                    })
                    .map_err(|err| {
                        read_error = Some(err);
                    })
                    .ok()
                })
                .take_while(|res| futures::future::ready(res.is_some()))
                .map(|r| r.expect("validated by take_while")),
        );

        let lines: Box<dyn Stream<Item = Bytes> + Send + Unpin> = match &self.ingestor.multiline {
            Some(config) => Box::new(
                line_agg::LineAgg::new(
                    lines.map(|line| ((), line, ())),
                    line_agg::Logic::new(config.clone()),
                )
                .map(|(_src, line, _context, _lastline_context)| line),
            ),
            None => lines,
        };

        let mut stream = lines.flat_map(|line| {
            let events = match self.ingestor.decoder.deserializer_parse(line) {
                Ok((events, _events_size)) => events,
                Err(_error) => {
                    SmallVec::new()
                }
            };

            let events = events
                .into_iter()
                .map(|mut event: Event| {
                    event = event.with_batch_notifier_option(&batch);
                    if let Some(log_event) = event.maybe_as_log_mut() {
                        handle_single_log(
                            log_event,
                            self.log_namespace,
                            &self.ingestor.bucket,
                            key,
                            &self.ingestor.dis_config.region,
                            &metadata,
                            timestamp,
                        );
                    }
                    events_received.emit(CountByteSize(1, event.estimated_json_encoded_size_of()));
                    event
                })
                .collect::<Vec<Event>>();
            futures::stream::iter(events)
        });

        let send_error = match self.out.send_event_stream(&mut stream).await {
            Ok(_) => None,
            Err(SendError::Closed) => {
                let (count, _) = stream.size_hint();
                emit!(StreamClosedError { count });
                Some(SendError::Closed)
            }
            Err(SendError::Timeout) => unreachable!("No timeout is configured here"),
        };

        drop(stream);

        let bucket = &self.ingestor.bucket;
        let key = key.to_string();

        if read_error.is_some() {
            emit!(ObsObjectProcessingFailed { bucket, key });
        } else {
            emit!(ObsObjectProcessingSucceeded { bucket, key });
        }

        match receiver {
            None => Ok(()),
            Some(receiver) => {
                let result = receiver.await;
                match result {
                    BatchStatus::Delivered => {
                        debug!(
                            message = "OBS object from DIS delivered",
                            bucket = bucket,
                            key = key,
                        );
                        Ok(())
                    }
                    BatchStatus::Errored => Err(ProcessingError::ErrorAcknowledgement {
                        bucket: bucket.clone(),
                        key: key.clone(),
                    }),
                    BatchStatus::Rejected => {
                        if self.ingestor.dis_config.delete_failed_message {
                            warn!(
                                message = "OBS object from DIS was rejected. Deleting failed message.",
                                bucket = bucket,
                                key = key,
                            );
                            Ok(())
                        } else {
                            Err(ProcessingError::ErrorAcknowledgement {
                                bucket: bucket.clone(),
                                key: key.clone(),
                            });
                        }
                    }
                }
            }
        }
    }

    async fn get_obs_object(&mut self, key: &str) -> Result<(Body, HashMap<String, String>, Option<chrono::Duration>), hyper::Error> {
        let url = format!("{}/{}/{}", self.ingestor.endpoint, self.ingestor.bucket, key);

        let req = Request::builder()
            .method(Method::GET)
            .uri(url)
            .header("Authorization", self.get_obs_auth_header(key).await?)
            .body(Body::empty())?;

        let resp = self.ingestor.http_client.request(req).await?;

        if resp.status() != StatusCode::OK {
            return Err(hyper::Error::new(
                hyper::error::Kind::Status,
                format!("Unexpected status: {}", resp.status()),
            ));
        }

        let mut metadata = HashMap::new();
        for (name, value) in resp.headers() {
            if name.as_str().starts_with("x-obs-") || 
               name.as_str() == "content-type" || 
               name.as_str() == "content-encoding" || 
               name.as_str() == "last-modified" {
                if let Ok(value_str) = value.to_str() {
                    metadata.insert(name.as_str().to_string(), value_str.to_string());
                }
            }
        }

        let last_modified = resp.headers()
            .get("last-modified")
            .and_then(|v| v.to_str().ok())
            .and_then(|s| chrono::DateTime::parse_from_rfc2822(s).ok())
            .map(|dt| dt.signed_duration_since(chrono::DateTime::from_utc(chrono::NaiveDateTime::from_timestamp(0, 0), chrono::Utc)));

        Ok((resp.into_body(), metadata, last_modified))
    }

    async fn get_obs_auth_header(&mut self, key: &str) -> Result<String, hyper::Error> {
        let date = chrono::Utc::now().to_rfc2822();
        let string_to_sign = format!("GET\n\n\n{}\n/{}/{}", date, self.ingestor.bucket, key);
        
        let signature = hmac_sha1::HmacSha1::from_key(self.ingestor.secret_key.as_bytes())
            .update(string_to_sign.as_bytes())
            .digest();
        
        let signature_base64 = base64::encode(signature.as_ref());
        
        Ok(format!("OBS {}:{}", self.ingestor.access_key, signature_base64))
    }
}

#[derive(Debug, Deserialize)]
struct DisReadRecordsResponse {
    records: Vec<DisRecord>,
}

#[derive(Debug, Deserialize)]
struct DisRecord {
    data: Vec<u8>,
    partition_id: String,
    timestamp: Option<chrono::Duration>,
}

#[derive(Debug, Deserialize)]
struct ObsNotification {
    objects: Vec<ObsObject>,
}

#[derive(Debug, Deserialize)]
struct ObsObject {
    key: String,
}

#[derive(Debug, Serialize)]
struct IamAuthRequest {
    auth: IamAuth,
}

#[derive(Debug, Serialize)]
struct IamAuth {
    identity: IamIdentity,
    scope: IamScope,
}

#[derive(Debug, Serialize)]
struct IamIdentity {
    methods: Vec<&'static str>,
    password: IamPassword,
}

#[derive(Debug, Serialize)]
struct IamPassword {
    user: IamUser,
}

#[derive(Debug, Serialize)]
struct IamUser {
    name: String,
    password: String,
    domain: IamDomain,
}

#[derive(Debug, Serialize)]
struct IamDomain {
    name: &'static str,
}

#[derive(Debug, Serialize)]
struct IamScope {
    project: IamProject,
}

#[derive(Debug, Serialize)]
struct IamProject {
    name: String,
}

#[derive(Debug)]
pub struct ObsObjectProcessingFailed<'a> {
    pub bucket: &'a str,
    pub key: &'a str,
}

impl vector_lib::internal_event::InternalEvent for ObsObjectProcessingFailed<'_> {
    fn emit(self, _handle: vector_lib::internal_event::InternalEventHandle) {
        error!(
            message = "Failed to process OBS object",
            bucket = self.bucket,
            key = self.key,
        );
    }
}

#[derive(Debug)]
pub struct ObsObjectProcessingSucceeded<'a> {
    pub bucket: &'a str,
    pub key: &'a str,
}

impl vector_lib::internal_event::InternalEvent for ObsObjectProcessingSucceeded<'_> {
    fn emit(self, _handle: vector_lib::internal_event::InternalEventHandle) {
        debug!(
            message = "Successfully processed OBS object",
            bucket = self.bucket,
            key = self.key,
        );
    }
}

async fn obs_object_decoder(
    compression: Compression,
    key: &str,
    content_encoding: Option<&str>,
    content_type: Option<&str>,
    body: Body,
) -> Box<dyn tokio::io::AsyncRead + Send + Unpin> {
    let body_reader = StreamReader::new(body.map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e)));
    let r = tokio::io::BufReader::new(body_reader);

    let compression = match compression {
        Auto => determine_compression(content_encoding, content_type, key).unwrap_or(None),
        _ => compression,
    };

    use Compression::*;
    match compression {
        Auto => unreachable!(),
        None => Box::new(r),
        Gzip => Box::new({
            let mut decoder = bufread::GzipDecoder::new(r);
            decoder.multiple_members(true);
            decoder
        }),
        Zstd => Box::new({
            let mut decoder = bufread::ZstdDecoder::new(r);
            decoder.multiple_members(true);
            decoder
        }),
    }
}

fn handle_single_log(
    log: &mut LogEvent,
    log_namespace: LogNamespace,
    bucket: &str,
    key: &str,
    region: &str,
    metadata: &HashMap<String, String>,
    timestamp: Option<DateTime<Utc>>,
) {
    log_namespace.insert_source_metadata(
        HuaweiObsConfig::NAME,
        log,
        Some(LegacyKey::Overwrite(path!("bucket"))),
        path!("bucket"),
        Bytes::from(bucket.as_bytes().to_vec()),
    );

    log_namespace.insert_source_metadata(
        HuaweiObsConfig::NAME,
        log,
        Some(LegacyKey::Overwrite(path!("object"))),
        path!("object"),
        Bytes::from(key.as_bytes().to_vec()),
    );

    log_namespace.insert_source_metadata(
        HuaweiObsConfig::NAME,
        log,
        Some(LegacyKey::Overwrite(path!("region"))),
        path!("region"),
        Bytes::from(region.as_bytes().to_vec()),
    );

    for (key, value) in metadata {
        log_namespace.insert_source_metadata(
            HuaweiObsConfig::NAME,
            log,
            Some(LegacyKey::Overwrite(path!(key))),
            path!("metadata", key.as_str()),
            value.clone(),
        );
    }

    log_namespace.insert_vector_metadata(
        log,
        log_schema().source_type_key(),
        path!("source_type"),
        Bytes::from_static(HuaweiObsConfig::NAME.as_bytes()),
    );

    match log_namespace {
        LogNamespace::Vector => {
            if let Some(timestamp) = timestamp {
                log.insert(metadata_path!(HuaweiObsConfig::NAME, "timestamp"), timestamp);
            }

            log.insert(metadata_path!("vector", "ingest_timestamp"), Utc::now());
        }
        LogNamespace::Legacy => {
            if let Some(timestamp_key) = log_schema().timestamp_key() {
                log.try_insert(
                    (PathPrefix::Event, timestamp_key),
                    timestamp.unwrap_or_else(Utc::now),
                );
            }
        }
    };
}

fn determine_compression(
    content_encoding: Option<&str>,
    content_type: Option<&str>,
    key: &str,
) -> Option<Compression> {
    content_encoding
        .and_then(content_encoding_to_compression)
        .or_else(|| content_type.and_then(content_type_to_compression))
        .or_else(|| object_key_to_compression(key))
}

fn content_encoding_to_compression(content_encoding: &str) -> Option<Compression> {
    match content_encoding {
        "gzip" => Some(Compression::Gzip),
        "zstd" => Some(Compression::Zstd),
        _ => None,
    }
}

fn content_type_to_compression(content_type: &str) -> Option<Compression> {
    match content_type {
        "application/gzip" | "application/x-gzip" => Some(Compression::Gzip),
        "application/zstd" => Some(Compression::Zstd),
        _ => None,
    }
}

fn object_key_to_compression(key: &str) -> Option<Compression> {
    let extension = std::path::Path::new(key)
        .extension()
        .and_then(std::ffi::OsStr::to_str);

    use Compression::*;
    extension.and_then(|extension| match extension {
        "gz" => Some(Gzip),
        "zst" => Some(Zstd),
        _ => Option::None,
    })
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn determine_compression() {
        use super::Compression;

        let cases = vec![
            ("out.log", Some("gzip"), None, Some(Compression::Gzip)),
            (
                "out.log",
                None,
                Some("application/gzip"),
                Some(Compression::Gzip),
            ),
            ("out.log.gz", None, None, Some(Compression::Gzip)),
            ("out.txt", None, None, None),
        ];
        for case in cases {
            let (key, content_encoding, content_type, expected) = case;
            assert_eq!(
                super::determine_compression(content_encoding, content_type, key),
                expected,
                "key={key:?} content_encoding={content_encoding:?} content_type={content_type:?}",
            );
        }
    }
}