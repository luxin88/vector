use vector_lib::configurable::configurable_component;

#[configurable_component(sink(
    "huawei_obs",
    "Send events to Huawei Cloud OBS (Object Storage Service)."
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

    /// The path prefix for objects in the bucket.
    #[configurable(metadata(docs::examples = "logs/"))]
    pub key_prefix: String,

    /// The compression type to use for objects.
    pub compression: Option<Compression>,
}

#[derive(Clone, Copy, Debug, Derivative, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "lowercase")]
#[derivative(Default)]
pub enum Compression {
    #[derivative(Default)]
    None,
    Gzip,
    Zstd,
}

impl_generate_config_from_default!(HuaweiObsConfig);