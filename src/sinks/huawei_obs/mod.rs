pub mod config;
pub mod service;
pub mod sink;

use self::{
    config::HuaweiObsConfig,
    sink::{HuaweiObsSink, HuaweiObsSinkError},
};
use vector_core::{
    config::{LogNamespace, Output},
    sink::VectorSink,
};

impl HuaweiObsConfig {
    pub fn build(&self, _cx: crate::context::SinkContext) -> crate::Result<(VectorSink, Output)> {
        let sink = HuaweiObsSink::new(self.clone());
        let output = Output::default();
        Ok((Box::new(sink), output))
    }
}