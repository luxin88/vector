use std::collections::HashMap;
use std::time::Duration;

use bytes::Bytes;
use futures::{future, stream, Stream, StreamExt, TryFutureExt, TryStreamExt};
use snafu::Snafu;
use tokio::pin;
use tower::ServiceExt;
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

use super::{
    config::HuaweiObsConfig,
    service::{HuaweiObsService, HuaweiObsServiceError},
};

#[derive(Debug, Snafu)]
pub enum HuaweiObsSinkError {
    #[snafu(display("Service error: {}", source))]
    ServiceError { source: HuaweiObsServiceError },
}

pub struct HuaweiObsSink {
    service: HuaweiObsService,
    batcher_settings: BatcherSettings,
}

impl HuaweiObsSink {
    pub fn new(config: HuaweiObsConfig) -> Self {
        let service = HuaweiObsService::new(config.clone());

        Self {
            service,
            batcher_settings: BatcherSettings::default(),
        }
    }
}

impl VectorSink for HuaweiObsSink {
    type Input = Event;
    type Error = HuaweiObsSinkError;

    fn run(mut self, input: impl Stream<Item = StreamResult<Self::Input>> + Send + 'static) -> DriverResponse {
        let input = input.filter_map(|result| future::ready(result.ok()));

        let batches = input.batched(self.batcher_settings);

        let stream = batches.for_each_concurrent(None, |batch| {
            let mut service = self.service.clone();
            let mut notifier = BatchNotifier::new_with_size(batch.len());

            async move {
                let events = batch.into_iter().collect::<Vec<_>>();
                let payload = serde_json::to_vec(&events)
                    .map_err(|e| HuaweiObsServiceError::SerializationFailed { source: e })?;

                let bytes = Bytes::from(payload);

                match service.oneshot(bytes).await {
                    Ok(_) => {
                        notifier.batch_notify(BatchStatus::Delivered);
                        Ok(())
                    }
                    Err(error) => {
                        error!(
                            message = "Failed to send events to Huawei OBS",
                            error = %error,
                        );
                        notifier.batch_notify(BatchStatus::Errored);
                        Err(HuaweiObsSinkError::ServiceError { source: error })
                    }
                }
            }
            .in_current_span()
        });

        Box::pin(stream)
    }
}