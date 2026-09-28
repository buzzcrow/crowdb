use std::{
    array,
    sync::{
        atomic::{AtomicU16, AtomicU64, AtomicU8, Ordering},
        Arc,
    },
    time::Instant,
};

use super::routes::Route;

const ROUTE_COUNT: usize = 9;
const OUTCOME_COUNT: usize = 7;

pub const ICEBERG_ROUTE_NAMES: [&str; ROUTE_COUNT] = [
    "config",
    "namespace_read",
    "namespace_write",
    "table_read",
    "table_write",
    "credentials",
    "file",
    "admin_metrics",
    "unsupported",
];
pub const ICEBERG_OUTCOME_NAMES: [&str; OUTCOME_COUNT] = [
    "success",
    "unauthorized",
    "conflict",
    "client_error",
    "unavailable",
    "server_error",
    "cancelled",
];

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, serde::Serialize)]
pub struct MetricCounts {
    pub requests: u64,
    pub request_bytes: u64,
    pub response_bytes: u64,
    pub dispatch_latency_ns: u64,
    pub lifetime_ns: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize)]
pub struct IcebergMetricsSnapshot {
    pub routes: [[MetricCounts; OUTCOME_COUNT]; ROUTE_COUNT],
    pub retry_new: u64,
    pub retry_resume: u64,
    pub retry_replay: u64,
    pub selected_versions: [u64; 3],
}

struct Counters {
    requests: AtomicU64,
    request_bytes: AtomicU64,
    response_bytes: AtomicU64,
    dispatch_latency_ns: AtomicU64,
    lifetime_ns: AtomicU64,
}

impl Counters {
    fn new() -> Self {
        Self {
            requests: AtomicU64::new(0),
            request_bytes: AtomicU64::new(0),
            response_bytes: AtomicU64::new(0),
            dispatch_latency_ns: AtomicU64::new(0),
            lifetime_ns: AtomicU64::new(0),
        }
    }

    fn snapshot(&self) -> MetricCounts {
        MetricCounts {
            requests: self.requests.load(Ordering::Relaxed),
            request_bytes: self.request_bytes.load(Ordering::Relaxed),
            response_bytes: self.response_bytes.load(Ordering::Relaxed),
            dispatch_latency_ns: self.dispatch_latency_ns.load(Ordering::Relaxed),
            lifetime_ns: self.lifetime_ns.load(Ordering::Relaxed),
        }
    }
}

pub(super) struct IcebergMetrics {
    routes: [[Counters; OUTCOME_COUNT]; ROUTE_COUNT],
    retry: [AtomicU64; 3],
    selected_versions: [AtomicU64; 3],
}

impl Default for IcebergMetrics {
    fn default() -> Self {
        Self {
            routes: array::from_fn(|_| array::from_fn(|_| Counters::new())),
            retry: array::from_fn(|_| AtomicU64::new(0)),
            selected_versions: array::from_fn(|_| AtomicU64::new(0)),
        }
    }
}

impl IcebergMetrics {
    pub(super) fn snapshot(&self) -> IcebergMetricsSnapshot {
        IcebergMetricsSnapshot {
            routes: array::from_fn(|route| array::from_fn(|outcome| self.routes[route][outcome].snapshot())),
            retry_new: self.retry[0].load(Ordering::Relaxed),
            retry_resume: self.retry[1].load(Ordering::Relaxed),
            retry_replay: self.retry[2].load(Ordering::Relaxed),
            selected_versions: array::from_fn(|index| self.selected_versions[index].load(Ordering::Relaxed)),
        }
    }
}

pub(super) struct RequestObservation {
    metrics: Arc<IcebergMetrics>,
    route: usize,
    started: Instant,
    request_bytes: AtomicU64,
    response_bytes: AtomicU64,
    dispatch_ns: AtomicU64,
    status: AtomicU16,
    retry: AtomicU8,
    version: AtomicU8,
}

impl RequestObservation {
    pub(super) fn new(metrics: Arc<IcebergMetrics>, route: usize) -> Arc<Self> {
        Arc::new(Self {
            metrics,
            route,
            started: Instant::now(),
            request_bytes: AtomicU64::new(0),
            response_bytes: AtomicU64::new(0),
            dispatch_ns: AtomicU64::new(0),
            status: AtomicU16::new(0),
            retry: AtomicU8::new(0),
            version: AtomicU8::new(0),
        })
    }

    pub(super) fn dispatched(&self, status: u16) {
        self.status.store(status, Ordering::Relaxed);
        self.dispatch_ns
            .store(elapsed_ns(self.started), Ordering::Relaxed);
    }

    pub(super) fn response_bytes(&self, length: usize) {
        self.response_bytes.fetch_add(length as u64, Ordering::Relaxed);
    }
}

impl Drop for RequestObservation {
    fn drop(&mut self) {
        let status = self.status.load(Ordering::Relaxed);
        let outcome = match status {
            0 => 6,
            200..=399 => 0,
            401 | 403 => 1,
            409 | 412 => 2,
            400..=499 => 3,
            503 | 504 => 4,
            _ => 5,
        };
        let counters = &self.metrics.routes[self.route][outcome];
        counters.requests.fetch_add(1, Ordering::Relaxed);
        counters
            .request_bytes
            .fetch_add(self.request_bytes.load(Ordering::Relaxed), Ordering::Relaxed);
        counters
            .response_bytes
            .fetch_add(self.response_bytes.load(Ordering::Relaxed), Ordering::Relaxed);
        counters
            .dispatch_latency_ns
            .fetch_add(self.dispatch_ns.load(Ordering::Relaxed), Ordering::Relaxed);
        counters
            .lifetime_ns
            .fetch_add(elapsed_ns(self.started), Ordering::Relaxed);
        let retry = self.retry.load(Ordering::Relaxed);
        if retry != 0 {
            self.metrics.retry[usize::from(retry - 1)].fetch_add(1, Ordering::Relaxed);
        }
        let version = self.version.load(Ordering::Relaxed);
        if (1..=3).contains(&version) {
            self.metrics.selected_versions[usize::from(version - 1)].fetch_add(1, Ordering::Relaxed);
        }
    }
}

fn elapsed_ns(started: Instant) -> u64 {
    u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX)
}

tokio::task_local! {
    static REQUEST_OBSERVATION: Arc<RequestObservation>;
}

pub(super) async fn observe<F: std::future::Future>(span: Arc<RequestObservation>, future: F) -> F::Output {
    REQUEST_OBSERVATION.scope(span, future).await
}

pub(super) fn record_request_bytes(length: usize) {
    let _ = REQUEST_OBSERVATION.try_with(|span| {
        span.request_bytes.fetch_add(length as u64, Ordering::Relaxed);
    });
}

pub(super) fn record_retry(kind: u8) {
    let _ = REQUEST_OBSERVATION.try_with(|span| span.retry.store(kind, Ordering::Relaxed));
}

pub(super) fn record_selected_version(version: u8) {
    let _ = REQUEST_OBSERVATION.try_with(|span| span.version.store(version, Ordering::Relaxed));
}

pub(super) fn route_index(method: &hyper::Method, path: &str) -> usize {
    if path.starts_with("/iceberg-") {
        return 6;
    }
    match Route::classify(method, path) {
        Some(Route::Config) => 0,
        Some(Route::AdminMetrics) => 7,
        Some(Route::NamespaceList | Route::NamespaceLoad | Route::NamespaceExists) => 1,
        Some(Route::NamespaceCreate | Route::NamespaceProperties | Route::NamespaceDrop) => 2,
        Some(Route::TableList | Route::TableLoad | Route::TableExists) => 3,
        Some(Route::TableCreate | Route::TableUpdate | Route::TableDrop | Route::TableRename) => 4,
        Some(Route::TableCredentials) => 5,
        None => 8,
    }
}
