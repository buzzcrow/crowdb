// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

/// Owned by the metrics task, sampled every 100 ms independently of the log
/// period. Sampling only reads bounded diagnostics, without backend I/O or
/// waiting for application work. Flushing writes the metrics log. No
/// application hot path accesses this state.
pub trait MetricsSampler: Send {
    fn sample(&mut self);
    /// Writes and advances the owned observation window.
    /// # Errors
    /// Returns an output error if the metrics writer rejects the window.
    fn flush(&mut self, writer: &mut dyn std::io::Write, timestamp: &str) -> std::io::Result<()>;
}
