use std::io;
use std::os::unix::process::ExitStatusExt;
use std::process::ExitStatus;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use rustix::process::{kill_process, Pid, Signal};
use tokio::process::Child;
use tokio::time::timeout;

use super::ProcessError;

pub(super) async fn stop(
    child: &mut Child,
    original_pid: u32,
    grace: Duration,
) -> Result<serde_json::Value, ProcessError> {
    let mut status = child.try_wait()?;
    let mut stop_path = "already_exited_before_stop";
    let mut term_sent_at_unix_ms = None;
    let mut term_clock = None;
    let mut kill_requested_at_unix_ms = None;
    let mut wait_error = None;
    if status.is_none() {
        stop_path = "owned_stop_requested";
        if let Some(pid) = child
            .id()
            .and_then(|value| i32::try_from(value).ok())
            .and_then(Pid::from_raw)
        {
            let attempted_clock = Instant::now();
            let attempted_at = unix_ms();
            match kill_process(pid, Signal::TERM) {
                Ok(()) => {
                    term_sent_at_unix_ms = attempted_at;
                    term_clock = Some(attempted_clock);
                }
                Err(error) if error == rustix::io::Errno::SRCH => {}
                Err(error) => return Err(io::Error::from_raw_os_error(error.raw_os_error()).into()),
            }
        }
        // No diagnostic I/O runs between TERM and this original grace wait.
        match timeout(grace, child.wait()).await {
            Ok(Ok(observed)) => status = Some(observed),
            // The original stop path ignored this inner wait error. Retain
            // that behavior, but do not manufacture a successful exit status.
            Ok(Err(error)) => wait_error = Some(error.to_string()),
            Err(_) => {
                stop_path = "owned_stop_grace_elapsed";
                child.start_kill()?;
                kill_requested_at_unix_ms = unix_ms();
                status = Some(child.wait().await?);
            }
        }
    }
    let wait_returned_at_unix_ms = unix_ms();
    let elapsed_since_term_ms = term_clock.map(|clock| clock.elapsed().as_millis());
    let observation = serde_json::json!({
        "original_pid": original_pid,
        "stop_path": stop_path,
        "status_observed": status.is_some(),
        "exit_code": status.as_ref().and_then(ExitStatus::code),
        "exit_signal": status.as_ref().and_then(ExitStatusExt::signal),
        "core_dumped": status.as_ref().map(ExitStatusExt::core_dumped),
        "success": status.as_ref().map(ExitStatus::success),
        "raw_wait_status": status.as_ref().map(|value| (*value).into_raw()),
        "term_sent_at_unix_ms": term_sent_at_unix_ms,
        "kill_requested_at_unix_ms": kill_requested_at_unix_ms,
        "wait_returned_at_unix_ms": wait_returned_at_unix_ms,
        "elapsed_since_term_ms": elapsed_since_term_ms,
        "grace_ms": grace.as_millis(),
        "wait_error": wait_error,
        "caller_stop_reason": "not supplied by existing ProcessManager::stop API",
    });
    Ok(observation)
}

fn unix_ms() -> Option<u128> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .map(|value| value.as_millis())
}
