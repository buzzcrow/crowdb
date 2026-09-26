use std::fs;
use std::io;
use std::os::unix::fs::{DirBuilderExt, FileTypeExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::time::Duration;

use thiserror::Error;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{UnixListener, UnixStream};
use tokio::task::JoinHandle;
use tokio::time::timeout;

const SOCKET: &str = "monitor.sock";
const DEADLINE: Duration = Duration::from_secs(2);

#[derive(Debug, Error)]
pub enum LivenessError {
    #[error("monitor liveness I/O failed: {0}")]
    Io(#[from] io::Error),
    #[error("monitor liveness timed out")]
    Timeout,
    #[error("monitor liveness state is invalid")]
    Invalid,
}

pub struct LivenessServer {
    task: JoinHandle<()>,
    path: PathBuf,
}

impl LivenessServer {
    /// # Errors
    /// Refuses a competing monitor or an unsafe health path.
    pub fn start(run_root: &Path) -> Result<Self, LivenessError> {
        let directory = run_root.join("health");
        match fs::symlink_metadata(&directory) {
            Ok(metadata)
                if metadata.file_type().is_dir() && metadata.permissions().mode() & 0o777 == 0o700 => {}
            Ok(_) => return Err(LivenessError::Invalid),
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                fs::DirBuilder::new().mode(0o700).create(&directory)?;
            }
            Err(error) => return Err(error.into()),
        }
        let path = directory.join(SOCKET);
        if fs::symlink_metadata(&path).is_ok() {
            return Err(LivenessError::Invalid);
        }
        let listener = UnixListener::bind(&path)?;
        let task = tokio::spawn(async move {
            while let Ok((mut connection, _)) = listener.accept().await {
                tokio::spawn(async move {
                    let mut ping = [0_u8; 4];
                    if timeout(DEADLINE, connection.read_exact(&mut ping))
                        .await
                        .is_ok_and(|result| result.is_ok())
                        && &ping == b"ping"
                    {
                        let _ = timeout(DEADLINE, connection.write_all(b"pong")).await;
                    }
                });
            }
        });
        Ok(Self { task, path })
    }
}

impl Drop for LivenessServer {
    fn drop(&mut self) {
        self.task.abort();
        if fs::symlink_metadata(&self.path).is_ok_and(|metadata| metadata.file_type().is_socket()) {
            let _ = fs::remove_file(&self.path);
        }
    }
}

/// # Errors
/// Requires a responsive local monitor event loop, not a stale PID snapshot.
pub async fn probe_liveness(run_root: &Path) -> Result<(), LivenessError> {
    let path = run_root.join("health").join(SOCKET);
    timeout(DEADLINE, async {
        let mut stream = UnixStream::connect(path).await?;
        stream.write_all(b"ping").await?;
        let mut pong = [0_u8; 4];
        stream.read_exact(&mut pong).await?;
        if &pong != b"pong" {
            return Err(LivenessError::Invalid);
        }
        Ok(())
    })
    .await
    .map_err(|_| LivenessError::Timeout)?
}
