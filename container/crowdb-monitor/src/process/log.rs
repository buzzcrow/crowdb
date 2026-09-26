use std::io;
use std::path::{Path, PathBuf};

use tokio::fs::{self, File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::process::{ChildStderr, ChildStdout};

use crate::LogProfile;

pub(super) async fn pump(
    mut stdout: ChildStdout,
    mut stderr: ChildStderr,
    directory: &Path,
    policy: LogProfile,
) -> io::Result<()> {
    let mut output = RotatingLog::open(directory, "service.log", policy).await?;
    let mut stdout_open = true;
    let mut stderr_open = true;
    let mut stdout_buffer = [0_u8; 8192];
    let mut stderr_buffer = [0_u8; 8192];
    while stdout_open || stderr_open {
        tokio::select! {
            read = stdout.read(&mut stdout_buffer), if stdout_open => {
                let size = read?;
                stdout_open = size != 0;
                if size != 0 {
                    output.write(&stdout_buffer[..size]).await?;
                }
            }
            read = stderr.read(&mut stderr_buffer), if stderr_open => {
                let size = read?;
                stderr_open = size != 0;
                if size != 0 {
                    output.write(&stderr_buffer[..size]).await?;
                }
            }
        }
    }
    output.sync().await
}

pub(crate) struct RotatingLog {
    directory: PathBuf,
    name: String,
    policy: LogProfile,
    file: File,
    size: u64,
}

impl RotatingLog {
    pub(crate) async fn open(directory: &Path, name: &str, policy: LogProfile) -> io::Result<Self> {
        fs::create_dir_all(directory).await?;
        let path = directory.join(name);
        let size = fs::metadata(&path).await.map_or(0, |metadata| metadata.len());
        let file = OpenOptions::new().create(true).append(true).open(&path).await?;
        Ok(Self {
            directory: directory.to_owned(),
            name: name.to_owned(),
            policy,
            file,
            size,
        })
    }

    pub(crate) async fn write(&mut self, mut bytes: &[u8]) -> io::Result<()> {
        while !bytes.is_empty() {
            if self.size >= self.policy.max_file_bytes {
                self.rotate().await?;
            }
            let room = usize::try_from(self.policy.max_file_bytes - self.size).unwrap_or(usize::MAX);
            let count = bytes.len().min(room);
            self.file.write_all(&bytes[..count]).await?;
            self.size += count as u64;
            bytes = &bytes[count..];
        }
        Ok(())
    }

    pub(crate) async fn sync(&self) -> io::Result<()> {
        self.file.sync_all().await
    }

    async fn rotate(&mut self) -> io::Result<()> {
        self.file.sync_all().await?;
        let current = self.directory.join(&self.name);
        if self.policy.max_files == 1 {
            remove_if_exists(&current).await?;
        } else {
            let oldest = self
                .directory
                .join(format!("{}.{}", self.name, self.policy.max_files - 1));
            remove_if_exists(&oldest).await?;
            for index in (1..self.policy.max_files - 1).rev() {
                let source = self.directory.join(format!("{}.{index}", self.name));
                let destination = self.directory.join(format!("{}.{}", self.name, index + 1));
                rename_if_exists(&source, &destination).await?;
            }
            rename_if_exists(&current, &self.directory.join(format!("{}.1", self.name))).await?;
        }
        self.file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&current)
            .await?;
        self.size = 0;
        Ok(())
    }
}

async fn remove_if_exists(path: &Path) -> io::Result<()> {
    match fs::remove_file(path).await {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error),
    }
}

async fn rename_if_exists(source: &Path, destination: &Path) -> io::Result<()> {
    match fs::rename(source, destination).await {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error),
    }
}
