use std::collections::BTreeMap;
use std::io;
use std::path::{Path, PathBuf};
use std::time::SystemTime;

struct LogFile {
    path: PathBuf,
    modified: SystemTime,
    active: bool,
}

pub(super) async fn prune(directory: &Path, current_pid: Option<u32>, max_files: u16) -> io::Result<()> {
    let mut entries = tokio::fs::read_dir(directory).await?;
    let mut groups: BTreeMap<String, Vec<LogFile>> = BTreeMap::new();
    while let Some(entry) = entries.next_entry().await? {
        let name = entry.file_name();
        let Some((prefix, pid, compressed)) = name.to_str().and_then(classify) else {
            continue;
        };
        let metadata = match tokio::fs::symlink_metadata(entry.path()).await {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
            Err(error) => return Err(error),
        };
        if !metadata.is_file() {
            continue;
        }
        groups.entry(prefix.to_owned()).or_default().push(LogFile {
            path: entry.path(),
            modified: metadata.modified()?,
            active: current_pid == Some(pid) && !compressed,
        });
    }
    for files in groups.values_mut() {
        files.sort_by(|left, right| {
            right
                .active
                .cmp(&left.active)
                .then_with(|| right.modified.cmp(&left.modified))
                .then_with(|| right.path.cmp(&left.path))
        });
        for file in files.iter().skip(usize::from(max_files)) {
            if !file.active {
                match tokio::fs::remove_file(&file.path).await {
                    Ok(()) => {}
                    Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                    Err(error) => return Err(error),
                }
            }
        }
    }
    Ok(())
}

fn classify(name: &str) -> Option<(&str, u32, bool)> {
    let (stem, compressed) = match name.strip_suffix(".log.gz") {
        Some(stem) => (stem, true),
        None => (name.strip_suffix(".log")?, false),
    };
    let (prefix, pid) = stem.rsplit_once('-')?;
    let pid = pid.parse().ok()?;
    let prefix = match prefix.rsplit_once('-') {
        Some((dated, time))
            if time.len() == 10
                && time.as_bytes()[6] == b'.'
                && time
                    .bytes()
                    .enumerate()
                    .all(|(index, byte)| index == 6 || byte.is_ascii_digit()) =>
        {
            let (prefix, date) = dated.rsplit_once('-')?;
            if date.len() != 8 || !date.bytes().all(|byte| byte.is_ascii_digit()) {
                return None;
            }
            prefix
        }
        _ => prefix,
    };
    Some((prefix, pid, compressed))
}
