use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};

use rand::rngs::OsRng;
use rand::RngCore;
use thiserror::Error;
use uuid::Uuid;

const SERVER_FILE: &str = "server.env";
const CLIENT_FILE: &str = "client.env";
const MAX_ENV_BYTES: u64 = 4096;

#[derive(Debug, Error)]
pub enum CredentialError {
    #[error("credential storage failed: {0}")]
    Io(#[from] std::io::Error),
    #[error("credential state is invalid: {0}")]
    Invalid(&'static str),
}

pub struct ServerCredentials {
    directory: PathBuf,
    s3_master_key: String,
    iceberg_read_token: String,
    iceberg_write_token: String,
    iceberg_manage_token: String,
    iceberg_clear_token: String,
}

impl ServerCredentials {
    /// # Errors
    /// Rejects missing or incompatible secret state without replacing it.
    pub fn load_or_create(data_root: &Path) -> Result<Self, CredentialError> {
        let directory = data_root.join("secrets");
        if !data_root.is_dir() {
            return Err(CredentialError::Invalid("data root is not a directory"));
        }
        match fs::symlink_metadata(&directory) {
            Ok(metadata) if !metadata.file_type().is_dir() => {
                return Err(CredentialError::Invalid("secrets directory is not a directory"));
            }
            Ok(metadata) if metadata.permissions().mode() & 0o777 != 0o700 => {
                return Err(CredentialError::Invalid("secrets directory must have mode 0700"));
            }
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                fs::DirBuilder::new().mode(0o700).create(&directory)?;
                File::open(data_root)?.sync_all()?;
            }
            Err(error) => return Err(error.into()),
        }
        let path = directory.join(SERVER_FILE);
        if fs::symlink_metadata(&path).is_ok() {
            let body = read_private(&path)?;
            return Self::parse(directory, &body);
        }
        if fs::read_dir(&directory)?.next().is_some() {
            return Err(CredentialError::Invalid(
                "secrets directory has no server credentials",
            ));
        }
        let result = Self {
            directory,
            s3_master_key: random_hex(),
            iceberg_read_token: random_hex(),
            iceberg_write_token: random_hex(),
            iceberg_manage_token: random_hex(),
            iceberg_clear_token: random_hex(),
        };
        atomic_private_write(&path, result.server_env().as_bytes())?;
        Ok(result)
    }

    #[must_use]
    pub fn server_env(&self) -> String {
        format!(
            "CROWDB_S3_MASTER_KEY={}\nCROWDB_ICEBERG_READ_TOKEN={}\nCROWDB_ICEBERG_WRITE_TOKEN={}\nCROWDB_ICEBERG_MANAGE_TOKEN={}\nCROWDB_ICEBERG_CLEAR_TOKEN={}\n",
            self.s3_master_key,
            self.iceberg_read_token,
            self.iceberg_write_token,
            self.iceberg_manage_token,
            self.iceberg_clear_token,
        )
    }

    #[must_use]
    pub fn s3_master_key(&self) -> &str {
        &self.s3_master_key
    }

    /// # Errors
    /// Requires an existing client file to match the authoritative user and endpoints.
    pub fn verify_client(&self, client: &ClientCredentials) -> Result<(), CredentialError> {
        client.validate()?;
        let existing = read_private(&self.directory.join(CLIENT_FILE))?;
        if existing != client.env(&self.iceberg_write_token) {
            return Err(CredentialError::Invalid("existing client credentials conflict"));
        }
        Ok(())
    }

    /// # Errors
    /// Rejects conflicting, incomplete, or invalid client credentials.
    pub fn persist_client(&self, client: &ClientCredentials) -> Result<(), CredentialError> {
        client.validate()?;
        let path = self.directory.join(CLIENT_FILE);
        if fs::symlink_metadata(&path).is_ok() {
            let existing = read_private(&path)?;
            if existing == client.env(&self.iceberg_write_token) {
                return Ok(());
            }
            return Err(CredentialError::Invalid("existing client credentials conflict"));
        }
        atomic_private_write(&path, client.env(&self.iceberg_write_token).as_bytes())
    }

    fn parse(directory: PathBuf, body: &str) -> Result<Self, CredentialError> {
        let mut values = body.lines();
        let s3_master_key = take_hex(&mut values, "CROWDB_S3_MASTER_KEY")?;
        let iceberg_read_token = take_hex(&mut values, "CROWDB_ICEBERG_READ_TOKEN")?;
        let iceberg_write_token = take_hex(&mut values, "CROWDB_ICEBERG_WRITE_TOKEN")?;
        let iceberg_manage_token = take_hex(&mut values, "CROWDB_ICEBERG_MANAGE_TOKEN")?;
        let iceberg_clear_token = take_hex(&mut values, "CROWDB_ICEBERG_CLEAR_TOKEN")?;
        if values.next().is_some() {
            return Err(CredentialError::Invalid(
                "server credentials contain extra values",
            ));
        }
        let tokens = [
            &iceberg_read_token,
            &iceberg_write_token,
            &iceberg_manage_token,
            &iceberg_clear_token,
        ];
        if tokens
            .iter()
            .enumerate()
            .any(|(index, token)| tokens[..index].contains(token))
        {
            return Err(CredentialError::Invalid("Iceberg tokens must be distinct"));
        }
        Ok(Self {
            directory,
            s3_master_key,
            iceberg_read_token,
            iceberg_write_token,
            iceberg_manage_token,
            iceberg_clear_token,
        })
    }
}

pub struct ClientCredentials {
    pub s3_endpoint: String,
    pub iceberg_endpoint: String,
    pub region: String,
    pub access_key_id: String,
    pub secret_access_key: String,
}

impl ClientCredentials {
    fn validate(&self) -> Result<(), CredentialError> {
        if !has_http_authority(&self.s3_endpoint)
            || !has_http_authority(&self.iceberg_endpoint)
            || ![
                &self.s3_endpoint,
                &self.iceberg_endpoint,
                &self.region,
                &self.access_key_id,
                &self.secret_access_key,
            ]
            .iter()
            .all(|value| {
                !value.is_empty()
                    && value
                        .bytes()
                        .all(|byte| byte.is_ascii_alphanumeric() || b"-._~:/".contains(&byte))
            })
        {
            return Err(CredentialError::Invalid("client credential value is invalid"));
        }
        Ok(())
    }

    fn env(&self, writer_token: &str) -> String {
        format!(
            "AWS_ENDPOINT_URL={}\nAWS_DEFAULT_REGION={}\nAWS_ACCESS_KEY_ID={}\nAWS_SECRET_ACCESS_KEY={}\nICEBERG_URI={}\nICEBERG_TOKEN={}\n",
            self.s3_endpoint,
            self.region,
            self.access_key_id,
            self.secret_access_key,
            self.iceberg_endpoint,
            writer_token,
        )
    }
}

fn has_http_authority(value: &str) -> bool {
    value
        .strip_prefix("http://")
        .or_else(|| value.strip_prefix("https://"))
        .is_some_and(|authority| !authority.is_empty() && !authority.starts_with('/'))
}

/// # Errors
/// Rejects missing, symlinked, malformed, or publicly readable client files.
pub fn show_client_credentials(data_root: &Path) -> Result<String, CredentialError> {
    let directory = data_root.join("secrets");
    let metadata = fs::symlink_metadata(&directory)?;
    if !metadata.file_type().is_dir() || metadata.permissions().mode() & 0o777 != 0o700 {
        return Err(CredentialError::Invalid("secrets directory must have mode 0700"));
    }
    let body = read_private(&directory.join(CLIENT_FILE))?;
    validate_client_env(&body)?;
    Ok(body)
}

fn validate_client_env(body: &str) -> Result<(), CredentialError> {
    let mut values = body.lines();
    let endpoint = take_value(&mut values, "AWS_ENDPOINT_URL")?;
    let region = take_value(&mut values, "AWS_DEFAULT_REGION")?;
    let access_key = take_value(&mut values, "AWS_ACCESS_KEY_ID")?;
    let secret_key = take_value(&mut values, "AWS_SECRET_ACCESS_KEY")?;
    let iceberg_uri = take_value(&mut values, "ICEBERG_URI")?;
    let writer_token = take_value(&mut values, "ICEBERG_TOKEN")?;
    if values.next().is_some() {
        return Err(CredentialError::Invalid(
            "client credentials contain extra values",
        ));
    }
    ClientCredentials {
        s3_endpoint: endpoint.into(),
        iceberg_endpoint: iceberg_uri.into(),
        region: region.into(),
        access_key_id: access_key.into(),
        secret_access_key: secret_key.into(),
    }
    .validate()?;
    if writer_token.len() != 64 || !writer_token.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(CredentialError::Invalid("Iceberg writer token is invalid"));
    }
    Ok(())
}

fn take_value<'a>(lines: &mut impl Iterator<Item = &'a str>, key: &str) -> Result<&'a str, CredentialError> {
    lines
        .next()
        .and_then(|line| line.strip_prefix(key))
        .and_then(|line| line.strip_prefix('='))
        .ok_or(CredentialError::Invalid("credential file is incomplete"))
}

fn take_hex<'a>(lines: &mut impl Iterator<Item = &'a str>, key: &str) -> Result<String, CredentialError> {
    let value = lines
        .next()
        .and_then(|line| line.strip_prefix(key))
        .and_then(|line| line.strip_prefix('='))
        .ok_or(CredentialError::Invalid("server credentials are incomplete"))?;
    if value.len() != 64 || !value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(CredentialError::Invalid("server credential value is invalid"));
    }
    Ok(value.to_owned())
}

fn random_hex() -> String {
    let mut bytes = [0_u8; 32];
    OsRng.fill_bytes(&mut bytes);
    let mut output = String::with_capacity(64);
    for byte in bytes {
        use std::fmt::Write as _;
        write!(output, "{byte:02x}").expect("writing to String cannot fail");
    }
    output
}

fn read_private(path: &Path) -> Result<String, CredentialError> {
    let metadata = fs::symlink_metadata(path)?;
    if !metadata.file_type().is_file()
        || metadata.permissions().mode() & 0o777 != 0o600
        || metadata.len() > MAX_ENV_BYTES
    {
        return Err(CredentialError::Invalid(
            "credential file must be a bounded mode-0600 regular file",
        ));
    }
    Ok(fs::read_to_string(path)?)
}

fn atomic_private_write(path: &Path, body: &[u8]) -> Result<(), CredentialError> {
    if body.len() as u64 > MAX_ENV_BYTES {
        return Err(CredentialError::Invalid("credential file exceeds size bound"));
    }
    let directory = path
        .parent()
        .ok_or(CredentialError::Invalid("credential path has no parent"))?;
    let temporary = directory.join(format!(".credential-{}.tmp", Uuid::new_v4()));
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(&temporary)?;
    let result = (|| {
        file.write_all(body)?;
        file.sync_all()?;
        fs::hard_link(&temporary, path)?;
        File::open(directory)?.sync_all()?;
        fs::remove_file(&temporary)?;
        File::open(directory)?.sync_all()
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    result.map_err(CredentialError::Io)
}
