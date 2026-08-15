//! The single local workspace profile.
//!
//! Local-only Zeron deliberately has no account or organization profile
//! selection. Keeping this small profile object preserves the existing on-disk
//! layout while making the storage boundary explicit for the engine.

use std::io::Write;
use std::path::{Path, PathBuf};

use uuid::Uuid;
use zeron_proto::WorkspaceScope;

use crate::EngineError;

const LOCAL_PROFILE_FILE: &str = "local-profile.json";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EngineProfile {
    device_root: PathBuf,
    store_root: PathBuf,
    uploads_root: PathBuf,
    profile_id: String,
}

impl EngineProfile {
    pub fn local(data_dir: &Path) -> Result<Self, EngineError> {
        let profile_id = load_or_create_local_profile_id(data_dir)?;
        let store_root = data_dir.join("profiles").join("local");
        Ok(Self {
            device_root: data_dir.to_path_buf(),
            uploads_root: store_root.join("uploads"),
            store_root,
            profile_id,
        })
    }

    pub fn scope(&self) -> WorkspaceScope {
        WorkspaceScope::Local
    }

    pub fn device_root(&self) -> &Path {
        &self.device_root
    }

    pub fn store_root(&self) -> &Path {
        &self.store_root
    }

    pub fn uploads_root(&self) -> &Path {
        &self.uploads_root
    }

    pub fn org_id(&self) -> &str {
        "local"
    }

    pub fn user_id(&self) -> &str {
        &self.profile_id
    }
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct LocalProfileFile {
    id: Uuid,
}

fn load_or_create_local_profile_id(data_dir: &Path) -> Result<String, EngineError> {
    std::fs::create_dir_all(data_dir)?;
    let path = data_dir.join(LOCAL_PROFILE_FILE);
    match std::fs::read(&path) {
        Ok(bytes) => {
            let profile: LocalProfileFile = serde_json::from_slice(&bytes)
                .map_err(|err| EngineError::Other(format!("read local profile: {err}")))?;
            return Ok(profile.id.to_string());
        }
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
        Err(err) => return Err(err.into()),
    }

    let id = Uuid::new_v4();
    let mut bytes = serde_json::to_vec_pretty(&LocalProfileFile { id })
        .map_err(|err| EngineError::Other(format!("serialize local profile: {err}")))?;
    bytes.push(b'\n');
    let temp_path = data_dir.join(format!(".{LOCAL_PROFILE_FILE}.tmp-{}", Uuid::new_v4()));
    let mut temp = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&temp_path)?;
    temp.write_all(&bytes)?;
    temp.sync_all()?;
    drop(temp);

    let result = std::fs::hard_link(&temp_path, &path);
    let _ = std::fs::remove_file(&temp_path);
    match result {
        Ok(()) => Ok(id.to_string()),
        Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => {
            let bytes = std::fs::read(path)?;
            let profile: LocalProfileFile = serde_json::from_slice(&bytes)
                .map_err(|err| EngineError::Other(format!("read local profile: {err}")))?;
            Ok(profile.id.to_string())
        }
        Err(err) => Err(err.into()),
    }
}
