use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::Path;

use crate::EngineError;

#[derive(Debug)]
pub struct InstanceLock {
    _file: File,
}

impl InstanceLock {
    pub fn acquire(data_dir: &Path) -> Result<Self, EngineError> {
        let path = data_dir.join("engine.lock");
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)?;

        #[cfg(unix)]
        {
            use std::os::unix::io::AsRawFd;
            let mut retries = 40u32;
            loop {
                let rc = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
                if rc == 0 {
                    break;
                }
                let errno = std::io::Error::last_os_error();
                match errno.raw_os_error() {
                    Some(libc::EINTR) => continue,
                    Some(libc::EWOULDBLOCK) if retries > 0 => {
                        retries -= 1;
                        std::thread::sleep(std::time::Duration::from_millis(25));
                    }
                    Some(libc::EWOULDBLOCK) => {
                        let holder = std::fs::read_to_string(&path).unwrap_or_default();
                        let holder = holder.trim();
                        return Err(EngineError::Other(format!(
                            "another zeron engine is already running on {} (pid {}); \
                             stop it or use a different data dir (ZERON_DATA_DIR)",
                            data_dir.display(),
                            if holder.is_empty() { "unknown" } else { holder },
                        )));
                    }
                    _ => return Err(EngineError::Io(errno)),
                }
            }
        }

        let _ = file.set_len(0);
        let _ = write!(file, "{}", std::process::id());
        let _ = file.flush();
        Ok(Self { _file: file })
    }

    pub fn holder(data_dir: &Path) -> Option<String> {
        let path = data_dir.join("engine.lock");
        #[cfg(unix)]
        {
            use std::os::unix::io::AsRawFd;
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .open(&path)
                .ok()?;
            let rc = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
            if rc == 0 {
                unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_UN) };
                return None;
            }
            let pid = std::fs::read_to_string(&path).unwrap_or_default();
            let pid = pid.trim();
            Some(if pid.is_empty() {
                "unknown".to_string()
            } else {
                pid.to_string()
            })
        }
        #[cfg(not(unix))]
        {
            let _ = path;
            None
        }
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;

    #[test]
    fn holder_probe_reports_pid_without_disturbing_the_lock() {
        let dir = tempfile::tempdir().unwrap();
        assert_eq!(InstanceLock::holder(dir.path()), None, "unlocked dir");
        let lock = InstanceLock::acquire(dir.path()).expect("acquire");
        assert_eq!(
            InstanceLock::holder(dir.path()).as_deref(),
            Some(std::process::id().to_string().as_str()),
        );
        InstanceLock::acquire(dir.path()).expect_err("still held after probe");
        drop(lock);
        assert_eq!(InstanceLock::holder(dir.path()), None, "released");
    }

    #[test]
    fn second_acquire_fails_while_held_then_succeeds_after_drop() {
        let dir = tempfile::tempdir().unwrap();
        let lock = InstanceLock::acquire(dir.path()).expect("first acquire");
        let err = InstanceLock::acquire(dir.path()).expect_err("second acquire must fail");
        let msg = err.to_string();
        assert!(msg.contains("already running"), "unexpected error: {msg}");
        assert!(
            msg.contains(&std::process::id().to_string()),
            "holder pid missing from error: {msg}"
        );
        drop(lock);
        InstanceLock::acquire(dir.path()).expect("acquire after release");
    }
}
