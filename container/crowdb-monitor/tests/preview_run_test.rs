use std::fs;
use std::os::unix::fs::symlink;
use std::path::{Path, PathBuf};

use crowdb_monitor::{run_preview, DeploymentProfile, MonitorPhase, StatusStore};
use uuid::Uuid;

struct TestRoot(PathBuf);

impl TestRoot {
    fn new() -> Self {
        let temp_root = if cfg!(target_os = "macos") {
            PathBuf::from("/tmp")
        } else {
            std::env::temp_dir()
        };
        let path = temp_root.join(format!("cm-preview-{}", Uuid::new_v4().simple()));
        for name in ["bin", "templates", "data", "run"] {
            fs::create_dir_all(path.join(name)).unwrap();
        }
        Self(path.canonicalize().unwrap())
    }

    fn profile_path(&self) -> PathBuf {
        let mut profile = DeploymentProfile::load(
            Path::new(env!("CARGO_MANIFEST_DIR")).join("../single-node-container/profile.toml"),
        )
        .unwrap();
        profile.paths.install_root.clone_from(&self.0);
        profile.paths.bin_root = self.0.join("bin");
        profile.paths.template_root = self.0.join("templates");
        profile.paths.data_root = self.0.join("data");
        profile.paths.run_root = self.0.join("run");
        profile.paths.log_root = self.0.join("data/log");
        for disk in &mut profile.disks {
            disk.path = self.0.join("data/disks").join(disk.path.file_name().unwrap());
        }
        let source = Path::new(env!("CARGO_MANIFEST_DIR")).join("../single-node-container/templates");
        for service in &mut profile.services {
            let name = service.program.file_name().unwrap();
            service.program = self.0.join("bin").join(name);
            if service.id == "kv" {
                let failing_program = if cfg!(target_os = "macos") {
                    "/usr/bin/false"
                } else {
                    "/bin/false"
                };
                symlink(failing_program, &service.program).unwrap();
                for argument in &mut service.args {
                    if argument == "/opt/crowdb/data/kv/node-1" {
                        *argument = self.0.join("data/kv/node-1").to_string_lossy().into_owned();
                    }
                }
            }
            if let Some(template) = &service.config_template {
                let name = template.file_name().unwrap();
                fs::copy(source.join(name), self.0.join("templates").join(name)).unwrap();
                service.config_template = Some(self.0.join("templates").join(name));
            }
        }
        profile.validate().unwrap();
        let path = self.0.join("profile.toml");
        fs::write(&path, toml::to_string(&profile).unwrap()).unwrap();
        path
    }
}

impl Drop for TestRoot {
    fn drop(&mut self) {
        if !std::thread::panicking() {
            fs::remove_dir_all(&self.0).unwrap();
        }
    }
}

#[tokio::test]
async fn failed_child_never_marks_preview_ready_and_template_drift_fails_closed() {
    let root = TestRoot::new();
    let profile = root.profile_path();
    assert!(run_preview(&profile).await.is_err());
    let status = StatusStore::open(&root.0.join("run"))
        .unwrap()
        .read(std::time::Duration::from_secs(10))
        .unwrap();
    assert_eq!(status.phase, MonitorPhase::Draining);
    assert!(root.0.join("data/bootstrap/manifest.json").exists());
    assert!(root.0.join("data/secrets/server.env").exists());
    assert!(StatusStore::open(&root.0.join("run"))
        .unwrap()
        .readiness(std::time::Duration::from_secs(10))
        .is_err());

    let template = root.0.join("templates/kv.toml");
    fs::write(&template, format!("{}\n", fs::read_to_string(&template).unwrap())).unwrap();
    let before = fs::read(root.0.join("data/bootstrap/manifest.json")).unwrap();
    assert!(run_preview(&profile).await.is_err());
    assert_eq!(
        fs::read(root.0.join("data/bootstrap/manifest.json")).unwrap(),
        before
    );
}

#[tokio::test]
async fn nonempty_uninitialized_root_is_not_adopted() {
    let root = TestRoot::new();
    let profile = root.profile_path();
    fs::write(root.0.join("data/foreign"), b"unrelated").unwrap();
    assert!(run_preview(&profile).await.is_err());
    assert!(!root.0.join("data/bootstrap").exists());
    assert!(!root.0.join("data/secrets").exists());
}
