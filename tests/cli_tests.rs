//! CLI integration tests.

use assert_cmd::Command;
use predicates::prelude::*;
use std::fs;
use tempfile::TempDir;

fn rdq_cmd() -> Command {
    Command::cargo_bin("rdq").unwrap()
}

#[test]
fn help_works() {
    rdq_cmd()
        .arg("--help")
        .assert()
        .success()
        .stdout(predicate::str::contains("Rust Durable Queue"));
}

#[test]
fn demo_help_works() {
    rdq_cmd()
        .args(["demo", "--help"])
        .assert()
        .success()
        .stdout(predicate::str::contains("--jobs"));
}

#[test]
fn demo_memory_succeeds() {
    rdq_cmd()
        .args(["demo", "--jobs", "5", "--workers", "2"])
        .assert()
        .success()
        .stdout(predicate::str::contains("Demo complete"));
}

#[test]
fn demo_wal_succeeds() {
    let dir = TempDir::new().unwrap();
    let data_dir = dir.path().to_str().unwrap();

    rdq_cmd()
        .args([
            "demo",
            "--jobs",
            "5",
            "--workers",
            "2",
            "--data-dir",
            data_dir,
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("Demo complete"));

    // Verify WAL was created.
    assert!(dir.path().join("wal.log").exists());
}

#[test]
fn inspect_valid_wal() {
    let dir = TempDir::new().unwrap();
    let data_dir = dir.path().to_str().unwrap();

    // Create WAL with demo.
    rdq_cmd()
        .args([
            "demo",
            "--jobs",
            "3",
            "--workers",
            "1",
            "--data-dir",
            data_dir,
        ])
        .assert()
        .success();

    // Inspect.
    rdq_cmd()
        .args(["inspect", "--data-dir", data_dir])
        .assert()
        .success()
        .stdout(predicate::str::contains("WAL Inspection"))
        .stdout(predicate::str::contains("Version: 1"))
        .stdout(predicate::str::contains("Total:"));
}

#[test]
fn inspect_missing_wal_fails() {
    let dir = TempDir::new().unwrap();
    let data_dir = dir.path().to_str().unwrap();

    rdq_cmd()
        .args(["inspect", "--data-dir", data_dir])
        .assert()
        .failure()
        .stderr(predicate::str::contains("WAL file not found"));
}

#[test]
fn verify_valid_wal() {
    let dir = TempDir::new().unwrap();
    let data_dir = dir.path().to_str().unwrap();

    // Create WAL with demo.
    rdq_cmd()
        .args([
            "demo",
            "--jobs",
            "3",
            "--workers",
            "1",
            "--data-dir",
            data_dir,
        ])
        .assert()
        .success();

    // Verify.
    rdq_cmd()
        .args(["verify", "--data-dir", data_dir])
        .assert()
        .success()
        .stdout(predicate::str::contains("WAL valid"))
        .stdout(predicate::str::contains("Records:"));
}

#[test]
fn verify_corrupt_wal_fails() {
    let dir = TempDir::new().unwrap();
    let data_dir = dir.path().to_str().unwrap();

    // Create WAL with demo.
    rdq_cmd()
        .args([
            "demo",
            "--jobs",
            "3",
            "--workers",
            "1",
            "--data-dir",
            data_dir,
        ])
        .assert()
        .success();

    // Corrupt the WAL by modifying bytes in the middle.
    let wal_path = dir.path().join("wal.log");
    let mut data = fs::read(&wal_path).unwrap();
    if data.len() > 50 {
        data[40] ^= 0xFF;
        data[41] ^= 0xFF;
        fs::write(&wal_path, data).unwrap();
    }

    // Verify should fail.
    rdq_cmd()
        .args(["verify", "--data-dir", data_dir])
        .assert()
        .failure()
        .stderr(predicate::str::contains("verification failed"));
}

#[test]
fn verify_missing_wal_fails() {
    let dir = TempDir::new().unwrap();
    let data_dir = dir.path().to_str().unwrap();

    rdq_cmd()
        .args(["verify", "--data-dir", data_dir])
        .assert()
        .failure()
        .stderr(predicate::str::contains("WAL file not found"));
}
