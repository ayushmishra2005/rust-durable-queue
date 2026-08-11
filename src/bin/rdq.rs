//! rdq - Rust Durable Queue CLI
//!
//! A small CLI for demonstrating and inspecting the queue.

use clap::{Parser, Subcommand};
use rust_durable_queue::{
    JobContext, JobError, JobSpec, JobState, QueueConfig, QueueName, Runtime, RuntimeConfig,
    StorageConfig,
};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

#[derive(Parser)]
#[command(name = "rdq")]
#[command(about = "Rust Durable Queue - demo and inspection CLI")]
#[command(version)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Run a self-contained demo of the queue
    Demo {
        /// Number of jobs to submit
        #[arg(long, default_value = "20")]
        jobs: u32,

        /// Number of worker threads
        #[arg(long, default_value = "4")]
        workers: usize,

        /// Queue capacity
        #[arg(long, default_value = "32")]
        capacity: usize,

        /// Data directory for WAL storage (uses memory if not specified)
        #[arg(long)]
        data_dir: Option<PathBuf>,
    },

    /// Inspect a WAL file (read-only)
    Inspect {
        /// Data directory containing the WAL
        #[arg(long)]
        data_dir: PathBuf,
    },

    /// Verify WAL integrity (read-only)
    Verify {
        /// Data directory containing the WAL
        #[arg(long)]
        data_dir: PathBuf,
    },
}

/// Demo payload types for deterministic behavior.
#[derive(Debug, Clone)]
enum DemoPayload {
    Success(u32),
    RetryOnce(u32),
    Fatal(u32),
}

impl DemoPayload {
    fn encode(&self) -> Vec<u8> {
        match self {
            DemoPayload::Success(id) => format!("success:{id}").into_bytes(),
            DemoPayload::RetryOnce(id) => format!("retry-once:{id}").into_bytes(),
            DemoPayload::Fatal(id) => format!("fatal:{id}").into_bytes(),
        }
    }

    fn decode(payload: &[u8]) -> Option<Self> {
        let s = std::str::from_utf8(payload).ok()?;
        if let Some(id) = s.strip_prefix("success:") {
            Some(DemoPayload::Success(id.parse().ok()?))
        } else if let Some(id) = s.strip_prefix("retry-once:") {
            Some(DemoPayload::RetryOnce(id.parse().ok()?))
        } else if let Some(id) = s.strip_prefix("fatal:") {
            Some(DemoPayload::Fatal(id.parse().ok()?))
        } else {
            None
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing with sensible defaults.
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive(tracing::Level::INFO.into()),
        )
        .with_target(false)
        .init();

    let cli = Cli::parse();

    match cli.command {
        Commands::Demo {
            jobs,
            workers,
            capacity,
            data_dir,
        } => run_demo(jobs, workers, capacity, data_dir).await?,
        Commands::Inspect { data_dir } => run_inspect(data_dir)?,
        Commands::Verify { data_dir } => run_verify(data_dir)?,
    }

    Ok(())
}

async fn run_demo(
    job_count: u32,
    workers: usize,
    capacity: usize,
    data_dir: Option<PathBuf>,
) -> Result<(), Box<dyn std::error::Error>> {
    let storage = match &data_dir {
        Some(path) => {
            std::fs::create_dir_all(path)?;
            StorageConfig::Wal { path: path.clone() }
        }
        None => StorageConfig::Memory,
    };

    let storage_desc = match &data_dir {
        Some(path) => format!("{}/wal.log", path.display()),
        None => "memory".to_string(),
    };

    println!("Starting demo...");
    println!("  Jobs: {job_count}");
    println!("  Workers: {workers}");
    println!("  Capacity: {capacity}");
    println!("  Storage: {storage_desc}");
    println!();

    let default_q = QueueName::new("default").ok_or("invalid queue name")?;
    let priority_q = QueueName::new("priority").ok_or("invalid queue name")?;

    let queues = vec![
        QueueConfig::new(default_q, capacity),
        QueueConfig::new(priority_q, capacity / 2),
    ];

    let config = RuntimeConfig::new(queues, 64)
        .with_storage(storage)
        .with_worker_concurrency(workers)
        .with_retry(rust_durable_queue::RetryConfig::new(
            Duration::from_millis(100),
            Duration::from_millis(500),
        ))
        .with_visibility_timeout(Duration::from_secs(30))
        .with_shutdown_timeout(Duration::from_secs(5));

    // Track retry attempts for retry-once payloads.
    let retry_tracker: Arc<std::sync::Mutex<std::collections::HashSet<u32>>> =
        Arc::new(std::sync::Mutex::new(std::collections::HashSet::new()));
    let retry_tracker_clone = retry_tracker.clone();

    let handler = move |ctx: JobContext| {
        let tracker = retry_tracker_clone.clone();
        async move {
            let payload = DemoPayload::decode(&ctx.payload).unwrap_or(DemoPayload::Success(0));

            match payload {
                DemoPayload::Success(_) => {
                    tracing::debug!(job_id = %ctx.id, "job succeeded");
                    Ok(())
                }
                DemoPayload::RetryOnce(id) => {
                    let mut seen = tracker.lock().unwrap();
                    if seen.contains(&id) {
                        tracing::debug!(job_id = %ctx.id, "retry-once job succeeded on retry");
                        Ok(())
                    } else {
                        seen.insert(id);
                        tracing::debug!(job_id = %ctx.id, "retry-once job failing first attempt");
                        Err(JobError::retryable(std::io::Error::other(
                            "simulated transient failure",
                        )))
                    }
                }
                DemoPayload::Fatal(_) => {
                    tracing::debug!(job_id = %ctx.id, "fatal job failing");
                    Err(JobError::fatal(std::io::Error::other(
                        "simulated fatal error",
                    )))
                }
            }
        }
    };

    let rt = Runtime::start(config, handler).await?;

    // Submit jobs with mixed payloads.
    let default_queue = QueueName::new("default").ok_or("invalid queue name")?;
    let priority_queue = QueueName::new("priority").ok_or("invalid queue name")?;

    let mut job_ids = Vec::new();
    let mut expected_dead = 0u32;

    for i in 0..job_count {
        let (payload, queue) = match i % 10 {
            0 => {
                expected_dead += 1;
                (DemoPayload::Fatal(i), &default_queue)
            }
            1 | 2 => (DemoPayload::RetryOnce(i), &priority_queue),
            _ => (DemoPayload::Success(i), &default_queue),
        };

        let spec = JobSpec::new(payload.encode());
        let record = rt.submit(queue.clone(), spec).await?;
        job_ids.push(record.id);
    }

    println!("Submitted {job_count} jobs");

    // Wait for all jobs to reach terminal states.
    let timeout = tokio::time::Instant::now() + Duration::from_secs(30);
    let (completed, dead, cancelled) = loop {
        let mut c = 0u32;
        let mut d = 0u32;
        let mut x = 0u32;
        let mut all_done = true;

        for &id in &job_ids {
            if let Ok(record) = rt.status(id).await {
                match record.state {
                    JobState::Completed => c += 1,
                    JobState::Dead => d += 1,
                    JobState::Cancelled => x += 1,
                    _ => all_done = false,
                }
            }
        }

        if all_done {
            break (c, d, x);
        }

        if tokio::time::Instant::now() > timeout {
            println!("Warning: timeout waiting for jobs to complete");
            break (c, d, x);
        }

        tokio::time::sleep(Duration::from_millis(50)).await;
    };

    let stats = rt.stats().await?;

    println!();
    println!("Results:");
    println!("  Completed: {completed}");
    println!("  Dead: {dead}");
    println!("  Cancelled: {cancelled}");
    println!("  Retries: {}", stats.retried);
    println!();

    // Validate expected outcomes.
    if dead != expected_dead {
        println!(
            "Note: Expected {expected_dead} dead jobs, got {dead} (may vary with recovery state)"
        );
    }

    rt.shutdown().await;
    println!("Demo complete.");

    Ok(())
}

fn run_inspect(data_dir: PathBuf) -> Result<(), Box<dyn std::error::Error>> {
    use rust_durable_queue::wal::{WAL_VERSION, scan_wal};

    let wal_path = data_dir.join("wal.log");

    if !wal_path.exists() {
        eprintln!("Error: WAL file not found at {}", wal_path.display());
        std::process::exit(1);
    }

    // Acquire lock for safe read (prevents reading during active write).
    let lock_path = data_dir.join("LOCK");
    let lock_file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(&lock_path)?;

    if fs4::fs_std::FileExt::try_lock_exclusive(&lock_file).is_err() {
        eprintln!("Error: WAL is currently in use by another process");
        std::process::exit(1);
    }

    let file_size = std::fs::metadata(&wal_path)?.len();
    let scan_result = scan_wal(&wal_path)?;

    // Count records by type.
    let mut submitted = 0u64;
    let mut leased = 0u64;
    let mut completed = 0u64;
    let mut dead = 0u64;
    let mut cancelled = 0u64;
    let mut retry_scheduled = 0u64;

    for record in &scan_result.records {
        match record {
            rust_durable_queue::wal::WalRecord::JobSubmitted { .. } => submitted += 1,
            rust_durable_queue::wal::WalRecord::JobLeased { .. } => leased += 1,
            rust_durable_queue::wal::WalRecord::JobCompleted { .. } => completed += 1,
            rust_durable_queue::wal::WalRecord::JobDead { .. } => dead += 1,
            rust_durable_queue::wal::WalRecord::JobCancelled { .. } => cancelled += 1,
            rust_durable_queue::wal::WalRecord::JobRetryScheduled { .. } => retry_scheduled += 1,
        }
    }

    println!("WAL Inspection: {}", wal_path.display());
    println!();
    println!("Format:");
    println!("  Version: {WAL_VERSION}");
    println!("  File size: {} bytes", file_size);
    println!();
    println!("Records:");
    println!("  Total: {}", scan_result.records.len());
    println!("  Last sequence: {}", scan_result.last_sequence);
    println!();
    println!("By type:");
    println!("  JobSubmitted: {submitted}");
    println!("  JobLeased: {leased}");
    println!("  JobCompleted: {completed}");
    println!("  JobDead: {dead}");
    println!("  JobCancelled: {cancelled}");
    println!("  JobRetryScheduled: {retry_scheduled}");
    println!();
    println!(
        "Tail: {}",
        if scan_result.had_truncated_tail {
            "truncated (crash tail detected)"
        } else {
            "clean"
        }
    );

    Ok(())
}

fn run_verify(data_dir: PathBuf) -> Result<(), Box<dyn std::error::Error>> {
    use rust_durable_queue::wal::scan_wal;

    let wal_path = data_dir.join("wal.log");

    if !wal_path.exists() {
        eprintln!("Error: WAL file not found at {}", wal_path.display());
        std::process::exit(1);
    }

    // Acquire lock for safe read.
    let lock_path = data_dir.join("LOCK");
    let lock_file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(&lock_path)?;

    if fs4::fs_std::FileExt::try_lock_exclusive(&lock_file).is_err() {
        eprintln!("Error: WAL is currently in use by another process");
        std::process::exit(1);
    }

    match scan_wal(&wal_path) {
        Ok(result) => {
            println!("WAL valid");
            println!("Records: {}", result.records.len());
            println!("Last sequence: {}", result.last_sequence);

            if result.had_truncated_tail {
                println!("Warning: truncated tail detected (recoverable on startup)");
            }
        }
        Err(e) => {
            eprintln!("WAL verification failed: {e}");
            std::process::exit(1);
        }
    }

    Ok(())
}
