use crate::nanocloud::cli::args::RestoreArgs;
use crate::nanocloud::engine::Snapshot;

use std::collections::HashMap;
use std::env;
use std::error::Error;
use std::path::PathBuf;
use std::time::Instant;

pub async fn handle_restore(args: &RestoreArgs) -> Result<(), Box<dyn Error + Send + Sync>> {
    if args.mappings.is_empty() {
        return Err("Provide at least one --map claim=PATH mapping".into());
    }

    let mut host_paths = HashMap::new();
    for (claim, path) in &args.mappings {
        let claim = claim.trim();
        let path = path.trim();
        if claim.is_empty() {
            return Err("Volume claim names cannot be empty".into());
        }
        if path.is_empty() {
            return Err(format!("Restore path for claim '{}' cannot be empty", claim).into());
        }
        let resolved = resolve_path(path)?;
        if host_paths
            .insert(claim.to_string(), resolved.to_string_lossy().into_owned())
            .is_some()
        {
            return Err(format!("Duplicate mapping for claim '{}'", claim).into());
        }
    }

    let snapshot = Snapshot::new(&args.artifact)?;
    let summary = snapshot.summary().ok().flatten();
    let restore_start = Instant::now();
    snapshot
        .restore(
            args.service.as_deref().unwrap_or("cli-restore"),
            &host_paths,
        )
        .await?;
    let restore_duration = restore_start.elapsed();
    let (volumes, total_bytes) = summary
        .map(|summary| {
            (
                summary.entries.len(),
                summary
                    .entries
                    .iter()
                    .map(|entry| entry.size_bytes)
                    .sum::<u64>(),
            )
        })
        .unwrap_or_else(|| (host_paths.len(), 0));
    let throughput = if restore_duration.as_secs_f64() > 0.0 && total_bytes > 0 {
        (total_bytes as f64 / (1 << 20) as f64) / restore_duration.as_secs_f64()
    } else {
        0.0
    };
    println!(
        "Restored {volumes} volume(s) ({bytes} bytes) from '{}' in {:.3}s ({throughput:.2} MiB/s)",
        args.artifact,
        restore_duration.as_secs_f64(),
        bytes = total_bytes
    );
    Ok(())
}

fn resolve_path(raw: &str) -> Result<PathBuf, Box<dyn Error + Send + Sync>> {
    let expanded = if let Some(stripped) = raw.strip_prefix("~/") {
        if let Ok(home) = env::var("HOME") {
            PathBuf::from(home).join(stripped)
        } else {
            PathBuf::from(raw)
        }
    } else {
        PathBuf::from(raw)
    };

    if expanded.is_absolute() {
        Ok(expanded)
    } else {
        Ok(env::current_dir()?.join(expanded))
    }
}
