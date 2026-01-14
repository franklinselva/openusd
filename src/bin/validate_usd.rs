//! USD file validation CLI tool.
//!
//! Validates that the openusd crate can parse all USDA, USDC, and USDZ files
//! in a given directory. Useful for testing parser coverage against the
//! usd-wg-assets test suite.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

use clap::Parser;
use indicatif::{ProgressBar, ProgressStyle};
use rayon::prelude::*;
use walkdir::WalkDir;

use openusd::composition::ComposedLayer;
use openusd::usda::TextReader;
use openusd::usdc::CrateData;
use openusd::usdz::Archive;

/// USD file validation result.
#[derive(Debug)]
enum ValidationResult {
    /// File parsed successfully with the given number of specs.
    Success { specs: usize },
    /// File was skipped (not a USD file or intentionally skipped).
    Skipped { reason: String },
    /// File failed to parse with the given error.
    Failed { error: String },
}

/// Validate USD files to test parser coverage.
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Directory or file to validate.
    #[arg(value_name = "PATH")]
    path: PathBuf,

    /// Stop on first error.
    #[arg(long, short = 'f')]
    fail_fast: bool,

    /// Show detailed output for each file.
    #[arg(long, short = 'v')]
    verbose: bool,

    /// Only show summary statistics.
    #[arg(long, short = 's')]
    summary: bool,

    /// Use composition layer (resolves sublayers, references, etc.).
    #[arg(long, short = 'c')]
    composed: bool,

    /// Skip files matching these patterns (can be specified multiple times).
    #[arg(long = "skip", short = 'x')]
    skip_patterns: Vec<String>,
}

/// Validate a single USDA file.
fn validate_usda(path: &Path) -> ValidationResult {
    match TextReader::read(path) {
        Ok(reader) => ValidationResult::Success {
            specs: reader.specs().len(),
        },
        Err(e) => ValidationResult::Failed {
            error: format!("{:#}", e),
        },
    }
}

/// Validate a single USDC file.
fn validate_usdc(path: &Path) -> ValidationResult {
    let file = match std::fs::File::open(path) {
        Ok(f) => f,
        Err(e) => {
            return ValidationResult::Failed {
                error: format!("Failed to open file: {}", e),
            }
        }
    };

    let reader = std::io::BufReader::new(file);
    match CrateData::open(reader, true) {
        Ok(data) => {
            let specs = data.into_specs();
            ValidationResult::Success { specs: specs.len() }
        }
        Err(e) => ValidationResult::Failed {
            error: format!("{:#}", e),
        },
    }
}

/// Validate a USDZ archive by checking its root USD file.
fn validate_usdz(path: &Path) -> ValidationResult {
    let mut archive = match Archive::open(path) {
        Ok(a) => a,
        Err(e) => {
            return ValidationResult::Failed {
                error: format!("Failed to open USDZ: {:#}", e),
            }
        }
    };

    // Try to find and read the root layer
    // USDZ archives should have a default layer at the root
    // Common names: [name].usdc, [name].usda, default.usdc, etc.
    let stem = path.file_stem().and_then(|s| s.to_str()).unwrap_or("default");

    let candidates = [
        format!("{}.usdc", stem),
        format!("{}.usda", stem),
        "default.usdc".to_string(),
        "default.usda".to_string(),
    ];

    for candidate in &candidates {
        match archive.read(candidate) {
            Ok(data) => {
                return ValidationResult::Success {
                    specs: if data.has_spec(&openusd::sdf::Path::abs_root()) {
                        1 // At minimum we have the root
                    } else {
                        0
                    },
                };
            }
            Err(_) => continue,
        }
    }

    ValidationResult::Failed {
        error: "Could not find root layer in USDZ archive".to_string(),
    }
}

/// Validate using ComposedLayer (resolves sublayers, etc.).
fn validate_composed(path: &Path) -> ValidationResult {
    match ComposedLayer::open(path) {
        Ok(composed) => ValidationResult::Success {
            specs: composed.specs.len(),
        },
        Err(e) => ValidationResult::Failed {
            error: format!("{:#}", e),
        },
    }
}

/// Validate a single file based on its extension.
fn validate_file(path: &Path, composed: bool) -> ValidationResult {
    let extension = path
        .extension()
        .and_then(|e| e.to_str())
        .unwrap_or("")
        .to_lowercase();

    match extension.as_str() {
        "usda" => {
            if composed {
                validate_composed(path)
            } else {
                validate_usda(path)
            }
        }
        "usdc" => {
            if composed {
                validate_composed(path)
            } else {
                validate_usdc(path)
            }
        }
        "usd" => {
            // Generic USD extension - try composition which auto-detects
            if composed {
                validate_composed(path)
            } else {
                // Try USDA first, then USDC
                let result = validate_usda(path);
                if matches!(result, ValidationResult::Failed { .. }) {
                    validate_usdc(path)
                } else {
                    result
                }
            }
        }
        "usdz" => validate_usdz(path),
        _ => ValidationResult::Skipped {
            reason: format!("Not a USD file: .{}", extension),
        },
    }
}

/// Check if a path should be skipped based on patterns.
fn should_skip(path: &Path, patterns: &[String]) -> bool {
    let path_str = path.to_string_lossy();
    patterns.iter().any(|p| path_str.contains(p))
}

/// Collect all USD files from a path (file or directory).
fn collect_usd_files(path: &Path, skip_patterns: &[String]) -> Vec<PathBuf> {
    if path.is_file() {
        if should_skip(path, skip_patterns) {
            vec![]
        } else {
            vec![path.to_path_buf()]
        }
    } else {
        WalkDir::new(path)
            .into_iter()
            .filter_map(|e| e.ok())
            .filter(|e| e.file_type().is_file())
            .map(|e| e.path().to_path_buf())
            .filter(|p| {
                let ext = p.extension().and_then(|e| e.to_str()).unwrap_or("");
                matches!(ext.to_lowercase().as_str(), "usda" | "usdc" | "usd" | "usdz")
            })
            .filter(|p| !should_skip(p, skip_patterns))
            .collect()
    }
}

fn main() {
    let args = Args::parse();

    let start = Instant::now();

    // Collect files
    let files = collect_usd_files(&args.path, &args.skip_patterns);

    if files.is_empty() {
        eprintln!("No USD files found in: {}", args.path.display());
        std::process::exit(1);
    }

    println!("Validating {} USD files...\n", files.len());

    // Progress bar
    let progress = if !args.summary && !args.verbose {
        let pb = ProgressBar::new(files.len() as u64);
        pb.set_style(
            ProgressStyle::default_bar()
                .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({eta})")
                .unwrap()
                .progress_chars("#>-"),
        );
        Some(pb)
    } else {
        None
    };

    // Counters
    let passed = AtomicUsize::new(0);
    let failed = AtomicUsize::new(0);
    let skipped = AtomicUsize::new(0);

    // Collect failures for summary
    let failures: std::sync::Mutex<Vec<(PathBuf, String)>> = std::sync::Mutex::new(Vec::new());

    // Validate files in parallel
    let fail_fast_triggered = std::sync::atomic::AtomicBool::new(false);

    files.par_iter().for_each(|file| {
        // Check if fail-fast was triggered
        if args.fail_fast && fail_fast_triggered.load(Ordering::Relaxed) {
            return;
        }

        let result = validate_file(file, args.composed);

        // Update progress
        if let Some(ref pb) = progress {
            pb.inc(1);
        }

        // Process result
        match &result {
            ValidationResult::Success { specs } => {
                passed.fetch_add(1, Ordering::Relaxed);
                if args.verbose {
                    let rel_path = file.strip_prefix(&args.path).unwrap_or(file);
                    println!("[PASS] {} ({} specs)", rel_path.display(), specs);
                }
            }
            ValidationResult::Skipped { reason } => {
                skipped.fetch_add(1, Ordering::Relaxed);
                if args.verbose {
                    let rel_path = file.strip_prefix(&args.path).unwrap_or(file);
                    println!("[SKIP] {} - {}", rel_path.display(), reason);
                }
            }
            ValidationResult::Failed { error } => {
                failed.fetch_add(1, Ordering::Relaxed);
                let rel_path = file.strip_prefix(&args.path).unwrap_or(file);

                if !args.summary {
                    println!("[FAIL] {}", rel_path.display());
                    println!("       Error: {}", error);
                    println!();
                }

                failures.lock().unwrap().push((rel_path.to_path_buf(), error.clone()));

                if args.fail_fast {
                    fail_fast_triggered.store(true, Ordering::Relaxed);
                }
            }
        }
    });

    // Finish progress bar
    if let Some(pb) = progress {
        pb.finish_and_clear();
    }

    let elapsed = start.elapsed();

    // Print summary
    println!();
    println!("================================================================================");
    println!("Validation Summary");
    println!("================================================================================");
    println!();

    let total = files.len();
    let pass_count = passed.load(Ordering::Relaxed);
    let fail_count = failed.load(Ordering::Relaxed);
    let skip_count = skipped.load(Ordering::Relaxed);

    let pass_pct = (pass_count as f64 / total as f64) * 100.0;
    let fail_pct = (fail_count as f64 / total as f64) * 100.0;
    let skip_pct = (skip_count as f64 / total as f64) * 100.0;

    println!("Total files:  {}", total);
    println!("Passed:       {} ({:.1}%)", pass_count, pass_pct);
    println!("Failed:       {} ({:.1}%)", fail_count, fail_pct);
    println!("Skipped:      {} ({:.1}%)", skip_count, skip_pct);
    println!();
    println!("Time elapsed: {:.2}s", elapsed.as_secs_f64());

    // List failed files
    let failures = failures.lock().unwrap();
    if !failures.is_empty() {
        println!();
        println!("Failed files:");
        for (path, error) in failures.iter() {
            println!("  - {}", path.display());
            if args.verbose || args.summary {
                println!("    {}", error);
            }
        }
    }

    // Exit with error code if there were failures
    if fail_count > 0 && !args.fail_fast {
        std::process::exit(1);
    } else if args.fail_fast && fail_fast_triggered.load(Ordering::Relaxed) {
        std::process::exit(1);
    }
}
