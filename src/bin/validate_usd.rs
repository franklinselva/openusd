//! USD file validation CLI tool.
//!
//! Validates that the openusd crate can parse all USDA, USDC, and USDZ files
//! in a given directory. Useful for testing parser coverage against the
//! usd-wg-assets test suite.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

use clap::Parser;
use indicatif::{ProgressBar, ProgressStyle};
use rayon::prelude::*;
use walkdir::WalkDir;

use openusd::composition::ComposedLayer;
use openusd::sdf::{self, Value};
use openusd::usda::TextReader;
use openusd::usdc::CrateData;
use openusd::usdz::Archive;

/// File type for tracking statistics.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum FileType {
    Usda,
    Usdc,
    Usd,
    Usdz,
}

impl FileType {
    fn from_extension(ext: &str) -> Option<Self> {
        match ext.to_lowercase().as_str() {
            "usda" => Some(FileType::Usda),
            "usdc" => Some(FileType::Usdc),
            "usd" => Some(FileType::Usd),
            "usdz" => Some(FileType::Usdz),
            _ => None,
        }
    }

    fn as_str(&self) -> &'static str {
        match self {
            FileType::Usda => "USDA",
            FileType::Usdc => "USDC",
            FileType::Usd => "USD",
            FileType::Usdz => "USDZ",
        }
    }
}

/// Asset reference found in a USD file.
#[derive(Debug, Clone)]
struct AssetRef {
    /// The asset path as written in the USD file.
    path: String,
    /// The type of reference (texture, sublayer, reference, payload).
    ref_type: AssetRefType,
    /// Whether the referenced file exists.
    exists: bool,
}

#[derive(Debug, Clone, Copy)]
enum AssetRefType {
    Texture,
    Sublayer,
    Reference,
    Payload,
}

impl AssetRefType {
    fn as_str(&self) -> &'static str {
        match self {
            AssetRefType::Texture => "texture",
            AssetRefType::Sublayer => "sublayer",
            AssetRefType::Reference => "reference",
            AssetRefType::Payload => "payload",
        }
    }
}

/// USD file validation result.
#[derive(Debug)]
enum ValidationResult {
    /// File parsed successfully with the given number of specs.
    Success {
        specs: usize,
        file_type: FileType,
        /// Missing asset references (only populated in asset-check mode).
        missing_assets: Vec<AssetRef>,
    },
    /// File was skipped (not a USD file or intentionally skipped).
    Skipped { reason: String },
    /// File failed to parse with the given error.
    Failed { error: String, file_type: FileType },
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

    /// Check that referenced assets (textures, sublayers, etc.) exist.
    #[arg(long, short = 'a')]
    check_assets: bool,

    /// Skip files matching these patterns (can be specified multiple times).
    #[arg(long = "skip", short = 'x')]
    skip_patterns: Vec<String>,
}

/// Extract asset references from parsed USD data.
fn extract_asset_refs(specs: &HashMap<sdf::Path, sdf::Spec>, base_path: &Path) -> Vec<AssetRef> {
    let mut refs = Vec::new();
    let base_dir = base_path.parent().unwrap_or(Path::new("."));

    for (_path, spec) in specs {
        for (key, value) in &spec.fields {
            // Check for sublayers
            if key == "subLayers" {
                if let Value::StringVec(layers) = value {
                    for layer in layers {
                        let asset_path = resolve_asset_path(layer, base_dir);
                        refs.push(AssetRef {
                            path: layer.clone(),
                            ref_type: AssetRefType::Sublayer,
                            exists: asset_path.map(|p| p.exists()).unwrap_or(false),
                        });
                    }
                }
            }

            // Check for references
            if key == "references" {
                extract_reference_list_op(value, base_dir, AssetRefType::Reference, &mut refs);
            }

            // Check for payloads
            if key == "payload" {
                extract_payload_list_op(value, base_dir, &mut refs);
            }

            // Check for asset-typed attributes (textures)
            if let Value::AssetPath(asset) = value {
                if !asset.is_empty() {
                    let asset_path = resolve_asset_path(asset, base_dir);
                    refs.push(AssetRef {
                        path: asset.clone(),
                        ref_type: AssetRefType::Texture,
                        exists: asset_path.map(|p| p.exists()).unwrap_or(false),
                    });
                }
            }
        }
    }

    refs
}

/// Extract references from a ReferenceListOp value.
fn extract_reference_list_op(
    value: &Value,
    base_dir: &Path,
    ref_type: AssetRefType,
    refs: &mut Vec<AssetRef>,
) {
    if let Value::ReferenceListOp(list_op) = value {
        for reference in list_op
            .explicit_items
            .iter()
            .chain(list_op.prepended_items.iter())
            .chain(list_op.appended_items.iter())
        {
            if !reference.asset_path.is_empty() {
                let asset_path = resolve_asset_path(&reference.asset_path, base_dir);
                refs.push(AssetRef {
                    path: reference.asset_path.clone(),
                    ref_type,
                    exists: asset_path.map(|p| p.exists()).unwrap_or(false),
                });
            }
        }
    }
}

/// Extract payloads from a PayloadListOp value.
fn extract_payload_list_op(value: &Value, base_dir: &Path, refs: &mut Vec<AssetRef>) {
    if let Value::PayloadListOp(list_op) = value {
        for payload in list_op
            .explicit_items
            .iter()
            .chain(list_op.prepended_items.iter())
            .chain(list_op.appended_items.iter())
        {
            if !payload.asset_path.is_empty() {
                let asset_path = resolve_asset_path(&payload.asset_path, base_dir);
                refs.push(AssetRef {
                    path: payload.asset_path.clone(),
                    ref_type: AssetRefType::Payload,
                    exists: asset_path.map(|p| p.exists()).unwrap_or(false),
                });
            }
        }
    }
}

/// Resolve an asset path relative to a base directory.
fn resolve_asset_path(asset_path: &str, base_dir: &Path) -> Option<PathBuf> {
    // Skip empty paths and search paths (starting with //)
    if asset_path.is_empty() || asset_path.starts_with("//") {
        return None;
    }

    // Handle relative paths
    if asset_path.starts_with("./") || asset_path.starts_with("../") {
        Some(base_dir.join(asset_path))
    } else if asset_path.starts_with('/') {
        // Absolute path
        Some(PathBuf::from(asset_path))
    } else {
        // Relative to base directory
        Some(base_dir.join(asset_path))
    }
}

/// Validate a single USDA file.
fn validate_usda(path: &Path, check_assets: bool) -> ValidationResult {
    match TextReader::read(path) {
        Ok(reader) => {
            let specs = reader.specs();
            let missing_assets = if check_assets {
                extract_asset_refs(&specs, path)
                    .into_iter()
                    .filter(|r| !r.exists)
                    .collect()
            } else {
                Vec::new()
            };
            ValidationResult::Success {
                specs: specs.len(),
                file_type: FileType::Usda,
                missing_assets,
            }
        }
        Err(e) => ValidationResult::Failed {
            error: format!("{:#}", e),
            file_type: FileType::Usda,
        },
    }
}

/// Validate a single USDC file.
fn validate_usdc(path: &Path, check_assets: bool) -> ValidationResult {
    let file = match std::fs::File::open(path) {
        Ok(f) => f,
        Err(e) => {
            return ValidationResult::Failed {
                error: format!("Failed to open file: {}", e),
                file_type: FileType::Usdc,
            }
        }
    };

    let reader = std::io::BufReader::new(file);
    match CrateData::open(reader, true) {
        Ok(data) => {
            let specs = data.into_specs();
            let missing_assets = if check_assets {
                extract_asset_refs(&specs, path)
                    .into_iter()
                    .filter(|r| !r.exists)
                    .collect()
            } else {
                Vec::new()
            };
            ValidationResult::Success {
                specs: specs.len(),
                file_type: FileType::Usdc,
                missing_assets,
            }
        }
        Err(e) => ValidationResult::Failed {
            error: format!("{:#}", e),
            file_type: FileType::Usdc,
        },
    }
}

/// Validate a USDZ archive by checking its root USD file.
fn validate_usdz(path: &Path, check_assets: bool) -> ValidationResult {
    let mut archive = match Archive::open(path) {
        Ok(a) => a,
        Err(e) => {
            return ValidationResult::Failed {
                error: format!("Failed to open USDZ: {:#}", e),
                file_type: FileType::Usdz,
            }
        }
    };

    // Find the root layer by scanning all files in the archive
    let root_layer = match archive.find_root_layer() {
        Some(layer) => layer,
        None => {
            return ValidationResult::Failed {
                error: "Could not find root layer in USDZ archive (no .usdc/.usda/.usd files found)"
                    .to_string(),
                file_type: FileType::Usdz,
            }
        }
    };

    // Read and validate the root layer
    match archive.read(&root_layer) {
        Ok(data) => {
            // For USDZ, asset checking would need to check files within the archive
            // For now, we skip detailed asset checking for USDZ
            let missing_assets = if check_assets {
                // Could implement checking files exist within the archive
                Vec::new()
            } else {
                Vec::new()
            };

            ValidationResult::Success {
                specs: if data.has_spec(&openusd::sdf::Path::abs_root()) {
                    1 // At minimum we have the root
                } else {
                    0
                },
                file_type: FileType::Usdz,
                missing_assets,
            }
        }
        Err(e) => ValidationResult::Failed {
            error: format!("Failed to read root layer '{}': {:#}", root_layer, e),
            file_type: FileType::Usdz,
        },
    }
}

/// Validate using ComposedLayer (resolves sublayers, etc.).
fn validate_composed(path: &Path, file_type: FileType) -> ValidationResult {
    match ComposedLayer::open(path) {
        Ok(composed) => ValidationResult::Success {
            specs: composed.specs.len(),
            file_type,
            missing_assets: Vec::new(),
        },
        Err(e) => ValidationResult::Failed {
            error: format!("{:#}", e),
            file_type,
        },
    }
}

/// Validate a single file based on its extension.
fn validate_file(path: &Path, composed: bool, check_assets: bool) -> ValidationResult {
    let extension = path
        .extension()
        .and_then(|e| e.to_str())
        .unwrap_or("")
        .to_lowercase();

    match extension.as_str() {
        "usda" => {
            if composed {
                validate_composed(path, FileType::Usda)
            } else {
                validate_usda(path, check_assets)
            }
        }
        "usdc" => {
            if composed {
                validate_composed(path, FileType::Usdc)
            } else {
                validate_usdc(path, check_assets)
            }
        }
        "usd" => {
            // Generic USD extension - try composition which auto-detects
            if composed {
                validate_composed(path, FileType::Usd)
            } else {
                // Try USDA first, then USDC
                let result = validate_usda(path, check_assets);
                if matches!(result, ValidationResult::Failed { .. }) {
                    validate_usdc(path, check_assets)
                } else {
                    // Update file type to USD
                    match result {
                        ValidationResult::Success {
                            specs,
                            missing_assets,
                            ..
                        } => ValidationResult::Success {
                            specs,
                            file_type: FileType::Usd,
                            missing_assets,
                        },
                        other => other,
                    }
                }
            }
        }
        "usdz" => validate_usdz(path, check_assets),
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

/// Statistics tracked per file type.
#[derive(Default)]
struct FileTypeStats {
    total: usize,
    passed: usize,
    failed: usize,
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
                .template(
                    "{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({eta})",
                )
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

    // File type statistics
    let type_stats: std::sync::Mutex<HashMap<FileType, FileTypeStats>> =
        std::sync::Mutex::new(HashMap::new());

    // Collect failures for summary
    let failures: std::sync::Mutex<Vec<(PathBuf, String)>> = std::sync::Mutex::new(Vec::new());

    // Collect asset warnings
    let asset_warnings: std::sync::Mutex<Vec<(PathBuf, Vec<AssetRef>)>> =
        std::sync::Mutex::new(Vec::new());

    // Validate files in parallel
    let fail_fast_triggered = std::sync::atomic::AtomicBool::new(false);

    files.par_iter().for_each(|file| {
        // Check if fail-fast was triggered
        if args.fail_fast && fail_fast_triggered.load(Ordering::Relaxed) {
            return;
        }

        let result = validate_file(file, args.composed, args.check_assets);

        // Update progress
        if let Some(ref pb) = progress {
            pb.inc(1);
        }

        // Process result
        match &result {
            ValidationResult::Success {
                specs,
                file_type,
                missing_assets,
            } => {
                passed.fetch_add(1, Ordering::Relaxed);

                // Update type stats
                {
                    let mut stats = type_stats.lock().unwrap();
                    let entry = stats.entry(*file_type).or_default();
                    entry.total += 1;
                    entry.passed += 1;
                }

                if args.verbose {
                    let rel_path = file.strip_prefix(&args.path).unwrap_or(file);
                    println!(
                        "[PASS] {} ({} specs, {})",
                        rel_path.display(),
                        specs,
                        file_type.as_str()
                    );
                }

                // Track missing assets
                if !missing_assets.is_empty() {
                    let rel_path = file.strip_prefix(&args.path).unwrap_or(file);
                    asset_warnings
                        .lock()
                        .unwrap()
                        .push((rel_path.to_path_buf(), missing_assets.clone()));
                }
            }
            ValidationResult::Skipped { reason } => {
                skipped.fetch_add(1, Ordering::Relaxed);
                if args.verbose {
                    let rel_path = file.strip_prefix(&args.path).unwrap_or(file);
                    println!("[SKIP] {} - {}", rel_path.display(), reason);
                }
            }
            ValidationResult::Failed { error, file_type } => {
                failed.fetch_add(1, Ordering::Relaxed);

                // Update type stats
                {
                    let mut stats = type_stats.lock().unwrap();
                    let entry = stats.entry(*file_type).or_default();
                    entry.total += 1;
                    entry.failed += 1;
                }

                let rel_path = file.strip_prefix(&args.path).unwrap_or(file);

                if !args.summary {
                    println!("[FAIL] {}", rel_path.display());
                    println!("       Error: {}", error);
                    println!();
                }

                failures
                    .lock()
                    .unwrap()
                    .push((rel_path.to_path_buf(), error.clone()));

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

    // Print file type breakdown
    let stats = type_stats.lock().unwrap();
    if !stats.is_empty() {
        println!("By file type:");
        for file_type in [FileType::Usda, FileType::Usdc, FileType::Usd, FileType::Usdz] {
            if let Some(s) = stats.get(&file_type) {
                let pct = if s.total > 0 {
                    (s.passed as f64 / s.total as f64) * 100.0
                } else {
                    0.0
                };
                println!(
                    "  {:<6} {:>4} files  ({:>3} passed, {:>3} failed) [{:.1}%]",
                    file_type.as_str(),
                    s.total,
                    s.passed,
                    s.failed,
                    pct
                );
            }
        }
        println!();
    }

    println!("Time elapsed: {:.2}s", elapsed.as_secs_f64());

    // List asset warnings
    if args.check_assets {
        let warnings = asset_warnings.lock().unwrap();
        if !warnings.is_empty() {
            println!();
            println!("Missing asset references:");
            for (path, missing) in warnings.iter() {
                println!("  {}:", path.display());
                for asset in missing {
                    println!("    - [{}] {}", asset.ref_type.as_str(), asset.path);
                }
            }
        }
    }

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
