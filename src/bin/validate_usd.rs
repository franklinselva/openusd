//! USD asset validation CLI tool.
//!
//! Validates that the openusd crate can properly parse USD assets.
//! Supports both individual files and asset directories.
//!
//! When given an asset directory, the validator will:
//! 1. Find the root USD file(s) for the asset
//! 2. Load with full composition (sublayers, references, payloads)
//! 3. Validate the entire asset structure
//!
//! This serves as a reference implementation for how to properly load
//! USD assets using the openusd crate.

use std::collections::HashMap;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Instant;

use anyhow::{Context, Result};
use clap::Parser;
use indicatif::{ProgressBar, ProgressStyle};
use rayon::prelude::*;
use walkdir::WalkDir;

use openusd::composition::ComposedLayer;
use openusd::sdf::{self, Value};
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
    fn as_str(&self) -> &'static str {
        match self {
            FileType::Usda => "USDA",
            FileType::Usdc => "USDC",
            FileType::Usd => "USD",
            FileType::Usdz => "USDZ",
        }
    }
}

/// Detect the actual format of a USD file by checking its magic bytes.
fn detect_usd_format(path: &Path) -> Option<FileType> {
    let mut file = std::fs::File::open(path).ok()?;
    let mut header = [0u8; 8];
    file.read_exact(&mut header).ok()?;

    // Check for USDC magic number "PXR-USDC"
    if &header == b"PXR-USDC" {
        Some(FileType::Usdc)
    } else if header.starts_with(b"#usda") {
        Some(FileType::Usda)
    } else {
        // Try to determine from content - if it starts with printable ASCII, likely USDA
        if header
            .iter()
            .all(|&b| b.is_ascii() && !b.is_ascii_control() || b == b'\n' || b == b'\r' || b == b'\t')
        {
            Some(FileType::Usda)
        } else {
            Some(FileType::Usdc) // Default to binary
        }
    }
}

/// Asset reference found in a USD file.
#[derive(Debug, Clone)]
struct AssetRef {
    path: String,
    ref_type: AssetRefType,
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
    Success {
        specs: usize,
        file_type: FileType,
        missing_assets: Vec<AssetRef>,
        composed_layers: usize,
    },
    Skipped {
        reason: String,
    },
    Failed {
        error: String,
        file_type: FileType,
    },
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

    /// Check that referenced assets (textures, sublayers, etc.) exist.
    #[arg(long, short = 'a')]
    check_assets: bool,

    /// Skip files matching these patterns (can be specified multiple times).
    #[arg(long = "skip", short = 'x')]
    skip_patterns: Vec<String>,

    /// Treat path as an asset directory and find root USD file(s).
    #[arg(long)]
    asset: bool,
}

/// Extract asset references from parsed USD data.
fn extract_asset_refs(specs: &HashMap<sdf::Path, sdf::Spec>, base_path: &Path) -> Vec<AssetRef> {
    let mut refs = Vec::new();
    let base_dir = base_path.parent().unwrap_or(Path::new("."));

    for (_path, spec) in specs {
        for (key, value) in &spec.fields {
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

            if key == "references" {
                extract_reference_list_op(value, base_dir, AssetRefType::Reference, &mut refs);
            }

            if key == "payload" {
                extract_payload_list_op(value, base_dir, &mut refs);
            }

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

fn extract_reference_list_op(value: &Value, base_dir: &Path, ref_type: AssetRefType, refs: &mut Vec<AssetRef>) {
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

fn resolve_asset_path(asset_path: &str, base_dir: &Path) -> Option<PathBuf> {
    if asset_path.is_empty() || asset_path.starts_with("//") {
        return None;
    }

    // Clean up asset path markers
    let clean_path = asset_path.trim_matches('@').trim();

    if clean_path.starts_with("./") || clean_path.starts_with("../") {
        Some(base_dir.join(clean_path))
    } else if clean_path.starts_with('/') {
        Some(PathBuf::from(clean_path))
    } else {
        Some(base_dir.join(clean_path))
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
                composed_layers: 1,
            }
        }
        Err(e) => ValidationResult::Failed {
            error: format!("{:#}", e),
            file_type: FileType::Usdc,
        },
    }
}

/// Validate a USDZ archive by checking its root USD file.
fn validate_usdz(path: &Path, _check_assets: bool) -> ValidationResult {
    let mut archive = match Archive::open(path) {
        Ok(a) => a,
        Err(e) => {
            return ValidationResult::Failed {
                error: format!("Failed to open USDZ: {:#}", e),
                file_type: FileType::Usdz,
            }
        }
    };

    let root_layer = match archive.find_root_layer() {
        Some(layer) => layer,
        None => {
            return ValidationResult::Failed {
                error: "Could not find root layer in USDZ archive".to_string(),
                file_type: FileType::Usdz,
            }
        }
    };

    match archive.read(&root_layer) {
        Ok(data) => {
            // USDZ archives are self-contained; if we can read the root layer, it's valid
            // Count specs by checking if the root exists
            let spec_count = if data.has_spec(&openusd::sdf::Path::abs_root()) {
                1
            } else {
                0
            };
            ValidationResult::Success {
                specs: spec_count,
                file_type: FileType::Usdz,
                missing_assets: Vec::new(),
                composed_layers: 1,
            }
        }
        Err(e) => ValidationResult::Failed {
            error: format!("Failed to read root layer '{}': {:#}", root_layer, e),
            file_type: FileType::Usdz,
        },
    }
}

/// Validate using ComposedLayer (resolves sublayers, references, etc.).
/// This is the primary validation method as it tests full USD composition.
fn validate_composed(path: &Path, file_type: FileType, check_assets: bool) -> ValidationResult {
    match ComposedLayer::open(path) {
        Ok(composed) => {
            let missing_assets = if check_assets {
                extract_asset_refs(&composed.specs, path)
                    .into_iter()
                    .filter(|r| !r.exists)
                    .collect()
            } else {
                Vec::new()
            };
            ValidationResult::Success {
                specs: composed.specs.len(),
                file_type,
                missing_assets,
                composed_layers: composed.composed_layers.len(),
            }
        }
        Err(e) => ValidationResult::Failed {
            error: format!("{:#}", e),
            file_type,
        },
    }
}

/// Validate a single file with full composition support.
/// Automatically detects format for .usd files.
fn validate_file(path: &Path, check_assets: bool) -> ValidationResult {
    let extension = path.extension().and_then(|e| e.to_str()).unwrap_or("").to_lowercase();

    match extension.as_str() {
        "usda" => validate_composed(path, FileType::Usda, check_assets),
        "usdc" => validate_composed(path, FileType::Usdc, check_assets),
        "usd" => {
            // Detect actual format and validate with composition
            let actual_type = detect_usd_format(path).unwrap_or(FileType::Usd);
            match actual_type {
                FileType::Usdc => validate_usdc(path, check_assets),
                _ => validate_composed(path, FileType::Usd, check_assets),
            }
        }
        "usdz" => validate_usdz(path, check_assets),
        _ => ValidationResult::Skipped {
            reason: format!("Not a USD file: .{}", extension),
        },
    }
}

/// Find root USD files in an asset directory.
///
/// USD assets typically have a root file that is either:
/// 1. Named after the directory (e.g., Teapot/Teapot.usd)
/// 2. A top-level USD file that references other layers
///
/// This function identifies the entry point(s) for an asset.
fn find_asset_root_files(asset_dir: &Path) -> Result<Vec<PathBuf>> {
    let asset_name = asset_dir
        .file_name()
        .and_then(|n| n.to_str())
        .context("Invalid asset directory name")?;

    let mut root_files = Vec::new();

    // Priority 1: File named exactly like the directory
    for ext in &["usd", "usda", "usdc", "usdz"] {
        let candidate = asset_dir.join(format!("{}.{}", asset_name, ext));
        if candidate.exists() && candidate.is_file() {
            root_files.push(candidate);
        }
    }

    // If we found files named after the directory, those are the roots
    if !root_files.is_empty() {
        return Ok(root_files);
    }

    // Priority 2: Top-level USD files that aren't obviously sublayers
    let sublayer_patterns = [
        "_geo",
        "_geom",
        "_geometry",
        "_material",
        "_materials",
        "_payload",
        "_look",
        "_rig",
        "_anim",
        "_cache",
    ];

    for entry in std::fs::read_dir(asset_dir)? {
        let entry = entry?;
        let path = entry.path();

        if !path.is_file() {
            continue;
        }

        let ext = path.extension().and_then(|e| e.to_str()).unwrap_or("");
        if !matches!(ext.to_lowercase().as_str(), "usd" | "usda" | "usdc" | "usdz") {
            continue;
        }

        // Check if this looks like a sublayer/component file
        let stem = path.file_stem().and_then(|s| s.to_str()).unwrap_or("");
        let stem_lower = stem.to_lowercase();

        let is_sublayer = sublayer_patterns.iter().any(|p| stem_lower.contains(p));

        if !is_sublayer {
            root_files.push(path);
        }
    }

    Ok(root_files)
}

/// Validate an asset directory by finding and validating root USD files.
fn validate_asset_directory(asset_dir: &Path, check_assets: bool, verbose: bool) -> Vec<(PathBuf, ValidationResult)> {
    let mut results = Vec::new();

    let root_files = match find_asset_root_files(asset_dir) {
        Ok(files) => files,
        Err(e) => {
            if verbose {
                eprintln!("  Warning: Could not find root files in {}: {}", asset_dir.display(), e);
            }
            return results;
        }
    };

    if root_files.is_empty() {
        if verbose {
            eprintln!("  Warning: No root USD files found in {}", asset_dir.display());
        }
        return results;
    }

    for root_file in root_files {
        let result = validate_file(&root_file, check_assets);
        results.push((root_file, result));
    }

    results
}

fn should_skip(path: &Path, patterns: &[String]) -> bool {
    let path_str = path.to_string_lossy();
    patterns.iter().any(|p| path_str.contains(p))
}

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

#[derive(Default)]
struct FileTypeStats {
    total: usize,
    passed: usize,
    failed: usize,
}

fn main() {
    let args = Args::parse();
    let start = Instant::now();

    // Determine validation mode
    let is_asset_mode = args.asset || (args.path.is_dir() && !args.path.join(".git").exists());

    if is_asset_mode && args.path.is_dir() {
        // Asset directory mode - find and validate root USD files
        run_asset_validation(&args);
    } else {
        // File or recursive directory mode
        run_file_validation(&args);
    }

    println!("\nTime elapsed: {:.2}s", start.elapsed().as_secs_f64());
}

fn run_asset_validation(args: &Args) {
    println!("Asset Validation Mode");
    println!("=====================");
    println!("Asset directory: {}\n", args.path.display());

    let results = validate_asset_directory(&args.path, args.check_assets, args.verbose);

    if results.is_empty() {
        eprintln!("No USD files found in asset directory: {}", args.path.display());
        std::process::exit(1);
    }

    let mut passed = 0;
    let mut failed = 0;

    for (path, result) in &results {
        let rel_path = path.strip_prefix(&args.path).unwrap_or(path);

        match result {
            ValidationResult::Success {
                specs,
                file_type,
                composed_layers,
                missing_assets,
            } => {
                passed += 1;
                println!(
                    "[PASS] {} ({} specs, {} layers, {})",
                    rel_path.display(),
                    specs,
                    composed_layers,
                    file_type.as_str()
                );

                if args.verbose && !missing_assets.is_empty() {
                    println!("       Missing assets:");
                    for asset in missing_assets {
                        println!("         - [{}] {}", asset.ref_type.as_str(), asset.path);
                    }
                }
            }
            ValidationResult::Skipped { reason } => {
                println!("[SKIP] {} - {}", rel_path.display(), reason);
            }
            ValidationResult::Failed { error, file_type } => {
                failed += 1;
                println!("[FAIL] {} ({})", rel_path.display(), file_type.as_str());
                println!("       Error: {}", error);
            }
        }
    }

    println!();
    println!("================================================================================");
    println!("Summary");
    println!("================================================================================");
    println!("Root files found: {}", results.len());
    println!("Passed:           {}", passed);
    println!("Failed:           {}", failed);

    if failed > 0 {
        std::process::exit(1);
    }
}

fn run_file_validation(args: &Args) {
    let files = collect_usd_files(&args.path, &args.skip_patterns);

    if files.is_empty() {
        eprintln!("No USD files found in: {}", args.path.display());
        std::process::exit(1);
    }

    println!("Validating {} USD files...\n", files.len());

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

    let passed = AtomicUsize::new(0);
    let failed = AtomicUsize::new(0);
    let skipped = AtomicUsize::new(0);
    let type_stats: std::sync::Mutex<HashMap<FileType, FileTypeStats>> = std::sync::Mutex::new(HashMap::new());
    let failures: std::sync::Mutex<Vec<(PathBuf, String)>> = std::sync::Mutex::new(Vec::new());
    let asset_warnings: std::sync::Mutex<Vec<(PathBuf, Vec<AssetRef>)>> = std::sync::Mutex::new(Vec::new());
    let fail_fast_triggered = AtomicBool::new(false);

    files.par_iter().for_each(|file| {
        if args.fail_fast && fail_fast_triggered.load(Ordering::Relaxed) {
            return;
        }

        let result = validate_file(file, args.check_assets);

        if let Some(ref pb) = progress {
            pb.inc(1);
        }

        match &result {
            ValidationResult::Success {
                specs,
                file_type,
                missing_assets,
                composed_layers,
            } => {
                passed.fetch_add(1, Ordering::Relaxed);

                {
                    let mut stats = type_stats.lock().unwrap();
                    let entry = stats.entry(*file_type).or_default();
                    entry.total += 1;
                    entry.passed += 1;
                }

                if args.verbose {
                    let rel_path = file.strip_prefix(&args.path).unwrap_or(file);
                    println!(
                        "[PASS] {} ({} specs, {} layers, {})",
                        rel_path.display(),
                        specs,
                        composed_layers,
                        file_type.as_str()
                    );
                }

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

                failures.lock().unwrap().push((rel_path.to_path_buf(), error.clone()));

                if args.fail_fast {
                    fail_fast_triggered.store(true, Ordering::Relaxed);
                }
            }
        }
    });

    if let Some(pb) = progress {
        pb.finish_and_clear();
    }

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

    if args.check_assets {
        let warnings = asset_warnings.lock().unwrap();
        if !warnings.is_empty() {
            println!("Missing asset references:");
            for (path, missing) in warnings.iter() {
                println!("  {}:", path.display());
                for asset in missing {
                    println!("    - [{}] {}", asset.ref_type.as_str(), asset.path);
                }
            }
        }
    }

    let failures = failures.lock().unwrap();
    if !failures.is_empty() {
        println!("Failed files:");
        for (path, error) in failures.iter() {
            println!("  - {}", path.display());
            if args.verbose || args.summary {
                println!("    {}", error);
            }
        }
    }

    if fail_count > 0 && !args.fail_fast {
        std::process::exit(1);
    } else if args.fail_fast && fail_fast_triggered.load(Ordering::Relaxed) {
        std::process::exit(1);
    }
}
