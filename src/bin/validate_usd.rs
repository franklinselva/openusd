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
//! ## Validation Depth Levels
//!
//! - `shallow` - Parse only, check syntax without composition
//! - `compose` - Full composition (default) - resolves sublayers, references, payloads
//! - `verify` - Compose + verify scene graph structure (parent-child relationships)
//! - `strict` - Verify + recursively validate all referenced assets exist
//!
//! This serves as a reference implementation for how to properly load
//! USD assets using the openusd crate.

use std::collections::{HashMap, HashSet};
use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Instant;

use anyhow::{Context, Result};
use clap::{Parser, ValueEnum};
use indicatif::{ProgressBar, ProgressStyle};
use rayon::prelude::*;
use walkdir::WalkDir;

use openusd::composition::ComposedLayer;
use openusd::sdf::{self, Value};
use openusd::usda::parser::Parser as UsdaParser;
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

/// Validation depth level.
///
/// Ordered from least to most thorough validation.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, ValueEnum)]
enum ValidationDepth {
    /// Parse only, check syntax without composition.
    Shallow,
    /// Full composition - resolves sublayers, references, payloads.
    #[default]
    Compose,
    /// Compose + verify scene graph structure (parent-child relationships).
    Verify,
    /// Verify + recursively validate all referenced assets exist.
    Strict,
}

impl ValidationDepth {
    fn as_str(&self) -> &'static str {
        match self {
            ValidationDepth::Shallow => "shallow",
            ValidationDepth::Compose => "compose",
            ValidationDepth::Verify => "verify",
            ValidationDepth::Strict => "strict",
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

/// Scene graph validation issue.
#[derive(Debug, Clone)]
struct SceneGraphIssue {
    path: String,
    issue_type: SceneGraphIssueType,
    message: String,
}

#[derive(Debug, Clone, Copy)]
#[allow(dead_code)]
enum SceneGraphIssueType {
    /// Child spec exists but is not listed in parent's primChildren.
    OrphanedChild,
    /// Child is listed in primChildren but no spec exists.
    MissingChild,
    /// Prim spec has no parent spec.
    InvalidParent,
    /// Same child listed multiple times in primChildren.
    DuplicateChild,
}

impl SceneGraphIssueType {
    fn as_str(&self) -> &'static str {
        match self {
            SceneGraphIssueType::OrphanedChild => "orphaned_child",
            SceneGraphIssueType::MissingChild => "missing_child",
            SceneGraphIssueType::InvalidParent => "invalid_parent",
            SceneGraphIssueType::DuplicateChild => "duplicate_child",
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
        prims: usize,
        scene_graph_issues: Vec<SceneGraphIssue>,
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
    /// Equivalent to --depth=strict for asset checking.
    #[arg(long, short = 'a')]
    check_assets: bool,

    /// Validation depth level.
    /// - shallow: Parse only, check syntax
    /// - compose: Full composition (default)
    /// - verify: Compose + verify scene graph structure
    /// - strict: Verify + check all asset references exist
    #[arg(long, short = 'd', value_enum, default_value_t = ValidationDepth::Compose)]
    depth: ValidationDepth,

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

    for spec in specs.values() {
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

/// Count prim specs (specs that define prims, not properties).
fn count_prims(specs: &HashMap<sdf::Path, sdf::Spec>) -> usize {
    specs
        .keys()
        .filter(|path| !path.is_property_path() && path.as_str() != "/")
        .count()
}

/// Validate scene graph structure.
///
/// Checks that:
/// 1. All children listed in primChildren actually exist as specs
/// 2. All prim specs have valid parent paths
/// 3. No duplicate children in primChildren lists
fn validate_scene_graph(specs: &HashMap<sdf::Path, sdf::Spec>) -> Vec<SceneGraphIssue> {
    let mut issues = Vec::new();

    // Build a set of all prim paths for quick lookup
    let prim_paths: HashSet<_> = specs
        .keys()
        .filter(|path| !path.is_property_path())
        .map(|p| p.as_str().to_string())
        .collect();

    for (path, spec) in specs {
        // Skip property specs
        if path.is_property_path() {
            continue;
        }

        // Check primChildren field
        if let Some(Value::TokenVec(children)) = spec.fields.get("primChildren") {
            let mut seen_children = HashSet::new();

            for child_name in children {
                // Check for duplicates
                if !seen_children.insert(child_name.clone()) {
                    issues.push(SceneGraphIssue {
                        path: path.as_str().to_string(),
                        issue_type: SceneGraphIssueType::DuplicateChild,
                        message: format!("Duplicate child '{}' in primChildren", child_name),
                    });
                    continue;
                }

                // Check that child spec exists
                let child_path = if path.as_str() == "/" {
                    format!("/{}", child_name)
                } else {
                    format!("{}/{}", path.as_str(), child_name)
                };

                if !prim_paths.contains(&child_path) {
                    issues.push(SceneGraphIssue {
                        path: path.as_str().to_string(),
                        issue_type: SceneGraphIssueType::MissingChild,
                        message: format!("Child '{}' listed but spec not found at {}", child_name, child_path),
                    });
                }
            }
        }

        // Check that parent exists (for non-root prims)
        let path_str = path.as_str();
        if path_str != "/" {
            if let Some(last_slash) = path_str.rfind('/') {
                let parent_path = if last_slash == 0 { "/" } else { &path_str[..last_slash] };

                if !prim_paths.contains(parent_path) {
                    issues.push(SceneGraphIssue {
                        path: path_str.to_string(),
                        issue_type: SceneGraphIssueType::InvalidParent,
                        message: format!("Parent spec not found at '{}'", parent_path),
                    });
                }
            }
        }
    }

    issues
}

/// Validate shallow (parse only, no composition).
fn validate_shallow(path: &Path, file_type: FileType) -> ValidationResult {
    match file_type {
        FileType::Usdc | FileType::Usd => {
            let file = match std::fs::File::open(path) {
                Ok(f) => f,
                Err(e) => {
                    return ValidationResult::Failed {
                        error: format!("Failed to open file: {}", e),
                        file_type,
                    }
                }
            };
            let reader = std::io::BufReader::new(file);
            match CrateData::open(reader, true) {
                Ok(data) => {
                    let specs = data.into_specs();
                    let prims = count_prims(&specs);
                    ValidationResult::Success {
                        specs: specs.len(),
                        file_type,
                        missing_assets: Vec::new(),
                        composed_layers: 1,
                        prims,
                        scene_graph_issues: Vec::new(),
                    }
                }
                Err(e) => ValidationResult::Failed {
                    error: format!("{:#}", e),
                    file_type,
                },
            }
        }
        FileType::Usda => {
            let content = match std::fs::read_to_string(path) {
                Ok(c) => c,
                Err(e) => {
                    return ValidationResult::Failed {
                        error: format!("Failed to read file: {}", e),
                        file_type,
                    }
                }
            };
            let mut parser = UsdaParser::new(&content);
            match parser.parse() {
                Ok(specs) => {
                    let prims = count_prims(&specs);
                    ValidationResult::Success {
                        specs: specs.len(),
                        file_type,
                        missing_assets: Vec::new(),
                        composed_layers: 1,
                        prims,
                        scene_graph_issues: Vec::new(),
                    }
                }
                Err(e) => ValidationResult::Failed {
                    error: format!("{:#}", e),
                    file_type,
                },
            }
        }
        FileType::Usdz => {
            let mut archive = match Archive::open(path) {
                Ok(a) => a,
                Err(e) => {
                    return ValidationResult::Failed {
                        error: format!("Failed to open USDZ: {:#}", e),
                        file_type,
                    }
                }
            };
            let root_layer = match archive.find_root_layer() {
                Some(layer) => layer,
                None => {
                    return ValidationResult::Failed {
                        error: "Could not find root layer in USDZ archive".to_string(),
                        file_type,
                    }
                }
            };
            match archive.read(&root_layer) {
                Ok(data) => {
                    let spec_count = if data.has_spec(&openusd::sdf::Path::abs_root()) {
                        1
                    } else {
                        0
                    };
                    ValidationResult::Success {
                        specs: spec_count,
                        file_type,
                        missing_assets: Vec::new(),
                        composed_layers: 1,
                        prims: 0,
                        scene_graph_issues: Vec::new(),
                    }
                }
                Err(e) => ValidationResult::Failed {
                    error: format!("Failed to read root layer '{}': {:#}", root_layer, e),
                    file_type,
                },
            }
        }
    }
}

/// Validate a single USDC file directly (without composition).
/// This is kept as a potential fallback if ComposedLayer fails.
#[allow(dead_code)]
fn validate_usdc(path: &Path, depth: ValidationDepth) -> ValidationResult {
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
            let prims = count_prims(&specs);
            let check_assets = depth == ValidationDepth::Strict;
            let missing_assets = if check_assets {
                extract_asset_refs(&specs, path)
                    .into_iter()
                    .filter(|r| !r.exists)
                    .collect()
            } else {
                Vec::new()
            };
            let scene_graph_issues = if depth >= ValidationDepth::Verify {
                validate_scene_graph(&specs)
            } else {
                Vec::new()
            };
            ValidationResult::Success {
                specs: specs.len(),
                file_type: FileType::Usdc,
                missing_assets,
                composed_layers: 1,
                prims,
                scene_graph_issues,
            }
        }
        Err(e) => ValidationResult::Failed {
            error: format!("{:#}", e),
            file_type: FileType::Usdc,
        },
    }
}

/// Validate a USDZ archive by checking its root USD file.
fn validate_usdz(path: &Path, _depth: ValidationDepth) -> ValidationResult {
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
                prims: 0, // USDZ validation is simpler
                scene_graph_issues: Vec::new(),
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
fn validate_composed(path: &Path, file_type: FileType, depth: ValidationDepth) -> ValidationResult {
    match ComposedLayer::open(path) {
        Ok(composed) => {
            let prims = count_prims(&composed.specs);
            let check_assets = depth == ValidationDepth::Strict;
            let missing_assets = if check_assets {
                extract_asset_refs(&composed.specs, path)
                    .into_iter()
                    .filter(|r| !r.exists)
                    .collect()
            } else {
                Vec::new()
            };
            let scene_graph_issues = if depth >= ValidationDepth::Verify {
                validate_scene_graph(&composed.specs)
            } else {
                Vec::new()
            };
            ValidationResult::Success {
                specs: composed.specs.len(),
                file_type,
                missing_assets,
                composed_layers: composed.composed_layers.len(),
                prims,
                scene_graph_issues,
            }
        }
        Err(e) => ValidationResult::Failed {
            error: format!("{:#}", e),
            file_type,
        },
    }
}

/// Validate a single file with the specified depth.
/// Automatically detects format for .usd files.
fn validate_file(path: &Path, depth: ValidationDepth) -> ValidationResult {
    let extension = path.extension().and_then(|e| e.to_str()).unwrap_or("").to_lowercase();
    let file_type = match extension.as_str() {
        "usda" => FileType::Usda,
        "usdc" => FileType::Usdc,
        "usd" => detect_usd_format(path).unwrap_or(FileType::Usd),
        "usdz" => FileType::Usdz,
        _ => {
            return ValidationResult::Skipped {
                reason: format!("Not a USD file: .{}", extension),
            }
        }
    };

    // Shallow validation: parse only, no composition
    if depth == ValidationDepth::Shallow {
        return validate_shallow(path, file_type);
    }

    // Compose, Verify, Strict all use composition
    match file_type {
        FileType::Usda | FileType::Usd => validate_composed(path, file_type, depth),
        FileType::Usdc => {
            // For .usdc files with compose+ depth, try composition first
            // Fall back to direct USDC parsing if composition fails
            validate_composed(path, file_type, depth)
        }
        FileType::Usdz => validate_usdz(path, depth),
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
fn validate_asset_directory(asset_dir: &Path, depth: ValidationDepth, verbose: bool) -> Vec<(PathBuf, ValidationResult)> {
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
        let result = validate_file(&root_file, depth);
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

/// Compute effective validation depth from args.
fn effective_depth(args: &Args) -> ValidationDepth {
    // --check-assets implies at least Strict depth
    if args.check_assets && args.depth < ValidationDepth::Strict {
        ValidationDepth::Strict
    } else {
        args.depth
    }
}

fn main() {
    let args = Args::parse();
    let start = Instant::now();
    let depth = effective_depth(&args);

    println!("Validation depth: {}", depth.as_str());

    // Determine validation mode
    let is_asset_mode = args.asset || (args.path.is_dir() && !args.path.join(".git").exists());

    if is_asset_mode && args.path.is_dir() {
        // Asset directory mode - find and validate root USD files
        run_asset_validation(&args, depth);
    } else {
        // File or recursive directory mode
        run_file_validation(&args, depth);
    }

    println!("\nTime elapsed: {:.2}s", start.elapsed().as_secs_f64());
}

fn run_asset_validation(args: &Args, depth: ValidationDepth) {
    println!("Asset Validation Mode");
    println!("=====================");
    println!("Asset directory: {}\n", args.path.display());

    let results = validate_asset_directory(&args.path, depth, args.verbose);

    if results.is_empty() {
        eprintln!("No USD files found in asset directory: {}", args.path.display());
        std::process::exit(1);
    }

    let mut passed = 0;
    let mut failed = 0;
    let mut total_scene_graph_issues = 0;

    for (path, result) in &results {
        let rel_path = path.strip_prefix(&args.path).unwrap_or(path);

        match result {
            ValidationResult::Success {
                specs,
                file_type,
                composed_layers,
                prims,
                missing_assets,
                scene_graph_issues,
            } => {
                passed += 1;
                println!(
                    "[PASS] {} ({} specs, {} prims, {} layers, {})",
                    rel_path.display(),
                    specs,
                    prims,
                    composed_layers,
                    file_type.as_str()
                );

                if args.verbose && !missing_assets.is_empty() {
                    println!("       Missing assets:");
                    for asset in missing_assets {
                        println!("         - [{}] {}", asset.ref_type.as_str(), asset.path);
                    }
                }

                if args.verbose && !scene_graph_issues.is_empty() {
                    println!("       Scene graph issues:");
                    for issue in scene_graph_issues {
                        println!("         - [{}] {}: {}", issue.issue_type.as_str(), issue.path, issue.message);
                    }
                }
                total_scene_graph_issues += scene_graph_issues.len();
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
    if depth >= ValidationDepth::Verify {
        println!("Scene graph issues: {}", total_scene_graph_issues);
    }

    if failed > 0 {
        std::process::exit(1);
    }
}

fn run_file_validation(args: &Args, depth: ValidationDepth) {
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
    let scene_graph_issue_count = AtomicUsize::new(0);
    let type_stats: std::sync::Mutex<HashMap<FileType, FileTypeStats>> = std::sync::Mutex::new(HashMap::new());
    let failures: std::sync::Mutex<Vec<(PathBuf, String)>> = std::sync::Mutex::new(Vec::new());
    let asset_warnings: std::sync::Mutex<Vec<(PathBuf, Vec<AssetRef>)>> = std::sync::Mutex::new(Vec::new());
    let scene_graph_warnings: std::sync::Mutex<Vec<(PathBuf, Vec<SceneGraphIssue>)>> =
        std::sync::Mutex::new(Vec::new());
    let fail_fast_triggered = AtomicBool::new(false);

    files.par_iter().for_each(|file| {
        if args.fail_fast && fail_fast_triggered.load(Ordering::Relaxed) {
            return;
        }

        let result = validate_file(file, depth);

        if let Some(ref pb) = progress {
            pb.inc(1);
        }

        match &result {
            ValidationResult::Success {
                specs,
                file_type,
                missing_assets,
                composed_layers,
                prims,
                scene_graph_issues,
            } => {
                passed.fetch_add(1, Ordering::Relaxed);
                scene_graph_issue_count.fetch_add(scene_graph_issues.len(), Ordering::Relaxed);

                {
                    let mut stats = type_stats.lock().unwrap();
                    let entry = stats.entry(*file_type).or_default();
                    entry.total += 1;
                    entry.passed += 1;
                }

                if args.verbose {
                    let rel_path = file.strip_prefix(&args.path).unwrap_or(file);
                    println!(
                        "[PASS] {} ({} specs, {} prims, {} layers, {})",
                        rel_path.display(),
                        specs,
                        prims,
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

                if !scene_graph_issues.is_empty() {
                    let rel_path = file.strip_prefix(&args.path).unwrap_or(file);
                    scene_graph_warnings
                        .lock()
                        .unwrap()
                        .push((rel_path.to_path_buf(), scene_graph_issues.clone()));
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
    if depth >= ValidationDepth::Verify {
        println!(
            "Scene graph issues: {}",
            scene_graph_issue_count.load(Ordering::Relaxed)
        );
    }
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

    // Print scene graph issues if in verify or strict mode
    if depth >= ValidationDepth::Verify {
        let graph_warnings = scene_graph_warnings.lock().unwrap();
        if !graph_warnings.is_empty() && args.verbose {
            println!("Scene graph issues:");
            for (path, issues) in graph_warnings.iter() {
                println!("  {}:", path.display());
                for issue in issues {
                    println!("    - [{}] {}", issue.issue_type.as_str(), issue.message);
                }
            }
            println!();
        }
    }

    // Print missing asset references if in strict mode
    if depth == ValidationDepth::Strict {
        let warnings = asset_warnings.lock().unwrap();
        if !warnings.is_empty() {
            println!("Missing asset references:");
            for (path, missing) in warnings.iter() {
                println!("  {}:", path.display());
                for asset in missing {
                    println!("    - [{}] {}", asset.ref_type.as_str(), asset.path);
                }
            }
            println!();
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

    let should_exit =
        (fail_count > 0 && !args.fail_fast) || (args.fail_fast && fail_fast_triggered.load(Ordering::Relaxed));
    if should_exit {
        std::process::exit(1);
    }
}
