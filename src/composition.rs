//! USD Composition implementation.
//!
//! This module handles USD's composition arcs, starting with sublayers.
//! Composition follows the LIVRPS strength ordering:
//! - **L**ocal (strongest)
//! - **I**nherits
//! - **V**ariantSets
//! - **R**eferences
//! - **P**ayloads
//! - **S**pecializes (weakest)
//!
//! Sublayers are composed at the layer level before LIVRPS is applied.

use anyhow::{Context, Result};
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

use crate::sdf::{schema::FieldKey, Path as SdfPath, Spec, SpecType, Value};
use crate::usda::TextReader;
use crate::usdc::CrateData;

/// A composed layer that includes content from all sublayers.
#[derive(Debug)]
pub struct ComposedLayer {
    /// The composed specs from this layer and all sublayers.
    pub specs: HashMap<SdfPath, Spec>,
    /// The source file path of the root layer.
    pub source_path: PathBuf,
    /// Paths of all layers that were composed (for debugging).
    pub composed_layers: Vec<PathBuf>,
}

impl ComposedLayer {
    /// Load and compose a USD layer from a file path.
    ///
    /// This will:
    /// 1. Parse the root layer
    /// 2. Recursively load and compose any sublayers
    /// 3. Merge specs according to USD composition rules
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let mut visited = HashSet::new();
        let canonical = path
            .canonicalize()
            .with_context(|| format!("Failed to canonicalize path: {}", path.display()))?;

        Self::open_recursive(&canonical, &mut visited)
    }

    /// Internal recursive layer loading.
    fn open_recursive(path: &Path, visited: &mut HashSet<PathBuf>) -> Result<Self> {
        // Check for circular references
        if visited.contains(path) {
            // Circular reference detected, return empty layer
            return Ok(ComposedLayer {
                specs: HashMap::new(),
                source_path: path.to_path_buf(),
                composed_layers: vec![],
            });
        }
        visited.insert(path.to_path_buf());

        // Parse the layer based on file extension
        let specs = Self::parse_layer(path)?;

        // Get the base directory for resolving relative sublayer paths
        let base_dir = path.parent().unwrap_or_else(|| Path::new("."));

        // Check for sublayers in the pseudo-root
        let sublayer_paths = Self::extract_sublayer_paths(&specs);

        if sublayer_paths.is_empty() {
            return Ok(ComposedLayer {
                specs,
                source_path: path.to_path_buf(),
                composed_layers: vec![path.to_path_buf()],
            });
        }

        // Load and compose sublayers (in reverse order - last sublayer is weakest)
        // We start with the weakest and let stronger layers override
        let mut composed_specs = HashMap::new();
        let mut all_composed_layers = Vec::new();

        // Process sublayers from weakest to strongest (reverse order)
        for sublayer_path in sublayer_paths.iter().rev() {
            let resolved_path = match Self::resolve_sublayer_path(sublayer_path, base_dir) {
                Ok(p) => p,
                Err(_) => continue, // Skip unresolvable paths
            };

            match Self::open_recursive(&resolved_path, visited) {
                Ok(sublayer) => {
                    // Merge sublayer specs into composed specs
                    // Later (stronger) layers will override
                    for (path, spec) in sublayer.specs {
                        composed_specs.insert(path, spec);
                    }

                    all_composed_layers.extend(sublayer.composed_layers);
                }
                Err(_) => {
                    // Skip failed sublayers
                }
            }
        }

        // Finally, apply the root layer (strongest) on top
        for (path, spec) in specs {
            Self::merge_spec(&mut composed_specs, path, spec);
        }

        all_composed_layers.push(path.to_path_buf());

        Ok(ComposedLayer {
            specs: composed_specs,
            source_path: path.to_path_buf(),
            composed_layers: all_composed_layers,
        })
    }

    /// Parse a layer file based on its extension.
    fn parse_layer(path: &Path) -> Result<HashMap<SdfPath, Spec>> {
        let extension = path.extension().and_then(|e| e.to_str()).unwrap_or("").to_lowercase();

        match extension.as_str() {
            "usda" | "usd" => {
                // Try text format first for .usd files
                let reader =
                    TextReader::read(path).with_context(|| format!("Failed to read USDA: {}", path.display()))?;
                Ok(reader.into_specs())
            }
            "usdc" => {
                let file =
                    std::fs::File::open(path).with_context(|| format!("Failed to open file: {}", path.display()))?;
                let reader = std::io::BufReader::new(file);
                let crate_data = CrateData::open(reader, false)
                    .with_context(|| format!("Failed to read USDC: {}", path.display()))?;
                Ok(crate_data.into_specs())
            }
            _ => {
                // Try to detect format from file contents
                let mut file = std::fs::File::open(path)?;
                let mut header = [0u8; 8];
                use std::io::Read;
                file.read_exact(&mut header)?;

                // Check for USDC magic number "PXR-USDC"
                if &header == b"PXR-USDC" {
                    drop(file);
                    let file = std::fs::File::open(path)?;
                    let reader = std::io::BufReader::new(file);
                    let crate_data = CrateData::open(reader, false)?;
                    Ok(crate_data.into_specs())
                } else {
                    // Assume text format
                    let reader = TextReader::read(path)?;
                    Ok(reader.into_specs())
                }
            }
        }
    }

    /// Extract sublayer paths from the pseudo-root spec.
    fn extract_sublayer_paths(specs: &HashMap<SdfPath, Spec>) -> Vec<String> {
        let root_path = SdfPath::abs_root();

        let Some(root_spec) = specs.get(&root_path) else {
            return vec![];
        };

        let Some(sublayers_value) = root_spec.fields.get(FieldKey::SubLayers.as_str()) else {
            return vec![];
        };

        match sublayers_value {
            Value::StringVec(paths) => paths.clone(),
            _ => vec![],
        }
    }

    /// Resolve a sublayer path relative to the base directory.
    fn resolve_sublayer_path(sublayer_path: &str, base_dir: &Path) -> Result<PathBuf> {
        // Clean up the path (remove @@ markers if present from asset paths)
        let clean_path = sublayer_path.trim_matches('@').trim();

        let resolved = if clean_path.starts_with('/') || clean_path.starts_with('\\') {
            // Absolute path
            PathBuf::from(clean_path)
        } else {
            // Relative path
            base_dir.join(clean_path)
        };

        resolved
            .canonicalize()
            .with_context(|| format!("Failed to resolve sublayer path: {}", clean_path))
    }

    /// Merge a spec into the composed specs map.
    ///
    /// For prims, this merges fields (stronger layer wins for conflicts).
    /// For other spec types, the stronger layer completely replaces.
    fn merge_spec(composed: &mut HashMap<SdfPath, Spec>, path: SdfPath, stronger_spec: Spec) {
        match composed.get_mut(&path) {
            Some(existing_spec) => {
                // Merge based on spec type
                match stronger_spec.ty {
                    SpecType::Prim | SpecType::PseudoRoot => {
                        // For prims, merge fields (stronger wins on conflict)
                        for (key, value) in stronger_spec.fields {
                            existing_spec.fields.insert(key, value);
                        }
                    }
                    _ => {
                        // For other types (attributes, relationships), stronger replaces
                        *existing_spec = stronger_spec;
                    }
                }
            }
            None => {
                // No existing spec, just insert
                composed.insert(path, stronger_spec);
            }
        }
    }

    /// Get a reference to a spec by path.
    pub fn get(&self, path: &SdfPath) -> Option<&Spec> {
        self.specs.get(path)
    }

    /// Check if a spec exists at the given path.
    pub fn has_spec(&self, path: &SdfPath) -> bool {
        self.specs.contains_key(path)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sdf;
    use std::fs;
    use std::sync::atomic::{AtomicU64, Ordering};

    static TEST_COUNTER: AtomicU64 = AtomicU64::new(0);

    fn create_temp_dir() -> PathBuf {
        let counter = TEST_COUNTER.fetch_add(1, Ordering::SeqCst);
        let dir = std::env::temp_dir().join(format!(
            "openusd_test_{}_{}_{}",
            std::process::id(),
            counter,
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let _ = fs::remove_dir_all(&dir); // Clean up any stale directory
        fs::create_dir_all(&dir).expect("Failed to create temp dir");
        dir
    }

    fn cleanup_temp_dir(dir: &Path) {
        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn test_compose_single_layer() {
        let temp_dir = create_temp_dir();
        let root_path = temp_dir.join("root.usda");

        fs::write(
            &root_path,
            r#"#usda 1.0
(
    defaultPrim = "World"
)

def Xform "World" {
    double3 xformOp:translate = (1, 2, 3)
}
"#,
        )
        .unwrap();

        let composed = ComposedLayer::open(&root_path).unwrap();

        assert_eq!(composed.composed_layers.len(), 1);
        assert!(composed.specs.contains_key(&SdfPath::abs_root()));
        assert!(composed.specs.contains_key(&sdf::path("/World").unwrap()));

        cleanup_temp_dir(&temp_dir);
    }

    #[test]
    fn test_compose_with_sublayer() {
        let temp_dir = create_temp_dir();

        // Create sublayer first
        let sub_path = temp_dir.join("sublayer.usda");
        fs::write(
            &sub_path,
            r#"#usda 1.0

def Xform "FromSublayer" {
    double3 xformOp:translate = (10, 20, 30)
}
"#,
        )
        .unwrap();

        // Create root layer that references sublayer
        let root_path = temp_dir.join("root.usda");
        fs::write(
            &root_path,
            r#"#usda 1.0
(
    subLayers = [
        @./sublayer.usda@
    ]
    defaultPrim = "World"
)

def Xform "World" {
    double3 xformOp:translate = (1, 2, 3)
}
"#,
        )
        .unwrap();

        let composed = ComposedLayer::open(&root_path).unwrap();

        // Should have content from both layers
        assert_eq!(composed.composed_layers.len(), 2);

        // Root layer prim should exist
        assert!(composed.specs.contains_key(&sdf::path("/World").unwrap()));

        // Sublayer prim should exist
        assert!(composed.specs.contains_key(&sdf::path("/FromSublayer").unwrap()));

        cleanup_temp_dir(&temp_dir);
    }

    #[test]
    fn test_stronger_layer_wins() {
        let temp_dir = create_temp_dir();

        // Create sublayer with initial value
        let sub_path = temp_dir.join("sublayer.usda");
        fs::write(
            &sub_path,
            r#"#usda 1.0

def Xform "Shared" {
    double3 xformOp:translate = (10, 20, 30)
    string comment = "from sublayer"
}
"#,
        )
        .unwrap();

        // Create root layer that overrides the value
        let root_path = temp_dir.join("root.usda");
        fs::write(
            &root_path,
            r#"#usda 1.0
(
    subLayers = [
        @./sublayer.usda@
    ]
)

def Xform "Shared" {
    double3 xformOp:translate = (1, 2, 3)
}
"#,
        )
        .unwrap();

        let composed = ComposedLayer::open(&root_path).unwrap();

        // The prim should exist
        assert!(composed.specs.contains_key(&sdf::path("/Shared").unwrap()));

        // The translate attribute should exist
        let translate_spec = composed
            .specs
            .get(&sdf::path("/Shared.xformOp:translate").unwrap())
            .expect("translate attribute should exist");

        // Root layer value should win
        let default_value = translate_spec.fields.get("default").expect("default field");
        match default_value {
            Value::Vec3d(values) => {
                assert_eq!(values, &[1.0, 2.0, 3.0], "Root layer value should win");
            }
            _ => panic!("Expected Vec3d, got {:?}", default_value),
        }

        cleanup_temp_dir(&temp_dir);
    }

    #[test]
    fn test_circular_reference_protection() {
        let temp_dir = create_temp_dir();

        // Create two layers that reference each other
        let layer_a = temp_dir.join("layer_a.usda");
        let layer_b = temp_dir.join("layer_b.usda");

        fs::write(
            &layer_a,
            r#"#usda 1.0
(
    subLayers = [
        @./layer_b.usda@
    ]
)

def Xform "FromA" {}
"#,
        )
        .unwrap();

        fs::write(
            &layer_b,
            r#"#usda 1.0
(
    subLayers = [
        @./layer_a.usda@
    ]
)

def Xform "FromB" {}
"#,
        )
        .unwrap();

        // Should not panic or infinite loop
        let result = ComposedLayer::open(&layer_a);
        assert!(result.is_ok(), "Should handle circular references gracefully");

        let composed = result.unwrap();
        // Should have content from both layers (one iteration each)
        assert!(composed.specs.contains_key(&sdf::path("/FromA").unwrap()));
        assert!(composed.specs.contains_key(&sdf::path("/FromB").unwrap()));

        cleanup_temp_dir(&temp_dir);
    }

    #[test]
    fn test_multiple_sublayers() {
        let temp_dir = create_temp_dir();

        // Create first sublayer
        let sub1_path = temp_dir.join("sub1.usda");
        fs::write(
            &sub1_path,
            r#"#usda 1.0

def Xform "FromSub1" {
    string source = "sub1"
}
"#,
        )
        .unwrap();

        // Create second sublayer
        let sub2_path = temp_dir.join("sub2.usda");
        fs::write(
            &sub2_path,
            r#"#usda 1.0

def Xform "FromSub2" {
    string source = "sub2"
}
"#,
        )
        .unwrap();

        // Create root layer with both sublayers
        let root_path = temp_dir.join("root.usda");
        fs::write(
            &root_path,
            r#"#usda 1.0
(
    subLayers = [
        @./sub1.usda@,
        @./sub2.usda@
    ]
)

def Xform "Root" {}
"#,
        )
        .unwrap();

        let composed = ComposedLayer::open(&root_path).unwrap();

        // Should have content from all three layers
        assert_eq!(composed.composed_layers.len(), 3);
        assert!(composed.specs.contains_key(&sdf::path("/Root").unwrap()));
        assert!(composed.specs.contains_key(&sdf::path("/FromSub1").unwrap()));
        assert!(composed.specs.contains_key(&sdf::path("/FromSub2").unwrap()));

        cleanup_temp_dir(&temp_dir);
    }

    #[test]
    fn test_nested_sublayers() {
        let temp_dir = create_temp_dir();

        // Create deepest layer
        let deep_path = temp_dir.join("deep.usda");
        fs::write(
            &deep_path,
            r#"#usda 1.0

def Xform "Deep" {}
"#,
        )
        .unwrap();

        // Create middle layer that includes deep
        let middle_path = temp_dir.join("middle.usda");
        fs::write(
            &middle_path,
            r#"#usda 1.0
(
    subLayers = [
        @./deep.usda@
    ]
)

def Xform "Middle" {}
"#,
        )
        .unwrap();

        // Create root layer that includes middle
        let root_path = temp_dir.join("root.usda");
        fs::write(
            &root_path,
            r#"#usda 1.0
(
    subLayers = [
        @./middle.usda@
    ]
)

def Xform "Root" {}
"#,
        )
        .unwrap();

        let composed = ComposedLayer::open(&root_path).unwrap();

        // Should have content from all three layers
        assert_eq!(composed.composed_layers.len(), 3);
        assert!(composed.specs.contains_key(&sdf::path("/Root").unwrap()));
        assert!(composed.specs.contains_key(&sdf::path("/Middle").unwrap()));
        assert!(composed.specs.contains_key(&sdf::path("/Deep").unwrap()));

        cleanup_temp_dir(&temp_dir);
    }
}
