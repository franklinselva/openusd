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
#[derive(Debug, Clone)]
pub struct ComposedLayer {
    /// The composed specs from this layer and all sublayers.
    pub specs: HashMap<SdfPath, Spec>,
    /// The source file path of the root layer.
    pub source_path: PathBuf,
    /// Paths of all layers that were composed (for debugging).
    pub composed_layers: Vec<PathBuf>,
}

/// Context for composition tracking, including cycle detection and caching.
struct CompositionContext {
    /// Paths currently being processed (for cycle detection)
    processing: HashSet<PathBuf>,
    /// Cache of already composed layers
    cache: HashMap<PathBuf, ComposedLayer>,
}

impl CompositionContext {
    fn new() -> Self {
        Self {
            processing: HashSet::new(),
            cache: HashMap::new(),
        }
    }
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
        let mut ctx = CompositionContext::new();
        let canonical = path
            .canonicalize()
            .with_context(|| format!("Failed to canonicalize path: {}", path.display()))?;

        Self::open_recursive(&canonical, &mut ctx)
    }

    /// Internal recursive layer loading.
    fn open_recursive(path: &Path, ctx: &mut CompositionContext) -> Result<Self> {
        // Check if we already have this layer cached
        if let Some(cached) = ctx.cache.get(path) {
            return Ok(cached.clone());
        }

        // Check for circular references (path is currently being processed)
        if ctx.processing.contains(path) {
            // Circular reference detected, return empty layer
            return Ok(ComposedLayer {
                specs: HashMap::new(),
                source_path: path.to_path_buf(),
                composed_layers: vec![],
            });
        }

        // Mark this path as being processed
        ctx.processing.insert(path.to_path_buf());

        // Parse the layer based on file extension
        let specs = Self::parse_layer(path)?;

        // Get the base directory for resolving relative sublayer paths
        let base_dir = path.parent().unwrap_or_else(|| Path::new("."));

        // Check for sublayers in the pseudo-root
        let sublayer_paths = Self::extract_sublayer_paths(&specs);
        let sublayer_offsets = Self::extract_sublayer_offsets(&specs);

        if sublayer_paths.is_empty() {
            // Apply LIVRPS composition arcs even without sublayers
            let mut specs = specs;
            // I - Inherits
            Self::compose_inherits(&mut specs);
            // V - Variants
            Self::compose_variants(&mut specs);
            // R - References
            Self::compose_references(&mut specs, base_dir, ctx);
            // P - Payloads
            Self::compose_payloads(&mut specs, base_dir, ctx);
            // S - Specializes
            Self::compose_specializes(&mut specs);

            let result = ComposedLayer {
                specs,
                source_path: path.to_path_buf(),
                composed_layers: vec![path.to_path_buf()],
            };

            // Cache the result and remove from processing
            ctx.processing.remove(path);
            ctx.cache.insert(path.to_path_buf(), result.clone());
            return Ok(result);
        }

        // Load and compose sublayers (in reverse order - last sublayer is weakest)
        // We start with the weakest and let stronger layers override
        let mut composed_specs = HashMap::new();
        let mut all_composed_layers = Vec::new();

        // Process sublayers from weakest to strongest (reverse order)
        // Pair each sublayer path with its corresponding offset
        let sublayer_with_offsets: Vec<_> = sublayer_paths
            .iter()
            .enumerate()
            .map(|(i, path)| {
                let offset = sublayer_offsets.get(i).cloned().unwrap_or_default();
                (path, offset)
            })
            .collect();

        for (sublayer_path, layer_offset) in sublayer_with_offsets.iter().rev() {
            let resolved_path = match Self::resolve_sublayer_path(sublayer_path, base_dir) {
                Ok(p) => p,
                Err(_) => continue,
            };

            match Self::open_recursive(&resolved_path, ctx) {
                Ok(sublayer) => {
                    // Merge sublayer specs into composed specs
                    // Later (stronger) layers will override fields, but weaker layer fields are preserved
                    for (path, mut spec) in sublayer.specs {
                        // Apply layer offset to time samples in this spec
                        Self::apply_layer_offset_to_spec(&mut spec, &layer_offset);
                        // Use merge_spec to preserve fields from weaker layers
                        Self::merge_spec(&mut composed_specs, path, spec);
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

        // Apply LIVRPS composition arcs
        // I - Inherits (after Local opinions from sublayers)
        Self::compose_inherits(&mut composed_specs);
        // V - Variants (selected variant content)
        Self::compose_variants(&mut composed_specs);
        // R - References
        Self::compose_references(&mut composed_specs, base_dir, ctx);
        // P - Payloads
        Self::compose_payloads(&mut composed_specs, base_dir, ctx);
        // S - Specializes (weakest arc)
        Self::compose_specializes(&mut composed_specs);

        let result = ComposedLayer {
            specs: composed_specs,
            source_path: path.to_path_buf(),
            composed_layers: all_composed_layers,
        };

        // Cache the result and remove from processing
        ctx.processing.remove(path);
        ctx.cache.insert(path.to_path_buf(), result.clone());
        Ok(result)
    }

    /// Parse a layer file based on its extension and content.
    fn parse_layer(path: &Path) -> Result<HashMap<SdfPath, Spec>> {
        let extension = path.extension().and_then(|e| e.to_str()).unwrap_or("").to_lowercase();

        match extension.as_str() {
            "usda" => {
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
            "usd" => {
                // For .usd files, detect format from file contents
                Self::parse_layer_auto_detect(path)
            }
            _ => {
                // For unknown extensions, try auto-detection
                Self::parse_layer_auto_detect(path)
            }
        }
    }

    /// Auto-detect USD format by checking magic bytes and parse accordingly.
    fn parse_layer_auto_detect(path: &Path) -> Result<HashMap<SdfPath, Spec>> {
        use std::io::Read;

        let mut file = std::fs::File::open(path).with_context(|| format!("Failed to open file: {}", path.display()))?;
        let mut header = [0u8; 8];
        file.read_exact(&mut header)
            .with_context(|| format!("Failed to read file header: {}", path.display()))?;
        drop(file);

        // Check for USDC magic number "PXR-USDC"
        if &header == b"PXR-USDC" {
            let file = std::fs::File::open(path)?;
            let reader = std::io::BufReader::new(file);
            let crate_data =
                CrateData::open(reader, false).with_context(|| format!("Failed to read USDC: {}", path.display()))?;
            Ok(crate_data.into_specs())
        } else {
            // Assume text format (USDA)
            let reader = TextReader::read(path).with_context(|| format!("Failed to read USDA: {}", path.display()))?;
            Ok(reader.into_specs())
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

    /// Extract sublayer offsets from the pseudo-root spec.
    fn extract_sublayer_offsets(specs: &HashMap<SdfPath, Spec>) -> Vec<crate::sdf::LayerOffset> {
        let root_path = SdfPath::abs_root();

        let Some(root_spec) = specs.get(&root_path) else {
            return vec![];
        };

        let Some(offsets_value) = root_spec.fields.get(FieldKey::SubLayerOffsets.as_str()) else {
            return vec![];
        };

        match offsets_value {
            Value::LayerOffsetVec(offsets) => offsets.clone(),
            _ => vec![],
        }
    }

    /// Apply a layer offset to all time samples in a spec.
    fn apply_layer_offset_to_spec(spec: &mut Spec, layer_offset: &crate::sdf::LayerOffset) {
        if layer_offset.is_identity() {
            return;
        }

        for value in spec.fields.values_mut() {
            if let Value::TimeSamples(samples) = value {
                *samples = samples
                    .iter()
                    .map(|(time, val)| (layer_offset.apply(*time), val.clone()))
                    .collect();
            }
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

    /// Extract inherit paths from a prim's spec.
    ///
    /// Returns paths from the PathListOp, combining explicit, prepended, appended, and added items.
    fn extract_inherit_paths(spec: &Spec) -> Vec<SdfPath> {
        let Some(value) = spec.fields.get(FieldKey::InheritPaths.as_str()) else {
            return vec![];
        };

        let Some(list_op) = value.clone().try_as_path_list_op() else {
            return vec![];
        };

        // Combine all items from the list op
        // Order: explicit items take precedence, then prepended, then added, then appended
        let mut paths = Vec::new();

        if list_op.explicit {
            paths.extend(list_op.explicit_items);
        } else {
            paths.extend(list_op.prepended_items);
            paths.extend(list_op.added_items);
            paths.extend(list_op.appended_items);
        }

        // Filter out deleted items
        paths.retain(|p| !list_op.deleted_items.contains(p));

        paths
    }

    /// Compose inherits arcs for all prims in the specs.
    ///
    /// Inherits are weaker than local opinions but stronger than references.
    /// For each prim with an `inheritPaths` field, copy fields from the
    /// inherited class prims that don't already exist on the inheriting prim.
    fn compose_inherits(specs: &mut HashMap<SdfPath, Spec>) {
        // Collect all prims that have inherits (to avoid borrowing issues)
        let prims_with_inherits: Vec<(SdfPath, Vec<SdfPath>)> = specs
            .iter()
            .filter(|(_, spec)| spec.ty == SpecType::Prim)
            .filter_map(|(path, spec)| {
                let inherit_paths = Self::extract_inherit_paths(spec);
                if inherit_paths.is_empty() {
                    None
                } else {
                    Some((path.clone(), inherit_paths))
                }
            })
            .collect();

        // Process each prim with inherits
        for (prim_path, inherit_paths) in prims_with_inherits {
            // Collect fields from inherited classes (later inherits are weaker)
            let mut inherited_fields: HashMap<String, Value> = HashMap::new();

            // Process inherits in order (first in list is strongest)
            for class_path in inherit_paths.iter() {
                if let Some(class_spec) = specs.get(class_path) {
                    // Copy fields from class to inherited_fields
                    // Earlier (stronger) inherits will override later ones
                    for (key, value) in &class_spec.fields {
                        // Skip composition-specific fields
                        if key == FieldKey::InheritPaths.as_str()
                            || key == FieldKey::Specifier.as_str()
                            || key == FieldKey::TypeName.as_str()
                        {
                            continue;
                        }
                        inherited_fields.insert(key.clone(), value.clone());
                    }
                }
            }

            // Apply inherited fields to the prim (local opinions win)
            if let Some(prim_spec) = specs.get_mut(&prim_path) {
                for (key, value) in inherited_fields {
                    // Only insert if prim doesn't already have this field (local wins)
                    prim_spec.fields.entry(key).or_insert(value);
                }
            }

            // Also inherit child properties (attributes/relationships) from class prims
            Self::compose_inherited_properties(specs, &prim_path, &inherit_paths);
        }
    }

    /// Compose inherited properties (attributes and relationships) from class prims.
    ///
    /// This handles properties defined under class prims that should be inherited
    /// by prims that inherit from those classes.
    fn compose_inherited_properties(
        specs: &mut HashMap<SdfPath, Spec>,
        prim_path: &SdfPath,
        inherit_paths: &[SdfPath],
    ) {
        // Collect all property paths from inherited classes
        let mut inherited_properties: Vec<(SdfPath, Spec)> = Vec::new();

        // Process inherits in order (first in list is strongest)
        for class_path in inherit_paths.iter() {
            // Find all properties under this class
            let class_prefix = class_path.as_str().to_string() + ".";
            for (path, spec) in specs.iter() {
                if path.as_str().starts_with(&class_prefix) {
                    // This is a property of the class
                    let prop_name = &path.as_str()[class_prefix.len()..];
                    // Create the corresponding path under the inheriting prim
                    if let Ok(new_path) = prim_path.append_property(prop_name) {
                        inherited_properties.push((new_path, spec.clone()));
                    }
                }
            }
        }

        // Apply inherited properties (local wins)
        for (prop_path, prop_spec) in inherited_properties {
            specs.entry(prop_path).or_insert(prop_spec);
        }
    }

    /// Compose references arcs for all prims in the specs.
    ///
    /// References are stronger than payloads but weaker than variants.
    /// For each prim with a `references` field, load the referenced layer,
    /// find the target prim, and merge its content under the referencing prim.
    fn compose_references(specs: &mut HashMap<SdfPath, Spec>, base_dir: &Path, ctx: &mut CompositionContext) {
        // Collect all prims and variants that have references (to avoid borrowing issues)
        // Both Prim and Variant specs can have references
        let prims_with_refs: Vec<(SdfPath, Vec<crate::sdf::Reference>)> = specs
            .iter()
            .filter(|(_, spec)| spec.ty == SpecType::Prim || spec.ty == SpecType::Variant)
            .filter_map(|(path, spec)| {
                let refs = Self::extract_references(spec);
                if refs.is_empty() {
                    None
                } else {
                    Some((path.clone(), refs))
                }
            })
            .collect();

        // Process each prim with references
        for (prim_path, references) in prims_with_refs {
            // Process references in order (first in list is strongest)
            for reference in references.iter() {
                // Skip internal references (empty asset path means same layer)
                if reference.asset_path.is_empty() {
                    Self::compose_internal_reference(specs, &prim_path, &reference.prim_path);
                    continue;
                }

                // Resolve the external layer path
                let resolved_path = match Self::resolve_sublayer_path(&reference.asset_path, base_dir) {
                    Ok(p) => p,
                    Err(_) => continue,
                };

                // Check if this is a MaterialX file
                if resolved_path.extension().map_or(false, |ext| ext == "mtlx") {
                    Self::compose_materialx_reference(specs, &resolved_path, &prim_path, &reference.prim_path);
                    continue;
                }

                // Load the referenced layer
                let ref_layer = match Self::open_recursive(&resolved_path, ctx) {
                    Ok(layer) => layer,
                    Err(_) => continue,
                };

                // Determine the target prim path in the referenced layer
                let target_path = if reference.prim_path.as_str().is_empty() {
                    // Use default prim if no explicit prim path
                    Self::get_default_prim_path(&ref_layer.specs)
                } else {
                    Some(reference.prim_path.clone())
                };

                let Some(target_path) = target_path else {
                    continue;
                };

                // Remap and merge specs from the referenced layer
                Self::compose_referenced_content(
                    specs,
                    &prim_path,
                    &ref_layer.specs,
                    &target_path,
                    &reference.layer_offset,
                );
            }
        }
    }

    /// Compose a MaterialX reference into USD-style specs.
    ///
    /// MaterialX files (.mtlx) contain material definitions in XML format.
    /// This function parses them and creates corresponding USD Material, Shader,
    /// and texture specs.
    fn compose_materialx_reference(
        specs: &mut HashMap<SdfPath, Spec>,
        mtlx_path: &Path,
        parent_path: &SdfPath,
        _target_path: &SdfPath,
    ) {
        // Parse the MaterialX file
        let mtlx_doc = match crate::mtlx::parse_mtlx_file(mtlx_path) {
            Ok(doc) => doc,
            Err(e) => {
                log::warn!("Failed to parse MaterialX file {}: {}", mtlx_path.display(), e);
                return;
            }
        };

        let mtlx_base_dir = mtlx_path.parent().unwrap_or(Path::new("."));

        log::debug!(
            "Composing MaterialX: {} materials from {}",
            mtlx_doc.materials.len(),
            mtlx_path.display()
        );

        // Create specs for each material
        for (mat_name, material) in &mtlx_doc.materials {
            // Create Material prim
            let mat_path: SdfPath = match parent_path.append_path(mat_name.as_str()) {
                Ok(p) => p,
                Err(_) => continue,
            };

            let mut mat_spec = Spec::new(SpecType::Prim);
            mat_spec.fields.insert(
                FieldKey::TypeName.as_str().to_string(),
                Value::Token("Material".into()),
            );
            mat_spec.fields.insert(
                FieldKey::Specifier.as_str().to_string(),
                Value::Token("def".into()),
            );

            // Add primChildren for the shader
            let shader_name = format!("{}_surface", mat_name);
            mat_spec.fields.insert(
                "primChildren".to_string(),
                Value::TokenVec(vec![shader_name.clone()]),
            );

            specs.insert(mat_path.clone(), mat_spec);

            // Find the shader this material uses
            let shader = match mtlx_doc.get_shader(&material.shader_name) {
                Some(s) => s,
                None => {
                    log::warn!("MaterialX material {} references unknown shader {}", mat_name, material.shader_name);
                    continue;
                }
            };

            // Create Shader prim (as UsdPreviewSurface equivalent)
            let shader_path: SdfPath = match mat_path.append_path(shader_name.as_str()) {
                Ok(p) => p,
                Err(_) => continue,
            };

            let mut shader_spec = Spec::new(SpecType::Prim);
            shader_spec.fields.insert(
                FieldKey::TypeName.as_str().to_string(),
                Value::Token("Shader".into()),
            );
            shader_spec.fields.insert(
                FieldKey::Specifier.as_str().to_string(),
                Value::Token("def".into()),
            );

            // Track texture prims to create
            let mut texture_children: Vec<String> = Vec::new();

            specs.insert(shader_path.clone(), shader_spec);

            // Create info:id attribute (UsdPreviewSurface)
            let info_id_path = match shader_path.append_property("info:id") {
                Ok(p) => p,
                Err(_) => continue,
            };
            let mut info_id_spec = Spec::new(SpecType::Attribute);
            info_id_spec.fields.insert(
                FieldKey::Default.as_str().to_string(),
                Value::Token("UsdPreviewSurface".into()),
            );
            specs.insert(info_id_path, info_id_spec);

            // Convert standard_surface inputs to UsdPreviewSurface inputs
            Self::add_materialx_shader_inputs(
                specs,
                &shader_path,
                &mat_path,
                shader,
                &mtlx_doc,
                mtlx_base_dir,
                &mut texture_children,
            );

            // Update shader's primChildren if we created textures
            if !texture_children.is_empty() {
                if let Some(shader_spec) = specs.get_mut(&shader_path) {
                    shader_spec.fields.insert(
                        "primChildren".to_string(),
                        Value::TokenVec(texture_children),
                    );
                }
            }

            // Create outputs:surface for material binding
            let surface_output_path = match mat_path.append_property("outputs:surface") {
                Ok(p) => p,
                Err(_) => continue,
            };
            let mut surface_output_spec = Spec::new(SpecType::Attribute);
            // Connection to the shader's surface output
            let connection_path = format!("{}/outputs:surface", shader_path.as_str());
            surface_output_spec.fields.insert(
                FieldKey::ConnectionPaths.as_str().to_string(),
                Value::PathListOp(crate::sdf::ListOp {
                    explicit: true,
                    explicit_items: vec![SdfPath::new(&connection_path).unwrap_or_else(|_| SdfPath::abs_root())],
                    ..Default::default()
                }),
            );
            specs.insert(surface_output_path, surface_output_spec);

            // Create shader's outputs:surface
            let shader_surface_output = match shader_path.append_property("outputs:surface") {
                Ok(p) => p,
                Err(_) => continue,
            };
            let shader_surface_spec = Spec::new(SpecType::Attribute);
            specs.insert(shader_surface_output, shader_surface_spec);
        }
    }

    /// Add shader input attributes from MaterialX standard_surface.
    fn add_materialx_shader_inputs(
        specs: &mut HashMap<SdfPath, Spec>,
        shader_path: &SdfPath,
        mat_path: &SdfPath,
        shader: &crate::mtlx::StandardSurface,
        mtlx_doc: &crate::mtlx::MtlxDocument,
        mtlx_base_dir: &Path,
        texture_children: &mut Vec<String>,
    ) {
        // Map MaterialX standard_surface inputs to UsdPreviewSurface inputs

        // Diffuse color
        if let Some(input) = &shader.base_color {
            Self::add_shader_input_from_mtlx(
                specs, shader_path, mat_path, "inputs:diffuseColor",
                input, mtlx_doc, mtlx_base_dir, texture_children, "diffuseTexture",
            );
        }

        // Metallic
        if let Some(input) = &shader.metalness {
            Self::add_shader_input_from_mtlx(
                specs, shader_path, mat_path, "inputs:metallic",
                input, mtlx_doc, mtlx_base_dir, texture_children, "metallicTexture",
            );
        }

        // Roughness (from specular_roughness)
        if let Some(input) = &shader.specular_roughness {
            Self::add_shader_input_from_mtlx(
                specs, shader_path, mat_path, "inputs:roughness",
                input, mtlx_doc, mtlx_base_dir, texture_children, "roughnessTexture",
            );
        }

        // Normal
        if let Some(input) = &shader.normal {
            Self::add_shader_input_from_mtlx(
                specs, shader_path, mat_path, "inputs:normal",
                input, mtlx_doc, mtlx_base_dir, texture_children, "normalTexture",
            );
        }

        // Emission
        if let Some(input) = &shader.emission_color {
            Self::add_shader_input_from_mtlx(
                specs, shader_path, mat_path, "inputs:emissiveColor",
                input, mtlx_doc, mtlx_base_dir, texture_children, "emissiveTexture",
            );
        }

        // Opacity
        if let Some(input) = &shader.opacity {
            Self::add_shader_input_from_mtlx(
                specs, shader_path, mat_path, "inputs:opacity",
                input, mtlx_doc, mtlx_base_dir, texture_children, "opacityTexture",
            );
        }
    }

    /// Add a single shader input from MaterialX.
    #[allow(clippy::too_many_arguments)]
    fn add_shader_input_from_mtlx(
        specs: &mut HashMap<SdfPath, Spec>,
        shader_path: &SdfPath,
        mat_path: &SdfPath,
        input_name: &str,
        input: &crate::mtlx::ShaderInput,
        mtlx_doc: &crate::mtlx::MtlxDocument,
        mtlx_base_dir: &Path,
        texture_children: &mut Vec<String>,
        texture_name: &str,
    ) {
        use crate::mtlx::{ShaderInput, ShaderValue};

        let input_path = match shader_path.append_property(input_name) {
            Ok(p) => p,
            Err(_) => return,
        };

        let mut input_spec = Spec::new(SpecType::Attribute);

        match input {
            ShaderInput::Value(value) => {
                // Direct value
                match value {
                    ShaderValue::Float(f) => {
                        input_spec.fields.insert(
                            FieldKey::Default.as_str().to_string(),
                            Value::Float(*f),
                        );
                    }
                    ShaderValue::Color3(rgb) => {
                        input_spec.fields.insert(
                            FieldKey::Default.as_str().to_string(),
                            Value::Vec3f(rgb.to_vec()),
                        );
                    }
                    ShaderValue::Vector3(v) => {
                        input_spec.fields.insert(
                            FieldKey::Default.as_str().to_string(),
                            Value::Vec3f(v.to_vec()),
                        );
                    }
                    ShaderValue::Color4(rgba) => {
                        input_spec.fields.insert(
                            FieldKey::Default.as_str().to_string(),
                            Value::Vec4f(rgba.to_vec()),
                        );
                    }
                }
            }
            ShaderInput::Connection { .. } | ShaderInput::NodeConnection { .. } => {
                // Texture connection - resolve the texture path and create a texture prim
                if let Some(tex_rel_path) = mtlx_doc.resolve_texture_path(input) {
                    // Create texture prim
                    let tex_path = mtlx_base_dir.join(&tex_rel_path);
                    let tex_path_str = tex_path.to_string_lossy().to_string();

                    // Create UsdUVTexture prim under the material
                    let tex_prim_path: SdfPath = match mat_path.append_path(texture_name) {
                        Ok(p) => p,
                        Err(_) => return,
                    };

                    let mut tex_prim_spec = Spec::new(SpecType::Prim);
                    tex_prim_spec.fields.insert(
                        FieldKey::TypeName.as_str().to_string(),
                        Value::Token("Shader".into()),
                    );
                    tex_prim_spec.fields.insert(
                        FieldKey::Specifier.as_str().to_string(),
                        Value::Token("def".into()),
                    );
                    specs.insert(tex_prim_path.clone(), tex_prim_spec);

                    texture_children.push(texture_name.to_string());

                    // Add info:id for UsdUVTexture
                    if let Ok(tex_info_path) = tex_prim_path.append_property("info:id") {
                        let mut tex_info_spec = Spec::new(SpecType::Attribute);
                        tex_info_spec.fields.insert(
                            FieldKey::Default.as_str().to_string(),
                            Value::Token("UsdUVTexture".into()),
                        );
                        specs.insert(tex_info_path, tex_info_spec);
                    }

                    // Add inputs:file
                    if let Ok(file_path) = tex_prim_path.append_property("inputs:file") {
                        let mut file_spec = Spec::new(SpecType::Attribute);
                        file_spec.fields.insert(
                            FieldKey::Default.as_str().to_string(),
                            Value::AssetPath(tex_path_str),
                        );
                        specs.insert(file_path, file_spec);
                    }

                    // Add outputs:rgb (or outputs:r for float textures)
                    let output_name = if input_name.contains("metallic") ||
                                        input_name.contains("roughness") ||
                                        input_name.contains("opacity") {
                        "outputs:r"
                    } else {
                        "outputs:rgb"
                    };
                    if let Ok(output_path) = tex_prim_path.append_property(output_name) {
                        let output_spec = Spec::new(SpecType::Attribute);
                        let output_path_clone: SdfPath = output_path.clone();
                        specs.insert(output_path, output_spec);

                        // Connect shader input to texture output
                        input_spec.fields.insert(
                            FieldKey::ConnectionPaths.as_str().to_string(),
                            Value::PathListOp(crate::sdf::ListOp {
                                explicit: true,
                                explicit_items: vec![output_path_clone],
                                ..Default::default()
                            }),
                        );
                    }
                }
            }
        }

        specs.insert(input_path, input_spec);
    }

    /// Compose an internal reference (reference to prim in same layer).
    fn compose_internal_reference(specs: &mut HashMap<SdfPath, Spec>, prim_path: &SdfPath, target_path: &SdfPath) {
        if target_path.as_str().is_empty() {
            return;
        }

        // Get fields from the target prim
        let target_fields: HashMap<String, Value> = if let Some(target_spec) = specs.get(target_path) {
            target_spec
                .fields
                .iter()
                .filter(|(key, _)| {
                    // Skip composition-specific fields
                    *key != FieldKey::References.as_str()
                        && *key != FieldKey::Payload.as_str()
                        && *key != FieldKey::InheritPaths.as_str()
                        && *key != FieldKey::Specializes.as_str()
                        && *key != FieldKey::Specifier.as_str()
                        && *key != FieldKey::TypeName.as_str()
                })
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect()
        } else {
            return;
        };

        // Apply target fields to the prim (local opinions win)
        if let Some(prim_spec) = specs.get_mut(prim_path) {
            for (key, value) in target_fields {
                prim_spec.fields.entry(key).or_insert(value);
            }
        }

        // Also copy child properties from target
        let target_prefix = target_path.as_str().to_string() + ".";
        let properties_to_add: Vec<(SdfPath, Spec)> = specs
            .iter()
            .filter(|(path, _)| path.as_str().starts_with(&target_prefix))
            .filter_map(|(path, spec)| {
                let prop_name = &path.as_str()[target_prefix.len()..];
                prim_path
                    .append_property(prop_name)
                    .ok()
                    .map(|new_path| (new_path, spec.clone()))
            })
            .collect();

        for (prop_path, prop_spec) in properties_to_add {
            specs.entry(prop_path).or_insert(prop_spec);
        }
    }

    /// Get the default prim path from the pseudo-root.
    fn get_default_prim_path(specs: &HashMap<SdfPath, Spec>) -> Option<SdfPath> {
        let root_spec = specs.get(&SdfPath::abs_root())?;
        let default_prim = root_spec.fields.get(FieldKey::DefaultPrim.as_str())?;

        match default_prim {
            Value::Token(name) | Value::String(name) => SdfPath::new(&format!("/{}", name)).ok(),
            _ => None,
        }
    }

    /// Compose referenced content by remapping paths and merging specs.
    fn compose_referenced_content(
        specs: &mut HashMap<SdfPath, Spec>,
        prim_path: &SdfPath,
        ref_specs: &HashMap<SdfPath, Spec>,
        target_path: &SdfPath,
        layer_offset: &crate::sdf::LayerOffset,
    ) {
        let target_str = target_path.as_str();
        let prim_str = prim_path.as_str();

        // Merge fields from the target prim to the referencing prim
        if let Some(target_spec) = ref_specs.get(target_path) {
            let target_fields: HashMap<String, Value> = target_spec
                .fields
                .iter()
                .filter(|(key, _)| {
                    // Skip composition-specific fields
                    *key != FieldKey::References.as_str()
                        && *key != FieldKey::Payload.as_str()
                        && *key != FieldKey::InheritPaths.as_str()
                        && *key != FieldKey::Specializes.as_str()
                        && *key != FieldKey::Specifier.as_str()
                })
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect();

            if let Some(prim_spec) = specs.get_mut(prim_path) {
                for (key, mut value) in target_fields {
                    // Apply layer offset to time samples
                    if let Value::TimeSamples(samples) = &mut value {
                        *samples = samples
                            .iter()
                            .map(|(time, val)| (layer_offset.apply(*time), val.clone()))
                            .collect();
                    }
                    prim_spec.fields.entry(key).or_insert(value);
                }
            }
        }

        // Remap and add properties from the target prim
        let target_prop_prefix = format!("{}.", target_str);
        for (path, spec) in ref_specs.iter() {
            if path.as_str().starts_with(&target_prop_prefix) {
                let prop_name = &path.as_str()[target_prop_prefix.len()..];
                if let Ok(new_path) = prim_path.append_property(prop_name) {
                    let mut new_spec = spec.clone();
                    Self::apply_layer_offset_to_spec(&mut new_spec, layer_offset);
                    specs.entry(new_path).or_insert(new_spec);
                }
            }
        }

        // Remap and add child prims from the target
        let target_child_prefix = format!("{}/", target_str);
        let mut direct_children: Vec<String> = Vec::new();

        for (path, spec) in ref_specs.iter() {
            if path.as_str().starts_with(&target_child_prefix) {
                let relative_path = &path.as_str()[target_str.len()..];
                let new_path_str = format!("{}{}", prim_str, relative_path);
                if let Ok(new_path) = SdfPath::new(&new_path_str) {
                    let mut new_spec = spec.clone();
                    Self::apply_layer_offset_to_spec(&mut new_spec, layer_offset);
                    specs.entry(new_path).or_insert(new_spec);

                    // Track direct children (one level deep, prim specs only)
                    if spec.ty == SpecType::Prim && !relative_path[1..].contains('/') {
                        let child_name = relative_path.trim_start_matches('/').to_string();
                        if !direct_children.contains(&child_name) {
                            direct_children.push(child_name);
                        }
                    }
                }
            }
        }

        // Update primChildren on the parent prim to include newly added children
        if !direct_children.is_empty() {
            if let Some(prim_spec) = specs.get_mut(prim_path) {
                let mut existing_children = prim_spec
                    .fields
                    .get("primChildren")
                    .and_then(|v| v.clone().try_as_token_vec())
                    .unwrap_or_default();

                for child_name in direct_children {
                    if !existing_children.contains(&child_name) {
                        existing_children.push(child_name);
                    }
                }

                prim_spec.add("primChildren", Value::TokenVec(existing_children));
            }
        }

        // Remap and add specs with variant selections
        // These have paths like /Target{variantSet=variantName}/... and need to be
        // remapped to /Prim{variantSet=variantName}/...
        let target_variant_prefix = format!("{}{}", target_str, "{");
        for (path, spec) in ref_specs.iter() {
            let path_str = path.as_str();
            if path_str.starts_with(&target_variant_prefix) {
                // Remap the path by replacing the target prefix with the prim prefix
                let relative_path = &path_str[target_str.len()..];
                let new_path_str = format!("{}{}", prim_str, relative_path);
                if let Ok(new_path) = SdfPath::new(&new_path_str) {
                    let mut new_spec = spec.clone();
                    Self::apply_layer_offset_to_spec(&mut new_spec, layer_offset);
                    specs.entry(new_path).or_insert(new_spec);
                }
            }
        }
    }

    /// Compose payloads arcs for all prims in the specs.
    ///
    /// Payloads are weaker than references but stronger than specializes.
    /// For each prim with a `payload` field, load the payload layer,
    /// find the target prim, and merge its content under the prim.
    fn compose_payloads(specs: &mut HashMap<SdfPath, Spec>, base_dir: &Path, ctx: &mut CompositionContext) {
        // Collect all prims that have payloads (to avoid borrowing issues)
        let prims_with_payloads: Vec<(SdfPath, Vec<crate::sdf::Payload>)> = specs
            .iter()
            .filter(|(_, spec)| spec.ty == SpecType::Prim)
            .filter_map(|(path, spec)| {
                let payloads = Self::extract_payloads(spec);
                if payloads.is_empty() {
                    None
                } else {
                    Some((path.clone(), payloads))
                }
            })
            .collect();

        // Process each prim with payloads
        for (prim_path, payloads) in prims_with_payloads {
            // Process payloads in order (first in list is strongest)
            for payload in payloads.iter() {
                // Skip internal payloads (empty asset path)
                if payload.asset_path.is_empty() {
                    Self::compose_internal_reference(specs, &prim_path, &payload.prim_path);
                    continue;
                }

                // Resolve the external layer path
                let resolved_path = match Self::resolve_sublayer_path(&payload.asset_path, base_dir) {
                    Ok(p) => p,
                    Err(_) => continue,
                };

                // Load the payload layer
                let payload_layer = match Self::open_recursive(&resolved_path, ctx) {
                    Ok(layer) => layer,
                    Err(_) => continue,
                };

                // Determine the target prim path in the payload layer
                let target_path = if payload.prim_path.as_str().is_empty() {
                    // Use default prim if no explicit prim path
                    Self::get_default_prim_path(&payload_layer.specs)
                } else {
                    Some(payload.prim_path.clone())
                };

                let Some(target_path) = target_path else {
                    continue;
                };

                // Get layer offset (use default if None)
                let layer_offset = payload.layer_offset.unwrap_or_default();

                // Remap and merge specs from the payload layer
                Self::compose_referenced_content(specs, &prim_path, &payload_layer.specs, &target_path, &layer_offset);
            }
        }
    }

    /// Extract references from a prim's spec.
    ///
    /// Returns references from the ReferenceListOp, combining explicit, prepended, appended, and added items.
    fn extract_references(spec: &Spec) -> Vec<crate::sdf::Reference> {
        let Some(value) = spec.fields.get(FieldKey::References.as_str()) else {
            return vec![];
        };

        let Some(list_op) = value.clone().try_as_reference_list_op() else {
            return vec![];
        };

        // Combine all items from the list op
        let mut refs = Vec::new();

        if list_op.explicit {
            refs.extend(list_op.explicit_items);
        } else {
            refs.extend(list_op.prepended_items);
            refs.extend(list_op.added_items);
            refs.extend(list_op.appended_items);
        }

        // Filter out deleted items
        refs.retain(|r| !list_op.deleted_items.contains(r));

        refs
    }

    /// Extract payloads from a prim's spec.
    ///
    /// Returns payloads from the PayloadListOp, combining explicit, prepended, appended, and added items.
    fn extract_payloads(spec: &Spec) -> Vec<crate::sdf::Payload> {
        let Some(value) = spec.fields.get(FieldKey::Payload.as_str()) else {
            return vec![];
        };

        let Some(list_op) = value.clone().try_as_payload_list_op() else {
            return vec![];
        };

        // Combine all items from the list op
        let mut payloads = Vec::new();

        if list_op.explicit {
            payloads.extend(list_op.explicit_items);
        } else {
            payloads.extend(list_op.prepended_items);
            payloads.extend(list_op.added_items);
            payloads.extend(list_op.appended_items);
        }

        // Filter out deleted items
        payloads.retain(|p| !list_op.deleted_items.contains(p));

        payloads
    }

    /// Extract specializes paths from a prim's spec.
    ///
    /// Returns paths from the PathListOp, combining explicit, prepended, appended, and added items.
    fn extract_specialize_paths(spec: &Spec) -> Vec<SdfPath> {
        let Some(value) = spec.fields.get(FieldKey::Specializes.as_str()) else {
            return vec![];
        };

        let Some(list_op) = value.clone().try_as_path_list_op() else {
            return vec![];
        };

        // Combine all items from the list op
        let mut paths = Vec::new();

        if list_op.explicit {
            paths.extend(list_op.explicit_items);
        } else {
            paths.extend(list_op.prepended_items);
            paths.extend(list_op.added_items);
            paths.extend(list_op.appended_items);
        }

        // Filter out deleted items
        paths.retain(|p| !list_op.deleted_items.contains(p));

        paths
    }

    /// Compose specializes arcs for all prims in the specs.
    ///
    /// Specializes is the weakest arc in LIVRPS ordering.
    /// For each prim with a `specializes` field, copy fields from the
    /// specialized prims that don't already exist on the specializing prim.
    fn compose_specializes(specs: &mut HashMap<SdfPath, Spec>) {
        // Collect all prims that have specializes (to avoid borrowing issues)
        let prims_with_specializes: Vec<(SdfPath, Vec<SdfPath>)> = specs
            .iter()
            .filter(|(_, spec)| spec.ty == SpecType::Prim)
            .filter_map(|(path, spec)| {
                let specialize_paths = Self::extract_specialize_paths(spec);
                if specialize_paths.is_empty() {
                    None
                } else {
                    Some((path.clone(), specialize_paths))
                }
            })
            .collect();

        // Process each prim with specializes
        for (prim_path, specialize_paths) in prims_with_specializes {
            // Collect fields from specialized prims
            let mut specialized_fields: HashMap<String, Value> = HashMap::new();

            // Process specializes in order (first in list is strongest among specializes)
            for spec_path in specialize_paths.iter() {
                if let Some(spec_spec) = specs.get(spec_path) {
                    for (key, value) in &spec_spec.fields {
                        // Skip composition-specific fields
                        if key == FieldKey::Specializes.as_str()
                            || key == FieldKey::InheritPaths.as_str()
                            || key == FieldKey::Specifier.as_str()
                            || key == FieldKey::TypeName.as_str()
                        {
                            continue;
                        }
                        specialized_fields.insert(key.clone(), value.clone());
                    }
                }
            }

            // Apply specialized fields to the prim (everything else wins over specializes)
            if let Some(prim_spec) = specs.get_mut(&prim_path) {
                for (key, value) in specialized_fields {
                    // Only insert if prim doesn't already have this field
                    prim_spec.fields.entry(key).or_insert(value);
                }
            }

            // Also specialize child properties
            Self::compose_specialized_properties(specs, &prim_path, &specialize_paths);
        }
    }

    /// Compose specialized properties (attributes and relationships) from specialized prims.
    fn compose_specialized_properties(
        specs: &mut HashMap<SdfPath, Spec>,
        prim_path: &SdfPath,
        specialize_paths: &[SdfPath],
    ) {
        // Collect all property paths from specialized prims
        let mut specialized_properties: Vec<(SdfPath, Spec)> = Vec::new();

        // Process specializes in order (first in list is strongest)
        for spec_path in specialize_paths.iter() {
            let spec_prefix = spec_path.as_str().to_string() + ".";
            for (path, spec) in specs.iter() {
                if path.as_str().starts_with(&spec_prefix) {
                    let prop_name = &path.as_str()[spec_prefix.len()..];
                    if let Ok(new_path) = prim_path.append_property(prop_name) {
                        specialized_properties.push((new_path, spec.clone()));
                    }
                }
            }
        }

        // Apply specialized properties (everything else wins)
        for (prop_path, prop_spec) in specialized_properties {
            specs.entry(prop_path).or_insert(prop_spec);
        }
    }

    /// Extract variant selections from a prim's spec.
    ///
    /// Returns a map of variant set name to selected variant.
    fn extract_variant_selections(spec: &Spec) -> HashMap<String, String> {
        let Some(value) = spec.fields.get(FieldKey::VariantSelection.as_str()) else {
            return HashMap::new();
        };

        match value {
            Value::VariantSelectionMap(map) => map.clone(),
            _ => HashMap::new(),
        }
    }

    /// Compose variant arcs for all prims in the specs.
    ///
    /// For each prim with variant selections, compose fields from the selected
    /// variant into the prim. This includes properties and child prims defined
    /// within the variant.
    ///
    /// This function handles nested variants by iterating until no more variant
    /// selections need to be processed.
    fn compose_variants(specs: &mut HashMap<SdfPath, Spec>) {
        // Keep processing until no more variants need composition
        // This handles nested variants (variants defined inside other variants)
        let mut max_iterations = 10; // Prevent infinite loops
        loop {
            if max_iterations == 0 {
                // Max iterations reached, possible infinite loop in variant composition
                break;
            }
            max_iterations -= 1;

            // Collect all prims that have variant selections
            let prims_with_variants: Vec<(SdfPath, HashMap<String, String>)> = specs
                .iter()
                .filter(|(_, spec)| spec.ty == SpecType::Prim)
                .filter_map(|(path, spec)| {
                    let selections = Self::extract_variant_selections(spec);
                    if selections.is_empty() {
                        None
                    } else {
                        Some((path.clone(), selections))
                    }
                })
                .collect();

            if prims_with_variants.is_empty() {
                break;
            }

            let mut composed_any = false;

            // Process each prim with variants
            for (prim_path, selections) in prims_with_variants {
                // Process each variant selection
                for (variant_set, selected_variant) in selections {
                    // Build the variant path: /Prim{variantSet=variant}
                    let variant_path = match prim_path.append_variant_selection(&variant_set, &selected_variant) {
                        Ok(p) => p,
                        Err(_) => continue,
                    };

                    // Check if the variant spec exists
                    if !specs.contains_key(&variant_path) {
                        continue;
                    }

                    composed_any = true;

                    // Get fields from the variant spec (including nested variant selections)
                    let variant_fields: HashMap<String, Value> = if let Some(variant_spec) = specs.get(&variant_path) {
                        variant_spec
                            .fields
                            .iter()
                            .filter(|(key, _)| {
                                // Skip composition-specific fields but KEEP variantSelection
                                *key != FieldKey::Specifier.as_str() && *key != "primChildren" && *key != "propertyChildren"
                            })
                            .map(|(k, v)| (k.clone(), v.clone()))
                            .collect()
                    } else {
                        continue;
                    };

                    // Apply variant fields to the prim (local opinions still win)
                    // This includes nested variant selections which will be processed in the next iteration
                    if let Some(prim_spec) = specs.get_mut(&prim_path) {
                        for (key, value) in variant_fields {
                            // Special handling for variant selections - merge them instead of replacing
                            if key == FieldKey::VariantSelection.as_str() {
                                if let Value::VariantSelectionMap(new_map) = &value {
                                    // Get existing selections or create new
                                    let mut merged_map = prim_spec
                                        .fields
                                        .get(&key)
                                        .and_then(|v| {
                                            if let Value::VariantSelectionMap(m) = v {
                                                Some(m.clone())
                                            } else {
                                                None
                                            }
                                        })
                                        .unwrap_or_default();

                                    // Add new selections (existing selections win)
                                    for (variant_set, selection) in new_map {
                                        merged_map.entry(variant_set.clone()).or_insert(selection.clone());
                                    }

                                    prim_spec.fields.insert(key, Value::VariantSelectionMap(merged_map));
                                }
                            } else {
                                prim_spec.fields.entry(key).or_insert(value);
                            }
                        }
                    }

                    // Compose properties from variant namespace into prim namespace
                    Self::compose_variant_properties(specs, &prim_path, &variant_path);

                    // Compose child prims from variant namespace into prim namespace
                    Self::compose_variant_children(specs, &prim_path, &variant_path);

                    // Also compose any nested variant sets defined in this variant
                    Self::compose_nested_variant_sets(specs, &prim_path, &variant_path);
                }
            }

            if !composed_any {
                break;
            }
        }
    }

    /// Compose nested variant sets from a variant into the parent prim.
    ///
    /// When a variant defines variant sets (e.g., modelVariant contains shadingVariant),
    /// those variant sets and their content need to be composed into the prim namespace.
    /// This includes:
    /// - Variant specs themselves (for further composition)
    /// - All prims, properties, and relationships defined inside nested variants
    fn compose_nested_variant_sets(specs: &mut HashMap<SdfPath, Spec>, prim_path: &SdfPath, variant_path: &SdfPath) {
        let variant_str = variant_path.as_str();
        let prim_str = prim_path.as_str();

        // Find ALL specs nested under this variant that have additional variant selections
        // This includes variant specs, prims inside variants, and properties
        // Pattern: /Prim{var=sel}{nestedVar=nestedSel}/... -> /Prim{nestedVar=nestedSel}/...
        let specs_to_translate: Vec<(SdfPath, Spec)> = specs
            .iter()
            .filter(|(path, _)| {
                let path_str = path.as_str();
                // Must start with variant path and have additional content after
                if !path_str.starts_with(variant_str) || path_str.len() <= variant_str.len() {
                    return false;
                }
                // The suffix must contain another variant selection
                let suffix = &path_str[variant_str.len()..];
                suffix.contains('{')
            })
            .map(|(path, spec)| {
                // Translate the path by removing the outer variant selection
                // /Prim{outer=sel}{inner=val}/Child -> /Prim{inner=val}/Child
                // /Prim{outer=sel}{inner=val}/Child.prop -> /Prim{inner=val}/Child.prop
                let suffix = &path.as_str()[variant_str.len()..];
                let new_path_str = format!("{}{}", prim_str, suffix);
                let new_path = SdfPath::new(&new_path_str).unwrap_or_else(|_| path.clone());
                (new_path, spec.clone())
            })
            .collect();

        // Add translated specs to the main namespace
        for (new_path, spec) in specs_to_translate {
            match specs.entry(new_path) {
                std::collections::hash_map::Entry::Occupied(mut entry) => {
                    // Merge fields (existing wins over new)
                    let existing = entry.get_mut();
                    for (key, value) in spec.fields {
                        existing.fields.entry(key).or_insert(value);
                    }
                }
                std::collections::hash_map::Entry::Vacant(entry) => {
                    entry.insert(spec);
                }
            }
        }
    }

    /// Compose properties from a variant path into the prim path.
    fn compose_variant_properties(specs: &mut HashMap<SdfPath, Spec>, prim_path: &SdfPath, variant_path: &SdfPath) {
        // Find all properties under the variant path
        let variant_prefix = variant_path.as_str().to_string() + ".";
        let properties_to_add: Vec<(SdfPath, Spec)> = specs
            .iter()
            .filter(|(path, _)| path.as_str().starts_with(&variant_prefix))
            .map(|(path, spec)| {
                // Extract property name and create new path under prim
                let prop_name = &path.as_str()[variant_prefix.len()..];
                let new_path = prim_path.append_property(prop_name).unwrap();
                (new_path, spec.clone())
            })
            .collect();

        // Add properties, merging with existing (local opinions win over variant)
        for (prop_path, variant_prop_spec) in properties_to_add {
            match specs.entry(prop_path) {
                std::collections::hash_map::Entry::Occupied(mut entry) => {
                    // Merge variant property fields into existing spec
                    let existing_spec = entry.get_mut();
                    for (field_key, field_value) in variant_prop_spec.fields {
                        existing_spec.fields.entry(field_key).or_insert(field_value);
                    }
                }
                std::collections::hash_map::Entry::Vacant(entry) => {
                    entry.insert(variant_prop_spec);
                }
            }
        }
    }

    /// Compose child prims from a variant path into the prim path.
    fn compose_variant_children(specs: &mut HashMap<SdfPath, Spec>, prim_path: &SdfPath, variant_path: &SdfPath) {
        // Find all child prims under the variant path (look for paths like /Prim{var=sel}/Child)
        let variant_str = variant_path.as_str();
        let child_prefix = format!("{}/", variant_str);

        // Collect paths that are direct children or deeper descendants
        let prims_to_add: Vec<(SdfPath, Spec)> = specs
            .iter()
            .filter(|(path, spec)| path.as_str().starts_with(&child_prefix) && spec.ty == SpecType::Prim)
            .map(|(path, spec)| {
                // Replace the variant prefix with the prim path
                let relative_path = &path.as_str()[variant_str.len()..];
                let new_path_str = format!("{}{}", prim_path.as_str(), relative_path);
                let new_path = SdfPath::new(&new_path_str).unwrap();
                (new_path, spec.clone())
            })
            .collect();

        // Also need to collect properties and other specs under variant children
        let all_specs_to_add: Vec<(SdfPath, Spec)> = specs
            .iter()
            .filter(|(path, _)| path.as_str().starts_with(&child_prefix))
            .map(|(path, spec)| {
                let relative_path = &path.as_str()[variant_str.len()..];
                let new_path_str = format!("{}{}", prim_path.as_str(), relative_path);
                let new_path = SdfPath::new(&new_path_str).unwrap();
                (new_path, spec.clone())
            })
            .collect();

        // Add all specs, merging with existing specs when present.
        // This is critical for `over` statements inside variants which overlay
        // attributes onto existing prims (e.g., material:binding on Geometry).
        // Variant opinions are weaker than local, so existing fields win.
        for (path, variant_spec) in all_specs_to_add {
            match specs.entry(path) {
                std::collections::hash_map::Entry::Occupied(mut entry) => {
                    // Merge variant spec fields into existing spec (existing fields win)
                    let existing_spec = entry.get_mut();
                    for (field_key, field_value) in variant_spec.fields {
                        // Only add if the field doesn't already exist (weaker opinion)
                        existing_spec.fields.entry(field_key).or_insert(field_value);
                    }
                }
                std::collections::hash_map::Entry::Vacant(entry) => {
                    entry.insert(variant_spec);
                }
            }
        }

        // Update prim children list
        if !prims_to_add.is_empty() {
            if let Some(prim_spec) = specs.get_mut(prim_path) {
                let mut existing_children = prim_spec
                    .fields
                    .get("primChildren")
                    .and_then(|v| v.clone().try_as_token_vec())
                    .unwrap_or_default();

                for (new_path, _) in &prims_to_add {
                    // Extract just the child name from the path
                    if let Some(child_name) = new_path.as_str().strip_prefix(&format!("{}/", prim_path.as_str())) {
                        // Only add if it's a direct child (no more slashes) and not already present
                        if !child_name.contains('/')
                            && !child_name.contains('.')
                            && !existing_children.contains(&child_name.to_string())
                        {
                            existing_children.push(child_name.to_string());
                        }
                    }
                }

                prim_spec.add("primChildren", Value::TokenVec(existing_children));
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

    #[test]
    fn test_inherits_basic() {
        let temp_dir = create_temp_dir();

        let root_path = temp_dir.join("inherits.usda");
        fs::write(
            &root_path,
            r#"#usda 1.0

class "_BaseClass"
{
    double roughness = 0.5
    double metallic = 0.0
    string shader = "UsdPreviewSurface"
}

def Xform "World"
{
    def Mesh "InheritingMesh" (
        inherits = </_BaseClass>
    )
    {
        double roughness = 0.2
    }

    def Mesh "PlainMesh"
    {
        double roughness = 1.0
    }
}
"#,
        )
        .unwrap();

        let composed = ComposedLayer::open(&root_path).unwrap();

        // Check that InheritingMesh exists and has inherited properties
        let inheriting_path = sdf::path("/World/InheritingMesh").unwrap();
        assert!(composed.specs.contains_key(&inheriting_path));

        // Check inherited attribute (metallic should be inherited)
        let metallic_path = sdf::path("/World/InheritingMesh.metallic").unwrap();
        assert!(
            composed.specs.contains_key(&metallic_path),
            "metallic attribute should be inherited from _BaseClass"
        );

        // Check that local opinion wins (roughness should be 0.2, not 0.5)
        let roughness_path = sdf::path("/World/InheritingMesh.roughness").unwrap();
        let roughness_spec = composed.specs.get(&roughness_path).expect("roughness should exist");
        let default_value = roughness_spec.fields.get("default").expect("default field");
        match default_value {
            Value::Double(v) => {
                assert!(
                    (*v - 0.2).abs() < 0.001,
                    "Local roughness (0.2) should win over inherited (0.5), got {}",
                    v
                );
            }
            _ => panic!("Expected Double, got {:?}", default_value),
        }

        // Check that PlainMesh doesn't have metallic (no inheritance)
        let plain_metallic_path = sdf::path("/World/PlainMesh.metallic").unwrap();
        assert!(
            !composed.specs.contains_key(&plain_metallic_path),
            "PlainMesh should not have metallic (no inheritance)"
        );

        cleanup_temp_dir(&temp_dir);
    }

    #[test]
    fn test_inherits_multiple_classes() {
        let temp_dir = create_temp_dir();

        let root_path = temp_dir.join("multi_inherit.usda");
        fs::write(
            &root_path,
            r#"#usda 1.0

class "_ClassA"
{
    double valueA = 1.0
    double shared = 10.0
}

class "_ClassB"
{
    double valueB = 2.0
    double shared = 20.0
}

def Mesh "MultiInherit" (
    inherits = [</_ClassA>, </_ClassB>]
)
{
}
"#,
        )
        .unwrap();

        let composed = ComposedLayer::open(&root_path).unwrap();

        // Should have valueA from _ClassA
        let value_a_path = sdf::path("/MultiInherit.valueA").unwrap();
        assert!(composed.specs.contains_key(&value_a_path), "Should inherit valueA");

        // Should have valueB from _ClassB
        let value_b_path = sdf::path("/MultiInherit.valueB").unwrap();
        assert!(composed.specs.contains_key(&value_b_path), "Should inherit valueB");

        // shared should come from _ClassA (first in list, stronger)
        let shared_path = sdf::path("/MultiInherit.shared").unwrap();
        let shared_spec = composed.specs.get(&shared_path).expect("shared should exist");
        let default_value = shared_spec.fields.get("default").expect("default field");
        match default_value {
            Value::Double(v) => {
                assert!(
                    (*v - 10.0).abs() < 0.001,
                    "_ClassA's shared (10.0) should win over _ClassB's (20.0), got {}",
                    v
                );
            }
            _ => panic!("Expected Double, got {:?}", default_value),
        }

        cleanup_temp_dir(&temp_dir);
    }

    #[test]
    fn test_specializes_basic() {
        let temp_dir = create_temp_dir();

        let root_path = temp_dir.join("specializes.usda");
        fs::write(
            &root_path,
            r#"#usda 1.0

def Xform "BasePrim"
{
    double baseValue = 1.0
    string description = "base description"
}

def Mesh "SpecializingMesh" (
    specializes = </BasePrim>
)
{
    string description = "specialized description"
}
"#,
        )
        .unwrap();

        let composed = ComposedLayer::open(&root_path).unwrap();

        // Check that SpecializingMesh has the specialized baseValue
        let base_value_path = sdf::path("/SpecializingMesh.baseValue").unwrap();
        assert!(
            composed.specs.contains_key(&base_value_path),
            "baseValue should be specialized from BasePrim"
        );

        // Check that local description wins over specialized
        let desc_path = sdf::path("/SpecializingMesh.description").unwrap();
        let desc_spec = composed.specs.get(&desc_path).expect("description should exist");
        let default_value = desc_spec.fields.get("default").expect("default field");
        match default_value {
            Value::String(v) => {
                assert_eq!(
                    v, "specialized description",
                    "Local description should win over specialized"
                );
            }
            _ => panic!("Expected String, got {:?}", default_value),
        }

        cleanup_temp_dir(&temp_dir);
    }

    #[test]
    fn test_inherits_wins_over_specializes() {
        let temp_dir = create_temp_dir();

        let root_path = temp_dir.join("inherit_vs_specialize.usda");
        fs::write(
            &root_path,
            r#"#usda 1.0

def Xform "BasePrim"
{
    double baseValue = 1.0
    string shared = "from base"
}

class "_InheritClass"
{
    double inheritValue = 2.0
    string shared = "from inherit"
}

def Mesh "InheritAndSpecialize" (
    inherits = </_InheritClass>
    specializes = </BasePrim>
)
{
}
"#,
        )
        .unwrap();

        let composed = ComposedLayer::open(&root_path).unwrap();

        // Should have baseValue from specializes
        let base_value_path = sdf::path("/InheritAndSpecialize.baseValue").unwrap();
        assert!(
            composed.specs.contains_key(&base_value_path),
            "baseValue should be specialized from BasePrim"
        );

        // Should have inheritValue from inherits
        let inherit_value_path = sdf::path("/InheritAndSpecialize.inheritValue").unwrap();
        assert!(
            composed.specs.contains_key(&inherit_value_path),
            "inheritValue should be inherited from _InheritClass"
        );

        // shared should come from inherits (stronger than specializes)
        let shared_path = sdf::path("/InheritAndSpecialize.shared").unwrap();
        let shared_spec = composed.specs.get(&shared_path).expect("shared should exist");
        let default_value = shared_spec.fields.get("default").expect("default field");
        match default_value {
            Value::String(v) => {
                assert_eq!(
                    v, "from inherit",
                    "Inherits should win over specializes for shared field"
                );
            }
            _ => panic!("Expected String, got {:?}", default_value),
        }

        cleanup_temp_dir(&temp_dir);
    }

    #[test]
    fn test_layer_offset_application() {
        // Test layer offset directly on specs without parsing USDA
        use crate::sdf::{LayerOffset, TimeSampleMap};

        let mut spec = sdf::Spec::new(sdf::SpecType::Attribute);

        // Create time samples: 0 -> value0, 10 -> value1, 20 -> value2
        let original_samples: TimeSampleMap = vec![
            (0.0, Value::Double(0.0)),
            (10.0, Value::Double(10.0)),
            (20.0, Value::Double(20.0)),
        ];
        spec.fields
            .insert("timeSamples".to_string(), Value::TimeSamples(original_samples));

        // Apply layer offset: offset=100, scale=2
        // Expected: 0 -> 100, 10 -> 120, 20 -> 140
        let layer_offset = LayerOffset::new(100.0, 2.0);
        ComposedLayer::apply_layer_offset_to_spec(&mut spec, &layer_offset);

        // Verify the times were transformed
        if let Some(Value::TimeSamples(samples)) = spec.fields.get("timeSamples") {
            let times: Vec<f64> = samples.iter().map(|(t, _)| *t).collect();
            assert!(
                times.contains(&100.0),
                "Time 0 should be offset to 100, got {:?}",
                times
            );
            assert!(
                times.contains(&120.0),
                "Time 10 should be offset to 120, got {:?}",
                times
            );
            assert!(
                times.contains(&140.0),
                "Time 20 should be offset to 140, got {:?}",
                times
            );
        } else {
            panic!("Expected TimeSamples");
        }
    }

    #[test]
    fn test_sublayer_with_offset_basic() {
        let temp_dir = create_temp_dir();

        // Create a sublayer with a simple prim
        let sub_path = temp_dir.join("sublayer.usda");
        fs::write(
            &sub_path,
            r#"#usda 1.0

def Xform "FromSublayer"
{
    double value = 42.0
}
"#,
        )
        .unwrap();

        // Create root layer with sublayer and offset
        let root_path = temp_dir.join("root.usda");
        fs::write(
            &root_path,
            r#"#usda 1.0
(
    subLayers = [
        @./sublayer.usda@ (offset = 10; scale = 2)
    ]
)
"#,
        )
        .unwrap();

        let composed = ComposedLayer::open(&root_path).unwrap();

        // The sublayer prim should be in the composed layer
        let prim_path = sdf::path("/FromSublayer").unwrap();
        assert!(
            composed.specs.contains_key(&prim_path),
            "FromSublayer should exist in composed layer"
        );

        cleanup_temp_dir(&temp_dir);
    }

    #[test]
    fn test_layer_offset_is_identity() {
        use crate::sdf::LayerOffset;

        // Identity offset should not transform
        let identity = LayerOffset::new(0.0, 1.0);
        assert!(identity.is_identity());
        assert_eq!(identity.apply(10.0), 10.0);

        // Non-identity offsets
        let with_offset = LayerOffset::new(100.0, 1.0);
        assert!(!with_offset.is_identity());
        assert_eq!(with_offset.apply(10.0), 110.0);

        let with_scale = LayerOffset::new(0.0, 2.0);
        assert!(!with_scale.is_identity());
        assert_eq!(with_scale.apply(10.0), 20.0);

        let with_both = LayerOffset::new(100.0, 2.0);
        assert!(!with_both.is_identity());
        assert_eq!(with_both.apply(10.0), 120.0);
    }

    #[test]
    fn test_variant_selection_parsing() {
        let temp_dir = create_temp_dir();

        let root_path = temp_dir.join("variants.usda");
        fs::write(
            &root_path,
            r#"#usda 1.0

def Xform "Model" (
    variants = {
        string lodVariant = "high"
        string colorVariant = "red"
    }
    prepend variantSets = ["lodVariant", "colorVariant"]
)
{
    double scale = 1.0
}
"#,
        )
        .unwrap();

        let composed = ComposedLayer::open(&root_path).unwrap();

        // Check that Model prim exists
        let model_path = sdf::path("/Model").unwrap();
        let model_spec = composed.specs.get(&model_path).expect("Model should exist");

        // Check variant selection is stored
        let variant_selection = model_spec
            .fields
            .get("variantSelection")
            .expect("variantSelection should exist");

        if let Value::VariantSelectionMap(map) = variant_selection {
            assert_eq!(map.get("lodVariant"), Some(&"high".to_string()));
            assert_eq!(map.get("colorVariant"), Some(&"red".to_string()));
        } else {
            panic!("Expected VariantSelectionMap, got {:?}", variant_selection);
        }

        // Check variant set names are stored
        let variant_set_names = model_spec
            .fields
            .get("variantSetNames")
            .expect("variantSetNames should exist");

        if let Value::StringListOp(list_op) = variant_set_names {
            assert!(list_op.prepended_items.contains(&"lodVariant".to_string()));
            assert!(list_op.prepended_items.contains(&"colorVariant".to_string()));
        } else {
            panic!("Expected StringListOp, got {:?}", variant_set_names);
        }

        cleanup_temp_dir(&temp_dir);
    }

    #[test]
    fn test_variant_set_parsing() {
        let temp_dir = create_temp_dir();

        let root_path = temp_dir.join("variantset.usda");
        fs::write(
            &root_path,
            r#"#usda 1.0

def Xform "Model"
{
    variantSet "lodVariant" = {
        "high" {
            double detail = 100.0
        }
        "low" {
            double detail = 10.0
        }
    }
}
"#,
        )
        .unwrap();

        let composed = ComposedLayer::open(&root_path).unwrap();

        // Check that Model prim exists
        let model_path = sdf::path("/Model").unwrap();
        assert!(composed.specs.contains_key(&model_path), "Model should exist");

        // Check that variant paths exist
        let high_path = sdf::path("/Model{lodVariant=high}").unwrap();
        assert!(
            composed.specs.contains_key(&high_path),
            "High variant should exist at {}",
            high_path
        );

        let low_path = sdf::path("/Model{lodVariant=low}").unwrap();
        assert!(
            composed.specs.contains_key(&low_path),
            "Low variant should exist at {}",
            low_path
        );

        // Check that variant attributes exist
        let high_detail_path = sdf::path("/Model{lodVariant=high}.detail").unwrap();
        let high_detail_spec = composed
            .specs
            .get(&high_detail_path)
            .expect("High variant detail attribute should exist");
        let high_detail_value = high_detail_spec.fields.get("default").expect("default field");
        match high_detail_value {
            Value::Double(v) => {
                assert!((v - 100.0).abs() < 0.001, "High detail should be 100.0, got {}", v);
            }
            _ => panic!("Expected Double, got {:?}", high_detail_value),
        }

        let low_detail_path = sdf::path("/Model{lodVariant=low}.detail").unwrap();
        let low_detail_spec = composed
            .specs
            .get(&low_detail_path)
            .expect("Low variant detail attribute should exist");
        let low_detail_value = low_detail_spec.fields.get("default").expect("default field");
        match low_detail_value {
            Value::Double(v) => {
                assert!((v - 10.0).abs() < 0.001, "Low detail should be 10.0, got {}", v);
            }
            _ => panic!("Expected Double, got {:?}", low_detail_value),
        }

        cleanup_temp_dir(&temp_dir);
    }

    #[test]
    fn test_variant_set_with_child_prims() {
        let temp_dir = create_temp_dir();

        let root_path = temp_dir.join("variant_children.usda");
        fs::write(
            &root_path,
            r#"#usda 1.0

def Xform "Model"
{
    variantSet "lodVariant" = {
        "high" {
            def Mesh "HighResMesh"
            {
                double size = 100.0
            }
        }
        "low" {
            def Mesh "LowResMesh"
            {
                double size = 10.0
            }
        }
    }
}
"#,
        )
        .unwrap();

        let composed = ComposedLayer::open(&root_path).unwrap();

        // Check that child prims within variants exist
        let high_mesh_path = sdf::path("/Model{lodVariant=high}/HighResMesh").unwrap();
        assert!(
            composed.specs.contains_key(&high_mesh_path),
            "HighResMesh should exist in high variant"
        );

        let low_mesh_path = sdf::path("/Model{lodVariant=low}/LowResMesh").unwrap();
        assert!(
            composed.specs.contains_key(&low_mesh_path),
            "LowResMesh should exist in low variant"
        );

        // Check attributes on child prims
        let high_size_path = sdf::path("/Model{lodVariant=high}/HighResMesh.size").unwrap();
        let high_size_spec = composed
            .specs
            .get(&high_size_path)
            .expect("HighResMesh size attribute should exist");
        let high_size_value = high_size_spec.fields.get("default").expect("default field");
        match high_size_value {
            Value::Double(v) => {
                assert!((v - 100.0).abs() < 0.001, "High size should be 100.0, got {}", v);
            }
            _ => panic!("Expected Double, got {:?}", high_size_value),
        }

        cleanup_temp_dir(&temp_dir);
    }

    #[test]
    fn test_variant_composition() {
        let temp_dir = create_temp_dir();

        let root_path = temp_dir.join("variant_compose.usda");
        fs::write(
            &root_path,
            r#"#usda 1.0

def Xform "Model" (
    variants = {
        string lodVariant = "high"
    }
)
{
    # Local opinion that should win over variant
    double localValue = 999.0

    variantSet "lodVariant" = {
        "high" {
            double detail = 100.0
            double localValue = 1.0
            def Mesh "VariantMesh"
            {
                double meshSize = 50.0
            }
        }
        "low" {
            double detail = 10.0
        }
    }
}
"#,
        )
        .unwrap();

        let composed = ComposedLayer::open(&root_path).unwrap();

        // Check that the Model prim has the "detail" attribute from the selected variant
        let detail_path = sdf::path("/Model.detail").unwrap();
        let detail_spec = composed
            .specs
            .get(&detail_path)
            .expect("detail attribute should exist on Model");
        let detail_value = detail_spec.fields.get("default").expect("default field");
        match detail_value {
            Value::Double(v) => {
                assert!(
                    (v - 100.0).abs() < 0.001,
                    "detail should be 100.0 from high variant, got {}",
                    v
                );
            }
            _ => panic!("Expected Double, got {:?}", detail_value),
        }

        // Check that local opinion wins over variant
        let local_path = sdf::path("/Model.localValue").unwrap();
        let local_spec = composed
            .specs
            .get(&local_path)
            .expect("localValue attribute should exist");
        let local_value = local_spec.fields.get("default").expect("default field");
        match local_value {
            Value::Double(v) => {
                assert!(
                    (v - 999.0).abs() < 0.001,
                    "localValue should be 999.0 (local wins over variant), got {}",
                    v
                );
            }
            _ => panic!("Expected Double, got {:?}", local_value),
        }

        // Check that child prim from variant appears under Model
        let mesh_path = sdf::path("/Model/VariantMesh").unwrap();
        assert!(
            composed.specs.contains_key(&mesh_path),
            "VariantMesh should be composed under Model"
        );

        // Check attribute on composed child prim
        let mesh_size_path = sdf::path("/Model/VariantMesh.meshSize").unwrap();
        let mesh_size_spec = composed
            .specs
            .get(&mesh_size_path)
            .expect("meshSize attribute should exist");
        let mesh_size_value = mesh_size_spec.fields.get("default").expect("default field");
        match mesh_size_value {
            Value::Double(v) => {
                assert!((v - 50.0).abs() < 0.001, "meshSize should be 50.0, got {}", v);
            }
            _ => panic!("Expected Double, got {:?}", mesh_size_value),
        }

        cleanup_temp_dir(&temp_dir);
    }

    #[test]
    fn test_reference_basic() {
        let temp_dir = create_temp_dir();

        // Create the referenced layer
        let ref_path = temp_dir.join("referenced.usda");
        fs::write(
            &ref_path,
            r#"#usda 1.0
(
    defaultPrim = "RefPrim"
)

def Xform "RefPrim"
{
    double refValue = 42.0
    string description = "from reference"

    def Mesh "RefChild"
    {
        double childValue = 100.0
    }
}
"#,
        )
        .unwrap();

        // Create root layer that references the other layer
        let root_path = temp_dir.join("root.usda");
        fs::write(
            &root_path,
            r#"#usda 1.0

def Xform "Model" (
    references = @./referenced.usda@
)
{
    double localValue = 999.0
}
"#,
        )
        .unwrap();

        let composed = ComposedLayer::open(&root_path).unwrap();

        // Check that Model prim exists
        let model_path = sdf::path("/Model").unwrap();
        assert!(composed.specs.contains_key(&model_path), "Model should exist");

        // Check that refValue was pulled in from the reference
        let ref_value_path = sdf::path("/Model.refValue").unwrap();
        assert!(
            composed.specs.contains_key(&ref_value_path),
            "refValue should be composed from reference"
        );

        // Check the value
        let ref_value_spec = composed.specs.get(&ref_value_path).expect("refValue should exist");
        let ref_value = ref_value_spec.fields.get("default").expect("default field");
        match ref_value {
            Value::Double(v) => {
                assert!((*v - 42.0).abs() < 0.001, "refValue should be 42.0, got {}", v);
            }
            _ => panic!("Expected Double, got {:?}", ref_value),
        }

        // Check that local opinion wins
        let local_path = sdf::path("/Model.localValue").unwrap();
        let local_spec = composed.specs.get(&local_path).expect("localValue should exist");
        let local_value = local_spec.fields.get("default").expect("default field");
        match local_value {
            Value::Double(v) => {
                assert!((*v - 999.0).abs() < 0.001, "localValue should be 999.0, got {}", v);
            }
            _ => panic!("Expected Double, got {:?}", local_value),
        }

        // Check that child prim from reference was composed
        let ref_child_path = sdf::path("/Model/RefChild").unwrap();
        assert!(
            composed.specs.contains_key(&ref_child_path),
            "RefChild should be composed from reference"
        );

        // Check child attribute
        let child_value_path = sdf::path("/Model/RefChild.childValue").unwrap();
        let child_value_spec = composed.specs.get(&child_value_path).expect("childValue should exist");
        let child_value = child_value_spec.fields.get("default").expect("default field");
        match child_value {
            Value::Double(v) => {
                assert!((*v - 100.0).abs() < 0.001, "childValue should be 100.0, got {}", v);
            }
            _ => panic!("Expected Double, got {:?}", child_value),
        }

        cleanup_temp_dir(&temp_dir);
    }

    #[test]
    fn test_reference_with_prim_path() {
        let temp_dir = create_temp_dir();

        // Create the referenced layer with multiple prims
        let ref_path = temp_dir.join("library.usda");
        fs::write(
            &ref_path,
            r#"#usda 1.0
(
    defaultPrim = "DefaultPrim"
)

def Xform "DefaultPrim"
{
    double defaultValue = 1.0
}

def Xform "SpecificPrim"
{
    double specificValue = 2.0
}
"#,
        )
        .unwrap();

        // Create root layer that references a specific prim
        let root_path = temp_dir.join("root.usda");
        fs::write(
            &root_path,
            r#"#usda 1.0

def Xform "Model" (
    references = @./library.usda@</SpecificPrim>
)
{
}
"#,
        )
        .unwrap();

        let composed = ComposedLayer::open(&root_path).unwrap();

        // Check that specificValue was pulled in (not defaultValue)
        let specific_path = sdf::path("/Model.specificValue").unwrap();
        assert!(
            composed.specs.contains_key(&specific_path),
            "specificValue should be composed from reference"
        );

        // DefaultValue should not be present (we referenced SpecificPrim, not DefaultPrim)
        let default_path = sdf::path("/Model.defaultValue").unwrap();
        assert!(
            !composed.specs.contains_key(&default_path),
            "defaultValue should NOT be composed (wrong prim)"
        );

        cleanup_temp_dir(&temp_dir);
    }

    #[test]
    fn test_payload_basic() {
        let temp_dir = create_temp_dir();

        // Create the payload layer
        let payload_path = temp_dir.join("payload.usda");
        fs::write(
            &payload_path,
            r#"#usda 1.0
(
    defaultPrim = "PayloadPrim"
)

def Xform "PayloadPrim"
{
    double payloadValue = 123.0

    def Mesh "PayloadChild"
    {
        double childValue = 456.0
    }
}
"#,
        )
        .unwrap();

        // Create root layer with payload
        let root_path = temp_dir.join("root.usda");
        fs::write(
            &root_path,
            r#"#usda 1.0

def Xform "Model" (
    payload = @./payload.usda@
)
{
    double localValue = 999.0
}
"#,
        )
        .unwrap();

        let composed = ComposedLayer::open(&root_path).unwrap();

        // Check that payloadValue was pulled in
        let payload_value_path = sdf::path("/Model.payloadValue").unwrap();
        assert!(
            composed.specs.contains_key(&payload_value_path),
            "payloadValue should be composed from payload"
        );

        // Check the value
        let payload_spec = composed
            .specs
            .get(&payload_value_path)
            .expect("payloadValue should exist");
        let payload_value = payload_spec.fields.get("default").expect("default field");
        match payload_value {
            Value::Double(v) => {
                assert!((*v - 123.0).abs() < 0.001, "payloadValue should be 123.0, got {}", v);
            }
            _ => panic!("Expected Double, got {:?}", payload_value),
        }

        // Check that local opinion wins
        let local_path = sdf::path("/Model.localValue").unwrap();
        let local_spec = composed.specs.get(&local_path).expect("localValue should exist");
        let local_value = local_spec.fields.get("default").expect("default field");
        match local_value {
            Value::Double(v) => {
                assert!((*v - 999.0).abs() < 0.001, "localValue should be 999.0, got {}", v);
            }
            _ => panic!("Expected Double, got {:?}", local_value),
        }

        // Check that child prim from payload was composed
        let payload_child_path = sdf::path("/Model/PayloadChild").unwrap();
        assert!(
            composed.specs.contains_key(&payload_child_path),
            "PayloadChild should be composed from payload"
        );

        cleanup_temp_dir(&temp_dir);
    }

    #[test]
    fn test_reference_wins_over_payload() {
        let temp_dir = create_temp_dir();

        // Create the reference layer
        let ref_path = temp_dir.join("reference.usda");
        fs::write(
            &ref_path,
            r#"#usda 1.0
(
    defaultPrim = "RefPrim"
)

def Xform "RefPrim"
{
    string shared = "from reference"
    double refOnly = 1.0
}
"#,
        )
        .unwrap();

        // Create the payload layer
        let payload_path = temp_dir.join("payload.usda");
        fs::write(
            &payload_path,
            r#"#usda 1.0
(
    defaultPrim = "PayloadPrim"
)

def Xform "PayloadPrim"
{
    string shared = "from payload"
    double payloadOnly = 2.0
}
"#,
        )
        .unwrap();

        // Create root layer with both reference and payload
        let root_path = temp_dir.join("root.usda");
        fs::write(
            &root_path,
            r#"#usda 1.0

def Xform "Model" (
    references = @./reference.usda@
    payload = @./payload.usda@
)
{
}
"#,
        )
        .unwrap();

        let composed = ComposedLayer::open(&root_path).unwrap();

        // Check that refOnly exists
        let ref_only_path = sdf::path("/Model.refOnly").unwrap();
        assert!(
            composed.specs.contains_key(&ref_only_path),
            "refOnly should be composed from reference"
        );

        // Check that payloadOnly exists
        let payload_only_path = sdf::path("/Model.payloadOnly").unwrap();
        assert!(
            composed.specs.contains_key(&payload_only_path),
            "payloadOnly should be composed from payload"
        );

        // Check that shared comes from reference (stronger than payload)
        let shared_path = sdf::path("/Model.shared").unwrap();
        let shared_spec = composed.specs.get(&shared_path).expect("shared should exist");
        let shared_value = shared_spec.fields.get("default").expect("default field");
        match shared_value {
            Value::String(v) => {
                assert_eq!(
                    v, "from reference",
                    "Reference should win over payload for shared field"
                );
            }
            _ => panic!("Expected String, got {:?}", shared_value),
        }

        cleanup_temp_dir(&temp_dir);
    }

    #[test]
    fn test_nested_references() {
        let temp_dir = create_temp_dir();

        // Create a deeply nested reference (layer A references layer B)
        let layer_b = temp_dir.join("layer_b.usda");
        fs::write(
            &layer_b,
            r#"#usda 1.0
(
    defaultPrim = "DeepPrim"
)

def Xform "DeepPrim"
{
    double deepValue = 999.0
}
"#,
        )
        .unwrap();

        // Layer A references layer B
        let layer_a = temp_dir.join("layer_a.usda");
        fs::write(
            &layer_a,
            r#"#usda 1.0
(
    defaultPrim = "MiddlePrim"
)

def Xform "MiddlePrim" (
    references = @./layer_b.usda@
)
{
    double middleValue = 500.0
}
"#,
        )
        .unwrap();

        // Root references layer A
        let root_path = temp_dir.join("root.usda");
        fs::write(
            &root_path,
            r#"#usda 1.0

def Xform "Model" (
    references = @./layer_a.usda@
)
{
    double topValue = 100.0
}
"#,
        )
        .unwrap();

        let composed = ComposedLayer::open(&root_path).unwrap();

        // Check that topValue exists (local)
        let top_path = sdf::path("/Model.topValue").unwrap();
        assert!(composed.specs.contains_key(&top_path), "topValue should exist");

        // Check that middleValue exists (from layer A)
        let middle_path = sdf::path("/Model.middleValue").unwrap();
        assert!(
            composed.specs.contains_key(&middle_path),
            "middleValue should be composed from layer_a reference"
        );

        // Check that deepValue exists (from layer B via layer A)
        let deep_path = sdf::path("/Model.deepValue").unwrap();
        assert!(
            composed.specs.contains_key(&deep_path),
            "deepValue should be composed from nested reference"
        );

        cleanup_temp_dir(&temp_dir);
    }

    #[test]
    fn test_nested_variant_selection() {
        // This test mirrors the Teapot structure where:
        // - modelVariant="Utah" variant has variants = { string shadingVariant = "..." }
        // - Inside Utah variant, there's a nested shadingVariant variantSet
        let temp_dir = create_temp_dir();

        let root_path = temp_dir.join("nested_variants.usda");
        fs::write(
            &root_path,
            r#"#usda 1.0

def Xform "Model" (
    variants = {
        string modelVariant = "TypeA"
    }
    prepend variantSets = "modelVariant"
)
{
    variantSet "modelVariant" = {
        "TypeA" (
            variants = {
                string colorVariant = "Red"
            }
            prepend variantSets = "colorVariant"
        ) {
            def Scope "Materials"
            {
                def Material "RedMaterial"
                {
                    string name = "Red"
                }
            }
            variantSet "colorVariant" = {
                "Red" {
                    over "Geometry" (
                        prepend apiSchemas = ["MaterialBindingAPI"]
                    )
                    {
                        rel material:binding = </Model/Materials/RedMaterial>
                    }
                }
                "Blue" {
                    over "Geometry"
                    {
                        rel material:binding = </Model/Materials/BlueMaterial>
                    }
                }
            }
        }
        "TypeB" {
            double attr = 1.0
        }
    }
}
"#,
        )
        .unwrap();

        let composed = ComposedLayer::open(&root_path).unwrap();

        // Check that /Model has the modelVariant selection
        let model_path = sdf::path("/Model").unwrap();
        let model_spec = composed.specs.get(&model_path).expect("Model should exist");
        let model_variant_selection = model_spec
            .fields
            .get("variantSelection")
            .expect("Model should have variantSelection");
        if let Value::VariantSelectionMap(map) = model_variant_selection {
            assert_eq!(map.get("modelVariant"), Some(&"TypeA".to_string()));
        }

        // Check that the TypeA variant spec has the colorVariant selection
        let type_a_path = sdf::path("/Model{modelVariant=TypeA}").unwrap();
        let type_a_spec = composed.specs.get(&type_a_path).expect("TypeA variant should exist");
        let nested_selection = type_a_spec.fields.get("variantSelection");

        // The TypeA variant should have variantSelection with colorVariant = Red
        if let Some(Value::VariantSelectionMap(map)) = nested_selection {
            assert_eq!(
                map.get("colorVariant"),
                Some(&"Red".to_string()),
                "TypeA variant should select colorVariant=Red"
            );
        } else {
            panic!("TypeA variant should have variantSelection field!");
        }

        // The key test: Does the Geometry material binding get composed to /Model/Geometry?
        let binding_path = sdf::path("/Model/Geometry.material:binding").unwrap();
        assert!(
            composed.specs.contains_key(&binding_path),
            "material:binding should be composed from nested variant to /Model/Geometry"
        );

        cleanup_temp_dir(&temp_dir);
    }

    #[test]
    fn test_teapot_material_binding() {
        // Test the actual Teapot USD file - this has nested variants that must be composed:
        // - modelVariant="Utah" variant has shadingVariant="CeramicLimeGreen"
        // - Inside the shadingVariant, there's an "over Geometry" with material:binding
        // - The material:binding should be composed to /Teapot/Geometry.material:binding
        let teapot_path = Path::new("vendor/usd-wg-assets/full_assets/Teapot/Teapot.usd");
        if !teapot_path.exists() {
            // Skip test if asset not found
            return;
        }

        let composed = ComposedLayer::open(teapot_path).unwrap();

        // Check that /Teapot/Geometry.material:binding exists
        let binding_path = sdf::path("/Teapot/Geometry.material:binding").unwrap();
        assert!(
            composed.specs.contains_key(&binding_path),
            "Teapot should have /Teapot/Geometry.material:binding composed from nested variants"
        );

        // Check that /Teapot/Geometry has typeName=Mesh (loaded from reference)
        let geometry_path = sdf::path("/Teapot/Geometry").unwrap();
        let geometry_spec = composed.specs.get(&geometry_path).expect("Geometry prim should exist");
        let type_name = geometry_spec
            .fields
            .get("typeName")
            .expect("Geometry should have typeName");
        assert!(
            matches!(type_name, Value::Token(t) if t == "Mesh"),
            "Geometry should be a Mesh type, got {:?}",
            type_name
        );
    }

    #[test]
    fn test_chess_set_composition() {
        // Test the Chess Set - uses 'add references' to include pieces
        let chess_path = Path::new("vendor/usd-wg-assets/full_assets/OpenChessSet/chess_set.usda");
        if !chess_path.exists() {
            return;
        }

        let composed = ComposedLayer::open(chess_path).unwrap();

        // Count mesh prims
        let mesh_count = composed
            .specs
            .iter()
            .filter(|(_, spec)| {
                spec.ty == SpecType::Prim
                    && spec
                        .fields
                        .get("typeName")
                        .map(|v| matches!(v, Value::Token(t) if t == "Mesh"))
                        .unwrap_or(false)
            })
            .count();

        println!("Chess Set: {} mesh prims found", mesh_count);

        // Check root primChildren
        let root_path = SdfPath::abs_root();
        if let Some(spec) = composed.specs.get(&root_path) {
            if let Some(children) = spec.fields.get("primChildren") {
                println!("Root (/) primChildren: {:?}", children);
            } else {
                println!("Root (/) has NO primChildren field!");
            }
        } else {
            println!("Root (/) spec NOT FOUND!");
        }

        // Check primChildren for key prims
        let chess_set_path = sdf::path("/ChessSet").unwrap();
        if let Some(spec) = composed.specs.get(&chess_set_path) {
            if let Some(children) = spec.fields.get("primChildren") {
                println!("/ChessSet primChildren: {:?}", children);
            } else {
                println!("/ChessSet has NO primChildren field!");
            }
        }

        let black_path = sdf::path("/ChessSet/Black").unwrap();
        if let Some(spec) = composed.specs.get(&black_path) {
            if let Some(children) = spec.fields.get("primChildren") {
                println!("/ChessSet/Black primChildren: {:?}", children);
            } else {
                println!("/ChessSet/Black has NO primChildren field!");
            }
        }

        let king_path = sdf::path("/ChessSet/Black/King").unwrap();
        if let Some(spec) = composed.specs.get(&king_path) {
            if let Some(children) = spec.fields.get("primChildren") {
                println!("/ChessSet/Black/King primChildren: {:?}", children);
            } else {
                println!("/ChessSet/Black/King has NO primChildren field!");
            }
            // Check for references field
            if let Some(refs) = spec.fields.get("references") {
                println!("/ChessSet/Black/King references: {:?}", refs);
            }
            // Check for variantSelection
            if let Some(vs) = spec.fields.get("variantSelection") {
                println!("/ChessSet/Black/King variantSelection: {:?}", vs);
            }
        }

        // Check King/Geom path
        let geom_path = sdf::path("/ChessSet/Black/King/Geom").unwrap();
        if let Some(spec) = composed.specs.get(&geom_path) {
            println!("/ChessSet/Black/King/Geom exists with type: {:?}", spec.fields.get("typeName"));
            if let Some(children) = spec.fields.get("primChildren") {
                println!("/ChessSet/Black/King/Geom primChildren: {:?}", children);
            }
        } else {
            println!("/ChessSet/Black/King/Geom NOT FOUND in specs!");
        }

        // Check King/Geom/Render path
        let render_path = sdf::path("/ChessSet/Black/King/Geom/Render").unwrap();
        if let Some(spec) = composed.specs.get(&render_path) {
            println!("/ChessSet/Black/King/Geom/Render exists with type: {:?}", spec.fields.get("typeName"));
        } else {
            println!("/ChessSet/Black/King/Geom/Render NOT FOUND in specs!");
        }

        // Chess set should have multiple meshes (21 expected)
        assert!(
            mesh_count >= 20,
            "Chess Set should have at least 20 meshes, got {}",
            mesh_count
        );
    }
}
