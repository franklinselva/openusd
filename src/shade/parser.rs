//! Parser for USD shading schemas.
//!
//! Converts raw `Spec` data into structured shader types.

use std::collections::HashMap;

use crate::sdf::{schema::FieldKey, Path as SdfPath, Spec, SpecType, Value};

use super::types::*;

/// Parse all materials from composed USD specs.
///
/// This walks the spec hierarchy, finds Material prims, and parses
/// their shader children into structured types.
///
/// # Example
///
/// ```ignore
/// use openusd::composition::ComposedLayer;
/// use openusd::shade::parse_materials;
///
/// let composed = ComposedLayer::open("model.usd")?;
/// let materials = parse_materials(&composed.specs);
/// ```
pub fn parse_materials(specs: &HashMap<SdfPath, Spec>) -> Vec<Material> {
    let mut materials = Vec::new();

    // Find all Material prims
    for (path, spec) in specs {
        if spec.ty != SpecType::Prim {
            continue;
        }

        let type_name = spec
            .fields
            .get(FieldKey::TypeName.as_str())
            .and_then(|v| v.try_as_token_ref())
            .map(|s| s.as_str())
            .unwrap_or("");

        if type_name == "Material" {
            if let Some(material) = parse_material(specs, path) {
                materials.push(material);
            }
        }
    }

    materials
}

/// Parse a single Material prim and its shader children.
fn parse_material(specs: &HashMap<SdfPath, Spec>, material_path: &SdfPath) -> Option<Material> {
    let spec = specs.get(material_path)?;

    let path_str = material_path.to_string();
    let name = path_str.split('/').last().unwrap_or("").to_string();

    let mut material = Material {
        prim_path: path_str.clone(),
        name,
        ..Default::default()
    };

    // Get child prims (shaders)
    let child_names = spec
        .fields
        .get("primChildren")
        .and_then(|v| match v {
            Value::TokenVec(names) => Some(names.clone()),
            _ => None,
        })
        .unwrap_or_default();

    // Parse each child shader
    for child_name in &child_names {
        let child_path_str = format!("{}/{}", path_str, child_name);
        if let Ok(child_path) = SdfPath::new(&child_path_str) {
            if let Some(child_spec) = specs.get(&child_path) {
                parse_shader_child(specs, &child_path, child_spec, &mut material);
            }
        }
    }

    // Build texture_files map for convenience
    for texture in &material.textures {
        if !texture.file.is_empty() {
            material
                .texture_files
                .insert(texture.prim_path.clone(), texture.file.clone());
        }
    }

    Some(material)
}

/// Parse a shader child prim and add it to the material.
fn parse_shader_child(
    specs: &HashMap<SdfPath, Spec>,
    shader_path: &SdfPath,
    shader_spec: &Spec,
    material: &mut Material,
) {
    if shader_spec.ty != SpecType::Prim {
        return;
    }

    let type_name = shader_spec
        .fields
        .get(FieldKey::TypeName.as_str())
        .and_then(|v| v.try_as_token_ref())
        .map(|s| s.as_str())
        .unwrap_or("");

    if type_name != "Shader" {
        return;
    }

    // Get shader info:id to determine shader type
    let shader_id = get_shader_id(specs, shader_path);

    match shader_id.as_str() {
        "UsdPreviewSurface" => {
            if let Some(surface) = parse_preview_surface(specs, shader_path) {
                material.surface = Some(surface);
            }
        }
        "UsdUVTexture" => {
            if let Some(texture) = parse_uv_texture(specs, shader_path) {
                material.textures.push(texture);
            }
        }
        "UsdPrimvarReader_float2" | "UsdPrimvarReader_float3" | "UsdPrimvarReader_float"
        | "UsdPrimvarReader_int" | "UsdPrimvarReader_string" | "UsdPrimvarReader_normal"
        | "UsdPrimvarReader_point" | "UsdPrimvarReader_vector" => {
            if let Some(reader) = parse_primvar_reader(specs, shader_path) {
                material.primvar_readers.push(reader);
            }
        }
        _ => {
            // Unknown shader type, skip
        }
    }
}

/// Get the info:id attribute for a shader.
fn get_shader_id(specs: &HashMap<SdfPath, Spec>, shader_path: &SdfPath) -> String {
    let info_id_path_str = format!("{}.info:id", shader_path);
    if let Ok(info_id_path) = SdfPath::new(&info_id_path_str) {
        if let Some(attr_spec) = specs.get(&info_id_path) {
            if let Some(value) = attr_spec.fields.get(FieldKey::Default.as_str()) {
                // info:id can be stored as Token or String depending on parser
                if let Some(token) = value.try_as_token_ref() {
                    return token.to_string();
                }
                if let Some(string) = value.try_as_string_ref() {
                    return string.to_string();
                }
            }
        }
    }
    String::new()
}

/// Parse a UsdPreviewSurface shader.
fn parse_preview_surface(specs: &HashMap<SdfPath, Spec>, shader_path: &SdfPath) -> Option<UsdPreviewSurface> {
    let path_str = shader_path.to_string();

    let mut surface = UsdPreviewSurface {
        prim_path: path_str.clone(),
        ..Default::default()
    };

    // Parse scalar attributes
    surface.diffuse_color =
        parse_color_or_texture_slot(specs, &path_str, "diffuseColor", [0.18, 0.18, 0.18, 1.0]);
    surface.metallic = parse_scalar_or_texture_slot(specs, &path_str, "metallic", 0.0);
    surface.roughness = parse_scalar_or_texture_slot(specs, &path_str, "roughness", 0.5);
    surface.emissive_color =
        parse_color_or_texture_slot(specs, &path_str, "emissiveColor", [0.0, 0.0, 0.0, 1.0]);
    surface.opacity = parse_scalar_or_texture_slot(specs, &path_str, "opacity", 1.0);

    // Optional texture slots
    surface.normal = parse_optional_texture_slot(specs, &path_str, "normal");
    surface.occlusion = parse_optional_texture_slot(specs, &path_str, "occlusion");

    // Scalar-only attributes
    surface.opacity_threshold = get_float_attr(specs, &path_str, "opacityThreshold").unwrap_or(0.0);
    surface.ior = get_float_attr(specs, &path_str, "ior").unwrap_or(1.5);
    surface.clearcoat = get_float_attr(specs, &path_str, "clearcoat").unwrap_or(0.0);
    surface.clearcoat_roughness = get_float_attr(specs, &path_str, "clearcoatRoughness").unwrap_or(0.01);
    surface.specular = get_float_attr(specs, &path_str, "specular").unwrap_or(0.5);
    surface.use_specular_workflow = get_int_attr(specs, &path_str, "useSpecularWorkflow").unwrap_or(0) != 0;

    Some(surface)
}

/// Parse a UsdUVTexture shader.
fn parse_uv_texture(specs: &HashMap<SdfPath, Spec>, shader_path: &SdfPath) -> Option<UsdUVTexture> {
    let path_str = shader_path.to_string();

    let mut texture = UsdUVTexture {
        prim_path: path_str.clone(),
        ..Default::default()
    };

    // inputs:file (asset path)
    if let Some(file) = get_asset_attr(specs, &path_str, "file") {
        texture.file = file.trim_matches('@').to_string();
    }

    // Wrap modes
    if let Some(wrap_s) = get_token_attr(specs, &path_str, "wrapS") {
        texture.wrap_s = WrapMode::from_token(&wrap_s);
    }
    if let Some(wrap_t) = get_token_attr(specs, &path_str, "wrapT") {
        texture.wrap_t = WrapMode::from_token(&wrap_t);
    }

    // Scale and bias (float4)
    texture.scale = get_float4_attr(specs, &path_str, "scale");
    texture.bias = get_float4_attr(specs, &path_str, "bias");
    texture.fallback = get_float4_attr(specs, &path_str, "fallback");

    // Source color space
    if let Some(color_space) = get_token_attr(specs, &path_str, "sourceColorSpace") {
        texture.source_color_space = SourceColorSpace::from_token(&color_space);
    }

    // ST connection (UV coordinates)
    texture.st_connection = get_connection(specs, &path_str, "st");

    Some(texture)
}

/// Parse a UsdPrimvarReader shader.
fn parse_primvar_reader(specs: &HashMap<SdfPath, Spec>, shader_path: &SdfPath) -> Option<UsdPrimvarReader> {
    let path_str = shader_path.to_string();

    let mut reader = UsdPrimvarReader {
        prim_path: path_str.clone(),
        ..Default::default()
    };

    // inputs:varname
    if let Some(varname) = get_token_attr(specs, &path_str, "varname") {
        reader.varname = varname;
    } else if let Some(varname) = get_string_attr(specs, &path_str, "varname") {
        reader.varname = varname;
    }

    // inputs:fallback (float2)
    reader.fallback = get_float2_attr(specs, &path_str, "fallback");

    Some(reader)
}

// =============================================================================
// Attribute Helpers
// =============================================================================

/// Parse a color input that may be connected to a texture.
fn parse_color_or_texture_slot(
    specs: &HashMap<SdfPath, Spec>,
    shader_path: &str,
    attr_name: &str,
    default: [f32; 4],
) -> TextureSlot {
    // Check for texture connection first
    if let Some(connection) = get_connection(specs, shader_path, attr_name) {
        let (texture_path, output) = parse_connection_path(&connection);
        return TextureSlot::Texture(TextureRef {
            texture_path,
            output,
        });
    }

    // Fall back to scalar value
    if let Some(color) = get_color3_attr(specs, shader_path, attr_name) {
        TextureSlot::Color([color[0], color[1], color[2], 1.0])
    } else {
        TextureSlot::Color(default)
    }
}

/// Parse a scalar input that may be connected to a texture.
fn parse_scalar_or_texture_slot(
    specs: &HashMap<SdfPath, Spec>,
    shader_path: &str,
    attr_name: &str,
    default: f32,
) -> TextureSlot {
    // Check for texture connection first
    if let Some(connection) = get_connection(specs, shader_path, attr_name) {
        let (texture_path, output) = parse_connection_path(&connection);
        return TextureSlot::Texture(TextureRef {
            texture_path,
            output,
        });
    }

    // Fall back to scalar value
    TextureSlot::Scalar(get_float_attr(specs, shader_path, attr_name).unwrap_or(default))
}

/// Parse an optional texture-only slot (like normal map).
fn parse_optional_texture_slot(specs: &HashMap<SdfPath, Spec>, shader_path: &str, attr_name: &str) -> Option<TextureSlot> {
    if let Some(connection) = get_connection(specs, shader_path, attr_name) {
        let (texture_path, output) = parse_connection_path(&connection);
        Some(TextureSlot::Texture(TextureRef {
            texture_path,
            output,
        }))
    } else {
        None
    }
}

/// Parse a connection path into (shader_path, output).
///
/// Input: `</Material/Texture.outputs:rgb>` or `/Material/Texture.outputs:rgb`
/// Output: (`/Material/Texture`, TextureOutput::RGB)
fn parse_connection_path(connection: &str) -> (String, TextureOutput) {
    let cleaned = connection.trim_start_matches('<').trim_end_matches('>');

    if let Some((shader_path, output_part)) = cleaned.split_once('.') {
        let output_name = output_part
            .trim_start_matches("outputs:")
            .split('.')
            .next()
            .unwrap_or("rgb");
        (shader_path.to_string(), TextureOutput::from_output_name(output_name))
    } else {
        (cleaned.to_string(), TextureOutput::RGB)
    }
}

/// Get a connection attribute value.
///
/// Connections in USD are stored at paths like:
/// `/Material/Shader.inputs:diffuseColor.connect`
/// with a `connectionPaths` field containing the target path.
fn get_connection(specs: &HashMap<SdfPath, Spec>, shader_path: &str, attr_name: &str) -> Option<String> {
    // Connections are stored with .connect suffix in the path
    let connect_path_str = format!("{}.inputs:{}.connect", shader_path, attr_name);
    let connect_path = SdfPath::new(&connect_path_str).ok()?;
    let attr_spec = specs.get(&connect_path)?;

    // Look for connectionPaths field
    if let Some(value) = attr_spec.fields.get(FieldKey::ConnectionPaths.as_str()) {
        if let Some(paths) = value.clone().try_as_path_list_op() {
            if let Some(first_path) = paths.explicit_items.first() {
                return Some(first_path.to_string());
            }
        }
    }

    None
}

/// Get a float attribute value.
fn get_float_attr(specs: &HashMap<SdfPath, Spec>, shader_path: &str, attr_name: &str) -> Option<f32> {
    let attr_path_str = format!("{}.inputs:{}", shader_path, attr_name);
    let attr_path = SdfPath::new(&attr_path_str).ok()?;
    let attr_spec = specs.get(&attr_path)?;

    if let Some(value) = attr_spec.fields.get(FieldKey::Default.as_str()) {
        if let Some(f) = value.clone().try_as_float() {
            return Some(f);
        }
        if let Some(d) = value.clone().try_as_double() {
            return Some(d as f32);
        }
    }

    None
}

/// Get an int attribute value.
fn get_int_attr(specs: &HashMap<SdfPath, Spec>, shader_path: &str, attr_name: &str) -> Option<i32> {
    let attr_path_str = format!("{}.inputs:{}", shader_path, attr_name);
    let attr_path = SdfPath::new(&attr_path_str).ok()?;
    let attr_spec = specs.get(&attr_path)?;

    if let Some(value) = attr_spec.fields.get(FieldKey::Default.as_str()) {
        if let Some(i) = value.clone().try_as_int() {
            return Some(i);
        }
    }

    None
}

/// Get a color3 attribute value.
fn get_color3_attr(specs: &HashMap<SdfPath, Spec>, shader_path: &str, attr_name: &str) -> Option<[f32; 3]> {
    let attr_path_str = format!("{}.inputs:{}", shader_path, attr_name);
    let attr_path = SdfPath::new(&attr_path_str).ok()?;
    let attr_spec = specs.get(&attr_path)?;

    if let Some(value) = attr_spec.fields.get(FieldKey::Default.as_str()) {
        if let Some(arr) = value.try_as_vec_3f_ref() {
            if arr.len() >= 3 {
                return Some([arr[0], arr[1], arr[2]]);
            }
        }
    }

    None
}

/// Get a float2 attribute value.
fn get_float2_attr(specs: &HashMap<SdfPath, Spec>, shader_path: &str, attr_name: &str) -> Option<[f32; 2]> {
    let attr_path_str = format!("{}.inputs:{}", shader_path, attr_name);
    let attr_path = SdfPath::new(&attr_path_str).ok()?;
    let attr_spec = specs.get(&attr_path)?;

    if let Some(value) = attr_spec.fields.get(FieldKey::Default.as_str()) {
        if let Some(arr) = value.try_as_vec_2f_ref() {
            if arr.len() >= 2 {
                return Some([arr[0], arr[1]]);
            }
        }
    }

    None
}

/// Get a float4 attribute value.
fn get_float4_attr(specs: &HashMap<SdfPath, Spec>, shader_path: &str, attr_name: &str) -> Option<[f32; 4]> {
    let attr_path_str = format!("{}.inputs:{}", shader_path, attr_name);
    let attr_path = SdfPath::new(&attr_path_str).ok()?;
    let attr_spec = specs.get(&attr_path)?;

    if let Some(value) = attr_spec.fields.get(FieldKey::Default.as_str()) {
        if let Some(arr) = value.try_as_vec_4f_ref() {
            if arr.len() >= 4 {
                return Some([arr[0], arr[1], arr[2], arr[3]]);
            }
        }
    }

    None
}

/// Get a token attribute value.
///
/// Token values can be stored as either Token or String depending on the parser.
fn get_token_attr(specs: &HashMap<SdfPath, Spec>, shader_path: &str, attr_name: &str) -> Option<String> {
    let attr_path_str = format!("{}.inputs:{}", shader_path, attr_name);
    let attr_path = SdfPath::new(&attr_path_str).ok()?;
    let attr_spec = specs.get(&attr_path)?;

    if let Some(value) = attr_spec.fields.get(FieldKey::Default.as_str()) {
        // Try token first
        if let Some(token) = value.try_as_token_ref() {
            return Some(token.to_string());
        }
        // Fall back to string (USDA parser stores tokens as strings)
        if let Some(s) = value.try_as_string_ref() {
            return Some(s.to_string());
        }
    }

    None
}

/// Get a string attribute value.
fn get_string_attr(specs: &HashMap<SdfPath, Spec>, shader_path: &str, attr_name: &str) -> Option<String> {
    let attr_path_str = format!("{}.inputs:{}", shader_path, attr_name);
    let attr_path = SdfPath::new(&attr_path_str).ok()?;
    let attr_spec = specs.get(&attr_path)?;

    if let Some(value) = attr_spec.fields.get(FieldKey::Default.as_str()) {
        if let Some(s) = value.try_as_string_ref() {
            return Some(s.to_string());
        }
        // Also try token (some parsers store strings as tokens)
        if let Some(token) = value.try_as_token_ref() {
            return Some(token.to_string());
        }
    }

    None
}

/// Get an asset path attribute value.
fn get_asset_attr(specs: &HashMap<SdfPath, Spec>, shader_path: &str, attr_name: &str) -> Option<String> {
    let attr_path_str = format!("{}.inputs:{}", shader_path, attr_name);
    let attr_path = SdfPath::new(&attr_path_str).ok()?;
    let attr_spec = specs.get(&attr_path)?;

    if let Some(value) = attr_spec.fields.get(FieldKey::Default.as_str()) {
        if let Some(asset) = value.clone().try_as_asset_path() {
            return Some(asset);
        }
        if let Some(s) = value.try_as_string_ref() {
            return Some(s.to_string());
        }
    }

    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_connection_path() {
        let (path, output) = parse_connection_path("</Material/Texture.outputs:rgb>");
        assert_eq!(path, "/Material/Texture");
        assert_eq!(output, TextureOutput::RGB);

        let (path, output) = parse_connection_path("/Mat/Tex.outputs:r");
        assert_eq!(path, "/Mat/Tex");
        assert_eq!(output, TextureOutput::R);
    }

    #[test]
    fn test_empty_specs_returns_no_materials() {
        let specs = HashMap::new();
        let materials = parse_materials(&specs);
        assert!(materials.is_empty());
    }
}
