//! Type definitions for USD shading schemas.

use std::collections::HashMap;

/// Wrap mode for texture sampling.
///
/// Corresponds to UsdUVTexture's `inputs:wrapS` and `inputs:wrapT`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum WrapMode {
    /// Repeat the texture (default).
    #[default]
    Repeat,
    /// Clamp to edge.
    Clamp,
    /// Mirror the texture.
    Mirror,
    /// Use black outside [0,1] range.
    Black,
}

impl WrapMode {
    /// Parse wrap mode from USD token string.
    pub fn from_token(token: &str) -> Self {
        match token.to_lowercase().as_str() {
            "clamp" => WrapMode::Clamp,
            "mirror" => WrapMode::Mirror,
            "black" => WrapMode::Black,
            "repeat" | _ => WrapMode::Repeat,
        }
    }
}

/// Source color space for texture interpretation.
///
/// Corresponds to UsdUVTexture's `inputs:sourceColorSpace`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum SourceColorSpace {
    /// Automatically determine color space (default).
    #[default]
    Auto,
    /// Linear/raw color space (no gamma correction).
    Raw,
    /// sRGB color space (gamma corrected).
    SRGB,
}

impl SourceColorSpace {
    /// Parse color space from USD token string.
    pub fn from_token(token: &str) -> Self {
        match token.to_lowercase().as_str() {
            "raw" | "linear" => SourceColorSpace::Raw,
            "srgb" => SourceColorSpace::SRGB,
            "auto" | _ => SourceColorSpace::Auto,
        }
    }
}

/// Output channel selection for texture sampling.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum TextureOutput {
    /// RGB color output.
    #[default]
    RGB,
    /// Red channel only.
    R,
    /// Green channel only.
    G,
    /// Blue channel only.
    B,
    /// Alpha channel only.
    A,
}

impl TextureOutput {
    /// Parse output from connection path suffix.
    ///
    /// Connection paths look like `</Material/Texture.outputs:rgb>`.
    pub fn from_output_name(name: &str) -> Self {
        match name.to_lowercase().as_str() {
            "r" => TextureOutput::R,
            "g" => TextureOutput::G,
            "b" => TextureOutput::B,
            "a" => TextureOutput::A,
            "rgb" | _ => TextureOutput::RGB,
        }
    }
}

/// A parsed UsdUVTexture shader node.
///
/// UsdUVTexture reads a texture file and samples it using UV coordinates.
/// It supports various inputs for controlling how the texture is read and processed.
///
/// # USD Schema Reference
/// See: <https://openusd.org/docs/api/class_usd_shade_shader.html>
#[derive(Debug, Clone)]
pub struct UsdUVTexture {
    /// The prim path of this shader (e.g., `/Material/DiffuseTexture`).
    pub prim_path: String,

    /// The texture file path (`inputs:file`).
    /// This is the asset path as written in USD, may be relative.
    pub file: String,

    /// Horizontal wrap mode (`inputs:wrapS`).
    pub wrap_s: WrapMode,

    /// Vertical wrap mode (`inputs:wrapT`).
    pub wrap_t: WrapMode,

    /// Scale factor applied to texture values (`inputs:scale`).
    /// RGBA values, applied as: `output = texture * scale + bias`.
    pub scale: Option<[f32; 4]>,

    /// Bias added to texture values (`inputs:bias`).
    /// RGBA values, applied as: `output = texture * scale + bias`.
    pub bias: Option<[f32; 4]>,

    /// Fallback value if texture cannot be read (`inputs:fallback`).
    pub fallback: Option<[f32; 4]>,

    /// Source color space interpretation (`inputs:sourceColorSpace`).
    pub source_color_space: SourceColorSpace,

    /// Connection to UV coordinate source (`inputs:st.connect`).
    /// Points to a UsdPrimvarReader's output.
    pub st_connection: Option<String>,
}

impl Default for UsdUVTexture {
    fn default() -> Self {
        Self {
            prim_path: String::new(),
            file: String::new(),
            wrap_s: WrapMode::Repeat,
            wrap_t: WrapMode::Repeat,
            scale: None,
            bias: None,
            fallback: None,
            source_color_space: SourceColorSpace::Auto,
            st_connection: None,
        }
    }
}

impl UsdUVTexture {
    /// Get the UV set index this texture uses.
    ///
    /// Resolves the st_connection to find the primvar reader,
    /// then maps the varname to a UV set index.
    pub fn get_uv_set(&self, primvar_readers: &[UsdPrimvarReader]) -> u32 {
        if let Some(connection) = &self.st_connection {
            // Extract shader path from connection (e.g., "</Mat/Reader.outputs:result>" -> "/Mat/Reader")
            let shader_path = connection
                .trim_start_matches('<')
                .trim_end_matches('>')
                .split('.')
                .next()
                .unwrap_or("");

            // Find the primvar reader
            if let Some(reader) = primvar_readers.iter().find(|r| r.prim_path == shader_path) {
                return resolve_uv_set(&reader.varname);
            }
        }
        0 // Default to TEXCOORD_0
    }
}

/// A parsed UsdPrimvarReader shader node.
///
/// UsdPrimvarReader reads primitive variables (primvars) from geometry.
/// Most commonly used to read UV coordinates for texture mapping.
///
/// # Common varnames
/// - `st` or `st0` - Primary UV set (TEXCOORD_0)
/// - `st1` - Secondary UV set (TEXCOORD_1)
/// - `UVMap` - Blender-style UV name
#[derive(Debug, Clone)]
pub struct UsdPrimvarReader {
    /// The prim path of this shader.
    pub prim_path: String,

    /// The primvar name to read (`inputs:varname`).
    /// Common values: "st", "st0", "st1", "UVMap".
    pub varname: String,

    /// Fallback value if primvar is not found (`inputs:fallback`).
    pub fallback: Option<[f32; 2]>,
}

impl Default for UsdPrimvarReader {
    fn default() -> Self {
        Self {
            prim_path: String::new(),
            varname: "st".to_string(),
            fallback: None,
        }
    }
}

/// Resolve UV set index from primvar variable name.
///
/// Maps common primvar names to glTF TEXCOORD indices.
pub fn resolve_uv_set(varname: &str) -> u32 {
    match varname.to_lowercase().as_str() {
        "st" | "st0" | "uvmap" | "texture_uv" | "map1" => 0,
        "st1" | "uvmap1" | "map2" => 1,
        "st2" | "uvmap2" | "map3" => 2,
        "st3" | "uvmap3" | "map4" => 3,
        _ => 0,
    }
}

/// A texture reference with connection info.
///
/// Used in material slots to reference a texture and specify
/// which output channel to use.
#[derive(Debug, Clone)]
pub struct TextureRef {
    /// Path to the UsdUVTexture shader.
    pub texture_path: String,
    /// Which output channel to use.
    pub output: TextureOutput,
}

/// A material input that can be either a texture or a constant value.
#[derive(Debug, Clone)]
pub enum TextureSlot {
    /// Connected to a texture.
    Texture(TextureRef),
    /// Constant color value (RGBA).
    Color([f32; 4]),
    /// Constant scalar value.
    Scalar(f32),
}

impl Default for TextureSlot {
    fn default() -> Self {
        TextureSlot::Color([0.5, 0.5, 0.5, 1.0])
    }
}

/// A parsed UsdPreviewSurface shader.
///
/// UsdPreviewSurface is the standard physically-based surface shader in USD.
/// It maps closely to glTF's PBR metallic-roughness model.
///
/// # USD Schema Reference
/// See: <https://openusd.org/docs/api/class_usd_preview_surface.html>
#[derive(Debug, Clone)]
pub struct UsdPreviewSurface {
    /// The prim path of this shader.
    pub prim_path: String,

    /// Base color / diffuse color (`inputs:diffuseColor`).
    /// Default: (0.18, 0.18, 0.18).
    pub diffuse_color: TextureSlot,

    /// Metallic factor (`inputs:metallic`).
    /// 0.0 = dielectric, 1.0 = metallic. Default: 0.0.
    pub metallic: TextureSlot,

    /// Roughness factor (`inputs:roughness`).
    /// 0.0 = smooth/glossy, 1.0 = rough/matte. Default: 0.5.
    pub roughness: TextureSlot,

    /// Normal map (`inputs:normal`).
    pub normal: Option<TextureSlot>,

    /// Ambient occlusion (`inputs:occlusion`).
    pub occlusion: Option<TextureSlot>,

    /// Emissive color (`inputs:emissiveColor`).
    /// Default: (0, 0, 0).
    pub emissive_color: TextureSlot,

    /// Opacity/alpha (`inputs:opacity`).
    /// Default: 1.0.
    pub opacity: TextureSlot,

    /// Alpha cutoff threshold (`inputs:opacityThreshold`).
    /// When > 0, enables alpha masking. Default: 0.0.
    pub opacity_threshold: f32,

    /// Index of refraction (`inputs:ior`).
    /// Default: 1.5.
    pub ior: f32,

    /// Clearcoat intensity (`inputs:clearcoat`).
    /// Default: 0.0.
    pub clearcoat: f32,

    /// Clearcoat roughness (`inputs:clearcoatRoughness`).
    /// Default: 0.01.
    pub clearcoat_roughness: f32,

    /// Specular level (`inputs:specular`).
    /// Default: 0.5.
    pub specular: f32,

    /// Use specular workflow instead of metallic (`inputs:useSpecularWorkflow`).
    /// Default: false (use metallic workflow).
    pub use_specular_workflow: bool,
}

impl Default for UsdPreviewSurface {
    fn default() -> Self {
        Self {
            prim_path: String::new(),
            diffuse_color: TextureSlot::Color([0.18, 0.18, 0.18, 1.0]),
            metallic: TextureSlot::Scalar(0.0),
            roughness: TextureSlot::Scalar(0.5),
            normal: None,
            occlusion: None,
            emissive_color: TextureSlot::Color([0.0, 0.0, 0.0, 1.0]),
            opacity: TextureSlot::Scalar(1.0),
            opacity_threshold: 0.0,
            ior: 1.5,
            clearcoat: 0.0,
            clearcoat_roughness: 0.01,
            specular: 0.5,
            use_specular_workflow: false,
        }
    }
}

/// A complete parsed USD Material.
///
/// Contains the surface shader and all referenced texture/primvar reader nodes.
#[derive(Debug, Clone)]
pub struct Material {
    /// The prim path of this material (e.g., `/Materials/WoodMaterial`).
    pub prim_path: String,

    /// The material name (last component of prim path).
    pub name: String,

    /// The surface shader (UsdPreviewSurface).
    pub surface: Option<UsdPreviewSurface>,

    /// All UsdUVTexture shaders referenced by this material.
    pub textures: Vec<UsdUVTexture>,

    /// All UsdPrimvarReader shaders referenced by this material.
    pub primvar_readers: Vec<UsdPrimvarReader>,

    /// Raw texture file paths discovered (for convenience).
    /// Maps texture shader path to file path.
    pub texture_files: HashMap<String, String>,
}

impl Default for Material {
    fn default() -> Self {
        Self {
            prim_path: String::new(),
            name: String::new(),
            surface: None,
            textures: Vec::new(),
            primvar_readers: Vec::new(),
            texture_files: HashMap::new(),
        }
    }
}

impl Material {
    /// Get all unique texture file paths used by this material.
    pub fn get_texture_paths(&self) -> Vec<&str> {
        self.textures
            .iter()
            .map(|t| t.file.as_str())
            .filter(|p| !p.is_empty())
            .collect()
    }

    /// Find a texture shader by prim path.
    pub fn get_texture(&self, path: &str) -> Option<&UsdUVTexture> {
        self.textures.iter().find(|t| t.prim_path == path)
    }

    /// Find a primvar reader by prim path.
    pub fn get_primvar_reader(&self, path: &str) -> Option<&UsdPrimvarReader> {
        self.primvar_readers.iter().find(|r| r.prim_path == path)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_wrap_mode_from_token() {
        assert_eq!(WrapMode::from_token("repeat"), WrapMode::Repeat);
        assert_eq!(WrapMode::from_token("Clamp"), WrapMode::Clamp);
        assert_eq!(WrapMode::from_token("MIRROR"), WrapMode::Mirror);
        assert_eq!(WrapMode::from_token("black"), WrapMode::Black);
        assert_eq!(WrapMode::from_token("unknown"), WrapMode::Repeat);
    }

    #[test]
    fn test_source_color_space_from_token() {
        assert_eq!(SourceColorSpace::from_token("auto"), SourceColorSpace::Auto);
        assert_eq!(SourceColorSpace::from_token("raw"), SourceColorSpace::Raw);
        assert_eq!(SourceColorSpace::from_token("sRGB"), SourceColorSpace::SRGB);
        assert_eq!(SourceColorSpace::from_token("linear"), SourceColorSpace::Raw);
    }

    #[test]
    fn test_texture_output_from_name() {
        assert_eq!(TextureOutput::from_output_name("rgb"), TextureOutput::RGB);
        assert_eq!(TextureOutput::from_output_name("r"), TextureOutput::R);
        assert_eq!(TextureOutput::from_output_name("G"), TextureOutput::G);
        assert_eq!(TextureOutput::from_output_name("a"), TextureOutput::A);
    }

    #[test]
    fn test_resolve_uv_set() {
        assert_eq!(resolve_uv_set("st"), 0);
        assert_eq!(resolve_uv_set("st0"), 0);
        assert_eq!(resolve_uv_set("st1"), 1);
        assert_eq!(resolve_uv_set("UVMap"), 0);
        assert_eq!(resolve_uv_set("st2"), 2);
    }

    #[test]
    fn test_material_get_texture_paths() {
        let mut mat = Material::default();
        mat.textures.push(UsdUVTexture {
            file: "diffuse.png".to_string(),
            ..Default::default()
        });
        mat.textures.push(UsdUVTexture {
            file: "normal.png".to_string(),
            ..Default::default()
        });
        mat.textures.push(UsdUVTexture {
            file: String::new(), // Empty, should be filtered
            ..Default::default()
        });

        let paths = mat.get_texture_paths();
        assert_eq!(paths.len(), 2);
        assert!(paths.contains(&"diffuse.png"));
        assert!(paths.contains(&"normal.png"));
    }
}
