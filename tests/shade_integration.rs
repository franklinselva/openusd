//! Integration tests for the shade module.

use openusd::composition::ComposedLayer;
use openusd::shade::{parse_materials, resolve_uv_set, SourceColorSpace, TextureOutput, TextureSlot, WrapMode};
use std::path::PathBuf;

fn fixture_path(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("fixtures").join(name)
}

#[test]
fn test_parse_textured_material() {
    let path = fixture_path("material_with_textures.usda");
    let composed = ComposedLayer::open(&path).expect("Failed to load USD file");
    let materials = parse_materials(&composed.specs);

    // Should find 3 materials
    assert_eq!(materials.len(), 3, "Expected 3 materials, found {}", materials.len());

    // Find the TexturedMaterial
    let textured = materials
        .iter()
        .find(|m| m.name == "TexturedMaterial")
        .expect("TexturedMaterial not found");

    // Check textures were parsed
    assert_eq!(textured.textures.len(), 4, "Expected 4 textures");
    assert_eq!(textured.primvar_readers.len(), 1, "Expected 1 primvar reader");

    // Check surface shader
    let surface = textured.surface.as_ref().expect("No surface shader");

    // Diffuse should be connected to texture
    match &surface.diffuse_color {
        TextureSlot::Texture(tex_ref) => {
            assert!(
                tex_ref.texture_path.contains("DiffuseTexture"),
                "Expected diffuse connected to DiffuseTexture"
            );
            assert_eq!(tex_ref.output, TextureOutput::RGB);
        }
        _ => panic!("Expected diffuse to be a texture"),
    }

    // Metallic should be connected to texture (R channel)
    match &surface.metallic {
        TextureSlot::Texture(tex_ref) => {
            assert!(tex_ref.texture_path.contains("MetallicTexture"));
            assert_eq!(tex_ref.output, TextureOutput::R);
        }
        _ => panic!("Expected metallic to be a texture"),
    }

    // Check UsdUVTexture details
    let diffuse_tex = textured
        .textures
        .iter()
        .find(|t| t.prim_path.contains("DiffuseTexture"))
        .expect("DiffuseTexture not found");

    assert_eq!(diffuse_tex.file, "textures/diffuse.png");
    assert_eq!(diffuse_tex.wrap_s, WrapMode::Repeat);
    assert_eq!(diffuse_tex.wrap_t, WrapMode::Repeat);
    assert_eq!(diffuse_tex.source_color_space, SourceColorSpace::SRGB);
    assert!(diffuse_tex.st_connection.is_some());

    // Check metallic texture has clamp wrapping
    let metallic_tex = textured
        .textures
        .iter()
        .find(|t| t.prim_path.contains("MetallicTexture"))
        .expect("MetallicTexture not found");

    assert_eq!(metallic_tex.wrap_s, WrapMode::Clamp);
    assert_eq!(metallic_tex.wrap_t, WrapMode::Clamp);
    assert_eq!(metallic_tex.source_color_space, SourceColorSpace::Raw);
}

#[test]
fn test_parse_simple_material() {
    let path = fixture_path("material_with_textures.usda");
    let composed = ComposedLayer::open(&path).expect("Failed to load USD file");
    let materials = parse_materials(&composed.specs);

    // Find SimpleMaterial
    let simple = materials
        .iter()
        .find(|m| m.name == "SimpleMaterial")
        .expect("SimpleMaterial not found");

    // Should have no textures
    assert!(simple.textures.is_empty(), "SimpleMaterial should have no textures");

    // Check surface shader values
    let surface = simple.surface.as_ref().expect("No surface shader");

    // Diffuse should be a color
    match &surface.diffuse_color {
        TextureSlot::Color(color) => {
            assert!((color[0] - 0.8).abs() < 0.01, "Red channel mismatch");
            assert!((color[1] - 0.2).abs() < 0.01, "Green channel mismatch");
            assert!((color[2] - 0.2).abs() < 0.01, "Blue channel mismatch");
        }
        _ => panic!("Expected diffuse to be a color"),
    }

    // Check scalar values
    match &surface.metallic {
        TextureSlot::Scalar(v) => assert!((v - 0.5).abs() < 0.01),
        _ => panic!("Expected metallic to be scalar"),
    }

    match &surface.roughness {
        TextureSlot::Scalar(v) => assert!((v - 0.3).abs() < 0.01),
        _ => panic!("Expected roughness to be scalar"),
    }

    // Check emissive color
    match &surface.emissive_color {
        TextureSlot::Color(color) => {
            assert!((color[0] - 0.1).abs() < 0.01, "Emissive red mismatch");
        }
        _ => panic!("Expected emissive to be a color"),
    }

    // Check opacity threshold
    assert!((surface.opacity_threshold - 0.5).abs() < 0.01);
}

#[test]
fn test_multi_uv_material() {
    let path = fixture_path("material_with_textures.usda");
    let composed = ComposedLayer::open(&path).expect("Failed to load USD file");
    let materials = parse_materials(&composed.specs);

    // Find MultiUVMaterial
    let multi_uv = materials
        .iter()
        .find(|m| m.name == "MultiUVMaterial")
        .expect("MultiUVMaterial not found");

    // Check primvar reader has st1
    assert_eq!(multi_uv.primvar_readers.len(), 1);
    let reader = &multi_uv.primvar_readers[0];
    assert_eq!(reader.varname, "st1");

    // Verify UV set resolution
    assert_eq!(resolve_uv_set("st1"), 1);
}

#[test]
fn test_material_texture_paths() {
    let path = fixture_path("material_with_textures.usda");
    let composed = ComposedLayer::open(&path).expect("Failed to load USD file");
    let materials = parse_materials(&composed.specs);

    let textured = materials
        .iter()
        .find(|m| m.name == "TexturedMaterial")
        .expect("TexturedMaterial not found");

    // Get all texture paths
    let paths = textured.get_texture_paths();
    assert_eq!(paths.len(), 4);
    assert!(paths.contains(&"textures/diffuse.png"));
    assert!(paths.contains(&"textures/metallic.png"));
    assert!(paths.contains(&"textures/roughness.png"));
    assert!(paths.contains(&"textures/normal.png"));
}

#[test]
fn test_texture_uv_set_resolution() {
    let path = fixture_path("material_with_textures.usda");
    let composed = ComposedLayer::open(&path).expect("Failed to load USD file");
    let materials = parse_materials(&composed.specs);

    let textured = materials
        .iter()
        .find(|m| m.name == "TexturedMaterial")
        .expect("TexturedMaterial not found");

    let diffuse_tex = textured
        .textures
        .iter()
        .find(|t| t.prim_path.contains("DiffuseTexture"))
        .expect("DiffuseTexture not found");

    // Get UV set for this texture
    let uv_set = diffuse_tex.get_uv_set(&textured.primvar_readers);
    assert_eq!(uv_set, 0, "TexturedMaterial uses st (UV set 0)");

    // MultiUV material uses st1
    let multi_uv = materials
        .iter()
        .find(|m| m.name == "MultiUVMaterial")
        .expect("MultiUVMaterial not found");

    let diffuse_tex = multi_uv
        .textures
        .iter()
        .find(|t| t.prim_path.contains("DiffuseTexture"))
        .expect("DiffuseTexture not found");

    let uv_set = diffuse_tex.get_uv_set(&multi_uv.primvar_readers);
    assert_eq!(uv_set, 1, "MultiUVMaterial uses st1 (UV set 1)");
}
