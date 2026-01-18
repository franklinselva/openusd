//! MaterialX XML parser.
//!
//! Parses .mtlx files into structured data that can be used for USD composition
//! and conversion to other formats like glTF.

use anyhow::{Context, Result};
use roxmltree::{Document, Node};
use std::path::Path;

use super::types::*;

/// Parse a MaterialX file from disk.
pub fn parse_mtlx_file(path: &Path) -> Result<MtlxDocument> {
    let content = std::fs::read_to_string(path)
        .with_context(|| format!("Failed to read MaterialX file: {}", path.display()))?;
    parse_mtlx(&content)
}

/// Parse MaterialX XML content into a document structure.
pub fn parse_mtlx(content: &str) -> Result<MtlxDocument> {
    let doc = Document::parse(content).context("Failed to parse MaterialX XML")?;
    let root = doc.root_element();

    // Verify this is a MaterialX document
    if root.tag_name().name() != "materialx" {
        anyhow::bail!(
            "Not a MaterialX document: root element is '{}'",
            root.tag_name().name()
        );
    }

    let version = root.attribute("version").unwrap_or("1.38").to_string();
    let colorspace = root.attribute("colorspace").map(String::from);

    let mut mtlx_doc = MtlxDocument {
        version,
        colorspace,
        ..Default::default()
    };

    // Parse all child elements
    for child in root.children().filter(|n| n.is_element()) {
        match child.tag_name().name() {
            "nodegraph" => {
                if let Some(ng) = parse_nodegraph(&child) {
                    mtlx_doc.nodegraphs.insert(ng.name.clone(), ng);
                }
            }
            "standard_surface" => {
                if let Some(shader) = parse_standard_surface(&child) {
                    mtlx_doc.standard_surfaces.insert(shader.name.clone(), shader);
                }
            }
            "surfacematerial" => {
                if let Some(mat) = parse_surface_material(&child) {
                    mtlx_doc.materials.insert(mat.name.clone(), mat);
                }
            }
            other => {
                log::debug!("Skipping unknown MaterialX element: {}", other);
            }
        }
    }

    log::debug!(
        "Parsed MaterialX: {} nodegraphs, {} shaders, {} materials",
        mtlx_doc.nodegraphs.len(),
        mtlx_doc.standard_surfaces.len(),
        mtlx_doc.materials.len()
    );

    Ok(mtlx_doc)
}

/// Parse a nodegraph element.
fn parse_nodegraph(node: &Node) -> Option<NodeGraph> {
    let name = node.attribute("name")?.to_string();
    let mut ng = NodeGraph::new(name);

    for child in node.children().filter(|n| n.is_element()) {
        match child.tag_name().name() {
            "image" => {
                if let Some(img) = parse_image_node(&child) {
                    ng.image_nodes.insert(img.name.clone(), img);
                }
            }
            "normalmap" => {
                if let Some(nm) = parse_normalmap_node(&child) {
                    ng.normalmap_nodes.insert(nm.name.clone(), nm);
                }
            }
            "output" => {
                if let Some(out) = parse_graph_output(&child) {
                    ng.outputs.insert(out.name.clone(), out);
                }
            }
            _ => {
                // Skip other node types (multiply, add, etc.) for now
            }
        }
    }

    Some(ng)
}

/// Parse an image node.
fn parse_image_node(node: &Node) -> Option<ImageNode> {
    let name = node.attribute("name")?.to_string();
    let output_type = node.attribute("type").unwrap_or("color3").to_string();

    // Find the file input
    let mut file = String::new();
    let mut colorspace = None;

    for input in node.children().filter(|n| n.is_element() && n.tag_name().name() == "input") {
        if input.attribute("name") == Some("file") {
            file = input.attribute("value").unwrap_or("").to_string();
            colorspace = input.attribute("colorspace").map(String::from);
        }
    }

    if file.is_empty() {
        log::warn!("Image node '{}' has no file input", name);
        return None;
    }

    Some(ImageNode {
        name,
        output_type,
        file,
        colorspace,
    })
}

/// Parse a normalmap node.
fn parse_normalmap_node(node: &Node) -> Option<NormalMapNode> {
    let name = node.attribute("name")?.to_string();

    // Find the input node reference
    let mut input_node = String::new();

    for input in node.children().filter(|n| n.is_element() && n.tag_name().name() == "input") {
        if input.attribute("name") == Some("in") {
            input_node = input.attribute("nodename").unwrap_or("").to_string();
        }
    }

    if input_node.is_empty() {
        return None;
    }

    Some(NormalMapNode { name, input_node })
}

/// Parse a graph output.
fn parse_graph_output(node: &Node) -> Option<GraphOutput> {
    let name = node.attribute("name")?.to_string();
    let output_type = node.attribute("type").unwrap_or("color3").to_string();
    let nodename = node.attribute("nodename")?.to_string();

    Some(GraphOutput {
        name,
        output_type,
        nodename,
    })
}

/// Parse a standard_surface shader.
fn parse_standard_surface(node: &Node) -> Option<StandardSurface> {
    let name = node.attribute("name")?.to_string();
    let mut shader = StandardSurface {
        name,
        ..Default::default()
    };

    for input in node.children().filter(|n| n.is_element() && n.tag_name().name() == "input") {
        let input_name = match input.attribute("name") {
            Some(n) => n,
            None => continue,
        };

        let shader_input = parse_shader_input(&input);

        match input_name {
            "base_color" => shader.base_color = shader_input,
            "base" => shader.base = shader_input,
            "metalness" => shader.metalness = shader_input,
            "specular_roughness" => shader.specular_roughness = shader_input,
            "specular" => shader.specular = shader_input,
            "specular_color" => shader.specular_color = shader_input,
            "normal" => shader.normal = shader_input,
            "emission_color" => shader.emission_color = shader_input,
            "emission" => shader.emission = shader_input,
            "opacity" => shader.opacity = shader_input,
            "subsurface" => shader.subsurface = shader_input,
            "subsurface_color" => shader.subsurface_color = shader_input,
            "subsurface_radius" => shader.subsurface_radius = shader_input,
            "subsurface_scale" => shader.subsurface_scale = shader_input,
            "transmission" => shader.transmission = shader_input,
            "coat" => shader.coat = shader_input,
            "coat_roughness" => shader.coat_roughness = shader_input,
            _ => {
                log::debug!("Skipping unknown standard_surface input: {}", input_name);
            }
        }
    }

    Some(shader)
}

/// Parse a shader input (value or connection).
fn parse_shader_input(node: &Node) -> Option<ShaderInput> {
    // Check for nodegraph connection
    if let Some(nodegraph) = node.attribute("nodegraph") {
        let output = node.attribute("output").unwrap_or("out").to_string();
        return Some(ShaderInput::Connection {
            nodegraph: nodegraph.to_string(),
            output,
        });
    }

    // Check for direct node connection
    if let Some(nodename) = node.attribute("nodename") {
        return Some(ShaderInput::NodeConnection {
            nodename: nodename.to_string(),
        });
    }

    // Check for value
    if let Some(value_str) = node.attribute("value") {
        let input_type = node.attribute("type").unwrap_or("float");
        return parse_value(value_str, input_type);
    }

    None
}

/// Parse a value string based on its type.
fn parse_value(value_str: &str, value_type: &str) -> Option<ShaderInput> {
    match value_type {
        "float" => {
            let f: f32 = value_str.parse().ok()?;
            Some(ShaderInput::Value(ShaderValue::Float(f)))
        }
        "color3" | "vector3" => {
            let parts: Vec<f32> = value_str
                .split(',')
                .filter_map(|s| s.trim().parse().ok())
                .collect();
            if parts.len() >= 3 {
                let arr = [parts[0], parts[1], parts[2]];
                if value_type == "color3" {
                    Some(ShaderInput::Value(ShaderValue::Color3(arr)))
                } else {
                    Some(ShaderInput::Value(ShaderValue::Vector3(arr)))
                }
            } else {
                None
            }
        }
        "color4" => {
            let parts: Vec<f32> = value_str
                .split(',')
                .filter_map(|s| s.trim().parse().ok())
                .collect();
            if parts.len() >= 4 {
                Some(ShaderInput::Value(ShaderValue::Color4([
                    parts[0], parts[1], parts[2], parts[3],
                ])))
            } else {
                None
            }
        }
        _ => {
            // Try parsing as float
            if let Ok(f) = value_str.parse::<f32>() {
                Some(ShaderInput::Value(ShaderValue::Float(f)))
            } else {
                None
            }
        }
    }
}

/// Parse a surfacematerial element.
fn parse_surface_material(node: &Node) -> Option<SurfaceMaterial> {
    let name = node.attribute("name")?.to_string();

    // Find the surfaceshader input
    let mut shader_name = String::new();

    for input in node.children().filter(|n| n.is_element() && n.tag_name().name() == "input") {
        if input.attribute("name") == Some("surfaceshader") {
            shader_name = input.attribute("nodename").unwrap_or("").to_string();
        }
    }

    if shader_name.is_empty() {
        log::warn!("Surface material '{}' has no shader reference", name);
        return None;
    }

    Some(SurfaceMaterial { name, shader_name })
}

#[cfg(test)]
mod tests {
    use super::*;

    const SAMPLE_MTLX: &str = r#"<?xml version="1.0"?>
<materialx version="1.38" colorspace="lin_rec709">
  <nodegraph name="NG_Test">
    <image name="img_basecolor" type="color3">
      <input name="file" type="filename" value="tex/basecolor.jpg" colorspace="srgb_texture" />
    </image>
    <image name="img_metallic" type="float">
      <input name="file" type="filename" value="tex/metallic.jpg" />
    </image>
    <image name="img_roughness" type="float">
      <input name="file" type="filename" value="tex/roughness.jpg" />
    </image>
    <image name="img_normal" type="vector3">
      <input name="file" type="filename" value="tex/normal.jpg" />
    </image>
    <normalmap name="normalmap1" type="vector3">
      <input name="in" type="vector3" nodename="img_normal" />
    </normalmap>
    <output name="base_color_output" type="color3" nodename="img_basecolor" />
    <output name="metalness_output" type="float" nodename="img_metallic" />
    <output name="roughness_output" type="float" nodename="img_roughness" />
    <output name="normal_output" type="vector3" nodename="normalmap1" />
  </nodegraph>

  <standard_surface name="TestShader" type="surfaceshader">
    <input name="base_color" type="color3" nodegraph="NG_Test" output="base_color_output" />
    <input name="metalness" type="float" nodegraph="NG_Test" output="metalness_output" />
    <input name="specular_roughness" type="float" nodegraph="NG_Test" output="roughness_output" />
    <input name="normal" type="vector3" nodegraph="NG_Test" output="normal_output" />
    <input name="base" type="float" value="1.0" />
  </standard_surface>

  <surfacematerial name="M_Test" type="material">
    <input name="surfaceshader" type="surfaceshader" nodename="TestShader" />
  </surfacematerial>
</materialx>"#;

    #[test]
    fn test_parse_mtlx() {
        let doc = parse_mtlx(SAMPLE_MTLX).unwrap();

        assert_eq!(doc.version, "1.38");
        assert_eq!(doc.colorspace, Some("lin_rec709".to_string()));
        assert_eq!(doc.nodegraphs.len(), 1);
        assert_eq!(doc.standard_surfaces.len(), 1);
        assert_eq!(doc.materials.len(), 1);
    }

    #[test]
    fn test_parse_nodegraph() {
        let doc = parse_mtlx(SAMPLE_MTLX).unwrap();
        let ng = doc.nodegraphs.get("NG_Test").unwrap();

        assert_eq!(ng.image_nodes.len(), 4);
        assert_eq!(ng.normalmap_nodes.len(), 1);
        assert_eq!(ng.outputs.len(), 4);

        let img = ng.image_nodes.get("img_basecolor").unwrap();
        assert_eq!(img.file, "tex/basecolor.jpg");
        assert_eq!(img.colorspace, Some("srgb_texture".to_string()));
    }

    #[test]
    fn test_parse_standard_surface() {
        let doc = parse_mtlx(SAMPLE_MTLX).unwrap();
        let shader = doc.standard_surfaces.get("TestShader").unwrap();

        assert!(shader.base_color.is_some());
        assert!(shader.metalness.is_some());
        assert!(shader.specular_roughness.is_some());
        assert!(shader.normal.is_some());

        // Check base value
        if let Some(ShaderInput::Value(ShaderValue::Float(f))) = &shader.base {
            assert_eq!(*f, 1.0);
        } else {
            panic!("Expected float value for base");
        }

        // Check connection
        if let Some(ShaderInput::Connection { nodegraph, output }) = &shader.base_color {
            assert_eq!(nodegraph, "NG_Test");
            assert_eq!(output, "base_color_output");
        } else {
            panic!("Expected connection for base_color");
        }
    }

    #[test]
    fn test_parse_material() {
        let doc = parse_mtlx(SAMPLE_MTLX).unwrap();
        let mat = doc.materials.get("M_Test").unwrap();

        assert_eq!(mat.name, "M_Test");
        assert_eq!(mat.shader_name, "TestShader");
    }

    #[test]
    fn test_resolve_texture_path() {
        let doc = parse_mtlx(SAMPLE_MTLX).unwrap();
        let shader = doc.standard_surfaces.get("TestShader").unwrap();

        // Resolve base color texture
        if let Some(input) = &shader.base_color {
            let path = doc.resolve_texture_path(input);
            assert_eq!(path, Some("tex/basecolor.jpg".to_string()));
        }

        // Resolve normal map (goes through normalmap node)
        if let Some(input) = &shader.normal {
            let path = doc.resolve_texture_path(input);
            assert_eq!(path, Some("tex/normal.jpg".to_string()));
        }
    }
}
