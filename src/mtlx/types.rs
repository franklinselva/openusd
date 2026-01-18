//! MaterialX data types for representing parsed .mtlx files.
//!
//! These types map to MaterialX XML elements and can be converted to USD shade types.

use std::collections::HashMap;

/// A parsed MaterialX document.
#[derive(Debug, Clone, Default)]
pub struct MtlxDocument {
    /// MaterialX version (e.g., "1.38")
    pub version: String,
    /// Default colorspace for the document
    pub colorspace: Option<String>,
    /// Node graphs containing texture and operation nodes
    pub nodegraphs: HashMap<String, NodeGraph>,
    /// Standard Surface shaders
    pub standard_surfaces: HashMap<String, StandardSurface>,
    /// Surface material definitions
    pub materials: HashMap<String, SurfaceMaterial>,
}

/// A node graph containing image nodes and outputs.
///
/// Node graphs define networks of operations that produce values
/// consumed by shaders (e.g., texture sampling, math operations).
#[derive(Debug, Clone, Default)]
pub struct NodeGraph {
    /// Name of the node graph
    pub name: String,
    /// Image texture nodes
    pub image_nodes: HashMap<String, ImageNode>,
    /// Normal map nodes
    pub normalmap_nodes: HashMap<String, NormalMapNode>,
    /// Graph outputs that can be connected to shaders
    pub outputs: HashMap<String, GraphOutput>,
}

/// An image texture sampling node.
#[derive(Debug, Clone)]
pub struct ImageNode {
    /// Node name
    pub name: String,
    /// Output type: "color3", "float", "vector3"
    pub output_type: String,
    /// Texture file path (relative to .mtlx file)
    pub file: String,
    /// Colorspace override (e.g., "srgb_texture", "lin_rec709")
    pub colorspace: Option<String>,
}

/// A normal map processing node.
#[derive(Debug, Clone)]
pub struct NormalMapNode {
    /// Node name
    pub name: String,
    /// Input node name
    pub input_node: String,
}

/// An output from a node graph.
#[derive(Debug, Clone)]
pub struct GraphOutput {
    /// Output name (e.g., "base_color_output")
    pub name: String,
    /// Output type: "color3", "float", "vector3"
    pub output_type: String,
    /// Name of the node this output reads from
    pub nodename: String,
}

/// Autodesk Standard Surface shader.
///
/// This is the most common shader type in MaterialX and maps well to glTF PBR.
/// See: https://autodesk.github.io/standard-surface/
#[derive(Debug, Clone, Default)]
pub struct StandardSurface {
    /// Shader name
    pub name: String,
    /// Base color (diffuse albedo)
    pub base_color: Option<ShaderInput>,
    /// Base weight (multiplier for base_color)
    pub base: Option<ShaderInput>,
    /// Metalness (0 = dielectric, 1 = metal)
    pub metalness: Option<ShaderInput>,
    /// Specular roughness (0 = mirror, 1 = diffuse)
    pub specular_roughness: Option<ShaderInput>,
    /// Specular weight
    pub specular: Option<ShaderInput>,
    /// Specular color
    pub specular_color: Option<ShaderInput>,
    /// Normal map
    pub normal: Option<ShaderInput>,
    /// Emission color
    pub emission_color: Option<ShaderInput>,
    /// Emission weight
    pub emission: Option<ShaderInput>,
    /// Opacity (1 = opaque, 0 = transparent)
    pub opacity: Option<ShaderInput>,
    /// Subsurface weight
    pub subsurface: Option<ShaderInput>,
    /// Subsurface color
    pub subsurface_color: Option<ShaderInput>,
    /// Subsurface radius
    pub subsurface_radius: Option<ShaderInput>,
    /// Subsurface scale
    pub subsurface_scale: Option<ShaderInput>,
    /// Transmission weight (for glass-like materials)
    pub transmission: Option<ShaderInput>,
    /// Coat weight (clearcoat layer)
    pub coat: Option<ShaderInput>,
    /// Coat roughness
    pub coat_roughness: Option<ShaderInput>,
}

/// Input to a shader - either a constant value or a connection to a node graph.
#[derive(Debug, Clone)]
pub enum ShaderInput {
    /// A constant value
    Value(ShaderValue),
    /// A connection to a node graph output
    Connection {
        /// Name of the node graph
        nodegraph: String,
        /// Name of the output within the node graph
        output: String,
    },
    /// A direct connection to a node (within same scope)
    NodeConnection {
        /// Name of the connected node
        nodename: String,
    },
}

/// Constant shader value types.
#[derive(Debug, Clone)]
pub enum ShaderValue {
    /// Single float value
    Float(f32),
    /// RGB color (0-1 range)
    Color3([f32; 3]),
    /// 3D vector
    Vector3([f32; 3]),
    /// RGBA color (0-1 range)
    Color4([f32; 4]),
}

impl ShaderValue {
    /// Convert to float, returning first component for vectors.
    pub fn as_float(&self) -> f32 {
        match self {
            ShaderValue::Float(f) => *f,
            ShaderValue::Color3(c) => c[0],
            ShaderValue::Vector3(v) => v[0],
            ShaderValue::Color4(c) => c[0],
        }
    }

    /// Convert to Color3, expanding float to grayscale.
    pub fn as_color3(&self) -> [f32; 3] {
        match self {
            ShaderValue::Float(f) => [*f, *f, *f],
            ShaderValue::Color3(c) => *c,
            ShaderValue::Vector3(v) => *v,
            ShaderValue::Color4(c) => [c[0], c[1], c[2]],
        }
    }
}

/// A surface material definition that references a shader.
#[derive(Debug, Clone)]
pub struct SurfaceMaterial {
    /// Material name (e.g., "M_King_B")
    pub name: String,
    /// Name of the surface shader this material uses
    pub shader_name: String,
}

impl MtlxDocument {
    /// Create a new empty MaterialX document.
    pub fn new() -> Self {
        Self::default()
    }

    /// Get a material by name.
    pub fn get_material(&self, name: &str) -> Option<&SurfaceMaterial> {
        self.materials.get(name)
    }

    /// Get a standard surface shader by name.
    pub fn get_shader(&self, name: &str) -> Option<&StandardSurface> {
        self.standard_surfaces.get(name)
    }

    /// Get a node graph by name.
    pub fn get_nodegraph(&self, name: &str) -> Option<&NodeGraph> {
        self.nodegraphs.get(name)
    }

    /// Resolve a texture file path from a shader input connection.
    ///
    /// Returns the texture file path if the input connects to an image node.
    pub fn resolve_texture_path(&self, input: &ShaderInput) -> Option<String> {
        match input {
            ShaderInput::Connection { nodegraph, output } => {
                let ng = self.nodegraphs.get(nodegraph)?;
                let graph_output = ng.outputs.get(output)?;

                // Check if output connects to normalmap node
                if let Some(normalmap) = ng.normalmap_nodes.get(&graph_output.nodename) {
                    // Follow to the image node
                    let image = ng.image_nodes.get(&normalmap.input_node)?;
                    Some(image.file.clone())
                } else {
                    // Direct connection to image node
                    let image = ng.image_nodes.get(&graph_output.nodename)?;
                    Some(image.file.clone())
                }
            }
            _ => None,
        }
    }
}

impl NodeGraph {
    /// Create a new empty node graph.
    pub fn new(name: String) -> Self {
        Self {
            name,
            image_nodes: HashMap::new(),
            normalmap_nodes: HashMap::new(),
            outputs: HashMap::new(),
        }
    }
}
