//! UsdShade module for parsing USD material and shader schemas.
//!
//! This module provides structured types for USD's shading concepts:
//! - `Material` - A complete USD material with surface shader and textures
//! - `UsdPreviewSurface` - The standard USD preview surface shader
//! - `UsdUVTexture` - Texture sampling shader node
//! - `UsdPrimvarReader` - Primvar (UV coordinate) reader node
//!
//! # Example
//!
//! ```ignore
//! use openusd::shade::parse_materials;
//! use openusd::composition::ComposedLayer;
//!
//! let composed = ComposedLayer::open("model.usd")?;
//! let materials = parse_materials(&composed.specs);
//!
//! for mat in materials {
//!     println!("Material: {}", mat.name);
//!     if let Some(surface) = &mat.surface {
//!         println!("  Roughness: {:?}", surface.roughness);
//!     }
//! }
//! ```

mod parser;
mod types;

pub use parser::parse_materials;
pub use types::*;
