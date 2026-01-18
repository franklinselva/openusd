//! MaterialX (.mtlx) parser module.
//!
//! MaterialX is an open standard for representing materials and looks in computer graphics.
//! This module provides parsing support for MaterialX XML files, which can be referenced
//! from USD files.
//!
//! # Supported Elements
//!
//! - `nodegraph` - Node graphs containing texture and operation nodes
//! - `standard_surface` - Autodesk Standard Surface shader
//! - `surfacematerial` - Material definitions
//!
//! # Example
//!
//! ```ignore
//! use openusd::mtlx::parse_mtlx_file;
//! use std::path::Path;
//!
//! let mtlx = parse_mtlx_file(Path::new("material.mtlx"))?;
//!
//! for (name, material) in &mtlx.materials {
//!     println!("Material: {}", name);
//!     if let Some(shader) = mtlx.get_shader(&material.shader_name) {
//!         if let Some(base_color) = &shader.base_color {
//!             if let Some(tex_path) = mtlx.resolve_texture_path(base_color) {
//!                 println!("  Base color texture: {}", tex_path);
//!             }
//!         }
//!     }
//! }
//! ```

mod parser;
mod types;

pub use parser::{parse_mtlx, parse_mtlx_file};
pub use types::*;
