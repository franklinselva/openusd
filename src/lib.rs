//! `openusd` is a native Rust implementation of USD family of formats.
//!
//! # Modules
//!
//! - `sdf` - Scene Description Foundations: core data types and traits
//! - `usda` - Text format (.usda) reader
//! - `usdc` - Binary format (.usdc) reader
//! - `usdz` - Archive format (.usdz) handler
//! - `composition` - Layer composition (sublayers, references, payloads)
//! - `shade` - Material and shader schema parsing

pub mod composition;
pub mod expr;
pub mod sdf;
pub mod shade;
pub mod usda;
pub mod usdc;
pub mod usdz;

pub use half::f16;
