use anyhow::{anyhow, bail, ensure, Context, Result};
use std::{any::type_name, str::FromStr};

use crate::sdf;
use crate::usda::token::Token;

use super::types::Type;

/// Primitive value parsing functions.
impl<'a> super::super::Parser<'a> {
    /// Parse basic types and roles.
    /// See
    /// - <https://openusd.org/dev/api/_usd__page__datatypes.html#Usd_Basic_Datatypes>
    /// - <https://openusd.org/dev/api/_usd__page__datatypes.html#Usd_Roles>
    pub(in crate::usda::parser) fn parse_data_type(ty: &str) -> Result<Type> {
        let data_type = match ty {
            // Bool
            "bool" => Type::Bool,
            "bool[]" => Type::BoolVec,

            // Ints
            "uchar" => Type::Uchar,
            "uchar[]" => Type::UcharVec,
            "int" => Type::Int,
            "int2" => Type::Int2,
            "int3" => Type::Int3,
            "int4" => Type::Int4,
            "int[]" => Type::IntVec,
            "int2[]" => Type::Int2Vec,
            "int3[]" => Type::Int3Vec,
            "int4[]" => Type::Int4Vec,
            "uint" => Type::Uint,
            "int64" => Type::Int64,
            "int64[]" => Type::Int64Vec,
            "uint64" => Type::Uint64,

            // Half
            "half" => Type::Half,
            "half2" | "texCoord2h" => Type::Half2,
            "half3" | "point3h" | "normal3h" | "vector3h" | "color3h" | "texCoord3h" => Type::Half3,
            "half4" | "color4h" => Type::Half4,
            "half[]" => Type::HalfVec,
            "half2[]" | "texCoord2h[]" => Type::Half2Vec,
            "half3[]" | "point3h[]" | "normal3h[]" | "vector3h[]" | "color3h[]" | "texCoord3h[]" => Type::Half3Vec,
            "half4[]" | "color4h[]" => Type::Half4Vec,

            // Float
            "float" => Type::Float,
            "float2" | "texCoord2f" => Type::Float2,
            "float3" | "point3f" | "normal3f" | "vector3f" | "color3f" | "texCoord3f" => Type::Float3,
            "float4" | "color4f" => Type::Float4,
            "float[]" => Type::FloatVec,
            "float2[]" | "texCoord2f[]" => Type::Float2Vec,
            "float3[]" | "point3f[]" | "normal3f[]" | "vector3f[]" | "color3f[]" | "texCoord3f[]" => Type::Float3Vec,
            "float4[]" | "color4f[]" => Type::Float4Vec,

            // Double
            "double" => Type::Double,
            "double2" | "texCoord2d" => Type::Double2,
            "double3" | "point3d" | "normal3d" | "vector3d" | "color3d" | "texCoord3d" => Type::Double3,
            "double4" | "color4d" => Type::Double4,
            "double[]" => Type::DoubleVec,
            "double2[]" | "texCoord2d[]" => Type::Double2Vec,
            "double3[]" | "point3d[]" | "normal3d[]" | "vector3d[]" | "color3d[]" | "texCoord3d[]" => Type::Double3Vec,
            "double4[]" => Type::Double4Vec,

            // Matrices
            "matrix2d" | "matrix2d[]" => Type::Matrix2d,
            "matrix3d" | "matrix3d[]" => Type::Matrix3d,
            "matrix4d" | "matrix4d[]" | "frame4d" | "frame4d[]" => Type::Matrix4d,

            // Quats
            "quatd" => Type::Quatd,
            "quatf" => Type::Quatf,
            "quath" => Type::Quath,
            "quatd[]" => Type::QuatdVec,
            "quatf[]" => Type::QuatfVec,
            "quath[]" => Type::QuathVec,

            // String, tokens
            "string" | "token" => Type::String,
            "string[]" | "token[]" => Type::TokenVec,
            "asset" => Type::Asset,
            "asset[]" => Type::AssetVec,

            "dictionary" => Type::Dictionary,

            _ => bail!("Unsupported data type: {ty}"),
        };

        Ok(data_type)
    }

    /// Parse single token as `T` which can be deserialized from string (such as `int`, `float`, etc).
    pub(in crate::usda::parser) fn parse_token<T: FromStr>(&mut self) -> Result<T>
    where
        <T as FromStr>::Err: std::fmt::Debug,
    {
        let token = self.fetch_next()?;
        let value_str = match token {
            Token::Number(s) | Token::Identifier(s) | Token::String(s) | Token::NamespacedIdentifier(s) => s,
            Token::Inf => "inf",
            Token::Punctuation('-') => {
                // Handle negative inf
                let next = self.fetch_next()?;
                if matches!(next, Token::Inf) {
                    "-inf"
                } else {
                    bail!("Expected number after '-', got {next:?}")
                }
            }
            Token::Punctuation('+') => {
                // Handle positive inf
                let next = self.fetch_next()?;
                if matches!(next, Token::Inf) {
                    "inf"
                } else {
                    bail!("Expected number after '+', got {next:?}")
                }
            }
            _ => bail!("Expected a number, identifier, or string, got {token:?}"),
        };
        let value = T::from_str(value_str)
            .map_err(|err| anyhow!("Failed to parse {} from '{}': {:?}", type_name::<T>(), value_str, err))?;

        Ok(value)
    }

    /// Parse USD's flexible boolean literal forms (identifiers, numeric, or string).
    pub(in crate::usda::parser) fn parse_bool(&mut self) -> Result<bool> {
        let token = self.fetch_next()?;
        match token {
            Token::Identifier(value) | Token::NamespacedIdentifier(value) => match value {
                "true" => Ok(true),
                "false" => Ok(false),
                other => bail!("Unexpected identifier for bool literal: {other}"),
            },
            Token::Number(value) => {
                let parsed = value.parse::<f64>().context("Unable to parse numeric bool")?;
                if parsed == 0.0 {
                    Ok(false)
                } else if parsed == 1.0 {
                    Ok(true)
                } else {
                    bail!("Numeric bool literals must be 0 or 1, got {value}");
                }
            }
            Token::String(value) => match value {
                "true" => Ok(true),
                "false" => Ok(false),
                other => bail!("Unexpected string for bool literal: {other}"),
            },
            other => bail!("Unexpected token for bool literal: {other:?}"),
        }
    }

    /// Parse an array of booleans, reusing the permissive literal parsing rules.
    pub(in crate::usda::parser) fn parse_bool_array(&mut self) -> Result<Vec<bool>> {
        let mut out = Vec::new();
        self.parse_array_fn(|this| {
            out.push(this.parse_bool()?);
            Ok(())
        })?;
        Ok(out)
    }

    /// Parse an asset path reference.
    pub(in crate::usda::parser) fn parse_asset_path(&mut self) -> Result<String> {
        let token = self.fetch_next()?;
        token
            .try_as_asset_ref()
            .map(|value| value.to_owned())
            .ok_or_else(|| anyhow!("Asset reference expected"))
    }

    /// Parse an array of asset paths.
    pub(in crate::usda::parser) fn parse_asset_path_array(&mut self) -> Result<Vec<String>> {
        let mut result = Vec::new();
        self.parse_array_fn(|this| {
            result.push(this.parse_asset_path()?);
            Ok(())
        })?;
        Ok(result)
    }

    /// Parse `subLayers` entries along with their optional `(offset/scale)` metadata.
    pub(in crate::usda::parser) fn parse_sublayers(&mut self) -> Result<(sdf::Value, sdf::Value)> {
        let mut sublayers = Vec::new();
        let mut sublayer_offsets = Vec::new();

        self.parse_array_fn(|this| {
            let asset_path = this
                .fetch_next()?
                .try_as_asset_ref()
                .ok_or_else(|| anyhow!("Asset ref expected, got {:?}", this.peek_next()))?;
            sublayers.push(asset_path.to_string());

            let mut layer_offset = sdf::LayerOffset::default();
            if this.is_next(Token::Punctuation('(')) {
                let mut offset = None;
                let mut scale = None;

                this.parse_seq_fn(';', |this, _| {
                    let token = this.fetch_next()?;
                    this.ensure_pun('=')?;
                    let value = this.parse_value(Type::Double)?;
                    match token {
                        Token::Offset => {
                            ensure!(offset.is_none(), "offset specified twice");
                            offset = Some(value);
                        }
                        Token::Scale => {
                            ensure!(scale.is_none(), "scale specified twice");
                            scale = Some(value);
                        }
                        _ => bail!("Unexpected token type: {token:?}"),
                    }
                    Ok(())
                })?;

                if let Some(offset) = offset {
                    layer_offset.offset = offset.try_as_double().context("Unexpected offset type, want double")?;
                }
                if let Some(scale) = scale {
                    layer_offset.scale = scale.try_as_double().context("")?;
                }
            }
            sublayer_offsets.push(layer_offset);
            Ok(())
        })?;

        debug_assert_eq!(sublayers.len(), sublayer_offsets.len());

        Ok((
            sdf::Value::StringVec(sublayers),
            sdf::Value::LayerOffsetVec(sublayer_offsets),
        ))
    }
}
