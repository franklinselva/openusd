pub mod arrays;
pub mod primitives;
pub mod types;

use anyhow::Result;

use crate::sdf;
pub use types::Type;

/// Value parsing dispatcher.
impl<'a> super::Parser<'a> {
    /// Decode a typed value based on USD's scalar/array/role type tables.
    pub(super) fn parse_value(&mut self, ty: Type) -> Result<sdf::Value> {
        let value = match ty {
            // Bool
            Type::Bool => sdf::Value::Bool(self.parse_bool()?),
            Type::BoolVec => sdf::Value::BoolVec(self.parse_bool_array()?),

            // Asset paths
            Type::Asset => sdf::Value::AssetPath(self.parse_asset_path()?),
            Type::AssetVec => sdf::Value::StringVec(self.parse_asset_path_array()?),

            // Ints
            Type::Uchar => sdf::Value::Uchar(self.parse_token()?),
            Type::UcharVec => sdf::Value::UcharVec(self.parse_array()?),

            Type::Int => sdf::Value::Int(self.parse_token()?),
            Type::Int2 => sdf::Value::Vec2i(self.parse_tuple::<_, 2>()?.into()),
            Type::Int3 => sdf::Value::Vec3i(self.parse_tuple::<_, 3>()?.into()),
            Type::Int4 => sdf::Value::Vec4i(self.parse_tuple::<_, 4>()?.into()),
            Type::IntVec => sdf::Value::IntVec(self.parse_array()?),
            Type::Int2Vec => sdf::Value::Vec2i(self.parse_array_of_tuples::<_, 2>()?),
            Type::Int3Vec => sdf::Value::Vec3i(self.parse_array_of_tuples::<_, 3>()?),
            Type::Int4Vec => sdf::Value::Vec4i(self.parse_array_of_tuples::<_, 4>()?),
            Type::Uint => sdf::Value::Uint(self.parse_token()?),
            Type::Int64 => sdf::Value::Int64(self.parse_token()?),
            Type::Int64Vec => sdf::Value::Int64Vec(self.parse_array()?),
            Type::Uint64 => sdf::Value::Uint64(self.parse_token()?),

            // Half
            Type::Half => sdf::Value::Half(self.parse_token()?),
            Type::Half2 => sdf::Value::HalfVec(self.parse_tuple::<_, 2>()?.into()),
            Type::Half3 => sdf::Value::Vec3h(self.parse_tuple::<_, 3>()?.into()),
            Type::Half4 => sdf::Value::Vec4h(self.parse_tuple::<_, 4>()?.into()),

            Type::HalfVec => sdf::Value::HalfVec(self.parse_array()?),
            Type::Half2Vec => sdf::Value::Vec2h(self.parse_array_of_tuples::<_, 2>()?),
            Type::Half3Vec => sdf::Value::Vec3h(self.parse_array_of_tuples::<_, 3>()?),
            Type::Half4Vec => sdf::Value::Vec4h(self.parse_array_of_tuples::<_, 4>()?),

            // Float
            Type::Float => sdf::Value::Float(self.parse_token()?),
            Type::Float2 => sdf::Value::Vec2f(self.parse_tuple::<_, 2>()?.into()),
            Type::Float3 => sdf::Value::Vec3f(self.parse_tuple::<_, 3>()?.into()),
            Type::Float4 => sdf::Value::Vec4f(self.parse_tuple::<_, 4>()?.into()),
            Type::FloatVec => sdf::Value::FloatVec(self.parse_array()?),
            Type::Float2Vec => sdf::Value::Vec2f(self.parse_array_of_tuples::<_, 2>()?),
            Type::Float3Vec => sdf::Value::Vec3f(self.parse_array_of_tuples::<_, 3>()?),
            Type::Float4Vec => sdf::Value::Vec4f(self.parse_array_of_tuples::<_, 4>()?),

            // Double
            Type::Double => sdf::Value::Double(self.parse_token()?),
            Type::Double2 => sdf::Value::Vec2d(self.parse_tuple::<_, 2>()?.into()),
            Type::Double3 => sdf::Value::Vec3d(self.parse_tuple::<_, 3>()?.into()),
            Type::Double4 => sdf::Value::Vec4d(self.parse_tuple::<_, 4>()?.into()),
            Type::DoubleVec => sdf::Value::DoubleVec(self.parse_array()?),
            Type::Double2Vec => sdf::Value::Vec2d(self.parse_array_of_tuples::<_, 2>()?),
            Type::Double3Vec => sdf::Value::Vec3d(self.parse_array_of_tuples::<_, 3>()?),
            Type::Double4Vec => sdf::Value::Vec4d(self.parse_array_of_tuples::<_, 4>()?),

            // Quats
            Type::Quath => sdf::Value::Quath(self.parse_tuple::<_, 4>()?.into()),
            Type::Quatf => sdf::Value::Quatf(self.parse_tuple::<_, 4>()?.into()),
            Type::Quatd => sdf::Value::Quatd(self.parse_tuple::<_, 4>()?.into()),
            Type::QuathVec => sdf::Value::Quath(self.parse_array_of_tuples::<_, 4>()?),
            Type::QuatfVec => sdf::Value::Quatf(self.parse_array_of_tuples::<_, 4>()?),
            Type::QuatdVec => sdf::Value::Quatd(self.parse_array_of_tuples::<_, 4>()?),

            // String and tokens
            Type::String => sdf::Value::String(self.fetch_str()?.to_owned()),
            Type::Token => sdf::Value::Token(self.fetch_str()?.to_owned()),

            Type::StringVec => sdf::Value::StringVec(self.parse_array()?),
            Type::TokenVec => sdf::Value::TokenVec(self.parse_array()?),

            Type::Matrix2d => sdf::Value::Matrix2d(self.parse_matrix_value::<2>()?),
            Type::Matrix3d => sdf::Value::Matrix3d(self.parse_matrix_value::<3>()?),
            Type::Matrix4d => sdf::Value::Matrix4d(self.parse_matrix_value::<4>()?),

            Type::Dictionary => self.parse_dictionary()?,
        };

        Ok(value)
    }
}
