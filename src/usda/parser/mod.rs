mod composition;
mod error;
mod hierarchy;
mod metadata;
mod token_ops;
mod value;

use logos::Logos;
use std::iter::Peekable;
use std::ops::Range;

use crate::usda::token::Token;

pub use error::ErrorHighlight;

/// Parser translates a list of tokens into structured data.
pub struct Parser<'a> {
    iter: Peekable<logos::SpannedIter<'a, Token<'a>>>,
    source: &'a str,
    last_span: Option<Range<usize>>,
}

impl<'a> Parser<'a> {
    /// Create a new parser from source text.
    pub fn new(data: &'a str) -> Self {
        Self {
            iter: Token::lexer(data).spanned().peekable(),
            source: data,
            last_span: None,
        }
    }

    /// Returns a highlight for the most recent token span processed by the parser.
    pub fn last_error_highlight(&self) -> Option<ErrorHighlight> {
        self.last_span
            .clone()
            .and_then(|span| ErrorHighlight::from_span(self.source, span))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sdf::{self, schema::FieldKey};
    use std::fs;

    #[test]
    fn parse_empty_array() {
        let mut parser = Parser::new("[]");
        let array = parser.parse_array::<u32>().unwrap();
        assert!(array.is_empty());
    }

    #[test]
    fn parse_tuple() {
        let mut parser = Parser::new("(1, 2, 3)");
        let result = parser.parse_tuple::<u32, 3>().unwrap();
        assert_eq!(result, [1_u32, 2, 3]);
    }

    #[test]
    fn parse_array() {
        let mut parser = Parser::new("[1, 2, 3]");
        let result = parser.parse_array::<u32>().unwrap();
        assert_eq!(result, vec![1_u32, 2, 3]);
    }

    #[test]
    fn parse_array_of_tuples() {
        let mut parser = Parser::new("[(1, 2), (3, 4)]");
        let result = parser.parse_array_of_tuples::<u32, 2>().unwrap();
        assert_eq!(result, vec![1_u32, 2, 3, 4]);
    }

    #[test]
    fn parse_pseudo_root() {
        let mut parser = Parser::new(
            r#"
            #usda 1.0
            (
                doc = """test string"""

                upAxis = "Y"
                metersPerUnit = 0.01

                defaultPrim = "World"
            )
            "#,
        );

        let pseudo_root = parser.read_pseudo_root().unwrap();

        assert!(pseudo_root
            .fields
            .get("doc")
            .and_then(|v| v.try_as_string_ref())
            .unwrap()
            .eq("test string"));

        assert!(pseudo_root
            .fields
            .get("upAxis")
            .and_then(|v| v.try_as_token_ref())
            .unwrap()
            .eq("Y"));
    }

    #[test]
    fn parse_dictionary_with_quoted_namespace_keys() {
        let mut parser = Parser::new(
            r#"
#usda 1.0
(
    customLayerData = {
        dictionary renderSettings = {
            bool "rtx:raytracing:fractionalCutoutOpacity" = 1
            token "rtx:rendermode" = "PathTracing"
        }
    }
)
"#,
        );

        let pseudo_root = parser.read_pseudo_root().unwrap();
        let custom_layer_data = pseudo_root
            .fields
            .get("customLayerData")
            .expect("customLayerData metadata present");
        let dict = match custom_layer_data {
            sdf::Value::Dictionary(dict) => dict,
            other => panic!("customLayerData parsed as unexpected value: {other:?}"),
        };

        let render_settings = match dict.get("renderSettings") {
            Some(sdf::Value::Dictionary(d)) => d,
            other => panic!("renderSettings parsed as unexpected value: {other:?}"),
        };

        assert!(render_settings.contains_key("rtx:raytracing:fractionalCutoutOpacity"));
        assert!(render_settings.contains_key("rtx:rendermode"));
    }

    #[test]
    fn parse_connection_fixture() {
        let data = fs::read_to_string(concat!(env!("CARGO_MANIFEST_DIR"), "/fixtures/connection.usda"))
            .expect("read connection fixture");
        let mut parser = Parser::new(&data);
        let specs = parser.parse().expect("connection.usda should parse");

        let board_mat_path = sdf::path("/boardMat").unwrap();
        assert!(
            specs.contains_key(&board_mat_path),
            "Should have /boardMat Material prim"
        );

        let primvar_attr = specs
            .get(&sdf::path("/boardMat.inputs:frame:stPrimvarName").unwrap())
            .expect("inputs:frame:stPrimvarName attribute");
        assert!(matches!(
            primvar_attr.fields.get(FieldKey::Default.as_str()),
            Some(sdf::Value::Token(v)) | Some(sdf::Value::String(v)) if v == "st"
        ));

        let surface_conn = specs
            .get(&sdf::path("/boardMat.outputs:surface.connect").unwrap())
            .expect("outputs:surface.connect attribute");
        let conn_paths = surface_conn
            .fields
            .get(FieldKey::ConnectionPaths.as_str())
            .and_then(|v| v.try_as_path_list_op_ref())
            .expect("connection paths");
        assert_eq!(conn_paths.explicit_items.len(), 1);
        assert_eq!(
            conn_paths.explicit_items[0].as_str(),
            "/TexModel/boardMat/PBRShader.outputs:surface"
        );

        let pbr_shader = specs
            .get(&sdf::path("/boardMat/PBRShader").unwrap())
            .expect("PBRShader prim");
        assert!(pbr_shader.fields.contains_key(FieldKey::TypeName.as_str()));

        let diffuse_conn = specs
            .get(&sdf::path("/boardMat/PBRShader.inputs:diffuseColor.connect").unwrap())
            .expect("inputs:diffuseColor.connect attribute");
        let diffuse_paths = diffuse_conn
            .fields
            .get(FieldKey::ConnectionPaths.as_str())
            .and_then(|v| v.try_as_path_list_op_ref())
            .expect("diffuseColor connection paths");
        assert_eq!(diffuse_paths.explicit_items.len(), 1);
    }

    #[test]
    fn parse_reference_fixture() {
        let data = fs::read_to_string(concat!(env!("CARGO_MANIFEST_DIR"), "/fixtures/reference.usda"))
            .expect("read reference fixture");
        let mut parser = Parser::new(&data);
        let specs = parser.parse().expect("reference.usda should parse");

        // Verify MarbleCollection exists
        let collection_path = sdf::path("/MarbleCollection").unwrap();
        assert!(
            specs.contains_key(&collection_path),
            "Should have /MarbleCollection prim"
        );

        // Verify Marble_Red with references
        let marble_red_path = sdf::path("/MarbleCollection/Marble_Red").unwrap();
        let marble_red = specs.get(&marble_red_path).expect("Marble_Red prim");

        // Should have references field
        let references = marble_red
            .fields
            .get(FieldKey::References.as_str())
            .expect("Marble_Red should have references");
        assert!(
            matches!(references, sdf::Value::ReferenceListOp(_)),
            "References should be a ReferenceListOp"
        );

        // Verify transform attribute exists
        let translate_attr = specs
            .get(&sdf::path("/MarbleCollection/Marble_Red.xformOp:translate").unwrap())
            .expect("xformOp:translate attribute");
        assert!(translate_attr.fields.contains_key(FieldKey::Default.as_str()));
    }

    #[test]
    fn parse_fields_fixture() {
        let data = fs::read_to_string(concat!(env!("CARGO_MANIFEST_DIR"), "/fixtures/fields.usda"))
            .expect("read fields fixture");
        let mut parser = Parser::new(&data);
        let specs = parser.parse().expect("fields.usda should parse");

        // Verify pseudo-root metadata
        let pseudo_root = specs.get(&sdf::path("/").unwrap()).expect("pseudo-root");
        assert!(pseudo_root.fields.contains_key("defaultPrim"));
        assert!(pseudo_root.fields.contains_key("upAxis"));
        assert!(pseudo_root.fields.contains_key("metersPerUnit"));

        // Verify World prim exists
        let world_path = sdf::path("/World").unwrap();
        assert!(specs.contains_key(&world_path), "Should have /World prim");

        // Verify boolean attribute
        let flip_normals = specs
            .get(&sdf::path("/World.flipNormals").unwrap())
            .expect("flipNormals attribute");
        let default_val = flip_normals.fields.get(FieldKey::Default.as_str());
        assert!(matches!(default_val, Some(sdf::Value::Bool(true))));

        // Verify int array attribute
        let face_counts = specs
            .get(&sdf::path("/World.faceVertexCounts").unwrap())
            .expect("faceVertexCounts attribute");
        assert!(face_counts.fields.contains_key(FieldKey::Default.as_str()));

        // Verify points attribute (point3f[])
        let points = specs
            .get(&sdf::path("/World.points").unwrap())
            .expect("points attribute");
        assert!(points.fields.contains_key(FieldKey::Default.as_str()));

        // Verify xformOpOrder (uniform token[])
        let xform_order = specs
            .get(&sdf::path("/World.xformOpOrder").unwrap())
            .expect("xformOpOrder attribute");
        assert!(xform_order.fields.contains_key(FieldKey::Default.as_str()));
    }

    #[test]
    fn parse_payload_fixture() {
        let data = fs::read_to_string(concat!(env!("CARGO_MANIFEST_DIR"), "/fixtures/payload.usda"))
            .expect("read payload fixture");
        let mut parser = Parser::new(&data);
        let specs = parser.parse().expect("payload.usda should parse");

        // Verify MySphere1 with payload
        let sphere1_path = sdf::path("/MySphere1").unwrap();
        let sphere1 = specs.get(&sphere1_path).expect("MySphere1 prim");

        // Should have payload field
        let payload1 = sphere1
            .fields
            .get(FieldKey::Payload.as_str())
            .expect("MySphere1 should have payload");
        assert!(
            matches!(payload1, sdf::Value::PayloadListOp(_)),
            "Payload should be a PayloadListOp"
        );

        // Verify MySphere2 with prepend payload
        let sphere2_path = sdf::path("/MySphere2").unwrap();
        let sphere2 = specs.get(&sphere2_path).expect("MySphere2 prim");

        let payload2 = sphere2
            .fields
            .get(FieldKey::Payload.as_str())
            .expect("MySphere2 should have payload");
        assert!(
            matches!(payload2, sdf::Value::PayloadListOp(_)),
            "Payload should be a PayloadListOp"
        );
    }
}
