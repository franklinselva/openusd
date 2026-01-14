use anyhow::{bail, ensure, Context, Result};
use std::collections::HashMap;

use crate::sdf;
use crate::sdf::schema::FieldKey;
use crate::usda::token::Token;

use super::value::types::{keyword_lexeme, Type};

/// Metadata and dictionary parsing functions.
impl<'a> super::Parser<'a> {
    /// Parse the metadata block attached to an attribute and stash entries on the spec.
    pub(super) fn parse_property_metadata(&mut self, spec: &mut sdf::Spec) -> Result<()> {
        self.ensure_pun('(')?;

        loop {
            if self.is_next(Token::Punctuation(')')) {
                self.fetch_next()?;
                break;
            }

            let name_token = self.fetch_next()?;
            let name = match name_token {
                Token::Identifier(s) | Token::NamespacedIdentifier(s) => s.to_owned(),
                Token::CustomData => "customData".to_owned(),
                Token::Doc => FieldKey::Documentation.as_str().to_owned(),
                // Allow other keywords as metadata keys
                other => {
                    if let Some(lexeme) = keyword_lexeme(&other) {
                        lexeme.to_owned()
                    } else {
                        bail!("Unexpected attribute metadata name token: {other:?}")
                    }
                }
            };

            self.ensure_pun('=')?;
            let value = self
                .parse_property_metadata_value()
                .with_context(|| format!("Unable to parse attribute metadata value for {name}"))?;
            spec.fields.insert(name, value);

            if self.is_next(Token::Punctuation(',')) {
                self.fetch_next()?;
            }
        }

        Ok(())
    }

    /// Parse a single attribute metadata value (scalar or array) from within a metadata block.
    pub(super) fn parse_property_metadata_value(&mut self) -> Result<sdf::Value> {
        // Handle array case first by peeking, so parse_array_fn can consume the '['
        if self.is_next(Token::Punctuation('[')) {
            let mut values = Vec::new();
            self.parse_array_fn(|this| {
                let entry = this.fetch_next()?;
                let value = match entry {
                    Token::String(v) => v.to_owned(),
                    Token::Identifier(v) | Token::NamespacedIdentifier(v) | Token::Number(v) => v.to_owned(),
                    other => bail!("Unsupported metadata array element: {other:?}"),
                };
                values.push(value);
                Ok(())
            })?;
            return Ok(sdf::Value::StringVec(values));
        }

        // Handle dictionary case by peeking, so parse_dictionary can consume the '{'
        if self.is_next(Token::Punctuation('{')) {
            return self.parse_dictionary();
        }

        let token = self.fetch_next()?;
        match token {
            Token::String(value) => Ok(sdf::Value::String(value.to_owned())),
            Token::Identifier(value) | Token::NamespacedIdentifier(value) => Ok(sdf::Value::Token(value.to_owned())),
            Token::Number(raw) => {
                if let Ok(int) = raw.parse::<i64>() {
                    Ok(sdf::Value::Int64(int))
                } else if let Ok(float) = raw.parse::<f64>() {
                    Ok(sdf::Value::Double(float))
                } else {
                    bail!("Unable to parse numeric metadata value: {raw}");
                }
            }
            other => bail!("Unsupported property metadata value token: {other:?}"),
        }
    }

    /// Check if a name is a valid USD data type.
    #[inline]
    fn is_type_hint_name(name: &str) -> bool {
        Self::parse_data_type(name).is_ok()
    }

    /// Parse a dictionary value from `{` to `}`.
    pub(super) fn parse_dictionary(&mut self) -> Result<sdf::Value> {
        self.ensure_pun('{').context("Dictionary must start with {")?;

        let mut dict = HashMap::new();

        loop {
            // Check for closing brace
            if self.is_next(Token::Punctuation('}')) {
                self.fetch_next()?;
                break;
            }

            // Parse the type (optional) or key
            let first_token = self.fetch_next()?;

            // Check if this is a type declaration (e.g., "string", "dictionary", "double3")
            let (_type_hint, key_token) = match first_token {
                Token::Identifier(name) if Self::is_type_hint_name(name) => {
                    // This is a type declaration, next token is the key
                    let key = self.fetch_next()?;
                    (Some(first_token), key)
                }
                Token::Dictionary => {
                    // This is a type declaration, next token is the key
                    let key = self.fetch_next()?;
                    (Some(first_token), key)
                }
                _ => (None, first_token),
            };

            let key = match key_token {
                Token::Identifier(s) | Token::NamespacedIdentifier(s) | Token::String(s) => s.to_owned(),
                // Allow keywords as dictionary keys by converting them to strings
                other => {
                    if let Some(lexeme) = keyword_lexeme(&other) {
                        lexeme.to_owned()
                    } else {
                        bail!("Expected identifier as dictionary key, got: {other:?}")
                    }
                }
            };

            self.ensure_pun('=')?;

            // Parse the value recursively
            let value = if let Some(type_hint_token) = _type_hint {
                let ty = match type_hint_token {
                    Token::Dictionary => Type::Dictionary,
                    Token::Identifier(type_name) => Self::parse_data_type(type_name)
                        .with_context(|| format!("Unable to parse dictionary value type {type_name}"))?,
                    other => bail!("Unsupported dictionary type hint: {other:?}"),
                };
                self.parse_value(ty)?
            } else {
                self.parse_property_metadata_value()?
            };
            dict.insert(key, value);

            // Handle optional trailing comma or newline
            if self.is_next(Token::Punctuation('}')) {
                self.fetch_next()?;
                break;
            }
        }

        Ok(sdf::Value::Dictionary(dict))
    }

    /// Parse prim metadata, optionally starting with a pre-fetched token.
    pub(super) fn read_prim_metadata(&mut self, spec: &mut sdf::Spec, first: Option<Token<'a>>) -> Result<()> {
        let mut current = first;

        loop {
            if self.is_next(Token::Punctuation(')')) || self.is_next(Token::Punctuation('{')) {
                break;
            }

            let token = match current.take() {
                Some(token) => token,
                None => self.fetch_next()?,
            };

            self.read_prim_metadata_entry(token, spec)
                .context("Unable to parse prim metadata entry")?;
        }

        Ok(())
    }

    /// Parse a single prim metadata assignment, honoring list ops for supported fields.
    pub(super) fn read_prim_metadata_entry(&mut self, token: Token<'a>, spec: &mut sdf::Spec) -> Result<()> {
        let (list_op, name_token) = match token {
            Token::Add | Token::Append | Token::Delete | Token::Prepend | Token::Reorder => {
                let name = self.fetch_next()?;
                (Some(token), name)
            }
            _ => (None, token),
        };

        let name = match name_token {
            Token::Identifier(s) | Token::NamespacedIdentifier(s) => s,
            Token::Kind => FieldKey::Kind.as_str(),
            Token::References => FieldKey::References.as_str(),
            Token::Payload => FieldKey::Payload.as_str(),
            Token::Inherits => FieldKey::InheritPaths.as_str(),
            Token::CustomData => "customData",
            Token::Doc => FieldKey::Documentation.as_str(),
            other => bail!("Unexpected metadata name token: {other:?}"),
        };

        self.ensure_pun('=')?;

        match name {
            n if n == FieldKey::Active.as_str() => {
                let value = self.parse_token::<bool>().context("Unable to parse active flag")?;
                spec.add(FieldKey::Active, sdf::Value::Bool(value));
            }
            "apiSchemas" => {
                let values = self.parse_token_list().context("Unable to parse apiSchemas list")?;
                let list_op = self
                    .apply_list_op(list_op, values)
                    .context("Unable to build apiSchemas listOp")?;
                spec.add("apiSchemas", sdf::Value::TokenListOp(list_op));
            }
            n if n == FieldKey::References.as_str() => {
                let references = self.parse_reference_list().context("Unable to parse references")?;
                let list_op = self
                    .apply_list_op(list_op, references)
                    .context("Unable to build references listOp")?;
                spec.add(FieldKey::References, sdf::Value::ReferenceListOp(list_op));
            }
            n if n == FieldKey::Payload.as_str() => {
                let payloads = self.parse_payload_list().context("Unable to parse payloads")?;
                let list_op = self
                    .apply_list_op(list_op, payloads)
                    .context("Unable to build payload listOp")?;
                spec.add(FieldKey::Payload, sdf::Value::PayloadListOp(list_op));
            }
            n if n == FieldKey::InheritPaths.as_str() => {
                let paths = if self.is_next(Token::Punctuation('[')) {
                    let mut collected = Vec::new();
                    self.parse_array_fn(|this| {
                        collected.push(this.parse_inherit_path()?);
                        Ok(())
                    })?;
                    collected
                } else {
                    vec![self.parse_inherit_path()?]
                };
                let list_op = self
                    .apply_list_op(list_op, paths)
                    .context("Unable to build inherits listOp")?;
                spec.add(FieldKey::InheritPaths, sdf::Value::PathListOp(list_op));
            }
            n if n == FieldKey::Kind.as_str() => {
                ensure!(list_op.is_none(), "kind metadata does not support list ops");
                let value = self.parse_token::<String>().context("Unable to parse kind metadata")?;
                spec.add(FieldKey::Kind, sdf::Value::Token(value));
            }
            "customData" => {
                ensure!(list_op.is_none(), "customData metadata does not support list ops");
                let value = self
                    .parse_property_metadata_value()
                    .context("Unable to parse customData dictionary")?;
                spec.add("customData", value);
            }
            n if n == FieldKey::Documentation.as_str() => {
                ensure!(list_op.is_none(), "doc metadata does not support list ops");
                let value = self.parse_token::<String>().context("Unable to parse doc metadata")?;
                spec.add(FieldKey::Documentation, sdf::Value::String(value));
            }
            other => bail!("Unsupported prim metadata: {other}"),
        }

        Ok(())
    }
}
