use anyhow::{anyhow, bail, ensure, Context, Result};
use std::collections::HashMap;

use crate::sdf;
use crate::sdf::schema::{ChildrenKey, FieldKey};
use crate::usda::token::Token;

use super::value::types::{keyword_lexeme, Type};

/// Hierarchy parsing functions for prims, attributes, and relationships.
impl<'a> super::Parser<'a> {
    /// Parse tokens to specs.
    /// Walks the entire token stream, seeding the pseudo root and recursing through every prim.
    pub fn parse(&mut self) -> Result<HashMap<sdf::Path, sdf::Spec>> {
        let mut data = HashMap::new();
        let current_path = sdf::Path::abs_root();

        // Read pseudo root.
        let mut pseudo_root_spec = self.read_pseudo_root().context("Unable to parse pseudo root")?;
        let mut root_children = Vec::new();

        // Read root defs.
        while self.peek_next().is_some() {
            self.read_prim(&current_path, &mut root_children, &mut data)?;
        }

        pseudo_root_spec.add(ChildrenKey::PrimChildren, sdf::Value::TokenVec(root_children));
        data.insert(current_path.clone(), pseudo_root_spec);
        Ok(data)
    }

    /// Parse the file header/pseudo-root to populate layer-level metadata before prim traversal.
    pub(super) fn read_pseudo_root(&mut self) -> Result<sdf::Spec> {
        // Make sure text file starts with #usda...
        let version = self
            .fetch_next()?
            .try_as_magic()
            .ok_or_else(|| anyhow!("Text file must start with magic token, got {:?}", self.peek_next()))?;
        ensure!(version == "1.0", "File must start with '#usda 1.0', got: {version:?}");

        let mut root = sdf::Spec::new(sdf::SpecType::PseudoRoot);

        if !self.is_next(Token::Punctuation('(')) {
            return Ok(root);
        }

        // Eat (
        self.ensure_pun('(')?;

        const KNOWN_PROPS: &[(&str, Type)] = &[
            (FieldKey::DefaultPrim.as_str(), Type::Token),
            (FieldKey::StartTimeCode.as_str(), Type::Double),
            (FieldKey::HasOwnedSubLayers.as_str(), Type::StringVec),
            ("doc", Type::String),
            ("endTimeCode", Type::Double),
            ("framesPerSecond", Type::Double),
            ("metersPerUnit", Type::Double),
            ("timeCodesPerSecond", Type::Double),
            ("upAxis", Type::Token),
        ];

        // Read pseudo root properties
        loop {
            let next = self.fetch_next().context("Unable to fetch next pseudo root property")?;

            match next {
                Token::Punctuation(')') => break,
                Token::String(str) => {
                    root.add(FieldKey::Documentation, str);
                }
                Token::Doc => {
                    self.ensure_pun('=')?;
                    let value = self.fetch_str()?;
                    root.add("doc", value);
                }
                Token::SubLayers => {
                    self.ensure_pun('=')?;
                    let (sublayers, sublayer_offsets) = self.parse_sublayers().context("Unable to parse subLayers")?;
                    root.add(FieldKey::SubLayers, sublayers);
                    root.add(FieldKey::SubLayerOffsets, sublayer_offsets);
                }
                Token::Identifier(name) => {
                    if let Some((known_name, ty)) = KNOWN_PROPS.iter().copied().find(|(n, _)| *n == name) {
                        self.ensure_pun('=')?;
                        let value = self
                            .parse_value(ty)
                            .with_context(|| format!("Unable to parse value for {known_name}"))?;
                        root.add(known_name, value);
                    } else {
                        self.ensure_pun('=')?;
                        let value = self
                            .parse_property_metadata_value()
                            .with_context(|| format!("Unable to parse pseudo root metadata value for {name}"))?;
                        root.fields.insert(name.to_owned(), value);
                    }
                }
                _ => bail!("Unexpected token {next:?}"),
            }
        }

        Ok(root)
    }

    /// Parse a prim declaration, capture its metadata, and recursively traverse nested prims/props.
    pub(super) fn read_prim(
        &mut self,
        current_path: &sdf::Path,
        parent_children: &mut Vec<String>,
        data: &mut HashMap<sdf::Path, sdf::Spec>,
    ) -> Result<()> {
        let mut spec = sdf::Spec::new(sdf::SpecType::Prim);

        let specifier = {
            let specifier_token = self.fetch_next().context("Unable to read prim specifier")?;
            match specifier_token {
                Token::Def => sdf::Specifier::Def,
                Token::Over => sdf::Specifier::Over,
                Token::Class => sdf::Specifier::Class,
                _ => bail!("Unexpected prim specifier: {specifier_token:?}"),
            }
        };

        // Check for optional type specifier (valid for def, over, and class)
        let mut name_token = self.fetch_next()?;
        if let Some(prim_type) = name_token.clone().try_as_identifier() {
            spec.add(FieldKey::TypeName, sdf::Value::Token(prim_type.to_string()));
            name_token = self.fetch_next()?;
        }

        let name = name_token
            .clone()
            .try_as_string()
            .ok_or_else(|| anyhow!("Unexpected token {name_token:?} (want String)"))?;
        parent_children.push(name.to_string());
        let prim_path = current_path.append_path(name)?;

        let mut properties = Vec::new();
        let mut brace_consumed = false;

        let brace = self.fetch_next()?;
        match brace {
            Token::Punctuation('(') => {
                self.read_prim_metadata(&mut spec, None)
                    .context("Unable to parse prim metadata")?;
                self.ensure_pun(')').context("Prim metadata must end with )")?;
            }
            Token::Punctuation('{') => {
                brace_consumed = true;
            }
            other => {
                // Support metadata without wrapping parentheses.
                self.read_prim_metadata(&mut spec, Some(other))
                    .context("Unable to parse prim metadata")?;
                brace_consumed = false;
            }
        };

        if !brace_consumed {
            self.ensure_pun('{')?;
        }

        let mut children = Vec::new();
        loop {
            let next = self
                .peek_next()
                .context("Unexpected end of prim body")?
                .as_ref()
                .map_err(|e| anyhow!("{e:?}"))?;

            match next {
                Token::Punctuation('}') => {
                    self.fetch_next()?;
                    break;
                }
                Token::Def | Token::Over | Token::Class => {
                    self.read_prim(&prim_path, &mut children, data)
                        .context("Unable to read nested primitive")?;
                }
                Token::VariantSet => {
                    self.fetch_next()?;
                    self.read_variant_set(&prim_path, data)
                        .context("Unable to read variant set")?;
                }
                Token::Rel => {
                    self.fetch_next()?;
                    self.read_relationship(&prim_path, &mut properties, data)
                        .context("Unable to read relationship")?;
                }
                // Handle list ops that may prefix relationships: prepend rel, add rel, etc.
                Token::Add | Token::Append | Token::Delete | Token::Prepend | Token::Reorder => {
                    // Peek ahead to check if this is a relationship or attribute
                    let list_op = self.fetch_next()?;
                    if self.is_next(Token::Rel) {
                        self.fetch_next()?; // consume 'rel'
                        self.read_relationship_with_list_op(&prim_path, &mut properties, data, list_op)
                            .context("Unable to read relationship")?;
                    } else {
                        self.read_attribute_with_list_op(&prim_path, &mut properties, data, list_op)
                            .context("Unable to read attribute")?;
                    }
                }
                _ => {
                    self.read_attribute(&prim_path, &mut properties, data)
                        .context("Unable to read attribute")?;
                }
            }
        }
        spec.add(ChildrenKey::PrimChildren, sdf::Value::TokenVec(children));

        spec.add(FieldKey::Specifier, sdf::Value::Specifier(specifier));
        spec.add(ChildrenKey::PropertyChildren, sdf::Value::TokenVec(properties));
        data.insert(prim_path, spec);

        Ok(())
    }

    /// Parse an attribute/property declaration, including variability, metadata, and default value.
    pub(super) fn read_attribute(
        &mut self,
        current_path: &sdf::Path,
        properties: &mut Vec<String>,
        data: &mut HashMap<sdf::Path, sdf::Spec>,
    ) -> Result<()> {
        let mut spec = sdf::Spec::new(sdf::SpecType::Attribute);
        let mut custom = false;
        let mut variability = sdf::Variability::Varying;

        if self.is_next(Token::Custom) {
            custom = true;
            self.fetch_next()?;
        }

        if self.is_next(Token::Varying) {
            self.fetch_next()?;
        } else if self.is_next(Token::Uniform) {
            variability = sdf::Variability::Uniform;
            self.fetch_next()?;
        }

        let type_token = self.fetch_next()?;
        let type_name = match type_token {
            Token::Identifier(s) | Token::NamespacedIdentifier(s) => s,
            other => bail!("Unexpected token type for attribute type, expected Identifier, got {other:?}"),
        };
        let data_type = Self::parse_data_type(type_name)?;

        let name_token = self.fetch_next()?;
        let name = match name_token {
            Token::Identifier(s) | Token::NamespacedIdentifier(s) => s,
            _ => keyword_lexeme(&name_token)
                .ok_or_else(|| anyhow!("Unexpected token type for attribute name: {name_token:?}"))?,
        };

        // Check for metadata before checking for assignment
        if self.is_next(Token::Punctuation('(')) {
            self.parse_property_metadata(&mut spec)
                .context("Unable to parse attribute metadata")?;
        }

        if name.contains(".connect") {
            if self.is_next(Token::Punctuation('=')) {
                self.fetch_next()?;
                let list_op = match self.peek_next() {
                    Some(Ok(Token::Add | Token::Append | Token::Prepend | Token::Delete | Token::Reorder)) => {
                        Some(self.fetch_next()?)
                    }
                    _ => None,
                };
                let targets = self
                    .parse_connection_targets()
                    .context("Unable to parse connection targets")?;
                let path = current_path.append_property(name)?;
                properties.push(name.to_string());

                spec.add(FieldKey::Custom, sdf::Value::Bool(custom));
                spec.add(FieldKey::Variability, sdf::Value::Variability(variability));
                spec.add(FieldKey::TypeName, sdf::Value::Token(type_name.to_string()));

                let list_op = self
                    .apply_list_op(list_op, targets)
                    .context("Unable to build connection listOp")?;
                spec.add(FieldKey::ConnectionPaths, sdf::Value::PathListOp(list_op));
                data.insert(path, spec);
            }
            return Ok(());
        }

        // Check if there's an assignment
        if !self.is_next(Token::Punctuation('=')) {
            let path = current_path.append_property(name)?;
            properties.push(name.to_string());

            spec.add(FieldKey::Custom, sdf::Value::Bool(custom));
            spec.add(FieldKey::Variability, sdf::Value::Variability(variability));
            spec.add(FieldKey::TypeName, sdf::Value::Token(type_name.to_string()));
            data.insert(path, spec);
            return Ok(());
        }

        self.ensure_pun('=')?;

        // Check for time samples: attribute.timeSamples = { time: value, ... }
        let (value, is_time_samples) = if self.is_next(Token::Punctuation('{')) {
            let samples = self
                .parse_time_samples(data_type)
                .context("Unable to parse time samples")?;
            (sdf::Value::TimeSamples(samples), true)
        } else {
            (self.parse_value(data_type)?, false)
        };

        let path = current_path.append_property(name)?;

        // Check for metadata after value (could appear here instead of before)
        if self.is_next(Token::Punctuation('(')) {
            self.parse_property_metadata(&mut spec)
                .context("Unable to parse attribute metadata")?;
        }

        properties.push(name.to_string());

        spec.add(FieldKey::Custom, sdf::Value::Bool(custom));
        spec.add(FieldKey::Variability, sdf::Value::Variability(variability));
        spec.add(FieldKey::TypeName, sdf::Value::Token(type_name.to_string()));
        if is_time_samples {
            spec.add(FieldKey::TimeSamples, value);
        } else {
            spec.add(FieldKey::Default, value);
        }
        data.insert(path, spec);

        Ok(())
    }

    /// Parse a relationship declaration with optional targets and metadata.
    pub(super) fn read_relationship(
        &mut self,
        current_path: &sdf::Path,
        properties: &mut Vec<String>,
        data: &mut HashMap<sdf::Path, sdf::Spec>,
    ) -> Result<()> {
        let name_token = self.fetch_next()?;
        let name = match name_token {
            Token::Identifier(s) | Token::NamespacedIdentifier(s) => s,
            other => bail!("Unexpected token in relationship declaration: {other:?}"),
        };

        let mut spec = sdf::Spec::new(sdf::SpecType::Relationship);

        // Check for metadata before or instead of assignment
        if self.is_next(Token::Punctuation('(')) {
            self.parse_property_metadata(&mut spec)
                .context("Unable to parse relationship metadata")?;
        }

        // Check if there's an assignment
        if !self.is_next(Token::Punctuation('=')) {
            let path = current_path.append_property(name)?;
            properties.push(name.to_string());

            spec.add(FieldKey::Custom, sdf::Value::Bool(false));
            spec.add(
                FieldKey::Variability,
                sdf::Value::Variability(sdf::Variability::Varying),
            );

            data.insert(path, spec);
            return Ok(());
        }

        self.ensure_pun('=')?;
        let list_op = match self.peek_next() {
            Some(Ok(Token::Add | Token::Append | Token::Prepend | Token::Delete | Token::Reorder)) => {
                Some(self.fetch_next()?)
            }
            _ => None,
        };
        let targets = self
            .parse_connection_targets()
            .context("Unable to parse relationship targets")?;

        let path = current_path.append_property(name)?;
        properties.push(name.to_string());

        let list_op = self
            .apply_list_op(list_op, targets)
            .context("Unable to build relationship targets listOp")?;
        spec.add(FieldKey::TargetPaths, sdf::Value::PathListOp(list_op));
        spec.add(FieldKey::Custom, sdf::Value::Bool(false));
        spec.add(
            FieldKey::Variability,
            sdf::Value::Variability(sdf::Variability::Varying),
        );

        if self.is_next(Token::Punctuation('(')) {
            self.parse_property_metadata(&mut spec)
                .context("Unable to parse relationship metadata")?;
        }

        data.insert(path, spec);
        Ok(())
    }

    /// Parse a relationship declaration with a pre-consumed list operation prefix.
    ///
    /// Handles syntax like: `prepend rel myRel = [<path>]`
    /// where the list op token has already been consumed by the caller.
    pub(super) fn read_relationship_with_list_op(
        &mut self,
        current_path: &sdf::Path,
        properties: &mut Vec<String>,
        data: &mut HashMap<sdf::Path, sdf::Spec>,
        list_op_token: Token<'a>,
    ) -> Result<()> {
        let name_token = self.fetch_next()?;
        let name = match name_token {
            Token::Identifier(s) | Token::NamespacedIdentifier(s) => s,
            other => bail!("Unexpected token in relationship declaration: {other:?}"),
        };

        let mut spec = sdf::Spec::new(sdf::SpecType::Relationship);

        // Check for metadata before assignment
        if self.is_next(Token::Punctuation('(')) {
            self.parse_property_metadata(&mut spec)
                .context("Unable to parse relationship metadata")?;
        }

        // For prepend/append/etc. relationships, expect an assignment
        self.ensure_pun('=')?;
        let targets = self
            .parse_connection_targets()
            .context("Unable to parse relationship targets")?;

        let path = current_path.append_property(name)?;
        properties.push(name.to_string());

        let list_op = self
            .apply_list_op(Some(list_op_token), targets)
            .context("Unable to build relationship targets listOp")?;
        spec.add(FieldKey::TargetPaths, sdf::Value::PathListOp(list_op));
        spec.add(FieldKey::Custom, sdf::Value::Bool(false));
        spec.add(
            FieldKey::Variability,
            sdf::Value::Variability(sdf::Variability::Varying),
        );

        if self.is_next(Token::Punctuation('(')) {
            self.parse_property_metadata(&mut spec)
                .context("Unable to parse relationship metadata")?;
        }

        data.insert(path, spec);
        Ok(())
    }

    /// Parse an attribute declaration with a pre-consumed list operation prefix.
    ///
    /// Handles syntax like: `prepend asset[] myAttr = [@file@]`
    /// where the list op token has already been consumed by the caller.
    pub(super) fn read_attribute_with_list_op(
        &mut self,
        current_path: &sdf::Path,
        properties: &mut Vec<String>,
        data: &mut HashMap<sdf::Path, sdf::Spec>,
        _list_op_token: Token<'a>,
    ) -> Result<()> {
        // For now, just delegate to the regular read_attribute
        // The list_op will be ignored since attributes don't use list ops in the same way
        // In practice, list ops before attributes are rare - they're typically used for relationships
        self.read_attribute(current_path, properties, data)
    }

    /// Parses a connection target list into USD paths.
    ///
    /// Handles:
    /// - `None` - Empty target list
    /// - `<path>` - Single target
    /// - `[<path1>, <path2>]` - Multiple targets
    pub(super) fn parse_connection_targets(&mut self) -> Result<Vec<sdf::Path>> {
        // Handle None value (empty relationship)
        if self.is_next(Token::None) {
            self.fetch_next()?;
            return Ok(Vec::new());
        }

        if self.is_next(Token::Punctuation('[')) {
            let mut paths = Vec::new();
            self.parse_array_fn(|this| {
                paths.push(this.parse_path_reference().context("Connection path expected")?);
                Ok(())
            })?;
            Ok(paths)
        } else {
            Ok(vec![self.parse_path_reference()?])
        }
    }

    /// Parses a single `<...>` path reference token into an `sdf::Path`.
    pub(super) fn parse_path_reference(&mut self) -> Result<sdf::Path> {
        let token = self.fetch_next()?;
        let path_str = token
            .clone()
            .try_as_path_ref()
            .ok_or_else(|| anyhow!("Path reference expected, got {token:?}"))?;
        sdf::Path::new(path_str)
    }

    /// Parse time samples in the format `{ time: value, time: value, ... }`.
    ///
    /// Example:
    /// ```text
    /// double3 xformOp:translate.timeSamples = {
    ///     0: (0, 0, 0),
    ///     100: (100, 0, 0),
    /// }
    /// ```
    pub(super) fn parse_time_samples(&mut self, data_type: super::value::types::Type) -> Result<sdf::TimeSampleMap> {
        self.ensure_pun('{').context("Time samples must start with {")?;

        let mut samples = Vec::new();

        loop {
            // Check for closing brace
            if self.is_next(Token::Punctuation('}')) {
                self.fetch_next()?;
                break;
            }

            // Parse the time (a number)
            let time_token = self.fetch_next()?;
            let time = match time_token {
                Token::Number(n) => n
                    .parse::<f64>()
                    .with_context(|| format!("Unable to parse time sample time: {n}"))?,
                other => bail!("Expected number for time sample time, got: {other:?}"),
            };

            // Expect colon separator
            self.ensure_pun(':').context("Expected ':' after time in time sample")?;

            // Parse the value
            let value = self.parse_value(data_type)?;

            samples.push((time, value));

            // Handle optional trailing comma
            if self.is_next(Token::Punctuation(',')) {
                self.fetch_next()?;
            }
        }

        Ok(samples)
    }

    /// Parse a variantSet block within a prim.
    ///
    /// Syntax: `variantSet "name" = { "variant1" { ... } "variant2" { ... } }`
    pub(super) fn read_variant_set(
        &mut self,
        prim_path: &sdf::Path,
        data: &mut HashMap<sdf::Path, sdf::Spec>,
    ) -> Result<()> {
        // Get the variant set name
        let name_token = self.fetch_next()?;
        let variant_set_name = name_token
            .clone()
            .try_as_string()
            .ok_or_else(|| anyhow!("Expected variant set name string, got {name_token:?}"))?;

        self.ensure_pun('=').context("Expected '=' after variant set name")?;
        self.ensure_pun('{')
            .context("Expected '{' to start variant set block")?;

        // Parse each variant within the set
        let mut variant_children = Vec::new();

        loop {
            // Check for closing brace
            if self.is_next(Token::Punctuation('}')) {
                self.fetch_next()?;
                break;
            }

            // Get the variant name (a string)
            let variant_token = self.fetch_next()?;
            let variant_name = variant_token
                .clone()
                .try_as_string()
                .ok_or_else(|| anyhow!("Expected variant name string, got {variant_token:?}"))?;

            variant_children.push(variant_name.to_string());

            // Create the variant path: /Prim{variantSet=variant}
            let variant_path = prim_path.append_variant_selection(variant_set_name, variant_name)?;

            // Create a spec for this variant
            let mut variant_spec = sdf::Spec::new(sdf::SpecType::Variant);

            // Check for optional variant metadata: "variantName" (metadata) { ... }
            if self.is_next(Token::Punctuation('(')) {
                self.fetch_next()?;
                self.read_prim_metadata(&mut variant_spec, None)
                    .context("Unable to parse variant metadata")?;
                self.ensure_pun(')').context("Variant metadata must end with )")?;
            }

            // Parse the variant body
            self.ensure_pun('{').context("Expected '{' to start variant body")?;

            let mut variant_properties = Vec::new();
            let mut variant_prim_children = Vec::new();

            loop {
                let next = self
                    .peek_next()
                    .context("Unexpected end of variant body")?
                    .as_ref()
                    .map_err(|e| anyhow!("{e:?}"))?;

                match next {
                    Token::Punctuation('}') => {
                        self.fetch_next()?;
                        break;
                    }
                    Token::Def | Token::Over | Token::Class => {
                        self.read_prim(&variant_path, &mut variant_prim_children, data)
                            .context("Unable to read nested prim in variant")?;
                    }
                    Token::VariantSet => {
                        self.fetch_next()?;
                        self.read_variant_set(&variant_path, data)
                            .context("Unable to read nested variant set in variant")?;
                    }
                    Token::Rel => {
                        self.fetch_next()?;
                        self.read_relationship(&variant_path, &mut variant_properties, data)
                            .context("Unable to read relationship in variant")?;
                    }
                    // Handle list ops that may prefix relationships: prepend rel, add rel, etc.
                    Token::Add | Token::Append | Token::Delete | Token::Prepend | Token::Reorder => {
                        let list_op = self.fetch_next()?;
                        if self.is_next(Token::Rel) {
                            self.fetch_next()?; // consume 'rel'
                            self.read_relationship_with_list_op(&variant_path, &mut variant_properties, data, list_op)
                                .context("Unable to read relationship in variant")?;
                        } else {
                            self.read_attribute_with_list_op(&variant_path, &mut variant_properties, data, list_op)
                                .context("Unable to read attribute in variant")?;
                        }
                    }
                    _ => {
                        self.read_attribute(&variant_path, &mut variant_properties, data)
                            .context("Unable to read attribute in variant")?;
                    }
                }
            }

            variant_spec.add(ChildrenKey::PrimChildren, sdf::Value::TokenVec(variant_prim_children));
            variant_spec.add(ChildrenKey::PropertyChildren, sdf::Value::TokenVec(variant_properties));
            data.insert(variant_path, variant_spec);
        }

        // Create a spec for the variant set itself
        let mut variant_set_spec = sdf::Spec::new(sdf::SpecType::VariantSet);
        variant_set_spec.add(ChildrenKey::VariantChildren, sdf::Value::TokenVec(variant_children));

        // Store under the prim path with variantSet marker
        // The path format for variant sets is typically stored differently, but for now
        // we store metadata on the prim's variantSetChildren
        // Get or create the prim spec to track variant set children
        if let Some(prim_spec) = data.get_mut(prim_path) {
            // Get existing variantSetChildren or create new
            let existing = prim_spec
                .fields
                .get(ChildrenKey::VariantSetChildren.as_str())
                .and_then(|v| v.clone().try_as_token_vec())
                .unwrap_or_default();

            let mut set_children = existing;
            set_children.push(variant_set_name.to_string());
            prim_spec.add(ChildrenKey::VariantSetChildren, sdf::Value::TokenVec(set_children));
        }

        Ok(())
    }
}
