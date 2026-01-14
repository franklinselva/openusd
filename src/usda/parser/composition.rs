use anyhow::{anyhow, bail, Context, Result};
use std::collections::HashMap;

use crate::sdf;
use crate::usda::token::Token;

use super::value::types::Type;

/// Composition arc parsing functions.
impl<'a> super::Parser<'a> {
    /// Parse a reference arc (asset path + optional prim path + layer offset).
    pub(super) fn parse_reference(&mut self) -> Result<sdf::Reference> {
        let asset_path = self
            .fetch_next()?
            .try_as_asset_ref()
            .ok_or_else(|| anyhow!("Asset reference expected"))?;

        let mut reference = sdf::Reference {
            asset_path: asset_path.to_string(),
            prim_path: sdf::Path::default(),
            layer_offset: sdf::LayerOffset::default(),
            custom_data: HashMap::new(),
        };

        if matches!(self.peek_next(), Some(Ok(Token::PathRef(..)))) {
            let path = self
                .fetch_next()?
                .try_as_path_ref()
                .ok_or_else(|| anyhow!("Path reference expected"))?;
            reference.prim_path = sdf::Path::new(path)?;
        }

        if self.is_next(Token::Punctuation('(')) {
            self.parse_reference_layer_offset(&mut reference.layer_offset)
                .context("Unable to parse reference layer offset")?;
        }

        Ok(reference)
    }

    /// Parse `(offset = ...; scale = ...)` blocks attached to references or sublayers.
    pub(super) fn parse_reference_layer_offset(&mut self, layer_offset: &mut sdf::LayerOffset) -> Result<()> {
        self.ensure_pun('(')?;

        self.parse_seq_fn(';', |this, _index| {
            let token = this.fetch_next()?;
            this.ensure_pun('=')?;
            let value = this.parse_value(Type::Double)?;

            match token {
                Token::Offset => {
                    layer_offset.offset = value.try_as_double().context("Expected double for offset")?;
                }
                Token::Scale => {
                    layer_offset.scale = value.try_as_double().context("Expected double for scale")?;
                }
                unexpected => bail!("Unexpected token in layer offset: {unexpected:?}"),
            }

            Ok(())
        })?;

        Ok(())
    }

    /// Parse a list-op friendly sequence of references.
    pub(super) fn parse_reference_list(&mut self) -> Result<Vec<sdf::Reference>> {
        if self.is_next(Token::Punctuation('[')) {
            let mut out = Vec::new();
            self.parse_array_fn(|this| {
                out.push(this.parse_reference()?);
                Ok(())
            })?;
            Ok(out)
        } else {
            Ok(vec![self.parse_reference()?])
        }
    }

    /// Parse a payload arc (asset path + optional prim path + optional layer offset).
    pub(super) fn parse_payload(&mut self) -> Result<sdf::Payload> {
        let asset_path = self
            .fetch_next()?
            .try_as_asset_ref()
            .ok_or_else(|| anyhow!("Asset reference expected for payload"))?;

        let mut payload = sdf::Payload {
            asset_path: asset_path.to_string(),
            prim_path: sdf::Path::default(),
            layer_offset: None,
        };

        if matches!(self.peek_next(), Some(Ok(Token::PathRef(..)))) {
            let path = self
                .fetch_next()?
                .try_as_path_ref()
                .ok_or_else(|| anyhow!("Path reference expected for payload"))?;
            payload.prim_path = sdf::Path::new(path)?;
        }

        if self.is_next(Token::Punctuation('(')) {
            let mut layer_offset = sdf::LayerOffset::default();
            self.parse_reference_layer_offset(&mut layer_offset)
                .context("Unable to parse payload layer offset")?;
            payload.layer_offset = Some(layer_offset);
        }

        Ok(payload)
    }

    /// Parse a list-op friendly sequence of payloads.
    pub(super) fn parse_payload_list(&mut self) -> Result<Vec<sdf::Payload>> {
        if self.is_next(Token::Punctuation('[')) {
            let mut out = Vec::new();
            self.parse_array_fn(|this| {
                out.push(this.parse_payload()?);
                Ok(())
            })?;
            Ok(out)
        } else {
            Ok(vec![self.parse_payload()?])
        }
    }

    /// Parse a single path reference for inherits.
    pub(super) fn parse_inherit_path(&mut self) -> Result<sdf::Path> {
        let token = self.fetch_next()?;
        let path_str = token
            .try_as_path_ref()
            .ok_or_else(|| anyhow!("Path reference expected for inherits metadata"))?;
        sdf::Path::new(path_str)
    }

    /// Parse a list of tokens (used for various list-op metadata).
    pub(super) fn parse_token_list(&mut self) -> Result<Vec<String>> {
        if self.is_next(Token::Punctuation('[')) {
            self.parse_array()
        } else {
            let value = self.parse_token::<String>()?;
            Ok(vec![value])
        }
    }

    /// Build a ListOp from an optional list operation token and items.
    pub(super) fn apply_list_op<T: Default + Clone + PartialEq>(
        &mut self,
        op: Option<Token<'a>>,
        items: Vec<T>,
    ) -> Result<sdf::ListOp<T>> {
        let mut list = sdf::ListOp::default();

        match op {
            None => {
                list.explicit = true;
                list.explicit_items = items;
            }
            Some(Token::Prepend) => list.prepended_items = items,
            Some(Token::Append) => list.appended_items = items,
            Some(Token::Add) => list.added_items = items,
            Some(Token::Delete) => list.deleted_items = items,
            Some(Token::Reorder) => list.ordered_items = items,
            other => bail!("Unsupported list op: {other:?}"),
        }

        Ok(list)
    }
}
