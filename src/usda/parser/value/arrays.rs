use anyhow::{bail, ensure, Context, Result};
use std::fmt::Debug;
use std::mem::MaybeUninit;
use std::str::FromStr;

use crate::usda::token::Token;

/// Array, tuple, and matrix parsing functions.
impl<'a> super::super::Parser<'a> {
    /// Generic array parser that delegates element parsing while handling delimiters.
    pub(in crate::usda::parser) fn parse_array_fn(
        &mut self,
        mut read_elements: impl FnMut(&mut Self) -> Result<()>,
    ) -> Result<()> {
        self.ensure_pun('[').context("Array must start with [")?;

        let mut index = 0;
        loop {
            if self.is_next(Token::Punctuation(']')) {
                self.fetch_next()?;
                break;
            }

            read_elements(self).with_context(|| format!("Unable to read array element {index}"))?;
            index += 1;

            match self.fetch_next()? {
                Token::Punctuation(',') => continue,
                Token::Punctuation(']') => break,
                t => bail!("Either comma or closing bracket expected after value, got: {t:?}"),
            }
        }
        Ok(())
    }

    /// Parse delimiter-separated sequences like `(a, b)` or `(offset = ...; scale = ...)`.
    pub(in crate::usda::parser) fn parse_seq_fn(
        &mut self,
        delim: char,
        mut read_element: impl FnMut(&mut Self, usize) -> Result<()>,
    ) -> Result<()> {
        self.ensure_pun('(').context("Open brace expected")?;

        let mut index = 0;
        loop {
            if self.is_next(Token::Punctuation(')')) {
                self.fetch_next()?;
                break;
            }

            read_element(self, index).with_context(|| format!("Unable to read element {index}"))?;
            index += 1;

            match self.fetch_next()? {
                Token::Punctuation(')') => break,
                Token::Punctuation(d) if d == delim => continue,
                t => bail!("Unexpected token between (): {t:?}"),
            }
        }
        Ok(())
    }

    /// Parse fixed-size tuples, preserving order and surfacing contextual errors.
    pub(in crate::usda::parser) fn parse_tuple<T, const N: usize>(&mut self) -> Result<[T; N]>
    where
        T: FromStr,
        <T as FromStr>::Err: Debug,
    {
        let mut result: [MaybeUninit<T>; N] = unsafe { MaybeUninit::uninit().assume_init() };
        self.parse_seq_fn(',', |this, i| {
            result[i] = MaybeUninit::new(this.parse_token::<T>()?);
            Ok(())
        })?;
        let result = unsafe { std::mem::transmute_copy::<_, [T; N]>(&result) };
        Ok(result)
    }

    /// Parse array or array of tuples.
    pub(in crate::usda::parser) fn parse_array<T>(&mut self) -> Result<Vec<T>>
    where
        T: FromStr + Default,
        <T as FromStr>::Err: Debug,
    {
        let mut out = Vec::new();
        self.parse_array_fn(|this| {
            out.push(this.parse_token::<T>()?);
            Ok(())
        })?;
        Ok(out)
    }

    /// Parse array of tuples.
    pub(in crate::usda::parser) fn parse_array_of_tuples<T, const N: usize>(&mut self) -> Result<Vec<T>>
    where
        T: FromStr,
        <T as FromStr>::Err: Debug,
    {
        let mut out = Vec::new();
        self.parse_array_fn(|this| {
            out.extend(this.parse_tuple::<T, N>()?);
            Ok(())
        })?;
        Ok(out)
    }

    /// Parse a single matrix literal, flattening rows in row-major order.
    pub(in crate::usda::parser) fn parse_matrix<const N: usize>(&mut self) -> Result<Vec<f64>> {
        let mut values = Vec::with_capacity(N * N);
        self.parse_seq_fn(',', |this, _| {
            let row = this.parse_tuple::<f64, N>()?;
            values.extend(row);
            Ok(())
        })?;

        ensure!(values.len() == N * N, "matrix{N}d literal must contain {N} rows");

        Ok(values)
    }

    /// Parse either a single matrix or an array of matrices, depending on the next token.
    pub(in crate::usda::parser) fn parse_matrix_value<const N: usize>(&mut self) -> Result<Vec<f64>> {
        if self.is_next(Token::Punctuation('[')) {
            self.parse_matrix_array::<N>()
        } else {
            self.parse_matrix::<N>()
        }
    }

    /// Parse an array of matrices, concatenating the row-major matrices into a single vector.
    pub(in crate::usda::parser) fn parse_matrix_array<const N: usize>(&mut self) -> Result<Vec<f64>> {
        let mut matrices = Vec::new();
        self.parse_array_fn(|this| {
            matrices.extend(this.parse_matrix::<N>()?);
            Ok(())
        })?;
        Ok(matrices)
    }
}
