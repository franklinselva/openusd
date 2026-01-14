use anyhow::{anyhow, ensure, Context, Result};

use crate::usda::token::Token;

type LexResult<'source> = std::result::Result<Token<'source>, ()>;

/// Token stream operations.
impl<'a> super::Parser<'a> {
    /// Fetch the next token from the stream and update the last span.
    #[inline]
    pub(super) fn fetch_next(&mut self) -> Result<Token<'a>> {
        let (token, span) = self.iter.next().context("Unexpected end of tokens")?;
        self.last_span = Some(span);
        token.map_err(|e| anyhow!("Logos error: {e:?}"))
    }

    /// Peek at the next token without consuming it.
    #[inline]
    pub(super) fn peek_next(&mut self) -> Option<&LexResult<'a>> {
        self.iter.peek().map(|(token, _)| token)
    }

    /// Check if the next token matches the expected token.
    #[inline]
    pub(super) fn is_next(&mut self, expected: Token) -> bool {
        matches!(self.peek_next(), Some(Ok(t)) if *t == expected)
    }

    /// Ensure the next token matches the expected token and consume it.
    pub(super) fn ensure_next(&mut self, expected_token: Token) -> Result<()> {
        let token = self.fetch_next()?;
        ensure!(
            token == expected_token,
            "Unexpected token (want: {expected_token:?}, got {token:?})"
        );
        Ok(())
    }

    /// Ensure the next token is a specific punctuation character.
    #[inline]
    pub(super) fn ensure_pun(&mut self, value: char) -> Result<()> {
        self.ensure_next(Token::Punctuation(value))
            .context("Punctuation token expected")
    }

    /// Fetch the next token and ensure it's a string.
    pub(super) fn fetch_str(&mut self) -> Result<&str> {
        let token = self.fetch_next()?;
        token
            .clone()
            .try_as_string()
            .ok_or_else(|| anyhow!("Unexpected token {token:?} (want String)"))
    }
}
