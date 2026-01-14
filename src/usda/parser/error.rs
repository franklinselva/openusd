use std::ops::Range;

/// Captures the line context for the most recent token consumed by the parser.
#[derive(Debug, Clone)]
pub struct ErrorHighlight {
    pub line: usize,
    pub column: usize,
    pub line_text: String,
    pub pointer_line: String,
}

impl ErrorHighlight {
    /// Renders a human-readable representation of the highlighted line.
    pub fn render(&self) -> String {
        format!(
            "line {} column {}\n{}\n{}",
            self.line, self.column, self.line_text, self.pointer_line
        )
    }

    /// Create an ErrorHighlight from a source string and span.
    pub fn from_span(source: &str, span: Range<usize>) -> Option<Self> {
        if source.is_empty() {
            return None;
        }

        let mut offset = span.start.min(source.len());
        if offset == source.len() && offset > 0 {
            offset -= 1;
        }

        // Calculate line and column by counting newlines up to the offset
        let mut line = 1;
        let mut line_start = 0;

        for (idx, ch) in source[..offset].char_indices() {
            if ch == '\n' {
                line += 1;
                line_start = idx + ch.len_utf8();
            }
        }

        // Find the end of the current line
        let line_end = source[line_start..]
            .find('\n')
            .map(|pos| line_start + pos)
            .unwrap_or(source.len());

        let line_text = source[line_start..line_end].trim_end_matches(['\r', '\n']).to_string();

        // Calculate column (character count from line start to offset)
        let mut column = 1;
        for ch in source[line_start..offset].chars() {
            if ch == '\n' || ch == '\r' {
                break;
            }
            column += 1;
        }

        // Build pointer line
        let mut pointer_line = String::new();
        for ch in source[line_start..offset].chars() {
            if ch == '\n' || ch == '\r' {
                break;
            }
            pointer_line.push(if ch == '\t' { '\t' } else { ' ' });
        }
        pointer_line.push('^');

        Some(ErrorHighlight {
            line,
            column,
            line_text,
            pointer_line,
        })
    }
}
