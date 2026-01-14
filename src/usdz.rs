//! USDZ archive format reader.
//!
//! USDZ is a compressed ZIP archive containing either USDA or USDC files.
//! According to the USD specification, USDZ files are zero-compression, unencrypted
//! ZIP archives designed for efficient direct consumption without extraction.

use std::{
    fs::File,
    io::{Cursor, Read},
    path::Path,
};

use anyhow::{bail, Context, Result};
use zip::ZipArchive;

use crate::{sdf, usda, usdc};

/// USDZ archive reader.
///
/// Provides access to USD files within a USDZ archive.
pub struct Archive {
    archive: ZipArchive<File>,
}

impl Archive {
    /// Open a USDZ archive from a file path.
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let file = File::open(path).with_context(|| format!("Failed to open USDZ archive: {}", path.display()))?;

        let archive =
            ZipArchive::new(file).with_context(|| format!("Failed to read ZIP archive: {}", path.display()))?;

        Ok(Self { archive })
    }

    /// Returns the number of files in the archive.
    pub fn len(&self) -> usize {
        self.archive.len()
    }

    /// Returns true if the archive is empty.
    pub fn is_empty(&self) -> bool {
        self.archive.is_empty()
    }

    /// Returns the name of the file at the given index.
    pub fn name_at(&mut self, index: usize) -> Option<String> {
        self.archive.by_index(index).ok().map(|f| f.name().to_string())
    }

    /// Returns a list of all file names in the archive.
    pub fn file_names(&self) -> Vec<String> {
        self.archive.file_names().map(|s| s.to_string()).collect()
    }

    /// Find the root USD layer in the archive.
    ///
    /// According to the USDZ specification, the root layer is typically the first
    /// USD file (.usdc, .usda, or .usd) encountered in the archive. This method
    /// iterates through all files and returns the path to the first USD file found.
    pub fn find_root_layer(&self) -> Option<String> {
        for name in self.archive.file_names() {
            let lower = name.to_lowercase();
            if lower.ends_with(".usdc") || lower.ends_with(".usda") || lower.ends_with(".usd") {
                return Some(name.to_string());
            }
        }
        None
    }

    /// Read either a USDA or USDC file from the archive.
    ///
    /// NOTE: Nested USDZ files are not yet supported.
    pub fn read(&mut self, file_path: &str) -> Result<Box<dyn sdf::AbstractData>> {
        let mut file = self
            .archive
            .by_name(file_path)
            .with_context(|| format!("File '{}' not found in archive", file_path))?;

        let mut buffer = Vec::new();
        file.read_to_end(&mut buffer)
            .with_context(|| format!("Failed to read file '{}' from archive", file_path))?;

        if file_path.ends_with(".usdc") {
            let cursor = Cursor::new(buffer);
            let data = usdc::CrateData::open(cursor, true)
                .with_context(|| format!("Failed to parse USDC data from '{}'", file_path))?;
            Ok(Box::new(data))
        } else if file_path.ends_with(".usda") {
            let content =
                String::from_utf8(buffer).with_context(|| format!("File '{}' is not valid UTF-8", file_path))?;

            let mut parser = usda::parser::Parser::new(&content);
            let data = parser
                .parse()
                .with_context(|| format!("Failed to parse USDA data from '{}'", file_path))?;

            Ok(Box::new(usda::TextReader::from_data(data)))
        } else if file_path.ends_with(".usdz") {
            // TODO: Implement nested USDZ files support.
            bail!("Nested USDZ files are not yet supported: '{}'", file_path)
        } else {
            bail!(
                "Unsupported file format for '{}'. Expected .usda or .usdc extension",
                file_path
            )
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_open_usdz() -> Result<()> {
        let mut archive = Archive::open("fixtures/test.usdz")?;
        let data = archive.read("file_1.usdc")?;
        let root = sdf::Path::abs_root();

        assert!(data.has_spec(&root));
        assert_eq!(data.spec_type(&root), Some(sdf::SpecType::PseudoRoot));

        Ok(())
    }
}
