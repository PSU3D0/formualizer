use std::fmt;

use serde::{Deserialize, Serialize};

/// A single manifest validation issue with a JSON Pointer-like path.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ManifestIssue {
    pub path: String,
    pub message: String,
}

impl ManifestIssue {
    pub fn new(path: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            path: path.into(),
            message: message.into(),
        }
    }
}

impl fmt::Display for ManifestIssue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.path, self.message)
    }
}

/// Aggregate validation error returned when one or more issues are detected.
#[derive(Debug)]
pub struct ValidationError {
    issues: Vec<ManifestIssue>,
}

impl ValidationError {
    pub fn new(issues: Vec<ManifestIssue>) -> Self {
        Self { issues }
    }

    pub fn issues(&self) -> &[ManifestIssue] {
        &self.issues
    }
}

impl fmt::Display for ValidationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "manifest validation failed with {} issue(s)",
            self.issues.len()
        )
    }
}

impl std::error::Error for ValidationError {}
