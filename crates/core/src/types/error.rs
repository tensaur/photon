use std::fmt;

use serde::{Deserialize, Serialize};

/// Transport-level error returned to clients. Separates public error
/// information from internal domain error details.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ApiError {
    /// The requested resource was not found.
    NotFound { message: String },
    /// The request was invalid.
    BadRequest { message: String },
    /// An internal error occurred. Details are logged server-side.
    Internal,
}

impl fmt::Display for ApiError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotFound { message } => write!(f, "not found: {message}"),
            Self::BadRequest { message } => write!(f, "bad request: {message}"),
            Self::Internal => write!(f, "internal error"),
        }
    }
}

impl std::error::Error for ApiError {}
