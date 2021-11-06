use indenter::indented;
use thiserror::Error;
use std::fmt::Write;

#[derive(Error, Debug)]
pub enum UpdateMismatchError {
    TypeMismatch {
        expected_type: String,
        actual_value: String,
    },

    ExtraKeys(Vec<(String, String)>),

    MissingKeys(Vec<String>),

    ArraySizeMismatch {
        expected: usize,
        actual: usize,
    },

    ValueMismatch {
        expected: String,
        actual: String,
    },

    NestedError(Vec<(String, UpdateMismatchError)>),
}

impl std::fmt::Display for UpdateMismatchError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            UpdateMismatchError::TypeMismatch { expected_type, actual_value } => {
                write!(f, "Chron update has `{}` where we expected something of type {}", actual_value, expected_type)
            }
            UpdateMismatchError::ExtraKeys(pairs) => {
                write!(f, "Chron update has extra keys:")?;
                for (key, value) in pairs {
                    write!(f, "\n    - {}: `{}`", key, value)?;
                }
                Ok(())
            }
            UpdateMismatchError::MissingKeys(keys) => {
                write!(f, "Chron update is missing keys: {}", keys.join(", "))
            }
            UpdateMismatchError::ArraySizeMismatch { actual, expected } => {
                write!(f, "Chron update has an array of length {} where we expected an array of length {}", actual, expected)
            }
            UpdateMismatchError::ValueMismatch { actual, expected } => {
                write!(f, "Chron update has `{}` where we expected `{}`", actual, expected)
            }
            UpdateMismatchError::NestedError(nested) => {
                for (key, err) in nested {
                    write!(f, "\n    - {}: ", key)?;
                    write!(indented(f).with_str("    "), "{}", err)?;
                }
                Ok(())
            }
        }
    }
}


#[derive(Error, Debug)]
pub enum IngestError {
    #[error("Update didn't match expected value")]
    UpdateMismatch(#[from] UpdateMismatchError),

}
