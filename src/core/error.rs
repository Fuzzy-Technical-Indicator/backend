use actix_web::error::{ErrorInternalServerError, ErrorNotFound, ErrorBadRequest};

#[derive(thiserror::Error, Debug)]
pub enum CustomError {
    #[error("This linguistic variable \"{0}\" does not exist")]
    LinguisticVarNotFound(String),

    #[error("This linguistic variable shape \"{0}\" does not exist")]
    LinguisticVarShapeNotFound(String),

    #[error("Settings not found")]
    SettingsNotFound,

    #[error("The rule already exists")]
    RuleAlreadyExist,

    #[error("{0}")]
    InternalError(String),
}

pub fn map_internal_err<T: std::fmt::Display>(e: T) -> CustomError {
    use CustomError::*;
    InternalError(e.to_string())
}

pub fn map_custom_err(e: CustomError) -> actix_web::Error {
    use CustomError::*;

    match e {
        LinguisticVarNotFound(_) => ErrorNotFound(e.to_string()),
        LinguisticVarShapeNotFound(_) => ErrorNotFound(e.to_string()),
        SettingsNotFound => ErrorNotFound(e.to_string()),
        RuleAlreadyExist => ErrorBadRequest(e.to_string()),
        _ => ErrorInternalServerError(e.to_string()),
    }
}
