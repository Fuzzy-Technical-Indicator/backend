use actix_web::error::{
    ErrorBadRequest, ErrorInternalServerError, ErrorNotFound, ErrorUnauthorized,
};

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

    #[error("The rule with id \"{0}\" was not found")]
    RuleNotFound(String),

    #[error("The rule need to have atleast one input and one output")]
    RuleNotValid,

    #[error("User {0} is not found")]
    UserNotFound(String),

    #[error("Expect atleast one signal condition")]
    ExpectAtlestOneSignalCondition,

    #[error("The time range given is invalid")]
    InvalidTimeRange,

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
        SettingsNotFound
        | RuleNotFound(_)
        | LinguisticVarNotFound(_)
        | LinguisticVarShapeNotFound(_) => ErrorNotFound(e.to_string()),
        RuleAlreadyExist | RuleNotValid | InvalidTimeRange | ExpectAtlestOneSignalCondition => {
            ErrorBadRequest(e.to_string())
        }
        UserNotFound(_) => ErrorUnauthorized(e.to_string()),
        _ => ErrorInternalServerError(e.to_string()),
    }
}
