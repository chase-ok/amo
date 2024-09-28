use std::fmt::Display;

use aws_sdk_dynamodb::{error::SdkError, operation::get_item::GetItemError, types::AttributeValue};


#[derive(Debug, Clone)]
pub struct DeserializeError;

impl DeserializeError {
    pub fn missing_required_field(item_type: &str, field: &str) -> Self {
        Self
    }
    
    pub fn unexpected_value_type(expected: &str, actual: AttributeValue) -> Self {
        todo!()
    }
    
    pub(crate) fn invalid(e: impl Display) -> DeserializeError {
        todo!()
    }
}


#[derive(Debug, Clone)]
pub struct SerializeError;

impl SerializeError {
}


#[derive(Debug, Clone)]
pub struct ReadError;

impl<R> From<SdkError<GetItemError, R>> for ReadError {
    fn from(value: SdkError<GetItemError, R>) -> Self {
        todo!()
    }
}

impl From<DeserializeError> for ReadError {
    fn from(value: DeserializeError) -> Self {
        todo!()
    }
}

impl From<SerializeError> for ReadError {
    fn from(value: SerializeError) -> Self {
        todo!()
    }
}

