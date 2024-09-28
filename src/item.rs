use std::collections::HashMap;

use aws_sdk_dynamodb::types::AttributeValue;

use crate::error::{DeserializeError, SerializeError};


pub trait Serialize {
    fn serialize(&self) -> impl Iterator<Item = Result<(String, AttributeValue), SerializeError>>;

    fn serialize_owned(&self) -> impl Iterator<Item = Result<(String, AttributeValue), SerializeError>>
    where
        Self: Sized,
    {
        self.serialize()
    }

    fn serialize_to_map(&self) -> Result<HashMap<String, AttributeValue>, SerializeError> {
        self.serialize().collect()
    }

    fn serialize_owned_to_map(self) -> Result<HashMap<String, AttributeValue>, SerializeError> where Self: Sized {
        self.serialize_owned().collect()
    }
}

pub trait Deserialize: Sized {
    fn deserialize_owned(value: impl Iterator<Item = (String, AttributeValue)>) -> Result<Self, DeserializeError> {
        // XX check duplicate keys?
        Deserialize::deserialize_owned_from_map(value.collect())
    }

    fn deserialize_owned_from_map(value: HashMap<String, AttributeValue>) -> Result<Self, DeserializeError>;
}
