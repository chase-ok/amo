use std::hash::Hash;
use std::sync::Arc;
use std::{collections::HashMap, hash::BuildHasher};

use aws_sdk_dynamodb::primitives::Blob;
use aws_sdk_dynamodb::types::AttributeValue;

use crate::error::{DeserializeError, SerializeError};

pub trait Type: private::SealedType {
    const NAME: &'static str;

    type Raw;

    fn to_attribute_value(raw: Self::Raw) -> AttributeValue;
    fn from_attribute_value(value: AttributeValue) -> Result<Self::Raw, AttributeValue>;
}

pub trait KeyType: Type + private::SealedKeyType {}
impl<T: Type + private::SealedKeyType> KeyType for T {}

pub trait Value: Serialize<Type = <Self as Value>::Type> + Deserialize<Type = <Self as Value>::Type> {
    type Type: Type;
}
impl<V: Serialize + Deserialize<Type = <V as Serialize>::Type>> Value for V {
    type Type = <V as Serialize>::Type;
}

pub trait Serialize {
    type Type: Type;

    fn serialize_raw(&self) -> Result<<Self::Type as Type>::Raw, SerializeError>;

    fn serialize_owned_raw(self) -> Result<<Self::Type as Type>::Raw, SerializeError> where Self: Sized{
        self.serialize_raw()
    }

    fn serialize(&self) -> Result<AttributeValue, SerializeError> {
        self.serialize_raw().map(Self::Type::to_attribute_value)
    }

    fn serialize_owned(self) -> Result<AttributeValue, SerializeError> where Self: Sized{
        self.serialize_owned_raw().map(Self::Type::to_attribute_value)
    }
}

pub trait Deserialize: Sized {
    type Type: Type;

    fn deserialize_owned_raw(raw: <Self::Type as Type>::Raw) -> Result<Self, DeserializeError>;

    fn deserialize_owned(value: AttributeValue) -> Result<Self, DeserializeError> {
        match Self::Type::from_attribute_value(value) {
            Ok(raw) => Self::deserialize_owned_raw(raw),
            Err(value) => Err(DeserializeError::unexpected_value_type(Self::Type::NAME, value))
        }
    }
}

mod private {
    #[doc(hidden)]
    pub trait SealedType {}

    #[doc(hidden)]
    pub trait SealedKeyType {}
}

pub struct S(());

impl Type for S { 
    const NAME: &'static str = "S";

    type Raw = String;
    
    fn to_attribute_value(raw: Self::Raw) -> AttributeValue {
        AttributeValue::S(raw)
    }
    
    fn from_attribute_value(value: AttributeValue) -> Result<Self::Raw, AttributeValue> {
        match value {
            AttributeValue::S(s) => Ok(s),
            value => Err(value),
        }
    } 
}

impl private::SealedType for S {}
impl private::SealedKeyType for S {}

pub struct N(());

impl Type for N { 
    const NAME: &'static str = "N";

    type Raw = String;
    
    fn to_attribute_value(raw: Self::Raw) -> AttributeValue {
        AttributeValue::N(raw)
    }
    
    fn from_attribute_value(value: AttributeValue) -> Result<Self::Raw, AttributeValue> {
        match value {
            AttributeValue::N(n) => Ok(n),
            value => Err(value),
        }
    } 
}

impl private::SealedType for N {}
impl private::SealedKeyType for N {}

pub struct B(());

impl Type for B { 
    const NAME: &'static str = "B";

    type Raw = Vec<u8>;
    
    fn to_attribute_value(raw: Self::Raw) -> AttributeValue {
        AttributeValue::B(Blob::new(raw))
    }
    
    fn from_attribute_value(value: AttributeValue) -> Result<Self::Raw, AttributeValue> {
        match value {
            AttributeValue::B(b) => Ok(b.into_inner()),
            value => Err(value),
        }
    } 
}

impl private::SealedType for B {}
impl private::SealedKeyType for B {}

pub struct Any(());

impl Type for Any { 
    const NAME: &'static str = "<any>";

    type Raw = AttributeValue;
    
    fn to_attribute_value(raw: Self::Raw) -> AttributeValue {
        raw
    }
    
    fn from_attribute_value(value: AttributeValue) -> Result<Self::Raw, AttributeValue> {
        Ok(value)
    } 
}

impl private::SealedType for Any {}
impl private::SealedKeyType for Any {}


// primitive impls

impl<V: Serialize + ?Sized> Serialize for &V {
    type Type = V::Type;

    fn serialize_raw(&self) -> Result<<Self::Type as Type>::Raw, SerializeError> {
        (**self).serialize_raw()
    }
}

impl<V: Serialize + ?Sized> Serialize for &mut V {
    type Type = V::Type;

    fn serialize_raw(&self) -> Result<<Self::Type as Type>::Raw, SerializeError> {
        (**self).serialize_raw()
    }
}

impl<V: Serialize + ?Sized> Serialize for Box<V> {
    type Type = V::Type;

    fn serialize_raw(&self) -> Result<<Self::Type as Type>::Raw, SerializeError> {
        (**self).serialize_raw()
    }
}

impl<V: Serialize + ?Sized> Serialize for Arc<V> {
    type Type = V::Type;

    fn serialize_raw(&self) -> Result<<Self::Type as Type>::Raw, SerializeError> {
        (**self).serialize_raw()
    }
}

impl<V: Deserialize> Deserialize for Box<V> {
    type Type = V::Type;

    fn deserialize_owned_raw(value: <Self::Type as Type>::Raw) -> Result<Self, DeserializeError> {
        Ok(Box::new(V::deserialize_owned_raw(value)?))
    }
}

impl<V: Deserialize> Deserialize for Arc<V> {
    type Type = V::Type;

    fn deserialize_owned_raw(value: <Self::Type as Type>::Raw) -> Result<Self, DeserializeError> {
        Ok(Arc::new(V::deserialize_owned_raw(value)?))
    }
}

// S

impl Serialize for str {
    type Type = S;

    fn serialize_raw(&self) -> Result<String, SerializeError> {
        Ok(self.to_owned())
    }
}

impl Serialize for String {
    type Type = S;

    fn serialize_raw(&self) -> Result<String, SerializeError> {
        Ok(self.clone())
    }

    fn serialize_owned_raw(self) -> Result<String, SerializeError> {
        Ok(self)
    }
}

impl Deserialize for String {
    type Type = S;

    fn deserialize_owned_raw(value: String) -> Result<Self, DeserializeError> {
        Ok(value)
    }
}

// N

// Any

impl Serialize for AttributeValue {
    type Type = Any;
    
    fn serialize_raw(&self) -> Result<<Self::Type as Type>::Raw, SerializeError> {
        Ok(self.clone())
    }

    fn serialize_owned_raw(self) -> Result<<Self::Type as Type>::Raw, SerializeError> {
        Ok(self)
    }
}

impl Deserialize for AttributeValue {
    type Type = Any;

    fn deserialize_owned_raw(raw: <Self::Type as Type>::Raw) -> Result<Self, DeserializeError> {
        Ok(raw)
    }
}

impl<K: Serialize<Type = S>, V: Serialize, H> Serialize for HashMap<K, V, H> {
    type Type = Any; // XX: M

    fn serialize_raw(&self) -> Result<<Self::Type as Type>::Raw, SerializeError> {
        Ok(AttributeValue::M(
            self.iter()
                .map(|(k, v)| Ok((k.serialize_raw()?, v.serialize()?)))
                .collect::<Result<_, SerializeError>>()?,
        ))
    }

    fn serialize_owned_raw(self) -> Result<<Self::Type as Type>::Raw, SerializeError> where Self: Sized {
        Ok(AttributeValue::M(
            self.into_iter()
                .map(|(k, v)| Ok((k.serialize_owned_raw()?, v.serialize_owned()?)))
                .collect::<Result<_, SerializeError>>()?,
        ))
    }
}

impl<K: Deserialize<Type = S> + Eq + Hash, V: Deserialize, H: BuildHasher + Default> Deserialize
    for HashMap<K, V, H>
{
    type Type = Any; // XX: M

    fn deserialize_owned_raw(raw: <Self::Type as Type>::Raw) -> Result<Self, DeserializeError> {
        match raw {
            AttributeValue::M(m) => m
                .into_iter()
                .map(|(k, v)| Ok((K::deserialize_owned_raw(k)?, V::deserialize_owned(v)?)))
                .collect::<Result<_, DeserializeError>>(),
            v => Err(DeserializeError::unexpected_value_type("M", v)),
        }
    }
}