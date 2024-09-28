use aws_sdk_dynamodb::operation::RequestId;
use aws_sdk_dynamodb::{
    operation::get_item::builders::GetItemFluentBuilder, types::ConsumedCapacity, Client,
};

use crate::operation::GetItem;
use crate::{
    error::{ReadError, SerializeError},
    item,
    value::{self, KeyType, Value},
};

pub trait Table: Send + Sync + Sized + Clone {
    type Item;

    fn name(&self) -> &str;

    fn client(&self) -> Client;

    fn all(&self) -> Scan<Self>
    where
        Self::Item: item::Deserialize,
    {
        todo!()
    }

    fn put(&self, item: Self::Item) -> PutItem<Self>
    where
        Self::Item: item::Serialize,
    {
        self.put_raw(item)
    }

    fn put_raw(&self, item: impl item::Serialize) -> PutItem<Self> {
        todo!()
    }
}

pub trait HashTable: Table {
    type HashKeyType: KeyType;
    const HASH_KEY_ATTRIBUTE: &'static str;

    fn get_raw(&self, hash: impl value::Serialize<Type = Self::HashKeyType>) -> GetItem<Self>
    where
        Self::Item: item::Deserialize,
    {
        let request = hash
            .serialize_owned()
            .map(|h| self.client().get_item().key(Self::HASH_KEY_ATTRIBUTE, h));
        GetItem::new(request)
    }
}

pub trait HashRangeTable: Table {
    type HashKeyType: KeyType;
    const HASH_KEY_ATTRIBUTE: &'static str;

    type RangeKeyType: KeyType;
    const RANGE_KEY_ATTRIBUTE: &'static str;

    fn get_raw(
        &self,
        hash: impl value::Serialize<Type = Self::HashKeyType>,
        range: impl value::Serialize<Type = Self::RangeKeyType>,
    ) -> GetItem<Self>
    where
        Self::Item: item::Deserialize,
    {
        let request = hash.serialize_owned().and_then(|h| {
            range.serialize_owned().map(|r| {
                self.client()
                    .get_item()
                    .key(Self::HASH_KEY_ATTRIBUTE, h)
                    .key(Self::RANGE_KEY_ATTRIBUTE, r)
            })
        });
        GetItem::new(request)
    }
}

pub struct Scan<T>(T);
pub struct PutItem<T>(T);
