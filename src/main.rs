use std::{
    borrow::Cow, collections::HashMap, default, marker::PhantomData, ops::Deref, sync::Arc, time::Instant
};

use amo::{error::{DeserializeError, SerializeError}, item, operation::{GetItem, SKeyCondition, SKeyConditionBuilder}, table::{HashRangeTable, Table}, value::{self, Type, Value}, value_type};
use aws_sdk_dynamodb::{
    types::AttributeValue,
    Client,
};


#[tokio::main]
async fn main() {
    println!("Hello, world!");

    let shared_config = aws_config::load_from_env().await;
    let client = Client::new(&shared_config);

    let table = TagTable {
        name: Arc::new("tags".into()),
        client,
    };
    let item = table
        .get(Arn("abc".into()), "some-key")
        // .consistency(Consistency::Strong)
        .send()
        .await
        .unwrap()
        .item;

    table
        .query()
        .by_resource(Arn("abc".into()))
        .all();
}

// XX rename amors?

#[derive(Debug, Clone, PartialEq, Eq)] //, amo::Table)]
pub struct Tag {
    // #[amo(primary, kind = hash)]
    resource: Arn,

    // #[amo(primary, kind = range)]
    // #[amo(secondary, index = "by-account", kind = range)]
    key: String,

    value: String,

    // view(arn)
    // #[amo(secondary, index = "by-account", kind = hash)]
    account: String,

    // #[amo(version)]
    version: u64,

    // #[amo(created)]
    created: Instant,

    // #[amo(updated)]
    updated: Instant,
}

impl item::Deserialize for Tag {
    fn deserialize_owned_from_map(value: HashMap<String, AttributeValue>) -> Result<Self, DeserializeError> {
        todo!()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
// XX derive SerializeS
pub struct Arn(String);

impl value::Serialize for Arn {
    type Type = value::S;

    fn serialize_raw(&self) -> Result<<Self::Type as Type>::Raw, SerializeError> {
        Ok(self.0.clone())
    }

    fn serialize_owned_raw(self) -> Result<<Self::Type as Type>::Raw, SerializeError> where Self: Sized {
        Ok(self.0)
    }
}

impl value::Deserialize for Arn {
    type Type = value::S;

    fn deserialize_owned_raw(raw: <Self::Type as Type>::Raw) -> Result<Self, DeserializeError> {
        Ok(Self(raw))
    }
}

value_type!(Arn, S);

// Derived
#[derive(Debug, Clone)]
pub struct TagTable {
    name: Arc<String>,
    client: Client,
}

impl Table for TagTable {
    type Item = Tag;

    fn name(&self) -> &str {
        &self.name
    }

    fn client(&self) -> Client {
        self.client.clone()
    }
}

impl HashRangeTable for TagTable {
    type HashKeyType = <Arn as Value>::Type;
    const HASH_KEY_ATTRIBUTE: &'static str = "resource";

    type RangeKeyType = <String as Value>::Type;
    const RANGE_KEY_ATTRIBUTE: &'static str = "key";
}

impl TagTable {
    pub fn get(&self, resource: impl Into<Arn>, key: impl Into<String>) -> GetItem<Self> {
        self.get_raw(resource.into(), key.into())
    }

    pub fn query(&self) -> TagTableQuery<'_> {
        TagTableQuery(self)
    }
}

pub struct TagTableQuery<'a>(&'a TagTable);

impl TagTableQuery<'_> {
    pub fn by_resource(&self, resource: impl Into<Arn>) -> TagByResourceQuery<'_> {
        self.by_resource_raw(resource.into())
    }

    pub fn by_resource_raw(&self, resource_key: impl value::Serialize<Type = <TagTable as HashRangeTable>::HashKeyType>) -> TagByResourceQuery<'_> {
        TagByResourceQuery {
            table: self.0,
            resource_key: resource_key.serialize_owned_raw().unwrap(),
        }
    }

    // pub fn by_account(&self, account: impl Into<String>) -> TagByAccountQuery<'_> {
    //     self.by_account_raw(account.into())
    // }

    // pub fn by_account_raw(&self, account_raw: impl SKey) -> TagByAccountQuery<'_> {
    //     TagByAccountQuery {
    //         table: self.0,
    //         key: account_raw.serialize_owned(),
    //     }
    // }
}

pub struct TagByResourceQuery<'a> {
    table: &'a TagTable,
    resource_key: <<TagTable as HashRangeTable>::HashKeyType as Type>::Raw,
}

impl TagByResourceQuery<'_> {
    pub fn all(self) -> PrimaryQuery<TagTable> {
        todo!()
    }

    pub fn matching_key(
        self,
        key: impl FnOnce(SKeyConditionBuilder<Arn>) -> SKeyCondition,
    ) -> PrimaryQuery<Self> {
        todo!()
    }
}

pub struct PrimaryQuery<T> {
    table: T,
}

// impl TagTable {
//     pub fn get<'a>(&self, resource: impl Into<Arn>, key: impl Into<String>) -> GetItem<Self> {
//         self.get_raw(resource.into(), key.into())
//     }

//     pub fn query(&self) -> TagTableQuery<'_> {
//         TagTableQuery(self)
//     }
// }


// impl TagTableQuery<'_> {
//     pub fn by_resource(&self, resource: impl Into<Arn>) -> TagByResourceQuery<'_> {
//         self.by_resource_raw(resource.into())
//     }

//     pub fn by_resource_raw(&self, resource_key: impl SKey) -> TagByResourceQuery<'_> {
//         TagByResourceQuery {
//             table: self.0,
//             resource_key: resource_key.serialize_owned(),
//         }
//     }

//     pub fn by_account(&self, account: impl Into<String>) -> TagByAccountQuery<'_> {
//         self.by_account_raw(account.into())
//     }

//     pub fn by_account_raw(&self, account_raw: impl SKey) -> TagByAccountQuery<'_> {
//         TagByAccountQuery {
//             table: self.0,
//             key: account_raw.serialize_owned(),
//         }
//     }
// }

// pub struct TagByResourceQuery<'a> {
//     table: &'a TagTable,
//     resource_key: String,
// }

// impl TagByResourceQuery<'_> {
//     pub fn all(&self) -> PrimaryQuery<Self> {
//         todo!()
//     }

//     pub fn matching_key(
//         &self,
//         key: impl FnOnce(SKeyConditionBuilder<Arn>) -> SKeyCondition,
//     ) -> PrimaryQuery<Self> {
//         todo!()
//     }
// }

// pub struct TagByAccountQuery<'a> {
//     table: &'a TagTable,
//     key: String,
// }

// impl TagByAccountQuery<'_> {
//     pub fn all(&self) -> SecondaryQuery<TagTable> {
//         todo!()
//     }

//     pub fn matching_key(
//         &self,
//         key: impl FnOnce(SKeyConditionBuilder<String>) -> SKeyCondition,
//     ) -> SecondaryQuery<TagTable> {
//         todo!()
//     }

//     pub fn matching_key_raw<K: SKey>(
//         &self,
//         key: impl FnOnce(SKeyConditionBuilder<K>) -> SKeyCondition,
//     ) -> SecondaryQuery<TagTable> {
//         todo!()
//     }
// }

// impl Table for TagTable {
//     type Item = Tag;

//     fn name(&self) -> &str {
//         &self.name
//     }

//     fn client(&self) -> Client {
//         self.client.clone()
//     }

//     fn all(&self) -> Scan<Self>
//     where
//         Self: Sized,
//     {
//         todo!()
//     }
//     // XX scan by secondary index too

//     fn put(&self, item: &Self::Item) -> PutItem<Self> {
//         todo!()
//     }
// }

// impl HashRangeTable for TagTable {
//     const HASH_KEY_ATTRIBUTE: &'static str = "resource";
//     const RANGE_KEY_ATTRIBUTE: &'static str = "key";

//     type HashKeyKind = S;
//     type RangeKeyKind = S;
// }

// pub trait KeyKind {}

// pub struct S;
// impl KeyKind for S {}

// pub struct N;
// impl KeyKind for N {}

// pub struct B;
// impl KeyKind for B {}

// pub trait SKey {
//     fn serialize_owned(self) -> String;
//     fn deserialize(value: String) -> Self;
// }

// pub trait NKey {
//     fn serialize_owned(self) -> String;
//     fn deserialize(value: String) -> Self;
// }

// pub trait BKey {
//     fn serialize_owned(self) -> Vec<u8>;
//     fn deserialize(value: Vec<u8>) -> Self;
// }

// pub trait Key<K: KeyKind> {
//     fn serialize_owned(self) -> AttributeValue;
//     // fn serialize(&self) -> AttributeValue;
//     // fn deserialize(value: AttributeValue) -> Self;
// }

// impl<K: SKey> Key<S> for K {
//     fn serialize_owned(self) -> AttributeValue {
//         AttributeValue::S(SKey::serialize_owned(self))
//     }
// }

// impl<K: NKey> Key<N> for K {
//     fn serialize_owned(self) -> AttributeValue {
//         AttributeValue::N(NKey::serialize_owned(self))
//     }
// }

// impl<K: BKey> Key<B> for K {
//     fn serialize_owned(self) -> AttributeValue {
//         AttributeValue::B(Blob::new(BKey::serialize_owned(self)))
//     }
// }

// impl SKey for String {
//     fn serialize_owned(self) -> String {
//         self
//     }

//     fn deserialize(value: String) -> Self {
//         value
//     }
// }

// // impl<T: SKey> ValueDeserialize for T {
// //     fn deserialize(value: AttributeValue) -> Result<Self, DeserializeError> {
// //         match value {
// //             AttributeValue::S(s) => Ok(SKey::deserialize(s)),
// //             _ => todo!(),
// //         }
// //     }
// // }

// // impl<T: NKey> ValueDeserialize for T {
// //     fn deserialize(value: AttributeValue) -> Result<Self, DeserializeError> {
// //         match value {
// //             AttributeValue::N(n) => Ok(NKey::deserialize(n)),
// //             _ => todo!(),
// //         }
// //     }
// // }

// pub trait Table: Send + Sync + Clone {
//     type Item: ItemDeserialize;

//     fn name(&self) -> &str;

//     fn client(&self) -> Client;

//     fn all(&self) -> Scan<Self>
//     where
//         Self: Sized;

//     fn put(&self, item: &Self::Item) -> PutItem<Self>;
// }

// pub trait HashTable: Table {
//     type HashKeyKind: KeyKind;

//     fn get_by_raw_key(&self, hash: impl Key<Self::HashKeyKind>) -> GetItem<Self>;
// }

// pub trait HashRangeTable: Table {
//     const HASH_KEY_ATTRIBUTE: &'static str;
//     const RANGE_KEY_ATTRIBUTE: &'static str;

//     type HashKeyKind: KeyKind;
//     type RangeKeyKind: KeyKind;

//     fn get_raw(
//         &self,
//         hash: impl Key<Self::HashKeyKind>,
//         range: impl Key<Self::RangeKeyKind>,
//     ) -> GetItem<Self> {
//         GetItem {
//             table: self.clone(),
//             request: self
//                 .client()
//                 .get_item()
//                 .table_name(self.name())
//                 .key(Self::HASH_KEY_ATTRIBUTE, hash.serialize_owned())
//                 .key(Self::RANGE_KEY_ATTRIBUTE, range.serialize_owned()),
//         }
//     }
// }

// pub trait VersionedTable: Table {
//     fn overwrite(&self, item: &Self::Item) -> PutItem<Self> {
//         self.put(item)
//     }
// }

// pub struct SKeyConditionBuilder<K> {
//     _key: PhantomData<K>,
// }

// pub struct SKeyCondition {}

// impl<K: SKey> SKeyConditionBuilder<K> {
//     pub fn equals(self, value: impl Into<K>) -> SKeyCondition {
//         SKeyCondition {}
//     }

//     pub fn starts_with(self, value: impl Into<K>) -> SKeyCondition {
//         SKeyCondition {}
//     }
// }

// pub trait ItemDeserialize: Sized {
//     fn deserialize(
//         item: HashMap<String, AttributeValue>
//     ) -> Result<Self, DeserializeError>;
// }

// pub trait ValueDeserialize: Sized {
//     fn deserialize(value: AttributeValue) -> Result<Self, DeserializeError>;
// }

// #[derive(Debug, Clone)]
// pub struct DeserializeError;

// impl DeserializeError {
//     pub fn missing_required_field(item_type: &str, field: &str) -> Self {
//         Self
//     }
// }

// pub trait ItemSerialize {
//     fn serialize(&self) -> impl Iterator<Item = (String, AttributeValue)>;
// }

// pub struct PutItem<T> {
//     _table: T,
// }

// pub struct Scan<T> {
//     _table: T,
// }

// pub struct PrimaryQuery<T> {
//     _table: T,
// }

// pub struct SecondaryQuery<T> {
//     _table: T,
// }

// pub struct GetItem<T> {
//     table: T,
//     request: GetItemFluentBuilder,
// }

// impl<T: Table> GetItem<T> {
//     pub fn consistency(mut self, consistency: Consistency) -> Self {
//         self.request = self
//             .request
//             .consistent_read(consistency == Consistency::Strong);
//         self
//     }

//     pub async fn send(self) -> Result<GetItemOutput<T::Item>, ReadError> {
//         let result = self.request.send().await?;
//         let request_id = result
//             .request_id()
//             .unwrap_or("<unknown request ID>")
//             .to_owned();
//         let item = result
//             .item
//             .map(<T::Item as ItemDeserialize>::deserialize)
//             .transpose()?;

//         Ok(GetItemOutput {
//             item,
//             consumed_capacity: result.consumed_capacity,
//             request_id,
//         })
//     }
// }

// #[derive(Debug, Clone)]
// pub struct ReadError;

// impl<R> From<SdkError<GetItemError, R>> for ReadError {
//     fn from(value: SdkError<GetItemError, R>) -> Self {
//         todo!()
//     }
// }

// impl From<DeserializeError> for ReadError {
//     fn from(value: DeserializeError) -> Self {
//         todo!()
//     }
// }

// #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
// pub enum Consistency {
//     #[default]
//     Eventual,
//     Strong,
// }

// #[derive(Debug, Clone)]
// #[non_exhaustive]
// pub struct GetItemOutput<I> {
//     pub item: Option<I>,
//     pub consumed_capacity: Option<ConsumedCapacity>,
//     pub request_id: String,
// }
