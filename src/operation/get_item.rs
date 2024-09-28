
use std::marker::PhantomData;

use aws_sdk_dynamodb::{operation::get_item::builders::GetItemFluentBuilder, types::ConsumedCapacity};
use aws_sdk_dynamodb::operation::RequestId;

use crate::{error::{ReadError, SerializeError}, item, table::Table};


#[derive(Debug, Clone)]
pub struct GetItem<T> {
    request: Result<GetItemFluentBuilder, SerializeError>,
    _table: PhantomData<T>,
}

#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct GetItemOutput<I> {
    pub item: Option<I>,
    pub consumed_capacity: Option<ConsumedCapacity>,
    pub request_id: String,
}

impl<T: Table> GetItem<T> where T::Item: item::Deserialize {
    pub(crate) fn new(request: Result<GetItemFluentBuilder, SerializeError>) -> Self {
        Self {
            request,
            _table: PhantomData,
        }
    }

    pub async fn send(self) -> Result<GetItemOutput<T::Item>, ReadError> {
        let result = self.request?.send().await?;
        let request_id = result
            .request_id()
            .unwrap_or("<unknown request ID>")
            .to_owned();
        let item = result
            .item
            .map(item::Deserialize::deserialize_owned_from_map)
            .transpose()?;

        Ok(GetItemOutput {
            item,
            consumed_capacity: result.consumed_capacity,
            request_id,
        })
    }
}