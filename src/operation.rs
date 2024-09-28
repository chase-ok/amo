
pub mod get_item;
use std::marker::PhantomData;

pub use get_item::{GetItem, GetItemOutput};

use crate::value::{Value, S};

pub struct SKeyConditionBuilder<T> {
    _value: PhantomData<T>
}

pub struct SKeyCondition {

}

impl<T: Value<Type = S>> SKeyConditionBuilder<T> {
    pub fn equals(self, value: T) -> SKeyCondition {
        todo!()
    }
}