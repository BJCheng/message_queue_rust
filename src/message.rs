use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct Message {
    pub value: String,
}

impl Message {
    pub fn new(value_str: String) -> Message {
        Message { value: value_str }
    }
}
