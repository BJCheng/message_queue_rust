use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct Message {
    pub offset: u64,
    pub value: String,
}

impl Message {
    pub fn new(offset: u64, value_str: String) -> Message {
        Message {
            offset,
            value: value_str,
        }
    }
}
