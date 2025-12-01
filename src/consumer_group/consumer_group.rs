use std::io;

use crate::message::Message;

pub struct ConsumerGroup {}

impl ConsumerGroup {
    pub fn new() -> Self {
        ConsumerGroup {}
    }

    pub fn append(&mut self) -> io::Result<u64> {}

    pub fn read(&mut self) -> io::Result<Message> {}
}
