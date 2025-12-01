use std::io;

use crate::{message::Message, queue::topic::Topic};

pub struct ConsumerGroup {}

impl ConsumerGroup {
    // pub fn new(topic_name: String) -> Self {
    //     let topic = Topic::load(topic_name)
    //         .unwrap_or_else(|e| panic!("cannot load topic with topic name: {}", topic_name));
    //     ConsumerGroup {}
    // }
    //
    // pub fn append(&mut self) -> io::Result<u64> {}
    //
    // pub fn read(&mut self) -> io::Result<Message> {}
}
