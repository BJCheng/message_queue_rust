use std::{collections::HashMap, io};

use crate::{message::Message, queue::topic::Topic};

pub struct ConsumerGroup {
    name: String,
    topic_index: HashMap<String, u64>,
}

impl ConsumerGroup {
    pub fn new(consumer_group_name: &str) -> Self {
        ConsumerGroup {
            name: String::from(consumer_group_name),
            topic_index: HashMap::new(),
        }
    }

    /// Returns
    /// Next offset for the given topic
    pub fn append(&mut self, topic_name: &str, payload: &str) -> io::Result<u64> {
        let topic = &mut Topic::load(topic_name)?;
        topic.append(String::from(payload))
    }

    pub fn read(&mut self, topic_name: &str, offset: u64) -> io::Result<Message> {
        let mut topic = Topic::load(topic_name).unwrap_or_else(|e| {
            panic!(
                "cannot load topic with topic name: {}. Error: {}",
                topic_name, e
            )
        });
        topic.read(offset)
    }
}

#[cfg(test)]
mod test {
    use crate::{consumer_group::consumer_group::ConsumerGroup, queue::topic::Topic};

    #[test]
    fn test_append_read() {
        // create a topic for this test, and save it for being read later
        let topic = Topic::new(String::from("the_topic_to_test_consumer_group"));
        topic.write().unwrap();

        let mut consumer_group = ConsumerGroup::new("testing_consumer_group");
        let next_offset = consumer_group
            .append(
                "the_topic_to_test_consumer_group",
                "testing consumer group's append and read",
            )
            .unwrap();

        let message = consumer_group
            .read("the_topic_to_test_consumer_group", next_offset - 1)
            .unwrap();

        assert_eq!(message.value, "testing consumer group's append and read");
        // fs::remove_file("./test.dat").unwrap();
    }
}
