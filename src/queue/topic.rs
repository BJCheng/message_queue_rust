use serde::{Deserialize, Serialize};

use crate::message::Message;
use crate::storage::segment::Segment;
use std::{
    fs,
    io::{self, ErrorKind},
    path::PathBuf,
};

#[derive(Serialize, Deserialize, Debug)]
pub struct Topic {
    name: String,
    base_directory: PathBuf,
    #[serde(skip)]
    segments: Vec<Segment>,
    next_offset: u64,
}

impl Topic {
    // todo: read from current .dat file to see if this topic exists already
    //       also read the current write offset
    //       also create a write method to wirte the current topic status into a physical file
    pub fn new(name: String) -> Self {
        let base_path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(format!("data/{}", &name));

        let first_segment_result = Segment::new(0, base_path.join(Segment::DEFAULT_LOG_PATH));

        let first_segment = match first_segment_result {
            Ok(s) => s,
            Err(e) => {
                panic!("not able to create first segment: {}", e)
            }
        };

        Topic {
            name,
            base_directory: base_path,
            segments: vec![first_segment],
            next_offset: 0,
        }
    }

    pub fn append(&mut self, payload: String) -> io::Result<u64> {
        let message = Message::new(self.next_offset, payload);

        let segment = self.find_active_segment().ok_or_else(|| {
            io::Error::new(
                ErrorKind::NotFound,
                format!("cannot find the active segment"),
            )
        })?;

        let next_offset = segment.append(&message)?;

        self.next_offset = next_offset;

        Ok(next_offset)
    }

    pub fn read(&mut self, offset: u64) -> io::Result<Message> {
        let segment = self.find_segment(offset);
        segment.read_from(offset)
    }

    pub fn write(&self) -> io::Result<()> {
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join(format!("data/{}/metadata.json", self.name));
        let json = serde_json::to_string(self).unwrap_or_else(|e| {
            panic!(
                "cannot serialize Topic: {} to json string. Error: {}",
                self.name, e
            )
        });
        fs::write(path, json).unwrap_or_else(|e| {
            panic!(
                "cannot write Topic: {} to local storage. Error: {}",
                self.name, e
            )
        });
        Ok(())
    }

    pub fn load(topic_name: &str) -> io::Result<Self> {
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join(format!("data/{}/metadata.json", topic_name));
        let json_string = fs::read_to_string(path).unwrap_or_else(|e| {
            panic!(
                "cannot read json string for topic: {}. Error: {}",
                topic_name, e
            )
        });
        let topic: Topic = serde_json::from_str(&json_string).unwrap_or_else(|e| {
            panic!(
                "cannot deserialize from json string to Topic for topic: {}. Error: {}",
                topic_name, e
            )
        });
        Ok(topic)
    }

    fn find_active_segment(&mut self) -> Option<&mut Segment> {
        let active_segment = self.segments.iter_mut().find(|s| s.is_active);
        active_segment
    }

    fn find_segment(&mut self, offset: u64) -> &mut Segment {
        self.segments
            .iter_mut()
            .find(|s| s.contains_offset(offset))
            .ok_or_else(|| format!("cannot find corresponding segment per offset: {}", offset))
            .unwrap()
    }
}

#[cfg(test)]
mod tests {
    use crate::queue::topic::Topic;

    #[test]
    fn test_append() {
        let mut my_topic = Topic::new(String::from("my_topic"));
        let next_offset = my_topic.append(String::from("my topic's first message"));
        assert_eq!(next_offset.unwrap(), 1);
    }

    #[test]
    fn test_append_and_read() {
        let mut my_second_topic = Topic::new(String::from("my_second_topic"));

        let msg = "message in the second topic";
        let next_offset = my_second_topic
            .append(String::from(msg))
            .unwrap_or_else(|e| {
                panic!(
                    "wasn't able to append message to topic: {}. error: {}",
                    &my_second_topic.name, e
                )
            });

        let msg_read = my_second_topic.read(next_offset - 1).unwrap_or_else(|e| {
            panic!(
                "wasn't able to read the message from topic: {} with offset: {}, error: {}",
                &my_second_topic.name,
                &next_offset - 1,
                e
            )
        });

        assert_eq!(msg_read.value, msg);

        // asset with second message
        let msg2 = "second message in the second topic!";
        let next_offset = my_second_topic
            .append(String::from(msg2))
            .unwrap_or_else(|e| {
                panic!(
                    "wasn't able to append 2nd message to topic: {}, error: {}",
                    my_second_topic.name, e
                )
            });

        let msg_read = my_second_topic.read(next_offset - 1).unwrap_or_else(|e| {
            panic!(
                "wasn't able to read the second message from topic: {} with offset: {}, error: {}",
                &my_second_topic.name,
                &next_offset - 1,
                e
            )
        });

        assert_eq!(msg_read.value, msg2);
    }

    #[test]
    pub fn test_json_serde() {
        let topic = &mut Topic::new(String::from("serde_testing_topic"));
        topic.next_offset = 100;
        topic.write().unwrap();

        let topic_read = Topic::load("serde_testing_topic").unwrap();

        assert_eq!(topic_read.name, "serde_testing_topic");
        assert_eq!(topic_read.next_offset, 100);
    }
}
