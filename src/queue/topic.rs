use crate::message::Message;
use crate::storage::segment::Segment;
use std::{
    io::{self, ErrorKind},
    path::PathBuf,
};

pub struct Topic {
    name: String,
    base_directory: PathBuf,
    segments: Vec<Segment>,
    next_offset: u64,
}

impl Topic {
    pub fn new(name: String) -> Self {
        let base_path =
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(format!("src/data/{}", &name));

        let first_segment_result = Segment::new(base_path.join(Segment::DEFAULT_LOG_PATH));

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

    fn find_active_segment(&mut self) -> Option<&mut Segment> {
        let active_segment = self.segments.iter_mut().find(|s| s.is_active);
        active_segment
    }
}

#[cfg(test)]
mod tests {
    use crate::queue::topic::Topic;

    #[test]
    fn test_append() {
        let mut my_topic = Topic::new(String::from("my_topic"));
        let current_offset = my_topic.append(String::from("my topic's first message"));
        assert_eq!(current_offset.unwrap(), 1);
    }
}
