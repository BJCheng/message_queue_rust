use std::{
    fs::{self, File, OpenOptions, create_dir_all, read},
    io::{self, ErrorKind, Read, Seek, Write},
    path::PathBuf,
};

use crate::message::Message;

#[derive(Debug)]
pub struct Segment {
    pub base_offset: u64,
    pub file: File,
    pub is_active: bool,
}

impl Segment {
    pub const DEFAULT_LOG_PATH: &str = "00000.dat";
    const DEFAULT_MESSAGE_COUNT: u32 = 5;

    pub fn new(base_offset: u64, path: PathBuf) -> io::Result<Self> {
        if let Some(parent) = path.parent() {
            create_dir_all(parent)?;
        }

        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(path)?;

        Ok(Self {
            base_offset,
            file,
            is_active: true,
        })
    }

    pub fn load(path: PathBuf) -> io::Result<Self> {
        let file_name = path
            .file_stem()
            .and_then(|name| name.to_str())
            .ok_or_else(|| {
                io::Error::new(
                    ErrorKind::NotFound,
                    format!(
                        "cannot find the file name part from the path: {}",
                        path.to_str().unwrap()
                    ),
                )
            })
            .unwrap();
        let base_offset: u64 = file_name.parse().unwrap_or_else(|e| {
            panic!(
                "cannot read the base_offset from file name: {}. Error: {}",
                &file_name, e
            )
        });
        let file = OpenOptions::new()
            .read(true)
            .append(true)
            .open(&path)
            .unwrap_or_else(|e| {
                panic!(
                    "cannot open the file with path: {}. Error: {}",
                    path.to_str().unwrap(),
                    e
                )
            });
        Ok(Segment {
            base_offset,
            file,
            // todo: part of the is_active fix
            is_active: true,
        })
    }

    pub fn append(&mut self, message: &Message) -> io::Result<u64> {
        println!(
            "======segment writing message with offset: {}=====",
            message.offset
        );
        let message_encoded =
            bincode::serialize(&message).map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        let message_length = message_encoded.len() as u32;

        self.file.write_all(&message_length.to_le_bytes())?;
        self.file.write_all(&message_encoded)?;
        self.file.flush()?;

        Ok(message.offset + 1)
    }

    // todo: handle error gracefully by using the match on Result
    /// # Arguments
    /// * offset - return the message with specified offset
    pub fn read_from(&mut self, offset: u64) -> io::Result<Message> {
        self.file.seek(io::SeekFrom::Start(0))?;

        loop {
            let mut next_msg_len_buffer = [0u8; 4];
            match self.file.read_exact(&mut next_msg_len_buffer) {
                Ok(_) => {}
                Err(e) if e.kind() == ErrorKind::UnexpectedEof => {
                    println!(
                        "reached end of the file and still not finding the message with expected offset: {}. Likely reading the wrong segment file.",
                        offset
                    );
                    break;
                }
                Err(e) => return Err(e.into()),
            }
            let next_msg_length = u32::from_le_bytes(next_msg_len_buffer) as usize;
            println!("successfully read message length: {}", next_msg_length);

            let mut next_msg_buffer = vec![0u8; next_msg_length];
            self.file
                .read_exact(&mut next_msg_buffer)
                .map_err(|e| io::Error::new(e.kind(), format!("failed to READ message: {}", e)))?;

            let message: Message = bincode::deserialize(&next_msg_buffer).map_err(|e| {
                io::Error::new(
                    io::ErrorKind::Other,
                    format!("failed to DESERIALIZE message: {}", e),
                )
            })?;
            if message.offset == offset {
                return Ok(message);
            } else {
                println!("found message with offset: {}", &message.offset);
            }
        }

        Err(io::Error::new(
            io::ErrorKind::NotFound,
            format!("did not find the message with offset: {}", offset),
        )
        .into())
    }

    pub fn contains_offset(&self, offset: u64) -> bool {
        offset >= self.base_offset
            && offset - self.base_offset <= Self::DEFAULT_MESSAGE_COUNT.into()
    }
}

#[cfg(test)]
mod tests {
    use std::{fs, path::PathBuf};

    use crate::{message::Message, storage::segment::Segment};

    #[test]
    fn test_append() {
        let first_msg_value = "hello";
        let message = Message::new(0, String::from(first_msg_value));
        let mut segment = Segment::new(0, PathBuf::from("test.dat")).unwrap();
        segment.append(&message).unwrap();

        let message_read = segment.read_from(0).unwrap();
        assert_eq!(message_read.value, first_msg_value);

        let second_message_value = ", world!";
        let second_message = Message::new(1, String::from(second_message_value));
        segment.append(&second_message).unwrap();

        let second_msg_read = segment.read_from(1).unwrap();
        assert_eq!(second_msg_read.value, second_message_value);

        // todo: call this method when assertions are failed
        fs::remove_file("./test.dat").unwrap();
    }

    #[test]
    fn test_load() {
        let segment = Segment::load(
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("data/my_topic/00000.dat"),
        )
        .unwrap();
        assert!(segment.is_active);
        assert_eq!(segment.base_offset, 0);
    }
}
