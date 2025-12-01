use std::{
    fs::{File, OpenOptions, create_dir_all},
    io::{self, ErrorKind, Read, Seek, Write},
    path::PathBuf,
};

use crate::message::Message;

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

    pub fn append(&mut self, message: &Message) -> io::Result<u64> {
        let message_encoded =
            bincode::serialize(&message).map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        let message_length = message_encoded.len() as u32;
        println!("appending message with length: {}", message_length);

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
}
