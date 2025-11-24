use std::{
    fs::{File, OpenOptions},
    io::{self, Read, Result, Seek, SeekFrom, Write},
};

use crate::message::Message;

pub struct Segment {
    pub file: File,
    pub is_active: bool,
}

impl Segment {
    pub fn new(path: String) -> Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(path)?;

        Ok(Self {
            file,
            is_active: false,
        })
    }

    pub fn append(&mut self, message: Message) -> io::Result<u64> {
        let offset = self.file.seek(SeekFrom::End(0))?;

        let message_encoded =
            bincode::serialize(&message).map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        let message_length = message_encoded.len() as u32;

        self.file.write_all(&message_length.to_le_bytes())?;
        self.file.write_all(&message_encoded)?;
        self.file.flush()?;

        Ok(offset + 1)
    }

    // todo: handle error gracefully by using the match on Result
    pub fn read_from(&mut self, offset: u64) -> Result<Message> {
        self.file.seek(io::SeekFrom::Start(offset))?;

        let mut next_msg_len_buffer = [0u8; 4];
        self.file.read_exact(&mut next_msg_len_buffer)?;
        let next_msg_length = u32::from_le_bytes(next_msg_len_buffer) as usize;

        let mut next_msg_buffer = vec![0u8; next_msg_length];
        self.file.read_exact(&mut next_msg_buffer)?;
        let message = bincode::deserialize(&next_msg_buffer)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;

        Ok(message)
    }
}

#[cfg(test)]
mod tests {
    use std::fs;

    use crate::{message::Message, storage::segment::Segment};

    #[test]
    fn test_append() {
        let first_msg_value = "hello";
        let message = Message::new(String::from(first_msg_value));
        let mut segment = Segment::new(String::from("test.dat")).unwrap();
        let second_msg_offset = segment.append(message).unwrap();

        println!("second message offset: {}", second_msg_offset);

        let message_read = segment.read_from(0).unwrap();
        assert_eq!(message_read.value, first_msg_value);

        let second_message_value = ", world!";
        let second_message = Message::new(String::from(second_message_value));
        segment.append(second_message).unwrap();
        let second_msg_read = segment.read_from(second_msg_offset).unwrap();
        assert_eq!(second_msg_read.value, second_message_value);

        fs::remove_file("./test.dat").unwrap();
    }
}
