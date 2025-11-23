use std::{
    fs::{File, OpenOptions},
    io::{self, Read, Result, Seek, Write},
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

    pub fn append(&mut self, message: Message) -> io::Result<u32> {
        let message_encoded =
            bincode::serialize(&message).map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        let message_length = message_encoded.len() as u32;

        self.file.write_all(&message_length.to_le_bytes())?;
        self.file.write_all(&message_encoded)?;
        self.file.flush()?;

        Ok(message_length)
    }

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
