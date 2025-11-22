use std::{
    fs::{File, OpenOptions},
    io::{self, Result, Write},
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
}
