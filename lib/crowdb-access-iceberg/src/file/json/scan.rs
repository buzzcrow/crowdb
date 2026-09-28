use std::io;

pub(super) struct JsonScan {
    max_depth: usize,
    depth: usize,
    string: bool,
    escaped: bool,
    started: bool,
    utf8_tail: Vec<u8>,
}

impl JsonScan {
    pub(super) fn new(max_depth: usize) -> Self {
        Self {
            max_depth,
            depth: 0,
            string: false,
            escaped: false,
            started: false,
            utf8_tail: Vec::with_capacity(4),
        }
    }

    pub(super) fn push(&mut self, bytes: &[u8]) -> io::Result<()> {
        self.utf8(bytes)?;
        for byte in bytes {
            if self.string {
                if self.escaped {
                    self.escaped = false;
                } else if *byte == b'\\' {
                    self.escaped = true;
                } else if *byte == b'"' {
                    self.string = false;
                }
            } else {
                if !self.started && !matches!(byte, b' ' | b'\t' | b'\n' | b'\r') {
                    if *byte != b'{' {
                        return Err(invalid());
                    }
                    self.started = true;
                }
                match byte {
                    b'"' => self.string = true,
                    b'{' | b'[' => {
                        self.depth += 1;
                        if self.depth > self.max_depth {
                            return Err(invalid());
                        }
                    }
                    b'}' | b']' => {
                        self.depth = self.depth.checked_sub(1).ok_or_else(invalid)?;
                    }
                    _ => {}
                }
            }
        }
        Ok(())
    }

    pub(super) fn finish(&self) -> io::Result<()> {
        if !self.started || self.string || self.depth != 0 || !self.utf8_tail.is_empty() {
            return Err(invalid());
        }
        Ok(())
    }

    fn utf8(&mut self, mut bytes: &[u8]) -> io::Result<()> {
        while !self.utf8_tail.is_empty() && !bytes.is_empty() {
            self.utf8_tail.push(bytes[0]);
            bytes = &bytes[1..];
            match std::str::from_utf8(&self.utf8_tail) {
                Ok(_) => self.utf8_tail.clear(),
                Err(error) if error.error_len().is_none() && self.utf8_tail.len() < 4 => {}
                Err(_) => return Err(invalid()),
            }
        }
        match std::str::from_utf8(bytes) {
            Ok(_) => Ok(()),
            Err(error) if error.error_len().is_none() => {
                self.utf8_tail.extend_from_slice(&bytes[error.valid_up_to()..]);
                Ok(())
            }
            Err(_) => Err(invalid()),
        }
    }
}

fn invalid() -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        "invalid UTF-8 JSON object or nesting bound",
    )
}
