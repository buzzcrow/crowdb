pub(super) fn normalized(path: &str) -> bool {
    let bytes = path.as_bytes();
    if bytes.first() != Some(&b'$') || bytes.len() > 4096 {
        return false;
    }
    let mut offset = 1;
    let mut depth = 0;
    while offset < bytes.len() {
        depth += 1;
        if depth > 32 || bytes[offset] != b'[' {
            return false;
        }
        offset += 1;
        if bytes.get(offset) == Some(&b'\'') {
            offset += 1;
            if !name(bytes, &mut offset) {
                return false;
            }
        } else {
            let start = offset;
            while bytes.get(offset).is_some_and(u8::is_ascii_digit) {
                offset += 1;
            }
            if start == offset || offset - start > 1 && bytes[start] == b'0' {
                return false;
            }
            if path[start..offset]
                .parse::<u64>()
                .map_or(true, |value| value > 9_007_199_254_740_991)
            {
                return false;
            }
        }
        if bytes.get(offset) != Some(&b']') {
            return false;
        }
        offset += 1;
    }
    true
}

fn name(bytes: &[u8], offset: &mut usize) -> bool {
    while let Some(byte) = bytes.get(*offset) {
        *offset += 1;
        match byte {
            b'\'' => return true,
            0..=31 => return false,
            b'\\' => {
                let Some(escaped) = bytes.get(*offset) else {
                    return false;
                };
                *offset += 1;
                if *escaped == b'u' {
                    let Some(hex) = bytes.get(*offset..*offset + 4) else {
                        return false;
                    };
                    if hex[..2] != *b"00"
                        || !matches!(hex[2], b'0' | b'1')
                        || !matches!(hex[3], b'0'..=b'9' | b'a'..=b'f')
                        || hex[2] == b'0' && matches!(hex[3], b'8' | b'9' | b'a' | b'c' | b'd')
                    {
                        return false;
                    }
                    *offset += 4;
                } else if !matches!(escaped, b'b' | b'f' | b'n' | b'r' | b't' | b'\'' | b'\\') {
                    return false;
                }
            }
            _ => {}
        }
    }
    false
}
