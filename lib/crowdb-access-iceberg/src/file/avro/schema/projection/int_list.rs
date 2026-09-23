use super::{AvroContainerError, AvroDatumLimits, Input};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AvroIntList<'data>(pub(super) &'data [u8]);

impl AvroIntList<'_> {
    /// Decodes one previously validated Avro integer array with an independent item cap.
    /// # Errors
    /// Rejects malformed block framing, integer overflow and excessive items.
    pub fn values(self, max_items: usize) -> Result<Vec<i32>, AvroContainerError> {
        if max_items > 4096 {
            return Err(AvroContainerError::Bounds);
        }
        let mut input = Input::new(
            self.0,
            AvroDatumLimits {
                depth: 1,
                values: 1,
                value_bytes: 8 * 1024 * 1024,
            },
        );
        let mut values = Vec::new();
        loop {
            let count = input.long()?;
            if count == 0 {
                input.finish()?;
                return Ok(values);
            }
            let negative = count < 0;
            let count = count
                .checked_abs()
                .and_then(|count| usize::try_from(count).ok())
                .ok_or(AvroContainerError::Bounds)?;
            if count > max_items - values.len() {
                return Err(AvroContainerError::Bounds);
            }
            let block_end = if negative {
                let length = input.size()?;
                Some(
                    input
                        .position()
                        .checked_add(length)
                        .ok_or(AvroContainerError::Bounds)?,
                )
            } else {
                None
            };
            for _ in 0..count {
                values.push(i32::try_from(input.long()?).map_err(|_| AvroContainerError::Schema)?);
            }
            if block_end.is_some_and(|end| input.position() != end) {
                return Err(AvroContainerError::Schema);
            }
        }
    }
}
