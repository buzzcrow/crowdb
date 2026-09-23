use super::{DeletionVectorError, Input};

pub(super) async fn array(input: &mut Input, cardinality: u32) -> Result<u16, DeletionVectorError> {
    let mut previous = None;
    for _ in 0..cardinality {
        let value = input.u16().await?;
        if previous.is_some_and(|previous| value <= previous) {
            return Err(DeletionVectorError::Invalid);
        }
        previous = Some(value);
    }
    previous.ok_or(DeletionVectorError::Invalid)
}

pub(super) async fn bitset(input: &mut Input, cardinality: u32) -> Result<u16, DeletionVectorError> {
    let mut actual = 0_u32;
    let mut maximum = None;
    for index in 0..1024_u32 {
        let word = input.u64().await?;
        actual += word.count_ones();
        if word != 0 {
            maximum = Some(index * 64 + 63 - word.leading_zeros());
        }
    }
    if actual != cardinality {
        return Err(DeletionVectorError::Invalid);
    }
    u16::try_from(maximum.ok_or(DeletionVectorError::Invalid)?).map_err(|_| DeletionVectorError::Invalid)
}

pub(super) async fn runs(input: &mut Input, cardinality: u32) -> Result<u16, DeletionVectorError> {
    let count = input.u16().await?;
    let mut previous_end = 0;
    let mut actual = 0;
    for _ in 0..count {
        let start = u32::from(input.u16().await?);
        let length = u32::from(input.u16().await?) + 1;
        let end = start + length;
        if start < previous_end || end > 65_536 {
            return Err(DeletionVectorError::Invalid);
        }
        actual += length;
        previous_end = end;
    }
    if actual != cardinality {
        return Err(DeletionVectorError::Invalid);
    }
    previous_end
        .checked_sub(1)
        .and_then(|value| u16::try_from(value).ok())
        .ok_or(DeletionVectorError::Invalid)
}
