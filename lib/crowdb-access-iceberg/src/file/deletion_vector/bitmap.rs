use super::{input::Input, DeletionVectorError};

mod containers;

pub(super) struct BitmapStats {
    pub(super) cardinality: u64,
    pub(super) maximum: Option<u32>,
}

struct Container {
    key: u16,
    cardinality: u32,
    run: bool,
    offset: Option<u32>,
}

pub(super) async fn validate(input: &mut Input) -> Result<BitmapStats, DeletionVectorError> {
    let start = input.position;
    let containers = header(input).await?;
    let mut stats = BitmapStats {
        cardinality: 0,
        maximum: None,
    };
    for container in containers {
        if container
            .offset
            .is_some_and(|offset| u64::from(offset) != input.position - start)
        {
            return Err(DeletionVectorError::Invalid);
        }
        let maximum = if container.run {
            containers::runs(input, container.cardinality).await?
        } else if container.cardinality <= 4096 {
            containers::array(input, container.cardinality).await?
        } else {
            containers::bitset(input, container.cardinality).await?
        };
        stats.cardinality += u64::from(container.cardinality);
        stats.maximum = Some((u32::from(container.key) << 16) | u32::from(maximum));
    }
    Ok(stats)
}

async fn header(input: &mut Input) -> Result<Vec<Container>, DeletionVectorError> {
    let cookie = input.u32().await?;
    let has_runs = cookie & 0xffff == 12347;
    let count = if has_runs {
        (cookie >> 16) + 1
    } else if cookie == 12346 {
        input.u32().await?
    } else {
        return Err(DeletionVectorError::Invalid);
    };
    if count > 65_536 {
        return Err(DeletionVectorError::Bounds);
    }
    let count = usize::try_from(count).map_err(|_| DeletionVectorError::Bounds)?;
    let mut runs = Vec::new();
    if has_runs {
        for _ in 0..count.div_ceil(8) {
            runs.push(input.take::<1>().await?[0]);
        }
    }
    let mut containers = Vec::with_capacity(count);
    let mut previous = None;
    for index in 0..count {
        let key = input.u16().await?;
        let cardinality = u32::from(input.u16().await?) + 1;
        if previous.is_some_and(|previous| key <= previous) {
            return Err(DeletionVectorError::Invalid);
        }
        previous = Some(key);
        containers.push(Container {
            key,
            cardinality,
            run: has_runs && runs[index / 8] & (1 << (index % 8)) != 0,
            offset: None,
        });
    }
    if !has_runs || count >= 4 {
        for container in &mut containers {
            container.offset = Some(input.u32().await?);
        }
    }
    Ok(containers)
}
