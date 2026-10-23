// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Reproducible request sizes independent of callback and thread ordering.

#[derive(Clone)]
pub(crate) struct PayloadProfile {
    sizes: Vec<usize>,
    cumulative: Vec<u64>,
    seed: u64,
}

impl PayloadProfile {
    pub fn new(fixed: usize, mix: Option<&str>, seed: u64) -> Result<Self, String> {
        let mut profile = Self {
            sizes: Vec::new(),
            cumulative: Vec::new(),
            seed,
        };
        let mut total = 0_u64;
        if let Some(mix) = mix {
            for entry in mix.split(',') {
                let (size, weight) = entry
                    .split_once(':')
                    .ok_or("payload mix must contain comma-separated bytes:weight entries")?;
                let size = size.parse::<usize>().map_err(|_| "invalid payload size")?;
                let weight = weight.parse::<u32>().map_err(|_| "invalid payload weight")?;
                if size > (1 << 20) || weight == 0 || profile.sizes.contains(&size) {
                    return Err("payload sizes must be unique, at most 1 MiB, with positive weights".into());
                }
                total += u64::from(weight);
                profile.sizes.push(size);
                profile.cumulative.push(total);
                if profile.sizes.len() > 16 {
                    return Err("payload mix supports at most 16 sizes".into());
                }
            }
        } else {
            profile.sizes.push(fixed);
            profile.cumulative.push(1);
        }
        Ok(profile)
    }

    pub fn sizes(&self) -> &[usize] {
        &self.sizes
    }

    pub fn maximum(&self) -> usize {
        self.sizes.iter().copied().max().unwrap_or(0)
    }

    pub fn select(&self, id: u64) -> usize {
        if self.sizes.len() == 1 {
            return 0;
        }
        let mut value = id.wrapping_add(self.seed).wrapping_add(0x9e37_79b9_7f4a_7c15);
        value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        value ^= value >> 31;
        let target = value % self.cumulative.last().copied().unwrap_or(1);
        self.cumulative.partition_point(|&weight| weight <= target)
    }
}
