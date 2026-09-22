use crate::error::ValidationError;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum FormatAction {
    Parse = 1,
    Read = 2,
    Create = 4,
    Write = 8,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct FormatSupport(u8);

impl FormatSupport {
    /// # Errors
    /// Rejects unknown bits and operations without their prerequisites.
    pub fn from_bits(bits: u8) -> Result<Self, ValidationError> {
        if bits & !0x0f != 0 || (bits & 2 != 0 && bits & 1 == 0) || (bits & 0x0c != 0 && bits & 2 == 0) {
            return Err(ValidationError::Capabilities);
        }
        Ok(Self(bits))
    }

    #[must_use]
    pub const fn supports(self, action: FormatAction) -> bool {
        self.0 & action as u8 != 0
    }

    #[must_use]
    pub const fn bits(self) -> u8 {
        self.0
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct Capabilities {
    pub versions: [FormatSupport; 3],
    pub upgrade_v1_v2: bool,
    pub upgrade_v2_v3: bool,
}

impl Capabilities {
    /// # Errors
    /// Rejects contradictory capabilities and unavailable upgrade targets.
    pub fn validate(&self) -> Result<(), ValidationError> {
        for (enabled, source, target) in [(self.upgrade_v1_v2, 0, 1), (self.upgrade_v2_v3, 1, 2)] {
            if enabled
                && !(self.versions[source].supports(FormatAction::Read)
                    && self.versions[target].supports(FormatAction::Write))
            {
                return Err(ValidationError::Capabilities);
            }
        }
        Ok(())
    }

    /// # Errors
    /// Rejects malformed or unsupported capability bits.
    pub fn from_bits(bits: u16) -> Result<Self, ValidationError> {
        if bits & 0xc000 != 0 {
            return Err(ValidationError::Capabilities);
        }
        let mut versions = [FormatSupport::default(); 3];
        for (index, support) in versions.iter_mut().enumerate() {
            let flags =
                u8::try_from((bits >> (index * 4)) & 0x0f).map_err(|_| ValidationError::Capabilities)?;
            *support = FormatSupport::from_bits(flags)?;
        }
        let result = Self {
            versions,
            upgrade_v1_v2: bits & 0x1000 != 0,
            upgrade_v2_v3: bits & 0x2000 != 0,
        };
        result.validate()?;
        Ok(result)
    }

    #[must_use]
    pub fn bits(&self) -> u16 {
        let mut bits = u16::from(self.upgrade_v1_v2) << 12 | u16::from(self.upgrade_v2_v3) << 13;
        for (index, support) in self.versions.iter().enumerate() {
            let flags = u16::from(support.bits());
            bits |= flags << (index * 4);
        }
        bits
    }
}
