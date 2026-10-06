use std::fmt;

use uuid::Uuid;

use crate::DatasetError;

macro_rules! identity {
    ($name:ident) => {
        #[derive(
            Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, serde::Deserialize, serde::Serialize,
        )]
        pub struct $name([u8; 16]);

        impl $name {
            #[must_use]
            pub fn random() -> Self {
                Self(*Uuid::new_v4().as_bytes())
            }

            /// # Errors
            /// Rejects zero and incorrectly sized identities.
            pub fn from_bytes(bytes: &[u8]) -> Result<Self, DatasetError> {
                let bytes: [u8; 16] = bytes.try_into().map_err(|_| DatasetError::IdentityLength)?;
                if bytes == [0; 16] {
                    return Err(DatasetError::ZeroIdentity);
                }
                Ok(Self(bytes))
            }

            #[must_use]
            pub const fn as_bytes(&self) -> &[u8; 16] {
                &self.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                for byte in self.0 {
                    write!(formatter, "{byte:02x}")?;
                }
                Ok(())
            }
        }
    };
}

identity!(DatasetId);
identity!(SnapshotId);
identity!(OperationId);
