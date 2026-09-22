use crate::error::ValidationError;

macro_rules! identity {
    ($name:ident) => {
        #[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
        pub struct $name([u8; 16]);

        impl $name {
            #[must_use]
            pub fn random() -> Self {
                Self(*uuid::Uuid::new_v4().as_bytes())
            }

            /// # Errors
            /// Rejects zero or incorrectly sized identities.
            pub fn from_bytes(bytes: &[u8]) -> Result<Self, ValidationError> {
                let bytes: [u8; 16] = bytes.try_into().map_err(|_| ValidationError::Identity)?;
                if bytes == [0; 16] {
                    return Err(ValidationError::Identity);
                }
                Ok(Self(bytes))
            }

            #[must_use]
            pub const fn as_bytes(&self) -> &[u8; 16] {
                &self.0
            }
        }

        impl std::fmt::Display for $name {
            fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                for byte in self.0 {
                    write!(formatter, "{byte:02x}")?;
                }
                Ok(())
            }
        }
    };
}

identity!(CatalogId);
identity!(NamespaceId);
identity!(TableId);
identity!(FileId);
identity!(OperationId);
