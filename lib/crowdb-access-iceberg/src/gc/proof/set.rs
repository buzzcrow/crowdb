use crate::{
    catalog::CatalogError,
    error::ValidationError,
    file::file_key,
    key::{CatalogScope, FileId, IcebergKey},
    operation::PayloadReference,
};

use super::super::{GcPage, GcRepository, GcTask};

enum Node {
    File(FileId),
    Branch { bit: u8, zero: Vec<u8>, one: Vec<u8> },
}

impl GcRepository {
    /// # Errors
    /// Rejects incomplete proofs, stale task state and missing or damaged proof pages.
    pub async fn proof_contains(&self, task: &GcTask, file: FileId) -> Result<bool, CatalogError> {
        self.verify_proof_task(task).await?;
        if !task.proof.complete {
            return Err(CatalogError::Busy);
        }
        let root = task.proof.root.as_ref().ok_or(ValidationError::Record)?;
        let (_, found) = self.mark_path(task, root, file).await?;
        Ok(found == file)
    }

    pub(in crate::gc) async fn insert_mark(
        &self,
        task: &GcTask,
        root: Option<&PayloadReference>,
        file: FileId,
    ) -> Result<(PayloadReference, bool), CatalogError> {
        let Some(root) = root else {
            return Ok((self.mark_leaf(task, file).await?, true));
        };
        let (path, found) = self.mark_path(task, root, file).await?;
        if found == file {
            return Ok((root.clone(), false));
        }
        let bit = differing_bit(file, found)?;
        let split = path
            .iter()
            .position(|(node, _)| node.bit() >= bit)
            .unwrap_or(path.len());
        let subtree = if split < path.len() {
            path[split].1.clone()
        } else if let Some((node, _)) = path.last() {
            node.child(file)?.to_vec()
        } else {
            root.page_key(0)?.encode()?
        };
        let leaf = self.mark_leaf(task, file).await?.page_key(0)?.encode()?;
        let (zero, one) = if has_bit(file, bit) {
            (subtree, leaf)
        } else {
            (leaf, subtree)
        };
        let mut result = self.mark_branch(task, bit, zero, one).await?;
        for (node, _) in path.into_iter().take(split).rev() {
            let Node::Branch {
                bit,
                mut zero,
                mut one,
            } = node
            else {
                return Err(ValidationError::Record.into());
            };
            if has_bit(file, bit) {
                one = result.page_key(0)?.encode()?;
            } else {
                zero = result.page_key(0)?.encode()?;
            }
            result = self.mark_branch(task, bit, zero, one).await?;
        }
        Ok((result, true))
    }

    async fn mark_path(
        &self,
        task: &GcTask,
        root: &PayloadReference,
        file: FileId,
    ) -> Result<(Vec<(Node, Vec<u8>)>, FileId), CatalogError> {
        let mut key = root.page_key(0)?.encode()?;
        let mut path = Vec::new();
        let mut previous = None;
        for _ in 0..=128 {
            let (page, reference) = self.proof_page(task, &key).await?;
            if path.is_empty() && reference != *root {
                return Err(ValidationError::Record.into());
            }
            let node = Node::decode(&page)?;
            match &node {
                Node::File(found) => return Ok((path, *found)),
                Node::Branch { bit, .. } => {
                    if previous.is_some_and(|previous| *bit <= previous) {
                        return Err(ValidationError::Record.into());
                    }
                    previous = Some(*bit);
                    let next = node.child(file)?.to_vec();
                    path.push((node, key));
                    key = next;
                }
            }
        }
        Err(ValidationError::Record.into())
    }

    async fn mark_leaf(&self, task: &GcTask, file: FileId) -> Result<PayloadReference, CatalogError> {
        self.put_proof_page(task, 0, 0, vec![file_key(task.context.catalog, file).encode()?])
            .await
    }

    async fn mark_branch(
        &self,
        task: &GcTask,
        bit: u8,
        zero: Vec<u8>,
        one: Vec<u8>,
    ) -> Result<PayloadReference, CatalogError> {
        let order = u64::from(zero > one);
        let mut entries = vec![zero, one];
        entries.sort();
        self.put_proof_page(task, 1, u64::from(bit) * 2 + order, entries)
            .await
    }
}

impl Node {
    fn decode(page: &GcPage) -> Result<Self, ValidationError> {
        match (page.kind, page.entries.len()) {
            (0, 1) if page.sequence == 0 => {
                let IcebergKey::Catalog {
                    scope: CatalogScope::File,
                    suffix,
                    ..
                } = IcebergKey::decode(&page.entries[0])?
                else {
                    return Err(ValidationError::Record);
                };
                Ok(Self::File(FileId::from_bytes(&suffix)?))
            }
            (1, 2) if page.sequence < 256 => {
                let zero = (page.sequence % 2) as usize;
                Ok(Self::Branch {
                    bit: u8::try_from(page.sequence / 2).map_err(|_| ValidationError::Record)?,
                    zero: page.entries[zero].clone(),
                    one: page.entries[1 - zero].clone(),
                })
            }
            _ => Err(ValidationError::Record),
        }
    }

    fn bit(&self) -> u8 {
        match self {
            Self::Branch { bit, .. } => *bit,
            Self::File(_) => 128,
        }
    }

    fn child(&self, file: FileId) -> Result<&[u8], ValidationError> {
        match self {
            Self::Branch { bit, zero, one } => Ok(if has_bit(file, *bit) { one } else { zero }),
            Self::File(_) => Err(ValidationError::Record),
        }
    }
}

fn has_bit(file: FileId, bit: u8) -> bool {
    file.as_bytes()[usize::from(bit / 8)] & (128 >> (bit % 8)) != 0
}

fn differing_bit(left: FileId, right: FileId) -> Result<u8, ValidationError> {
    for (index, (left, right)) in left.as_bytes().iter().zip(right.as_bytes()).enumerate() {
        let difference = left ^ right;
        if difference != 0 {
            return u8::try_from(index * 8 + difference.leading_zeros() as usize)
                .map_err(|_| ValidationError::Record);
        }
    }
    Err(ValidationError::Record)
}
