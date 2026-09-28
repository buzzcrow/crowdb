use super::{ManifestContent, ManifestListEntry, ManifestListSelection, SnapshotManifestError as Error};

pub(super) struct SnapshotRowAssignments {
    snapshot_id: i64,
    range: Option<(i64, i64)>,
    next: i64,
    current: Option<i64>,
}

impl SnapshotRowAssignments {
    pub(super) fn legacy(snapshot_id: i64) -> Self {
        Self {
            snapshot_id,
            range: None,
            next: 0,
            current: None,
        }
    }
    pub(super) fn new(selection: &ManifestListSelection) -> Result<Self, Error> {
        let range = match (selection.first_row_id, selection.added_rows) {
            (Some(first), Some(rows)) => Some((first, first.checked_add(rows).ok_or(Error::RowIds)?)),
            (None, None) => None,
            _ => return Err(Error::RowIds),
        };
        Ok(Self {
            snapshot_id: selection.snapshot_id,
            range,
            next: selection.first_row_id.unwrap_or(0),
            current: None,
        })
    }

    pub(super) fn begin(&mut self, reference: &ManifestListEntry) -> Result<(), Error> {
        self.current = None;
        let Some((first, end)) = self.range else {
            return Ok(());
        };
        if reference.content == ManifestContent::Deletes {
            return Ok(());
        }
        let assigned = reference.first_row_id.ok_or(Error::RowIds)?;
        if assigned > end
            || (assigned >= first && assigned < self.next)
            || (reference.added_snapshot_id == self.snapshot_id && assigned < first)
        {
            return Err(Error::RowIds);
        }
        self.current = Some(assigned);
        Ok(())
    }

    pub(super) fn check(&self, next_row_id: Option<i64>) -> Result<(), Error> {
        let (Some((first, end)), Some(assigned)) = (self.range, self.current) else {
            return Ok(());
        };
        let next = next_row_id.ok_or(Error::RowIds)?;
        let bound = if assigned < first { first } else { end };
        if next < assigned || next > bound {
            return Err(Error::RowIds);
        }
        Ok(())
    }

    pub(super) fn finish_manifest(&mut self, next_row_id: Option<i64>) -> Result<(), Error> {
        self.check(next_row_id)?;
        if let (Some((first, _)), Some(assigned)) = (self.range, self.current) {
            if assigned >= first {
                self.next = next_row_id.ok_or(Error::RowIds)?;
            }
        }
        self.current = None;
        Ok(())
    }
}
