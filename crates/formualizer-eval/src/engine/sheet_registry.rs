use std::collections::HashMap;

use crate::SheetId;

#[derive(Default, Debug)]
pub struct SheetRegistry {
    id_by_name: HashMap<String, SheetId>,
    name_by_id: Vec<String>,
}

impl SheetRegistry {
    pub fn new() -> Self {
        SheetRegistry::default()
    }

    pub fn id_for(&mut self, name: &str) -> SheetId {
        if let Some(&id) = self.id_by_name.get(name) {
            return id;
        }

        let id = self.name_by_id.len() as SheetId;
        self.name_by_id.push(name.to_string());
        self.id_by_name.insert(name.to_string(), id);
        id
    }

    pub fn name(&self, id: SheetId) -> &str {
        if (id as usize) < self.name_by_id.len() {
            &self.name_by_id[id as usize]
        } else {
            ""
        }
    }

    pub fn get_id(&self, name: &str) -> Option<SheetId> {
        self.id_by_name.get(name).copied()
    }

    /// Get all sheet IDs and names (excluding removed sheets)
    pub fn all_sheets(&self) -> Vec<(SheetId, String)> {
        self.name_by_id
            .iter()
            .enumerate()
            .filter(|(_, name)| !name.is_empty())
            .map(|(id, name)| (id as SheetId, name.clone()))
            .collect()
    }

    /// Remove a sheet from the registry
    /// Note: This doesn't actually free the ID, it just marks it as removed
    pub fn remove(&mut self, id: SheetId) -> Result<(), formualizer_common::ExcelError> {
        use formualizer_common::{ExcelError, ExcelErrorKind};

        // Check if the ID exists
        if id as usize >= self.name_by_id.len() {
            return Err(
                ExcelError::new(ExcelErrorKind::Value).with_message("Sheet ID does not exist")
            );
        }

        // Get the name to remove from id_by_name
        let name = self.name_by_id[id as usize].clone();
        if name.is_empty() {
            // Already removed
            return Ok(());
        }

        // Remove from id_by_name mapping
        self.id_by_name.remove(&name);

        // Mark as removed in name_by_id (we can't actually remove it to preserve IDs)
        self.name_by_id[id as usize] = String::new();

        Ok(())
    }

    /// Rename a sheet
    pub fn rename(
        &mut self,
        id: SheetId,
        new_name: &str,
    ) -> Result<(), formualizer_common::ExcelError> {
        use formualizer_common::{ExcelError, ExcelErrorKind};

        // Check if the ID exists
        if id as usize >= self.name_by_id.len() {
            return Err(
                ExcelError::new(ExcelErrorKind::Value).with_message("Sheet ID does not exist")
            );
        }

        // Get the old name
        let old_name = self.name_by_id[id as usize].clone();

        // Check if new name is already taken by another sheet
        if let Some(&existing_id) = self.id_by_name.get(new_name)
            && existing_id != id
        {
            return Err(ExcelError::new(ExcelErrorKind::Value)
                .with_message(format!("Sheet name '{new_name}' already exists")));
        }

        // Remove old name mapping
        self.id_by_name.remove(&old_name);

        // Update to new name
        self.name_by_id[id as usize] = new_name.to_string();
        self.id_by_name.insert(new_name.to_string(), id);

        Ok(())
    }
}
