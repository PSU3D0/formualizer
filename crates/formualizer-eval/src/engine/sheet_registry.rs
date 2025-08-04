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
        &self.name_by_id[id as usize]
    }

    pub fn get_id(&self, name: &str) -> Option<SheetId> {
        self.id_by_name.get(name).copied()
    }
}
