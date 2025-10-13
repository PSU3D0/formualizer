use crate::binding::{BoundPort, ManifestBindings, RecordBinding, ScalarBinding, TableBinding};
use crate::error::SheetPortError;
use crate::location::{AreaLocation, FieldLocation, ScalarLocation, TableLocation};
use formualizer_workbook::Workbook;

/// Validates bound manifest selectors against a concrete workbook instance.
pub struct WorkbookContext<'a> {
    workbook: &'a Workbook,
}

impl<'a> WorkbookContext<'a> {
    pub fn new(workbook: &'a Workbook) -> Self {
        Self { workbook }
    }

    pub fn validate(&self, bindings: &ManifestBindings) -> Result<(), SheetPortError> {
        for binding in bindings.bindings() {
            match &binding.kind {
                BoundPort::Scalar(scalar) => self.validate_scalar(binding.id.as_str(), scalar)?,
                BoundPort::Record(record) => self.validate_record(binding.id.as_str(), record)?,
                BoundPort::Range(range) => {
                    self.ensure_area(binding.id.as_str(), &range.location)?
                }
                BoundPort::Table(table) => self.validate_table(binding.id.as_str(), table)?,
            }
        }
        Ok(())
    }

    fn validate_scalar(
        &self,
        port_id: &str,
        binding: &ScalarBinding,
    ) -> Result<(), SheetPortError> {
        match &binding.location {
            ScalarLocation::Cell(addr) => self.ensure_sheet(port_id, &addr.sheet),
            ScalarLocation::Name(name) => Err(SheetPortError::UnsupportedSelector {
                port: port_id.to_string(),
                reason: format!("named range `{name}` is not yet supported"),
            }),
            ScalarLocation::StructRef(struct_ref) => Err(SheetPortError::UnsupportedSelector {
                port: port_id.to_string(),
                reason: format!("structured reference `{struct_ref}` is not yet supported"),
            }),
        }
    }

    fn validate_record(
        &self,
        port_id: &str,
        binding: &RecordBinding,
    ) -> Result<(), SheetPortError> {
        self.ensure_area(port_id, &binding.location)?;
        for (field_name, field) in &binding.fields {
            match &field.location {
                FieldLocation::Cell(addr) => self.ensure_sheet(port_id, &addr.sheet)?,
                FieldLocation::Name(name) => {
                    return Err(SheetPortError::UnsupportedSelector {
                        port: port_id.to_string(),
                        reason: format!(
                            "record field `{field_name}` references named range `{name}` which is not yet supported"
                        ),
                    });
                }
                FieldLocation::StructRef(struct_ref) => {
                    return Err(SheetPortError::UnsupportedSelector {
                        port: port_id.to_string(),
                        reason: format!(
                            "record field `{field_name}` uses structured reference `{struct_ref}` which is not yet supported"
                        ),
                    });
                }
            }
        }
        Ok(())
    }

    fn ensure_area(&self, port_id: &str, location: &AreaLocation) -> Result<(), SheetPortError> {
        match location {
            AreaLocation::Range(addr) => self.ensure_sheet(port_id, &addr.sheet),
            AreaLocation::Name(name) => Err(SheetPortError::UnsupportedSelector {
                port: port_id.to_string(),
                reason: format!("area selector named range `{name}` is not yet supported"),
            }),
            AreaLocation::StructRef(struct_ref) => Err(SheetPortError::UnsupportedSelector {
                port: port_id.to_string(),
                reason: format!("structured reference `{struct_ref}` is not yet supported"),
            }),
            AreaLocation::Layout(layout) => self.ensure_sheet(port_id, &layout.sheet),
        }
    }

    fn validate_table(&self, port_id: &str, binding: &TableBinding) -> Result<(), SheetPortError> {
        match &binding.location {
            TableLocation::Table(table) => Err(SheetPortError::UnsupportedSelector {
                port: port_id.to_string(),
                reason: format!(
                    "workbook table `{}` is not yet supported for table ports",
                    table.name
                ),
            }),
            TableLocation::Layout(layout) => self.ensure_sheet(port_id, &layout.sheet),
        }
    }

    fn ensure_sheet(&self, port_id: &str, sheet: &str) -> Result<(), SheetPortError> {
        if self.workbook.has_sheet(sheet) {
            Ok(())
        } else {
            Err(SheetPortError::MissingSheet {
                port: port_id.to_string(),
                sheet: sheet.to_string(),
            })
        }
    }
}
