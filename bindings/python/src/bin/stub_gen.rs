#[cfg(not(target_os = "emscripten"))]
use pyo3_stub_gen::Result;
#[cfg(not(target_os = "emscripten"))]
use std::fs;
#[cfg(not(target_os = "emscripten"))]
use std::path::PathBuf;

#[cfg(target_os = "emscripten")]
fn main() {}

#[cfg(not(target_os = "emscripten"))]
fn main() -> Result<()> {
    // `stub_info` is defined in `src/lib.rs` by `define_stub_info_gatherer!`.
    let stub = formualizer_py::stub_info()?;
    stub.generate()?;

    // pyo3-stub-gen 0.23 follows maturin's private extension module path and
    // writes `formualizer/formualizer_py/__init__.pyi`. The public Python package
    // re-exports that module, so publish the declarations as a companion
    // `formualizer_py.pyi`, emit a public re-export facade at the package root,
    // and remove the intermediate package-shaped directory.
    let generated_stub_path: PathBuf = [
        env!("CARGO_MANIFEST_DIR"),
        "formualizer",
        "formualizer_py",
        "__init__.pyi",
    ]
    .iter()
    .collect();
    let generated_stub_dir = generated_stub_path
        .parent()
        .expect("generated stub path has a parent");
    let private_stub_path: PathBuf = [
        env!("CARGO_MANIFEST_DIR"),
        "formualizer",
        "formualizer_py.pyi",
    ]
    .iter()
    .collect();
    let public_stub_path: PathBuf = [env!("CARGO_MANIFEST_DIR"), "formualizer", "__init__.pyi"]
        .iter()
        .collect();

    let mut contents = fs::read_to_string(&generated_stub_path)?;
    fs::remove_dir_all(generated_stub_dir)?;

    for (generated, corrected) in [
        (
            "def __eq__(self, other: typing.Any)",
            "def __eq__(self, other: typing.Any, /)",
        ),
        (
            "def __ne__(self, other: typing.Any)",
            "def __ne__(self, other: typing.Any, /)",
        ),
        (
            "def __contains__(self, address: builtins.str)",
            "def __contains__(self, address: builtins.str, /)",
        ),
        (
            "def __getitem__(self, index: builtins.int)",
            "def __getitem__(self, index: builtins.int, /)",
        ),
        (
            "def __getitem__(self, name: builtins.str)",
            "def __getitem__(self, name: builtins.str, /)",
        ),
        (
            "def __getitem__(self, address: builtins.str)",
            "def __getitem__(self, address: builtins.str, /)",
        ),
    ] {
        assert!(
            contents.contains(generated),
            "generated stub is missing the expected magic-method signature: {generated}"
        );
        contents = contents.replace(generated, corrected);
    }

    let additional_public_exports = r#"    "CalculationDeferred",
    "DependencyStateUnavailableError",
    "ExcelEvaluationError",
    "FormualizerHostError",
    "InspectionError",
    "InspectionResourceExhaustedError",
    "InspectionRevisionMismatchError",
    "InvalidInspectionAddressError",
    "InvalidInspectionOptionsError",
    "ParserError",
    "SheetNotFoundError",
    "PyFormulaDialect",
    "PyRefWalker",
    "PyToken",
    "PyTokenSubType",
    "PyTokenType",
    "PyTokenizer",
    "PyTokenizerIter",
    "SheetPortConstraintError",
    "SheetPortError",
    "SheetPortManifestError",
    "SheetPortWorkbookError",
    "TokenizerError",
"#;
    let all_end = "]\n\n@typing.final";
    assert!(
        contents.contains(all_end),
        "generated stub is missing the expected __all__ terminator"
    );
    contents = contents.replacen(all_end, &format!("{additional_public_exports}{all_end}"), 1);

    contents.push_str(
        r#"

# Exceptions created with pyo3::create_exception! are public runtime types but
# are not currently discovered by pyo3-stub-gen's inventory.
class CalculationDeferred(Exception):
    kind: str
    reason: str
    context: str
class TokenizerError(Exception): ...
class ParserError(Exception): ...
class FormualizerHostError(Exception): ...
class ExcelEvaluationError(Exception):
    kind: str
    excel_kind: str
    message: str | None
    excel_message: str | None
    context: dict[str, typing.Any] | None
    extra: dict[str, typing.Any] | None
    # Present on resource failures; cancellation uses kind == "Cancelled".
    resource_reason: str
    limit: int
    observed: int
    request_id: int | None
class InspectionError(Exception): ...
class SheetNotFoundError(InspectionError): ...
class InvalidInspectionAddressError(InspectionError): ...
class InvalidInspectionOptionsError(InspectionError): ...
class DependencyStateUnavailableError(InspectionError): ...
class InspectionRevisionMismatchError(InspectionError):
    expected: StateStamp
    actual: StateStamp
class InspectionResourceExhaustedError(InspectionError): ...
class SheetPortError(Exception): ...
class SheetPortManifestError(SheetPortError): ...
class SheetPortConstraintError(SheetPortError): ...
class SheetPortWorkbookError(SheetPortError): ...

# Backwards compatible Py* aliases
#
# Historically this package exported most symbols with a `Py...` prefix.
# Keep these aliases so older code continues to type-check.
PyToken = Token
PyTokenizer = Tokenizer
PyTokenizerIter = TokenizerIter
PyRefWalker = RefWalker
PyTokenType = TokenType
PyTokenSubType = TokenSubType
PyFormulaDialect = FormulaDialect

# Private type-check sentinels make a deleted generated member a mypy error,
# while stubtest still rejects invented members that are absent at runtime.
if typing.TYPE_CHECKING:
    _inspection_enum_members = (
        Staleness.Current, Staleness.Dirty, Staleness.NeverEvaluated, Staleness.Unknown,
        Provenance.Declared, Provenance.Observed, Provenance.Unknown,
        LinkDisposition.Expanded, LinkDisposition.Convergent, LinkDisposition.Cycle, LinkDisposition.Elided, LinkDisposition.Unknown,
        TraceDirection.Precedents, TraceDirection.Dependents,
        OmittedCountKind.Exact, OmittedCountKind.AtLeast, OmittedCountKind.Unknown,
        SpillRoleKind.Anchor, SpillRoleKind.Member, SpillRoleKind.Unknown,
        ReferenceKind.Cell, ReferenceKind.Range, ReferenceKind.Name, ReferenceKind.Table, ReferenceKind.External, ReferenceKind.Unsupported, ReferenceKind.Unknown,
        NameResolutionKind.Cell, NameResolutionKind.Range, NameResolutionKind.Literal, NameResolutionKind.Formula, NameResolutionKind.Unresolved, NameResolutionKind.Unknown,
        TraceLinkKindType.Formula, TraceLinkKindType.SpillAnchor, TraceLinkKindType.SpillReader, TraceLinkKindType.Unknown,
    )
"#,
    );
    let all_start = contents
        .find("__all__ = [")
        .expect("generated stub is missing __all__");
    let all_close = all_start
        + contents[all_start..]
            .find("]\n\n@typing.final")
            .expect("generated stub is missing the end of __all__");
    let mut public_all = contents[all_start..=all_close].to_owned();
    public_all.insert_str(
        public_all.len() - 1,
        "    \"ReferenceLike\",\n    \"visitor\",\n",
    );

    // The runtime records live in Python, while native functions transport the
    // shared Rust DTOs. Keep generated signatures precise without inventing
    // native runtime exports for these typing-only declarations.
    let source_imports = "from ._source_types import SourceCalculationPolicy, SourceRecalculateLimits, SourceRecalculateResult\n";
    contents.push_str(source_imports);
    let lines: Vec<String> = contents
        .lines()
        .map(|line| {
            if line.starts_with("def recalculate_xlsx_bytes(")
                || line.starts_with("def recalculate_xlsx_file(")
            {
                let mut line = line.to_owned();
                for (name, shape) in [
                    ("calculation", "SourceCalculationPolicy"),
                    ("limits", "SourceRecalculateLimits"),
                ] {
                    let start = line.find(&format!("{name}: ")).expect("source argument");
                    let end = start
                        + line[start..]
                            .find(" = None")
                            .expect("optional source argument");
                    line.replace_range(start..end, &format!("{name}: {shape} | None"));
                }
                line.replace("-> typing.Any:", "-> SourceRecalculateResult:")
            } else {
                line.to_owned()
            }
        })
        .collect();
    contents = lines.join("\n") + "\n";
    public_all.insert_str(public_all.len() - 1,
        "    \"SourceCalculationPolicy\",\n    \"SourceRecalculateLimits\",\n    \"SourceRecalculateResult\",\n    \"SourceOperationDiagnostics\",\n    \"SourceUnavailableOperation\",\n    \"SourceDependencyEvidence\",\n");
    fs::write(&private_stub_path, contents)?;
    fs::write(
        &public_stub_path,
        format!(
            r#"# This file is automatically generated by pyo3_stub_gen
# ruff: noqa: E501, F401, F403, F405

from . import visitor as visitor
from ._types import ReferenceLike as ReferenceLike
from ._source_types import *
from .formualizer_py import *

{public_all}
"#
        ),
    )?;

    Ok(())
}
