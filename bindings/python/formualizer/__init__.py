"""Formualizer for Python.

This package exposes high-performance Excel-formula parsing and evaluation via Rust (PyO3).

Most of the public API lives in the native extension module ``formualizer.formualizer_py``
and is re-exported here for convenience.

See ``bindings/python/README.md`` in the repository for longer, runnable examples.
"""

from . import _source_types, visitor
from . import formualizer_py as _py
from ._source_types import *  # noqa: F403
from ._types import ReferenceLike
from .formualizer_py import *  # noqa: F403

__all__ = [*_py.__all__, *_source_types.__all__, "ReferenceLike", "visitor"]

# ---------------------------------------------------------------------------
# Backwards compatible aliases
# ---------------------------------------------------------------------------
#
# Earlier versions exposed most symbols with a `Py...` prefix.
# Keep these aliases so older code keeps working.
PyToken = _py.Token
PyTokenizer = _py.Tokenizer
PyTokenizerIter = _py.TokenizerIter
PyRefWalker = _py.RefWalker
PyTokenType = _py.TokenType
PyTokenSubType = _py.TokenSubType
PyFormulaDialect = _py.FormulaDialect
