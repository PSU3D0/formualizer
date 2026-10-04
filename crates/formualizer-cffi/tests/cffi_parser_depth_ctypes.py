import ctypes
import sys

if len(sys.argv) != 2:
    raise SystemExit(f"usage: {sys.argv[0]} <library-path>")

LIB_PATH = sys.argv[1]
def expected_error(shape):
    if shape in ("power", "arithmetic", "postfix"):
        return b"Formula AST height limit exceeded"
    return b"Formula nesting too deep (max 72)"


class Buffer(ctypes.Structure):
    _fields_ = [
        ("data", ctypes.POINTER(ctypes.c_uint8)),
        ("len", ctypes.c_size_t),
        ("cap", ctypes.c_size_t),
    ]


class Status(ctypes.Structure):
    _fields_ = [("code", ctypes.c_int), ("error", Buffer)]


class ParseOptions(ctypes.Structure):
    _fields_ = [
        ("include_spans", ctypes.c_bool),
        ("dialect", ctypes.c_int),
    ]


lib = ctypes.CDLL(LIB_PATH)
lib.fz_parse_ast.argtypes = [
    ctypes.c_char_p,
    ParseOptions,
    ctypes.c_int,
    ctypes.POINTER(Status),
]
lib.fz_parse_ast.restype = Buffer
lib.fz_parse_canonical_formula.argtypes = [
    ctypes.c_char_p,
    ctypes.c_int,
    ctypes.POINTER(Status),
]
lib.fz_parse_canonical_formula.restype = Buffer
lib.fz_buffer_free.argtypes = [Buffer]
lib.fz_buffer_free.restype = None


def read_buffer(buffer):
    if not buffer.data or buffer.len == 0:
        return b""
    return ctypes.string_at(buffer.data, buffer.len)


def accepted_formulas():
    return [
        ("parentheses", "=" + "(" * 64 + "1" + ")" * 64),
        ("sum", "=" + "SUM(" * 64 + "1" + ")" * 64),
        ("flat-height", "=" + "A1+" * 255 + "A1"),
        ("postfix-height", "=1" + "%" * 255),
        ("power-height", "=" + "1^" * 255 + "1"),
        ("if", "=" + "IF(A1>0," * 64 + "1" + ",0)" * 64),
    ]


def hostile_formulas():
    return [
        ("parentheses", "=" + "(" * 1000 + "1" + ")" * 1000),
        ("unary", "=" + "-" * 1000 + "1"),
        ("sum", "=" + "SUM(" * 1000 + "1" + ")" * 1000),
        ("right-infix", "=" + "1+(" * 1000 + "1" + ")" * 1000),
        ("if", "=" + "IF(A1>0," * 1000 + "1" + ",0)" * 1000),
        ("arrays", "=" + "{" * 1000 + "1" + "}" * 1000),
        ("power", "=" + "1^" * 1000 + "1"),
        ("arithmetic", "=" + "1+" * 1000 + "1"),
        ("postfix", "=1" + "%" * 1000),
    ]


def ast_call(formula):
    status = Status(0, Buffer())
    output = lib.fz_parse_ast(
        formula.encode(), ParseOptions(False, 0), 0, ctypes.byref(status)
    )
    output_bytes = read_buffer(output)
    error_bytes = read_buffer(status.error)
    code = status.code
    lib.fz_buffer_free(output)
    lib.fz_buffer_free(status.error)
    return code, output_bytes, error_bytes


def canonical_call(formula):
    status = Status(0, Buffer())
    output = lib.fz_parse_canonical_formula(formula.encode(), 0, ctypes.byref(status))
    output_bytes = read_buffer(output)
    error_bytes = read_buffer(status.error)
    code = status.code
    lib.fz_buffer_free(output)
    lib.fz_buffer_free(status.error)
    return code, output_bytes, error_bytes


for shape, formula in accepted_formulas():
    code, output, error = ast_call(formula)
    assert code == 0 and output and not error, (shape, code, error)
    code, output, error = canonical_call(formula)
    assert code == 0 and output and not error, (shape, code, error)

for shape, formula in hostile_formulas():
    code, output, error = ast_call(formula)
    assert code == 1 and not output and expected_error(shape) in error, (shape, code, error)
    code, output, error = canonical_call(formula)
    assert code == 1 and not output and expected_error(shape) in error, (shape, code, error)

code, output, error = ast_call("=A1+1")
assert code == 0 and output and not error
code, output, error = canonical_call("=SUM(A1:A2)")
assert code == 0 and output and not error

print("cffi ctypes parser depth smoke passed")
