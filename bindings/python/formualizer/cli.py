"""Console entry for the shared Rust CLI (native platforms only)."""

import sys
from collections.abc import Callable, Sequence
from threading import Event, Thread
from typing import Protocol, cast

from . import formualizer_py as _native


class _CancelToken(Protocol):
    def cancel(self) -> None: ...


# Native-only bridge (absent from the Pyodide build, so not in the stubs).
_RunCli = Callable[[list[str], _CancelToken], tuple[int, bytes, bytes]]


def _run(run_cli: _RunCli, argv: list[str], cancel: _CancelToken) -> int:
    result: list[tuple[int, bytes, bytes]] = []
    errors: list[BaseException] = []
    done = Event()

    def work() -> None:
        try:
            result.append(run_cli(["formualizer", *argv], cancel))
        except BaseException as exc:
            errors.append(exc)
        finally:
            done.set()

    worker = Thread(target=work, name="formualizer-cli")
    worker.start()
    # Completion is tracked separately: CPython can mark a thread stopped if
    # SIGINT interrupts join(), even though native work is still running.
    while not done.is_set():
        try:
            worker.join(0.05)
            if not worker.is_alive():
                done.wait(0.05)
        except KeyboardInterrupt:
            cancel.cancel()
    if errors:
        raise errors[0]
    code, stdout, stderr = result[0]
    sys.stdout.buffer.write(stdout)
    sys.stderr.buffer.write(stderr)
    return code


def main(argv: Sequence[str] | None = None) -> int:
    """Run recalc, returning the Rust exit status unchanged."""
    run_cli = getattr(_native, "_run_cli", None)
    token_type = getattr(_native, "_CliCancelToken", None)
    if run_cli is None or token_type is None:
        print("CLI not available on this platform", file=sys.stderr)
        return 1
    args = list(sys.argv[1:] if argv is None else argv)
    return _run(cast(_RunCli, run_cli), args, cast(_CancelToken, token_type()))
