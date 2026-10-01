"""Console entry for the shared Rust CLI (native platforms only)."""

import sys
from threading import Event, Thread

from . import formualizer_py as _native


def _run(argv, cancel):
    result = []
    errors = []
    done = Event()

    def work():
        try:
            result.append(_native._run_cli(["formualizer", *argv], cancel))
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


def main(argv=None) -> int:
    """Run recalc, returning the Rust exit status unchanged."""
    if not hasattr(_native, "_run_cli"):
        print("CLI not available on this platform", file=sys.stderr)
        return 1
    return _run(list(sys.argv[1:] if argv is None else argv), _native._CliCancelToken())
