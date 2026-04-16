import fs from "node:fs/promises";
import path from "node:path";
import { pathToFileURL } from "node:url";

const [wheelPath, smokeScriptPath, pyodideModulePath] = process.argv.slice(2);
if (!wheelPath) {
  throw new Error(
    "usage: node scripts/pyodide-smoke.mjs <wheel> [smoke-script.py] [pyodide-module-path]",
  );
}

const smokePath = smokeScriptPath ?? new URL("./pyodide-smoke.py", import.meta.url);
const pyodideModuleUrl = pyodideModulePath
  ? pathToFileURL(pyodideModulePath).href
  : "pyodide";

try {
  const { loadPyodide } = await import(pyodideModuleUrl);

  const pyodide = await loadPyodide({
    indexURL: pyodideModulePath
      ? pathToFileURL(`${path.dirname(pyodideModulePath)}${path.sep}`).href
      : undefined,
    stderr: (message) => process.stderr.write(`${message}\n`),
    stdout: (message) => process.stdout.write(`${message}\n`),
  });

  await pyodide.loadPackage("micropip");

  const wheelData = await fs.readFile(wheelPath);
  const smokeSource = await fs.readFile(smokePath, "utf8");
  const emfsWheelPath = `/tmp/${path.basename(wheelPath)}`;

  pyodide.FS.writeFile(emfsWheelPath, wheelData);
  pyodide.globals.set("FORMUALIZER_WHEEL_PATH", emfsWheelPath);

  await pyodide.runPythonAsync(`
import sysconfig
import zipfile

import micropip

wheel_path = FORMUALIZER_WHEEL_PATH
try:
    # micropip.install() wants a URL or package name. Use emfs:/ so it
    # reads the wheel from the Emscripten FS we just wrote into.
    await micropip.install(f"emfs:{wheel_path}")
    FORMUALIZER_INSTALL_METHOD = "micropip"
except Exception as exc:
    platlib = sysconfig.get_paths()["platlib"]
    with zipfile.ZipFile(wheel_path) as archive:
        archive.extractall(platlib)
    FORMUALIZER_INSTALL_METHOD = f"zipfile-fallback: {exc}"
`);

  const result = await pyodide.runPythonAsync(smokeSource);
  console.log(result);
} catch (error) {
  console.error("Pyodide smoke failure:");
  console.error(error?.message ?? error);
  if (error?.cause) {
    console.error("Cause:", error.cause?.message ?? error.cause);
  }
  process.exitCode = 1;
}
