let wasmModulePromise: Promise<typeof import('formualizer')> | null = null;

export async function loadFormualizer() {
  if (!wasmModulePromise) {
    wasmModulePromise = import('formualizer').then(async (mod) => {
      await mod.default();
      return mod;
    });
  }

  return wasmModulePromise;
}
