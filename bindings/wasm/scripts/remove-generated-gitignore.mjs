import { rm } from "node:fs/promises";

await rm(new URL("../pkg/.gitignore", import.meta.url), { force: true });
