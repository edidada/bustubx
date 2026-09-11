import { WASI } from 'node:wasi';
import { readFile, mkdir } from 'node:fs/promises';
import { resolve } from 'node:path';

const wasmPath = resolve(process.argv[2] ?? 'target/wasm32-wasip1/release/bustubx-wasm.wasm');
const dataPath = resolve(process.argv[3] ?? 'target/wasi-data');
await mkdir(dataPath, { recursive: true });
const wasi = new WASI({
  version: 'preview1',
  args: ['bustubx-wasm', '/data/database.db'],
  preopens: { '/data': dataPath },
  returnOnExit: true,
});
const module = await WebAssembly.compile(await readFile(wasmPath));
const instance = await WebAssembly.instantiate(module, wasi.getImportObject());
process.exitCode = wasi.start(instance);
