import assert from 'node:assert/strict';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { resolve, join, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';
import { spawnSync } from 'node:child_process';

const runner = join(dirname(fileURLToPath(import.meta.url)), 'run-wasi.mjs');
const wasm = resolve(process.argv[2] ?? 'target/wasm32-wasip1/release/bustubx-wasm.wasm');
const dataPath = mkdtempSync(join(tmpdir(), 'bustubx-wasi-test-'));
function run(sql) {
  const result = spawnSync(process.execPath, [runner, wasm, dataPath], {
    input: sql.join('\n') + '\n', encoding: 'utf8', timeout: 60000,
  });
  assert.ifError(result.error);
  assert.equal(result.status, 0, result.stderr);
  return result.stdout;
}
try {
  const first = run([
    'create table t (a int)',
    'insert into t values (11), (22), (33)',
    'create index idx on t (a)',
    'select a from t where a > 11 order by a desc',
    'select count(a), avg(a) from t',
    'create table r (b int)',
    'insert into r values (22)',
    'select a, b from t inner join r on a = b',
  ]);
  assert(!first.includes('ERROR'), first);
  assert(first.includes('OK rows=2'), first);
  assert.match(first, /33[\s\S]*22/);
  const aggregate = run(['select count(a), avg(a) from t']);
  assert(aggregate.includes('OK rows=1'), aggregate);
  assert.match(aggregate, /\b3\b[^\r\n]*\b22\b/);
  const joined = run(['select a, b from t inner join r on a = b']);
  assert(joined.includes('OK rows=1'), joined);
  assert.match(joined, /\b22\b[^\r\n]*\b22\b/);
  const reopened = run(['select a from t order by a']);
  assert(reopened.includes('OK rows=3'), reopened);
  assert.match(reopened, /11[\s\S]*22[\s\S]*33/);
  const errors = run(['select missing from t', 'select 1234']);
  assert(errors.includes('ERROR'), errors);
  assert(errors.includes('1234'), errors);
  console.log('WASI smoke tests passed: SQL, indexes, joins, aggregation, reopen and error recovery.');
} finally {
  rmSync(dataPath, { recursive: true, force: true });
}
