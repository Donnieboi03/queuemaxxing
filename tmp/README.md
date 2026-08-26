# tmp/

Local scratch directory (gitignored except this README).

Use for:

- Stress WAL roots: `tmp/data/...`
- Stress JSON reports: `tmp/stress-*.json`
- Ad-hoc server data when you set `QUEUEMAXXING_DATA=tmp/data`

Do **not** put source code here — that lives under `src_py/`, `src_cpp/`, `tests/`, `demo/`.
