# Contributing

Use Python 3.10 or newer and install the locked development environment:

```bash
uv sync --group dev
```

Before opening a pull request, run the same checks as CI:

```bash
uv run ruff check .
uv run ruff format --check .
uv run pyright
uv run pytest --cov=sk_zmq --cov-report=term-missing -q
uv build
```

Keep changes focused. Preserve the gateway ordering contract (`stream_id`, `seq`,
`snapshot_seq`) and add a regression test for every behavior change. Do not mask
transport, recovery, or cleanup failures with empty data or a successful default.
