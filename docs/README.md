# CatSync documentation

Start with the guide that matches what you are doing. Each page is written to
be skimmed first and read deeply only when needed.

| Guide | Use it when you want to… |
| --- | --- |
| 🚀 [Usage](usage.md) | Install CatSync, run a first transfer, look up commands/options, resume a run, or troubleshoot. |
| 🏗️ [Architecture](architecture.md) | Understand planning, transfer backends, checkpoints, the daemon protocol, and the SSH lifecycle handoff. |
| 📊 [Benchmark](benchmark.md) | See measured transport performance, real SSH handoff tests, and known limitations. |
| 📦 [Releasing](releasing.md) | Tag a release, verify archives, or update Homebrew. |

Other useful files:

- [README](../README.md) — project overview and quick start.
- [SSH lifecycle handoff diagram](assets/ssh-handoff.svg) — visual overview of direct TCP/QUIC mode.
- [Benchmark raw data](assets/benchmarks/) — `results.json`, `lifecycle.json`, `cleanup.json`, `network.json`, and SVG charts.
- [Third-party notices](../third_party/) — licenses of linked Go modules.

## The one rule that explains most behavior

CatSync never turns “copy” into “mirror”. `copy` only adds or updates files;
`sync` is the explicit, final destructive step. Everything else — planning,
parallelism, checkpoints, verification — is designed around keeping that
boundary.
