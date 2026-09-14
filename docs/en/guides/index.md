# Guides

This section covers broader engineering decisions and production-oriented topics.

Use these pages when you are moving beyond the first demo and want to design a more durable system.

---

## Guide map

- [Spec-Driven Authoring](/specstar/guides/spec-driven) — write resources in prose, the LLM generates declarative Python; AST-validated and lock-tracked
- [Backend setup](/specstar/guides/backend-setup) — choose and configure metadata, resource, blob, and job backends
- [Storage](/specstar/guides/storage) — compare storage factories and understand persistence trade-offs
- [Performance](/specstar/guides/performance) — understand trade-offs, bottlenecks, and scaling considerations
- [From demo to production](/specstar/guides/from-demo-to-production) — move from a local prototype to a deployable service
- [Upgrading from autocrud 0.4.x](/specstar/guides/upgrade-from-0.4) — export with `autocrud==0.4.6`, import with `SpecStar.load()`; no data left behind
- [Upgrading to 0.11](/specstar/guides/upgrade-0.11) — spec-driven authoring layer (additive, no breaking changes)
- [Upgrading to 0.10](/specstar/guides/upgrade-0.10)
- [Upgrading to 0.9](/specstar/guides/upgrade-0.9) — breaking changes and migration steps from 0.8.5

---

## Suggested reading order

1. [Backend setup](/specstar/guides/backend-setup)
2. [Storage](/specstar/guides/storage)
3. [From demo to production](/specstar/guides/from-demo-to-production)
4. [Performance](/specstar/guides/performance)
