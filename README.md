# ALEC — Learning Loop (v4 experiment)

A real-time **agentic learning loop**: an experiment in giving LLM agents a memory that
*improves with feedback* instead of starting every session from zero. Agents retrieve
relevant past lessons, act, and the system attributes the outcome back to the lessons it
used — reinforcing what helped and demoting what didn't.

> **Read this first — what this repo is.** This is a learn-by-doing AI-engineering
> experiment, not a product. It is the **v4** line of the ALEC project: an earlier, more
> ambitious concept than the one that became the current
> [`Acatium/ALEC`](https://github.com/Acatium/ALEC) (a multi-agent knowledge-discovery
> system on a simpler PostgreSQL-only stack). I extracted v4 into its own repo because the
> idea here — a closed feedback loop over "atomic knowledge units" — is interesting on its
> own and was otherwise buried in an archive. The code is real and substantial
> (~14.5K lines of core Python), but it requires a Kafka + Redis + PostgreSQL stack to run
> and is **not production-ready**. See [Status & how to run](#status--how-to-run) for the
> honest version.

---

## What this explores

The central bet: an agent gets better not by being a bigger model, but by **remembering
what worked in situations like this one**. The loop is built from four event-driven
services (`core/learning_loop/*/service.py`):

| Service | File | What it does |
|---------|------|--------------|
| **ADVISOR** | `core/learning_loop/advisor/service.py` | At task time: vector-search for relevant lessons, cluster them, filter out ones known to have caused failures, rank by Thompson Sampling, write the survivors to Redis for the session. |
| **REFLECTOR** | `core/learning_loop/reflector/service.py` | After a turn: analyze what happened, attribute the outcome to the lessons that were used, update their counters, and extract new lessons. |
| **CURATOR** | `core/learning_loop/curator/service.py` | Quality-gate and de-duplicate new lessons before they enter the store. |
| **CLUSTERER** | `core/learning_loop/clusterer/service.py` | Group lessons by the problems they solve and maintain the edges between problem-clusters and lessons. |

The ideas worth pointing at:

- **Atomic Knowledge Units (AKUs) — `When [situation] → [assertion]`.** Each lesson is a
  situation paired with a piece of advice, stored with helpful / harmful / neutral counters.
  Schema: `docker/init/25_simplify_to_akus.sql`; design: `ARCHITECTURE.md`.

- **Two embedding spaces, on purpose.** The *situation* is embedded for **retrieval**
  ("find lessons for a problem like this"); the *assertion* is embedded separately for
  **deduplication** ("have we already said this?"). ADVISOR searches on the situation
  embedding; CURATOR dedups on the assertion embedding. (`advisor/service.py`,
  `curator/service.py`, and `core/learning_loop/tests/test_embedding_alignment.py`.)

- **Thompson Sampling with age decay** picks which lessons to surface
  (`advisor/service.py:531`). Each candidate is sampled from a Beta posterior
  (`alpha = helpful + 1`, `beta = harmful + 0.2·neutral + 1`), filtered against a floor,
  then scored `similarity × ts_sample × age_decay`, where
  `age_decay = max(0.50, 0.995^age_days)`. The point: surface lessons by *expected
  effectiveness under uncertainty*, not by similarity alone — and let stale lessons fade.

- **Cluster-exclusion edges.** Outcomes create typed edges between problem-clusters and
  lessons: `caused_failure` (written by REFLECTOR) and `solved_by` (written by CLUSTERER,
  `clusterer/service.py:164`). ADVISOR uses `caused_failure` edges to *exclude* a lesson
  for the specific kind of problem it previously hurt — so a lesson can be good in general
  but suppressed where it's been shown to backfire.

- **A real statistical-evaluation harness.** `evaluation/analysis/statistical_tests.py`
  implements McNemar's exact test (`:14`), paired t-test (`:85`), Mann–Whitney U (`:139`),
  Fisher's exact test (`:193`), plus Bonferroni / FDR / sequential-Bonferroni corrections;
  `evaluation/analysis/bootstrap.py` adds bootstrap confidence intervals. This is the
  machinery for asking "did the learning loop *actually* help, or is it noise?" rather than
  eyeballing a demo.

- **An AppWorld benchmark runner** (`evaluation/appworld/runner/`) to drive the agent
  against the [AppWorld](https://appworld.dev/) benchmark and record per-task outcomes for
  the analysis above.

## What I learned

Grounded in what the code actually settled on:

- **Retrieval similarity is not quality.** A comment in `advisor/service.py` records the
  finding directly: the embedding model "gives lower cosine similarity than expected even
  for semantically related concepts," so the retrieval threshold was loosened (to `0.35`)
  and **Thompson Sampling was made responsible for quality filtering**. Separating "is this
  relevant?" (retrieval) from "is this any good?" (the bandit) was the key structural
  decision.

- **One embedding space can't do two jobs.** Using the same vector for "find me similar
  problems" and "is this a duplicate piece of advice" conflates two different notions of
  similarity — hence the two-space model.

- **Feedback has to be *attributed*, not just collected.** The hard part isn't storing
  lessons; it's deciding *which* lessons deserve credit or blame for an outcome. That's the
  whole reason REFLECTOR and the per-cluster `caused_failure` edges exist.

- **Event-driven made the loop legible but heavy.** Splitting ADVISOR / REFLECTOR /
  CURATOR / CLUSTERER into independent Kafka consumers made each step easy to reason about
  and test in isolation — at the cost of needing real Kafka + Redis + Postgres to run
  anything end-to-end. That weight is a large part of why the *next* iteration (the current
  [`Acatium/ALEC`](https://github.com/Acatium/ALEC)) deliberately dropped Kafka/Redis for a
  PostgreSQL-only design.

## Status & how to run

**Honest status:** this is a working local-development experiment, not a deployable system.
There is no auth, monitoring, or service mesh. The architecture doc's performance figures
(e.g. "P95 < 500ms", "vector search < 100ms" in `ARCHITECTURE.md`) are **design targets —
they are not benchmarked, and no test measures them.** Treat them as intentions, not
results.

**Infrastructure required (not optional):** Kafka, Redis, and PostgreSQL + pgvector. All
three are wired into `docker-compose.yml`, and their clients are hard dependencies
(`aiokafka`, `redis`, `asyncpg` / `pgvector` in `requirements.txt`).

```bash
# Bring up Kafka (KRaft) + Redis + Postgres/pgvector + the services
docker-compose up -d

# Run the test suite (the Makefile auto-selects Docker vs. local virtualenv)
make test
```

**Tests:** there are 600+ test functions across the Python suite, but the **majority
require the full stack** — the integration tests under
`core/learning_loop/tests/integration/` need live Postgres/Kafka/Redis. A pure-unit subset
lives in `core/learning_loop/tests/unit/`. There is **no offline one-command green path**
here, and that is a real limitation of this experiment — a deliberate contrast with
`Acatium/ALEC`, whose unit suite runs green with no infrastructure. (No CI badge is claimed
for this repo for exactly that reason.)

## Architecture

See [`ARCHITECTURE.md`](ARCHITECTURE.md) for the full design — the AKU data model, the
event topics that connect the four services, the two-space embedding scheme, and the edge
types. The high-level shape:

```
task ──▶ ADVISOR ──(relevant AKUs)──▶ Redis ──▶ agent acts
                                                   │
agent turn ──▶ REFLECTOR ──▶ attribute outcome ──▶ update AKU counters
                          └─▶ propose new AKUs ──▶ CURATOR (gate + dedup) ──▶ AKU store
                                                          │
                                          CLUSTERER ──▶ clusters + solved_by / caused_failure edges
```

## Repo layout

```
core/learning_loop/        ADVISOR · REFLECTOR · CURATOR · CLUSTERER (+ shared clients)
core/agents/               agent runtime that consumes AKUs
core/session/              session-side AKU search / domain
evaluation/analysis/       statistical tests + bootstrap CIs
evaluation/appworld/       AppWorld benchmark runner
docker/init/               SQL schema (incl. the AKU table)
frontend/, mockup-react/   exploratory UI mockups (not the focus)
```

## Provenance

Extracted from the `archive/v4/` directory of
[`Acatium/ALEC`](https://github.com/Acatium/ALEC) so the v4 concept is visible and
inspectable on its own. The ~150 MB of database backups that lived alongside it in that
archive were intentionally left out of this repo.

## License

[Apache License 2.0](LICENSE)
