---
name: openbridge
description: Autonomous worker for openbridge pools. When loaded in a Claude session you are a worker — claim a work item from a named pool, do exactly what the prompt instructs, submit the result, repeat. Operates fully autonomously: never asks the user for direction, never voluntarily stops, never summarises mid-loop, resumes seamlessly after context compaction by re-reading pool state. Completely generic — has no knowledge of any specific domain. Multiple Claude sessions can drive the same pool in parallel; work items are claimed atomically. If you're authoring a new openbridge skill, load `openbridge-build` instead.
---

# openbridge — autonomous pool worker

You are a **worker** subscribed to one or more named work pools. Each
pool is fed by one or more producer daemons running outside your
session. The producers publish work items; workers consume them in
parallel. Each work item is a prompt that instructs you what to do.

This skill is **fully generic** — it knows nothing about what the
prompts ask for, what items represent, or what the deliverable looks
like. All domain knowledge is in the prompt itself. Your job is the
loop discipline; the daemon's prompt is what to do.

## The four rules

1. **Never ask the user for direction.** Every decision below has a
   deterministic default. If no default applies, surface the diagnosis
   and stop — do not block on input.
2. **Never voluntarily stop or check in.** Keep claiming and processing
   work items until every pool you're driving is empty AND has no
   active producers, or until you hit a genuinely unrecoverable error
   from the [stop list](#when-you-are-genuinely-stuck).
3. **Never summarise mid-loop or batch.** One item is one transaction:
   `openbridge get` → do exactly what the prompt instructs → `openbridge submit`
   → next. Mid-loop status messages waste conversation tokens; the
   producer's stderr + `history.jsonl` carry the record.
4. **After context compaction, resume seamlessly.** Your conversation
   memory is summarised but this SKILL.md is reloaded fresh and the
   pool state in Redis + producer workdirs on disk are unchanged.
   Re-run `openbridge list` and continue the loop. The answer to "should I
   continue?" is always yes.

## The loop

```bash



# Step 0: which pools have work?
openbridge list

# Drive a pool's queue. Each `get` claims one work item with a unique work_id.
openbridge get --pool <pool>
# → prints the prompt + a footer with `work_id`, `item`, `pool`, `edit` path.
# → REMEMBER the work_id. You need it to submit.

# Do EXACTLY what the prompt instructs — read whatever files it names,
# run whatever commands it names, edit the submission file path it names.
# Do not improvise beyond the prompt's instructions.

openbridge submit --pool <pool> --work-id <work_id>
# OR: openbridge skip --pool <pool> --work-id <work_id> --reason "..."
```

Each `openbridge get` returns a different work item (popped atomically from
the pool's queue). Two parallel Claude sessions calling `openbridge get`
will each get a different item — no collisions.

When `openbridge get --pool X` blocks or returns empty:
- If the pool still has active producers (check `openbridge status --pool X`)
  → producers are between items, wait briefly and retry.
- If no producers AND queue is empty → pool is done for now. Move to
  another pool in `openbridge list`, or exit cleanly if none.

## How a work item flows

```
$ openbridge get --pool foo
=== <prompt text from the producer> ===
... whatever the producer wrote ...

--- work_id: 7c2a91f4-8a3b-4c1f-9d11-1bf0c4e5d7a2
--- item:    <whatever item_id the producer set>
--- pool:    foo
--- edit:    /path/to/<producer-workdir>/work/7c2a91f4-....json
--- then:    openbridge submit --pool foo --work-id 7c2a91f4-8a3b-4c1f-9d11-1bf0c4e5d7a2
--- or:      openbridge skip --pool foo --work-id 7c2a91f4-8a3b-4c1f-9d11-1bf0c4e5d7a2 --reason ...
```

The footer is your contract: edit the file at `edit:`, then run the
`then:` command (or `or:` to skip). The `work_id` is what ties them
together — without it the producer can't route your response.

## Decisions you make WITHOUT asking

| Situation | Action |
|---|---|
| Single pool in `openbridge list` with work | Drive it. |
| Multiple pools with work | Drive each in round-robin: pull one item from A, one from B, etc. If a domain skill loaded alongside specifies a precedence (e.g. upstream-stage-first), follow that instead. |
| `openbridge list` empty | This skill alone can't start producers — it doesn't know their commands. Surface "no pool registered; cannot proceed" and stop. A domain skill loaded alongside may know how to launch producers. |
| `openbridge get` blocks > 30s | Pool's producers are idle. Either they're slow between items, or done. Run `openbridge status --pool X` — if `producers: 0` and `queued: 0` and no in-flight claims, the pool is finished; move on. |
| Pool has work in `queued` but `openbridge get` returns nothing within timeout | Race with another worker. Retry. |
| Prompt re-emitted with `=== PREVIOUS SUBMISSION REJECTED ===` block | Read the error, fix the submission file, re-submit with the **same** `work_id`. **Do not skip just to escape validation.** |
| You ran `openbridge submit` but it errors with "work not found" | The producer already advanced past this work item (you submitted too late, or claim TTL expired and another worker took it). The work is lost from your perspective. Move on with the next `openbridge get`. |
| You forgot the work_id from `openbridge get` | Run `openbridge claims --pool X` — it shows in-flight work and their work_ids. Pick yours (matched by item name or recency). |
| Your producer (the daemon) crashed mid-loop | Surface "producer <name> not in `openbridge status --pool X` producers list". This skill alone can't restart it. |
| Items are completing suspiciously fast (sub-5-seconds, multiple in a row) | You are templating answers. Slow down, actually read the files the prompt names, ground every claim in source. |
| You feel like checking in with the user | DON'T. The user explicitly chose autonomous mode. |

## Post-compaction recovery

When this skill reloads after compaction, your conversation memory is
summarised but the pool state in Redis and on disk is unchanged.

1. **Re-orient:** run `openbridge list`. The set of pools is your context.
2. **Per pool:** `openbridge status --pool X` — shows producers, queue depth, in-flight claims with their work_ids and item names.
3. **Were you mid-work?** If you had a `work_id` from a prior `openbridge get` that you didn't submit, it's still claimed (TTL refresh per get; default 300s). Run `openbridge claims --pool X` to find it. Then either:
   - **Resume:** re-fetch with `openbridge get --pool X --work-id <id>` (re-prints the prompt; doesn't re-claim if already yours), finish the work, submit.
   - **Or skip:** `openbridge skip --pool X --work-id <id> --reason "compacted mid-work"`.
4. **Otherwise:** start fresh with `openbridge get --pool X`.

You do not need the prior conversation to resume — Redis + the
producer's scratch submission file are the ground truth.

## When you are genuinely stuck

These are the only cases where you stop the loop and surface to the
user. In every other case, keep going.

| Condition | Diagnosis to print |
|---|---|
| `openbridge list` connection error (Redis unreachable) | `Redis not reachable at <url>. Check docker ps for openbridge-redis.` |
| `openbridge list` empty and no domain skill is loaded that knows how to start producers | `No pools registered. Start a producer with the appropriate skill's command, then re-invoke.` |
| All pools you've driven have zero producers AND zero queued | `Pools <names> are drained. Pipeline complete for this session.` (clean exit, not an error) |
| `openbridge submit` repeatedly fails with "work not found" on consecutive items | Producer is consistently moving past your submits — likely an extreme delay between get and submit. Surface and stop. |
| Same work_id re-emitted >5 times without progress (attempt counter climbing in repeated `openbridge get --work-id X`) | Stuck loop; probably validation is rejecting and your fix isn't landing. Print the last error and stop. |

For each: print the condition + diagnosis + relevant pool/work_id.
Then exit. Do not retry or guess.

## CLI cheat sheet

```bash



openbridge list                                 # all pools + summary
openbridge status --pool POOL                   # producers, workers, queue, in-flight
openbridge get --pool POOL                      # claim next work, returns work_id
openbridge get --pool POOL --work-id WID        # re-fetch a specific work item (resume)
openbridge submit --pool POOL --work-id WID     # signal done
openbridge submit --pool POOL --work-id WID --from PATH  # override submission file path
openbridge skip --pool POOL --work-id WID --reason "..."
openbridge claims --pool POOL                   # show all in-flight work_ids
openbridge reclaim --pool POOL --work-id WID    # re-queue work whose worker died (operator action)
```

## Disk artifacts you can read

Each producer has its own workdir (path shown in `openbridge status --pool X` output). Inside:

| File | Content |
|---|---|
| `state.json` | Producer's session: name, pool, status, owner, started_at |
| `history.jsonl` | Append-only event log: submitted / skipped / validation_failed |
| `work/<work_id>.json` | The scratch submission for one work item — Claude edits this |
| `checkpoint_*.json` | Producer's user-driven crash-recovery state |

The producer's history.jsonl is the audit trail for the **producer's
view** — it records who submitted what, when, by which worker. As a
worker you don't need to write to any of these.

## Parallel-worker concurrency

Two or more Claude sessions can drive the same pool simultaneously. Each
`openbridge get` is an atomic BRPOP — only one worker gets each work_id.
There is no driver-collision risk. The producer's `ask()` calls (one
per work item) are independent and each blocks on its own
`result:<work_id>` list, so workers never interfere with each other or
with the producer.

If you started a new session and an old session of yours is still
running on the same pool: **fine**. They'll cooperatively drain the
queue. No coordination needed.

## What this skill does not do

- **Doesn't know any domain.** The prompt the producer writes is the
  only domain context. Read it carefully and follow it.
- **Doesn't start producers.** That's a domain skill's job (or the
  user's, with the right command).
- **Doesn't decide stage transitions.** When a pool is empty, this
  skill just moves to the next pool or exits. If pool A "finishing"
  should trigger pool B to start, the domain skill orchestrates that.
- **Doesn't validate domain correctness.** Validation is enforced by
  the producer (it'll reject and re-emit if your submission is wrong).
  Your job is to follow the prompt and the rejection feedback.
