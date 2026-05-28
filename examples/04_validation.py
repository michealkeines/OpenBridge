"""Pattern 4 — Validation reprompt.

A non-trivial validator enforces several rules. Bad submissions are
re-published with the error appended; the worker's previous draft is
preserved as the new template, so fixing-in-place is easy. One worker
is spawned automatically.

Run:
    python examples/04_validation.py
"""
from openbridge import Bridge
from openbridge.spawn import spawn_workers

WORDS = ["aurora", "lament"]
VALID_POS = {"noun", "verb", "adjective", "adverb"}

bridge = Bridge(name="validated", pool="validated")


def validate(data: dict) -> str | None:
    """Return None to accept, an error string to reject (auto-reprompt)."""
    pos = (data.get("part_of_speech") or "").strip().lower()
    notes = (data.get("notes") or "").strip()

    if not pos:
        return "part_of_speech is required"
    if pos not in VALID_POS:
        return (f"part_of_speech must be one of {sorted(VALID_POS)}, "
                f"got {pos!r}")
    if len(notes) < 20:
        return (f"notes must be ≥ 20 chars (got {len(notes)}); "
                "explain your reasoning briefly")
    if pos in notes.lower():
        return (f"notes echoes the label ({pos!r}). Explain WHY, "
                "don't restate the label.")
    return None


async def main() -> None:
    async with spawn_workers(bridge, count=1):
        for word in WORDS:
            try:
                result = await bridge.ask(
                    item_id=word,
                    prompt=(
                        f"Classify {word!r}.\n"
                        f"Edit submission.json:\n"
                        f"  - part_of_speech: one of {sorted(VALID_POS)}\n"
                        f"  - notes: ≥ 20 chars, briefly explaining your reasoning\n"
                        f"           (don't just restate the label)"
                    ),
                    template={"word": word, "part_of_speech": "", "notes": ""},
                    validate=validate,
                    max_validation_retries=4,
                )
                print(f"  {word}: {result.data['part_of_speech']} — "
                      f"{result.data['notes'][:60]}")
            except RuntimeError as e:
                # Validation exhausted retries; record the failure and move on
                # so one stuck item doesn't kill the whole batch.
                print(f"  {word}: GAVE UP — {e}")


if __name__ == "__main__":
    bridge.serve(main())
