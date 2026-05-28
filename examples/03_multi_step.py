"""Pattern 3 — Multi-step per item.

Each logical item triggers two ask() calls: first to classify, then to
write — and the second prompt is shaped by the first's answer.

Run:
    python examples/03_multi_step.py

In a worker terminal (or Claude session):
    openbridge get --pool multistep   # ... then submit ...
    # each TOPIC produces TWO work items: <topic>#classify and <topic>#write
"""
from openbridge import Bridge

TOPICS = [
    "the role of mitochondria in eukaryotic cells",
    "how to sharpen a chisel",
    "the difference between transit and stop signs",
]

bridge = Bridge(name="multistep", pool="multistep")


async def process(topic: str) -> dict:
    # Step 1 — classify complexity. The worker chooses simple/complex/skip.
    classify = await bridge.ask(
        item_id=f"{topic}#classify",
        prompt=(
            f"Topic: {topic!r}.\n"
            "Set submission.json.label to one of: simple, complex, skip.\n"
            "simple = under 100 words is enough; complex = needs detail; "
            "skip = topic is unsuitable."
        ),
        template={"topic": topic, "label": ""},
        validate=lambda d: (None
                            if d.get("label") in {"simple", "complex", "skip"}
                            else "label must be simple|complex|skip"),
    )
    label = classify.data.get("label")
    if label == "skip":
        return {"topic": topic, "result": "(skipped at classify step)"}

    # Step 2 — write, with prompt shape dictated by step 1's answer.
    target_words = 80 if label == "simple" else 250
    write = await bridge.ask(
        item_id=f"{topic}#write",
        prompt=(
            f"Topic: {topic!r}. You classified it as {label!r}.\n"
            f"Write a ~{target_words}-word summary into submission.summary.\n"
            "Plain prose, no headings, no bullet points."
        ),
        template={"topic": topic, "summary": ""},
        validate=lambda d: (None
                            if len(d.get("summary", "")) >= target_words // 2
                            else f"summary too short (< {target_words // 2} chars)"),
    )
    return {"topic": topic, "label": label, "summary": write.data["summary"]}


async def main() -> None:
    for topic in TOPICS:
        result = await process(topic)
        print(f"  {topic}: {result.get('label', '-')}  "
              f"({len(result.get('summary', ''))} chars)")


if __name__ == "__main__":
    bridge.serve(main())
