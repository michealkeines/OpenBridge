"""Pattern 5 — Stage-to-stage pipeline.

TWO producers, TWO pools, ONE shared deliverable file on disk:

    stage 1 (pool=extract)  →  facts.json  →  stage 2 (pool=summarise)

Run them in separate terminals. Stage 2 can start as soon as stage 1
has produced any completed facts — they pipeline concurrently.

Run stage 1:
    python examples/05_pipeline.py extract

Run stage 2 (in another terminal):
    python examples/05_pipeline.py summarise

Drive each pool with its own workers:
    openbridge get --pool extract
    openbridge get --pool summarise
"""
import asyncio
import json
import sys
from pathlib import Path
from openbridge import Bridge

DOCUMENTS = [
    {"id": "doc-1", "text": "The mitochondrion is the powerhouse of the cell."},
    {"id": "doc-2", "text": "Photosynthesis converts sunlight into chemical energy."},
    {"id": "doc-3", "text": "Tectonic plates move at roughly the rate fingernails grow."},
]

FACTS_FILE = Path("pipeline_facts.json")
SUMMARIES_FILE = Path("pipeline_summaries.json")


def run_stage1_extract() -> None:
    """Extract structured facts from each document into pipeline_facts.json."""
    bridge = Bridge(name="extractor", pool="extract")

    async def main():
        facts = json.loads(FACTS_FILE.read_text()) if FACTS_FILE.exists() else {}
        for doc in DOCUMENTS:
            if doc["id"] in facts:
                continue
            r = await bridge.ask(
                item_id=doc["id"],
                prompt=(
                    f"Extract structured facts from this text:\n\n{doc['text']!r}\n\n"
                    "Edit submission.json.facts to a list of short factual claims."
                ),
                template={"doc_id": doc["id"], "facts": []},
                validate=lambda d: (None if d.get("facts")
                                    else "facts must be a non-empty list"),
            )
            facts[doc["id"]] = r.data["facts"]
            FACTS_FILE.write_text(json.dumps(facts, indent=2))
            print(f"[stage1] {doc['id']}: extracted {len(r.data['facts'])} facts")

    bridge.serve(main())


def run_stage2_summarise() -> None:
    """Compose one-sentence summaries from stage-1 facts."""
    bridge = Bridge(name="composer", pool="summarise")

    async def main():
        summaries = (json.loads(SUMMARIES_FILE.read_text())
                     if SUMMARIES_FILE.exists() else {})
        # Poll the deliverable for newly-completed facts. Stage 2 can run
        # before stage 1 finishes — it picks up items as they're ready.
        while True:
            facts = (json.loads(FACTS_FILE.read_text())
                     if FACTS_FILE.exists() else {})
            pending = [doc_id for doc_id in facts if doc_id not in summaries]
            if not pending and len(summaries) >= len(DOCUMENTS):
                break
            for doc_id in pending:
                r = await bridge.ask(
                    item_id=doc_id,
                    prompt=(
                        f"Document {doc_id!r} has these facts:\n"
                        + "\n".join(f"  - {f}" for f in facts[doc_id])
                        + "\n\nWrite a 1-sentence summary into submission.summary."
                    ),
                    template={"doc_id": doc_id, "summary": ""},
                )
                summaries[doc_id] = r.data["summary"]
                SUMMARIES_FILE.write_text(json.dumps(summaries, indent=2))
                print(f"[stage2] {doc_id}: {r.data['summary'][:60]}")
            if not pending:
                await asyncio.sleep(2)   # wait for more stage-1 output

    bridge.serve(main())


if __name__ == "__main__":
    if len(sys.argv) < 2 or sys.argv[1] not in ("extract", "summarise"):
        print("usage: python 05_pipeline.py extract|summarise")
        sys.exit(2)
    if sys.argv[1] == "extract":
        run_stage1_extract()
    else:
        run_stage2_summarise()
