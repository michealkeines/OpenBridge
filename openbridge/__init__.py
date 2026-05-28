"""OpenBridge — orchestrate multiple Claude sessions programmatically.

Producer (Python process):

    from openbridge import Bridge

    bridge = Bridge(name="my-producer", pool="my-pool")

    async def main():
        result = await bridge.ask(
            item_id="task-1",
            prompt="What is 2+2?",
            template={"answer": ""},
            validate=lambda d: None if d.get("answer") else "answer required",
        )
        print(result.data["answer"])

    bridge.serve(main())

Worker (Claude session, runs `openbridge` CLI):

    openbridge get --pool my-pool
    # edit submission.json
    openbridge submit --pool my-pool --work-id <work_id>

See README.md for the full architecture and operator/author guides.
"""
from .bridge import Bridge, AskResult

__all__ = ["Bridge", "AskResult"]
__version__ = "0.1.0"
