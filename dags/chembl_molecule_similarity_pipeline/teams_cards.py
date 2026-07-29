"""Adaptive Card builders for Teams notifications."""

import random

# Fixed joke distractors: (option text, reveal text). The real cause is added
# at build time from the actual exception, then all options are shuffled so the
# true answer is not always in the same slot.
_JOKE_OPTIONS = [
    (
        "FINGERPRINT_SAMPLE_SIZE was set to 67",
        "\u274c That value is a cry for help, not a root cause.",
    ),
    (
        "Someone asked the molecules to be similar and they refused",
        "\u274c Understandable, but no. The molecules are contractually "
        "obligated to be scored.",
    ),
    (
        "Airflow orchestrated everything except success",
        "\u274c Airflow executed the plan flawlessly. The plan was doomed "
        "but that's not orchestration's fault.",
    ),
]

_LETTERS = ["a", "b", "c", "d"]

# just a sanity ceiling on a huge error.
_MAX_ANSWER_CHARS = 240


def _condense_error(real_error: str) -> str:
    """Reduce a raw exception string to the one line worth showing as an answer."""
    text = real_error or ""
    if "stderr (tail):" in text:
        text = text.split("stderr (tail):", 1)[1]
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    condensed = " ".join((lines[-1] if lines else "").split())
    if len(condensed) > _MAX_ANSWER_CHARS:
        condensed = condensed[: _MAX_ANSWER_CHARS - 1].rstrip() + "\u2026"
    return condensed or "Unknown error"


def build_failure_quiz_card(
    dag_id: str,
    task_id: str,
    when: str,
    real_error: str,
    submitted_by: str = "unknown",
) -> dict:
    """Return the Teams webhook payload for the 'pop quiz' failure card."""
    real_text = _condense_error(real_error)
    real_reveal = f"\u2705 Correct. This is what actually killed {task_id}."

    options = [
        {"label": text, "reveal": reveal, "is_real": False}
        for text, reveal in _JOKE_OPTIONS
    ]
    options.append({"label": real_text, "reveal": real_reveal, "is_real": True})
    random.shuffle(options)

    result_ids = [f"res{i}" for i in range(len(options))]

    option_blocks = []
    for i, opt in enumerate(options):
        option_blocks.append({
            "type": "ColumnSet",
            "spacing": "Small",
            "columns": [
                {
                    "type": "Column",
                    "width": "auto",
                    "verticalContentAlignment": "Center",
                    "items": [
                        {
                            "type": "ActionSet",
                            "actions": [
                                {
                                    "type": "Action.ToggleVisibility",
                                    "title": f"({_LETTERS[i]})",
                                    # Show this option's reveal, hide the rest.
                                    "targetElements": [
                                        {"elementId": rid, "isVisible": (j == i)}
                                        for j, rid in enumerate(result_ids)
                                    ],
                                }
                            ],
                        }
                    ],
                },
                {
                    "type": "Column",
                    "width": "stretch",
                    "verticalContentAlignment": "Center",
                    "items": [
                        {"type": "TextBlock", "text": opt["label"], "wrap": True}
                    ],
                },
            ],
        })
        # Hidden reveal for this option, shown when its button is tapped.
        option_blocks.append({
            "type": "TextBlock",
            "id": result_ids[i],
            "isVisible": False,
            "wrap": True,
            "spacing": "Small",
            "color": "good" if opt["is_real"] else "warning",
            "weight": "Bolder" if opt["is_real"] else "Default",
            "text": opt["reveal"],
        })

    card = {
        "type": "AdaptiveCard",
        "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
        "version": "1.5",
        "body": [
            {
                "type": "Container",
                "style": "attention",
                "bleed": True,
                "items": [
                    {
                        "type": "TextBlock",
                        "text": "\U0001f6a9 PIPELINE FAILURE \U0001f6a9 POP QUIZ \U0001f6a9",
                        "weight": "Bolder", "size": "Large", "wrap": True
                    },
                    {
                        "type": "TextBlock",
                        "text": f"{dag_id} \u00b7 {task_id} \u00b7 {when}",
                        "isSubtle": True, "spacing": "None", "wrap": True
                    },
                ],
            },
            {"type": "TextBlock", "text": "It failed. Why? Choose one:",
             "weight": "Bolder", "wrap": True},
            *option_blocks,
            {"type": "TextBlock", "text": f"By {submitted_by}",
             "isSubtle": True, "spacing": "Large",
             "horizontalAlignment": "Right", "wrap": True},
        ],
    }

    return {
        "type": "message",
        "attachments": [
            {
                "contentType": "application/vnd.microsoft.card.adaptive",
                "content": card,
            }
        ],
    }
