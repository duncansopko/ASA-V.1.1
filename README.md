# ASA v1.1(A) — Behavioral Job Application Strategy Analyzer

A Python + SQLite analytics system for informing job-search activity.




ASA is a lightweight system for **tracking and understanding how you actually run a job search** — not just how many applications you send.

It focuses on *behavior over time*: what you apply to, how you follow up, where responses come from, and what patterns emerge across your full search.

This repository contains **v1.1**, the first complete, working version of the ASA system.

---

## What problem this solves

Most job tracking tools answer surface questions:
- “How many jobs did I apply to?”
- “What’s the status of this role?”

ASA answers deeper ones:
- Which applications am I actually engaging with?
- Am I following up — or just submitting and moving on?
- Which outreach channels are producing real responses?
- Is my search structured, stalled, or fragmented over time?

Importantly, ASA does **not** coach, judge, or automate decisions.
It simply reflects what’s happening — clearly and consistently.

---

## What ASA does (plain English)

ASA tracks three simple things:

1. **Applications**  
   When and where you applied.

2. **Outreach**  
   Messages, follow-ups, and contact attempts (e.g. LinkedIn, email).

3. **Responses**  
   Any reply from the market (human response, rejection, interview, etc.).

From this, ASA automatically produces:

- a clear **state for each application** (active, unengaged, idle, closed)
- **signal strength by outreach channel** (no signal, emerging, stable)
- a **portfolio-level view** of how your overall search is behaving

All outputs are short, neutral summaries designed to support reflection and better decisions.

---

## Example output

For a single application, ASA might surface:

- “This application has ongoing outreach activity.”
- “A response has been received for this application.”

At the portfolio level:

- “Applications are being submitted, but engagement across them has been uneven.”
- “Most responses have come from a single outreach channel.”

No advice. No pressure. Just signal.

---

## How it’s built (high level)

Internally, ASA is structured into four layers:

- **Pillar A — Input**  
  Records what happened (applications, outreach, responses).  
  No interpretation.

- **Pillar B — Metrics**  
  Computes objective counts and time-based measures.

- **Pillar C — State**  
  Classifies behavioral patterns from metrics.

- **Pillar D — Narratives**  
  Translates states into restrained, human-readable summaries.

Each layer only depends on the one below it.  
Metrics are immutable; interpretation is layered on top.

---

## Current status

This is **v1.1**, the first complete end-to-end implementation:
- working CLI
- stable data model
- time-aware metrics
- application, channel, and portfolio insights

Planned future iterations may include:
- response latency modeling
- decay-weighted signals
- opportunity scoring
- expanded CLI views (e.g. “today”, “next”)

---

## Running the system

To view the full analytics output:

```bash

python3 scripts/score_applications.py

To interact with the CLI:
python3 -m scripts.cli add --company Adobe --role "Sales Velocity Analyst"
python3 -m scripts.cli outreach --application-id 1 --channel LinkedIn
python3 -m scripts.cli status --application-id 1

