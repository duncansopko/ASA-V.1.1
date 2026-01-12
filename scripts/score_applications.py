import sqlite3
from pathlib import Path
from datetime import datetime, timezone

# ==================================================
# Configuration
# ==================================================

DB_PATH = Path(__file__).resolve().parents[1] / "data" / "asa.db"

# ==================================================
# Thresholds (v1.1 – canonical)
# ==================================================

IDLE_DAYS_THRESHOLD = 7
MIN_CHANNEL_SAMPLE_SIZE = 5

HIGH_IDLE_RATE_THRESHOLD = 0.30
LOW_FOLLOW_UP_RATE_THRESHOLD = 0.50

STABLE_RESPONSE_COUNT_THRESHOLD = 3
CHANNEL_DEPENDENCY_THRESHOLD = 0.60
STABLE_CHANNEL_RATIO_THRESHOLD = 0.30

ACTIVITY_RATE_THRESHOLD = 1.0
INACTIVITY_RATE_THRESHOLD = 0.5

# ==================================================
# Database connection
# ==================================================

def get_connection():
    return sqlite3.connect(DB_PATH)

# ==================================================
# Time utilities (single source of truth)
# ==================================================

def _utcnow():
    return datetime.now(timezone.utc)

def _parse_ts(ts):
    if ts is None:
        return None
    return datetime.fromisoformat(ts).replace(tzinfo=timezone.utc)

# ==================================================
# Pillar A — Core Write Functions
# ==================================================

def add_application(company, role, application_link=None):
    """
    Creates a new application record.
    Returns the generated application_id.
    """
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute(
        """
        INSERT INTO applications (company, role, application_link)
        VALUES (?, ?, ?)
        """,
        (company, role, application_link),
    )

    conn.commit()
    application_id = cursor.lastrowid
    conn.close()

    return application_id


def add_outreach(application_id, channel, outreach_type="initial"):
    """
    Logs an outreach attempt.
    outreach_type examples:
      - 'initial'
      - 'follow_up'
    """
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute(
        """
        INSERT INTO outreach_events (application_id, channel, outreach_type)
        VALUES (?, ?, ?)
        """,
        (application_id, channel, outreach_type),
    )

    conn.commit()
    conn.close()


def add_response(application_id, channel, response_type):
    """
    Logs a market response.
    response_type examples:
      - 'reply'
      - 'rejection'
      - 'interview'
    """
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute(
        """
        INSERT INTO response_events (application_id, channel, response_type)
        VALUES (?, ?, ?)
        """,
        (application_id, channel, response_type),
    )

    conn.commit()
    conn.close()

def add_customization(
    application_id,
    resume_customized=False,
    cover_letter_customized=False,
):
    """
    Logs whether resume and/or cover letter
    were customized for this application.
    Behavioral signal only (binary).
    """

    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute(
        """
        INSERT OR REPLACE INTO application_customization
        (application_id, resume_customized, cover_letter_customized, timestamp)
        VALUES (?, ?, ?, ?)
        """,
        (
            application_id,
            int(resume_customized),
            int(cover_letter_customized),
            _utcnow().isoformat(),
        ),
    )

    conn.commit()
    conn.close()

# ==================================================
# Pillar B — Canonical Metrics & Time Awareness
# ==================================================

# ----------------------
# B.0 — Thresholds
# ----------------------

IDLE_DAYS_THRESHOLD = 7
MIN_CHANNEL_SAMPLE_SIZE = 5
HIGH_IDLE_RATE_THRESHOLD = 0.30
LOW_FOLLOW_UP_RATE_THRESHOLD = 0.50
STABLE_RESPONSE_COUNT_THRESHOLD = 3


# ----------------------
# B.1 — Time Core
# ----------------------

def _latest_timestamp(application_id):
    """
    Returns the most recent timestamp across all events
    for a given application_id.
    """
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT MAX(ts) FROM (
            SELECT created_at AS ts FROM applications WHERE application_id = ?
            UNION ALL
            SELECT timestamp AS ts FROM outreach_events WHERE application_id = ?
            UNION ALL
            SELECT timestamp AS ts FROM response_events WHERE application_id = ?
            UNION ALL
            SELECT timestamp AS ts FROM status_history WHERE application_id = ?
        )
        """,
        (application_id, application_id, application_id, application_id),
    )

    row = cursor.fetchone()
    conn.close()

    return row[0]


def days_since_last_action(application_id):
    ts = _latest_timestamp(application_id)
    if ts is None:
        return None

    last = datetime.fromisoformat(ts).replace(tzinfo=timezone.utc)
    return (datetime.now(timezone.utc) - last).days


# ----------------------
# B.2 — Application-Level Counts
# ----------------------

def total_outreach_count(application_id):
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute(
        "SELECT COUNT(*) FROM outreach_events WHERE application_id = ?",
        (application_id,),
    )

    count = cursor.fetchone()[0]
    conn.close()
    return count


def follow_up_count(application_id):
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT COUNT(*)
        FROM outreach_events
        WHERE application_id = ?
          AND outreach_type = 'follow_up'
        """,
        (application_id,),
    )

    count = cursor.fetchone()[0]
    conn.close()
    return count


def response_count(application_id):
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute(
        "SELECT COUNT(*) FROM response_events WHERE application_id = ?",
        (application_id,),
    )

    count = cursor.fetchone()[0]
    conn.close()
    return count


def status_change_count(application_id):
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute(
        "SELECT COUNT(*) FROM status_history WHERE application_id = ?",
        (application_id,),
    )

    count = cursor.fetchone()[0]
    conn.close()
    return count

def customization_flags(application_id):
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT resume_customized, cover_letter_customized
        FROM application_customization
        WHERE application_id = ?
        """,
        (application_id,),
    )

    row = cursor.fetchone()
    conn.close()

    if row is None:
        return {
            "resume_customized": False,
            "cover_letter_customized": False,
            "any_customization": False,
        }

    resume, cover_letter = row

    return {
        "resume_customized": bool(resume),
        "cover_letter_customized": bool(cover_letter),
        "any_customization": bool(resume or cover_letter),
    }

# ----------------------
# B.3 — Derived Application Flags
# ----------------------

def has_follow_up(application_id):
    return follow_up_count(application_id) > 0


def has_response(application_id):
    return response_count(application_id) > 0


def total_action_count(application_id):
    return (
        total_outreach_count(application_id)
        + response_count(application_id)
        + status_change_count(application_id)
    )


def effort_score_raw(application_id):
    """
    Pure effort proxy.
    No weighting. No interpretation.
    """
    return total_action_count(application_id)


# ----------------------
# B.4 — Application Status
# ----------------------

def current_status(application_id):
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT status
        FROM status_history
        WHERE application_id = ?
        ORDER BY timestamp DESC
        LIMIT 1
        """,
        (application_id,),
    )

    row = cursor.fetchone()
    conn.close()

    return row[0] if row else "open"


# ----------------------
# B.5 — Application Metrics View (Canonical)
# ----------------------

def application_metrics_view():
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("SELECT application_id FROM applications")
    application_ids = [r[0] for r in cursor.fetchall()]
    conn.close()

    rows = []

    for app_id in application_ids:
        customization = customization_flags(app_id)

        days_idle = days_since_last_action(app_id)
        outreach_total = total_outreach_count(app_id)
        follow_ups = follow_up_count(app_id)

        rows.append({
            "application_id": app_id,
            "current_status": current_status(app_id),
            "days_since_last_action": days_idle,
            "total_outreach_count": outreach_total,
            "follow_up_count": follow_ups,
            "has_follow_up": follow_ups > 0,
            "responded_flag": has_response(app_id),
            "total_action_count": total_action_count(app_id),
            "effort_score_raw": effort_score_raw(app_id),
            "has_zero_outreach": outreach_total == 0,
            "has_no_follow_up": outreach_total > 0 and follow_ups == 0,
            "is_idle_application": (
                days_idle is not None and days_idle > IDLE_DAYS_THRESHOLD
            ),
            "resume_customized": customization["resume_customized"],
            "cover_letter_customized": customization["cover_letter_customized"],
            "any_customization": customization["any_customization"],
        })

    return rows

# ----------------------
# B.6 — Channel Metrics View (Canonical)
# ----------------------

def channel_metrics_view():
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("SELECT DISTINCT channel FROM outreach_events")
    channels = [r[0] for r in cursor.fetchall()]

    rows = []

    for channel in channels:
        cursor.execute(
            "SELECT COUNT(*) FROM outreach_events WHERE channel = ?",
            (channel,),
        )
        outreach_count = cursor.fetchone()[0]

        cursor.execute(
            "SELECT COUNT(DISTINCT application_id) FROM outreach_events WHERE channel = ?",
            (channel,),
        )
        app_coverage = cursor.fetchone()[0]

        cursor.execute(
            "SELECT COUNT(*) FROM response_events WHERE channel = ?",
            (channel,),
        )
        responses = cursor.fetchone()[0]

        rows.append({
            "channel_name": channel,
            "outreach_count_by_channel": outreach_count,
            "application_coverage_by_channel": app_coverage,
            "response_count_by_channel": responses,
            "response_rate_by_channel": (
                responses / app_coverage if app_coverage > 0 else None
            ),
            "is_low_sample_channel": outreach_count < MIN_CHANNEL_SAMPLE_SIZE,
        })

    conn.close()
    return rows

# ==================================================
# Pillar B — Portfolio Metrics View
# ==================================================

def portfolio_metrics_view():
    app_rows = application_metrics_view()
    applications_total = len(app_rows)

    if applications_total == 0:
        return {
            "applications_total": 0,
            "applications_per_week": None,
            "follow_up_rate": None,
            "zero_outreach_rate": None,
            "idle_application_rate": None,
            "high_idle_portfolio": None,
            "low_follow_up_portfolio": None,
        }

    follow_up_rate = sum(1 for r in app_rows if r["has_follow_up"]) / applications_total
    zero_outreach_rate = sum(
        1 for r in app_rows if r["total_outreach_count"] == 0
    ) / applications_total
    idle_application_rate = sum(
        1 for r in app_rows if r["is_idle_application"]
    ) / applications_total

    high_idle_portfolio = idle_application_rate > HIGH_IDLE_RATE_THRESHOLD
    low_follow_up_portfolio = follow_up_rate < LOW_FOLLOW_UP_RATE_THRESHOLD

    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT MIN(created_at), MAX(created_at) FROM applications")
    min_ts, max_ts = cursor.fetchone()
    conn.close()

    if min_ts and max_ts:
        start = datetime.fromisoformat(min_ts)
        end = datetime.fromisoformat(max_ts)
        weeks_active = max(1, ((end - start).days + 6) // 7)
        applications_per_week = applications_total / weeks_active
    else:
        applications_per_week = None

    return {
        "applications_total": applications_total,
        "applications_per_week": applications_per_week,
        "follow_up_rate": follow_up_rate,
        "zero_outreach_rate": zero_outreach_rate,
        "idle_application_rate": idle_application_rate,
        "high_idle_portfolio": high_idle_portfolio,
        "low_follow_up_portfolio": low_follow_up_portfolio,
    }


# ==================================================
# Pillar C.1 — Application State
# ==================================================

def application_state(metrics_row):
    """
    Determines the canonical state of an application.
    Exactly one state per application.
    """

    if metrics_row["current_status"] == "closed":
        return "closed"

    if metrics_row["total_outreach_count"] == 0:
        return "unengaged"

    days_idle = metrics_row.get("days_since_last_action")

    if days_idle is not None and days_idle > IDLE_DAYS_THRESHOLD:
        return "engaged_idle"

    return "active"


def application_state_view():
    rows = application_metrics_view()
    for r in rows:
        r["application_state"] = application_state(r)
    return rows

# ==================================================
# Pillar C.2 — Channel Signal State
# ==================================================

def channel_signal_state(channel_row):
    """
    Determines signal strength for a single channel.
    """

    outreach_count = channel_row["outreach_count_by_channel"]
    response_count = channel_row["response_count_by_channel"]

    flags = {
        "no_response_flag": response_count == 0
    }

    if response_count == 0:
        return "no_signal", flags

    if outreach_count < MIN_CHANNEL_SAMPLE_SIZE:
        return "insufficient_data", flags

    if response_count >= STABLE_RESPONSE_COUNT_THRESHOLD:
        return "stable_signal", flags

    return "emerging_signal", flags


def channel_signal_state_view():
    rows = channel_metrics_view()

    for r in rows:
        state, flags = channel_signal_state(r)
        r["channel_signal_state"] = state
        r["channel_flags"] = flags

    return rows

# ==================================================
# Pillar C.3 — Portfolio Pattern
# ==================================================

ACTIVITY_RATE_THRESHOLD = 1.0
INACTIVITY_RATE_THRESHOLD = 0.5
CHANNEL_DEPENDENCY_THRESHOLD = 0.60
STABLE_CHANNEL_RATIO_THRESHOLD = 0.30


def portfolio_pattern(portfolio_row):
    """
    Determines the dominant behavioral pattern
    across the full application portfolio.
    """

    apps_per_week = portfolio_row["applications_per_week"]
    idle_rate = portfolio_row["idle_application_rate"]
    follow_up_rate = portfolio_row["follow_up_rate"]

    if apps_per_week is None or apps_per_week < INACTIVITY_RATE_THRESHOLD:
        return "inactive"

    if apps_per_week >= ACTIVITY_RATE_THRESHOLD and portfolio_row["high_idle_portfolio"]:
        return "stalled"

    if apps_per_week >= ACTIVITY_RATE_THRESHOLD and idle_rate >= HIGH_IDLE_RATE_THRESHOLD:
        return "unstructured_bursting"

    if (
        apps_per_week >= ACTIVITY_RATE_THRESHOLD
        and idle_rate < HIGH_IDLE_RATE_THRESHOLD
        and follow_up_rate >= LOW_FOLLOW_UP_RATE_THRESHOLD
    ):
        return "steady_engagement"

    return "unstructured_bursting"


def portfolio_pattern_view():
    """
    Attaches portfolio pattern and structural flags.
    """

    row = portfolio_metrics_view()
    row["portfolio_pattern"] = portfolio_pattern(row)

    channel_rows = channel_signal_state_view()

    # --- Low signal environment ---
    stable_channels = sum(
        1 for c in channel_rows
        if c["channel_signal_state"] == "stable_signal"
    )
    total_channels = max(1, len(channel_rows))

    row["low_signal_environment_flag"] = (
        stable_channels / total_channels
    ) < STABLE_CHANNEL_RATIO_THRESHOLD

    # --- Channel dependency ---
    total_responses = sum(c["response_count_by_channel"] for c in channel_rows)
    max_responses = max(
        (c["response_count_by_channel"] for c in channel_rows),
        default=0,
    )

    if total_responses == 0:
        row["channel_dependency_flag"] = False
    else:
        row["channel_dependency_flag"] = (
            max_responses / total_responses
        ) >= CHANNEL_DEPENDENCY_THRESHOLD

    return row

# ==================================================
# PILLAR D — NARRATIVES, SUPPRESSION, ORCHESTRATION
# ==================================================
# Converts states → restrained language → safe bundles
# No metrics, no logic mutation, no inference
# ==================================================


# ==================================================
# D.1 — Application-Level Narratives
# ==================================================

# --------------------------------------------------
# D.1.1 — Phrase Templates
# --------------------------------------------------

APPLICATION_STATE_TEMPLATES = {
    "unengaged": {
        "base": "This application has been submitted, but no outreach has been logged yet.",
    },
    "engaged_idle": {
        "base": "Outreach has occurred, but there has been no recent activity on this application.",
    },
    "active": {
        "base": "This application has ongoing outreach activity.",
    },
    "closed": {
        "base": "This application is marked as closed.",
    },
}

APPLICATION_FLAG_TEMPLATES = {
    "responded_flag": "A response has been received for this application.",
    "no_follow_up_flag": "No follow-up has been logged after initial outreach.",
}


# --------------------------------------------------
# D.1.2 — Assembly Logic
# --------------------------------------------------

def _assemble_application_narrative(application_state, flags):
    """
    Assembles up to 2 sentences:
      - Required base sentence
      - Optional single modifier
    """

    base = APPLICATION_STATE_TEMPLATES.get(application_state, {}).get("base")
    if base is None:
        return []

    sentences = [base]

    for flag_name, sentence in APPLICATION_FLAG_TEMPLATES.items():
        if flags.get(flag_name):
            sentences.append(sentence)
            break  # enforce max 2 sentences

    return sentences


# --------------------------------------------------
# D.1.3 — Public Interface
# --------------------------------------------------

def describe_application(
    *,
    application_state,
    no_follow_up_flag=False,
    responded_flag=False,
):
    """
    Returns 1–2 neutral, descriptive sentences
    describing the current application state.
    """

    flags = {
        "no_follow_up_flag": no_follow_up_flag,
        "responded_flag": responded_flag,
    }

    return _assemble_application_narrative(application_state, flags)

# --------------------------------------------------
# D.1.4 — Public Interface
# --------------------------------------------------

def application_narratives_view():
    """
    Returns application_id → list of narrative sentences
    """
    rows = application_state_view()

    narratives = {}

    for r in rows:
        application_id = r["application_id"]
        application_state = r["application_state"]
        flags = r.get("flags", {})

        narratives[application_id] = _assemble_application_narrative(
            application_state,
            flags,
        )

    return narratives

# ==================================================
# D.2 — Channel-Level Summaries
# ==================================================

# --------------------------------------------------
# D.2.1 — Phrase Templates
# --------------------------------------------------

CHANNEL_SIGNAL_TEMPLATES = {
    "no_signal": "This channel has not produced any responses yet.",
    "insufficient_data": "This channel has produced some responses, but there is not yet enough data to interpret patterns.",
    "emerging_signal": "This channel is beginning to show response patterns, though the signal is still forming.",
    "stable_signal": "This channel has produced enough activity to support reliable pattern observation.",
}

CHANNEL_FLAG_TEMPLATES = {
    "no_response_flag": "No responses have been observed on this channel so far.",
}


# --------------------------------------------------
# D.2.2 — Eligibility Logic
# --------------------------------------------------

def _eligible_channel_sentences(channel_row):
    """
    Determines eligible sentences for a channel summary.
    Returns:
      base_sentence (str)
      optional_sentences (list[str])
    """

    state = channel_row.get("channel_signal_state")
    flags = channel_row.get("channel_flags", {})

    base_sentence = CHANNEL_SIGNAL_TEMPLATES.get(state)
    optional_sentences = []

    if base_sentence is None:
        return None, []

    # Suppress modifiers if signal is weak
    if state in {"no_signal", "insufficient_data"}:
        return base_sentence, []

    for flag_name, sentence in CHANNEL_FLAG_TEMPLATES.items():
        if flags.get(flag_name):
            optional_sentences.append(sentence)

    return base_sentence, optional_sentences


# --------------------------------------------------
# D.2.3 — Assembly Rules
# --------------------------------------------------

def _assemble_channel_summary(channel_row):
    """
    Assembles up to 2 sentences:
      - Required base signal sentence
      - Optional single modifier
    """

    base_sentence, optional_sentences = _eligible_channel_sentences(channel_row)

    if base_sentence is None:
        return []

    if not optional_sentences:
        return [base_sentence]

    return [base_sentence, optional_sentences[0]]


# --------------------------------------------------
# D.2.4 — Public Interface
# --------------------------------------------------

def describe_channel(channel_row):
    """
    Returns 1–2 neutral, descriptive sentences
    summarizing the channel's current signal characteristics.
    """

    return _assemble_channel_summary(channel_row)


# ==================================================
# D.3 — Portfolio-Level Narratives
# ==================================================

# --------------------------------------------------
# D.3.1 — Phrase Templates
# --------------------------------------------------

PORTFOLIO_PATTERN_TEMPLATES = {
    "inactive": "Overall activity across applications has been limited recently.",
    "unstructured_bursting": "Applications are being submitted, but engagement across them has been uneven.",
    "steady_engagement": "Applications are being submitted and engaged with consistently.",
    "stalled": "Earlier activity occurred, but many applications have since become inactive.",
}

PORTFOLIO_FLAG_TEMPLATES = {
    "low_follow_up_portfolio_flag": "Follow-up activity has been limited across applications.",
    "high_idle_portfolio_flag": "A sizable portion of applications have not been touched recently.",
    "channel_dependency_flag": "Most responses have come from a single outreach channel.",
    "low_signal_environment_flag": "Across channels, response signals remain limited.",
}


# --------------------------------------------------
# D.3.2 — Eligibility Logic
# --------------------------------------------------

def _eligible_portfolio_sentences(portfolio_row):
    """
    Determines eligible portfolio-level sentences.
    Returns:
      primary_sentence (str)
      secondary_sentences (list[str])
    """

    pattern = portfolio_row.get("portfolio_pattern")
    primary_sentence = PORTFOLIO_PATTERN_TEMPLATES.get(pattern)

    if primary_sentence is None:
        return None, []

    secondary_sentences = []

    for flag_name, sentence in PORTFOLIO_FLAG_TEMPLATES.items():
        if portfolio_row.get(flag_name):
            secondary_sentences.append((flag_name, sentence))

    # Redundancy suppression
    if pattern == "stalled":
        secondary_sentences = [
            (f, s) for (f, s) in secondary_sentences
            if f != "high_idle_portfolio_flag"
        ]

    return primary_sentence, secondary_sentences


# --------------------------------------------------
# D.3.3 — Assembly Rules
# --------------------------------------------------

def _assemble_portfolio_summary(portfolio_row):
    """
    Assembles up to 3 sentences:
      - 1 primary pattern sentence
      - Up to 2 secondary flag sentences
    """

    primary_sentence, secondary_sentences = _eligible_portfolio_sentences(portfolio_row)

    if primary_sentence is None:
        return []

    priority_order = [
        "channel_dependency_flag",
        "low_follow_up_portfolio_flag",
        "high_idle_portfolio_flag",
        "low_signal_environment_flag",
    ]

    ordered = []
    for flag in priority_order:
        for f, sentence in secondary_sentences:
            if f == flag:
                ordered.append(sentence)

    return [primary_sentence] + ordered[:2]


# --------------------------------------------------
# D.3.4 — Public Interface
# --------------------------------------------------

def describe_portfolio(portfolio_row):
    """
    Returns 1–3 neutral, descriptive sentences
    summarizing overall portfolio posture.
    """
    if not portfolio_row:
        return []

    summary = _assemble_portfolio_summary(portfolio_row)

    if isinstance(summary, list):
        return summary

    return [summary]

def get_application_base(application_id):
    """
    Returns base application identity data.
    """
    conn = get_connection()
    cur = conn.cursor()

    cur.execute(
        """
        SELECT company, role, application_link
        FROM applications
        WHERE application_id = ?
        """,
        (application_id,),
    )

    row = cur.fetchone()
    conn.close()

    if not row:
        return None

    return {
        "company": row[0],
        "role": row[1],
        "application_link": row[2],
    }


def get_application_snapshot(application_id):
    """
    Public read-only snapshot for CLI / UI layers.
    Combines identity, metrics, state, and narratives.
    """

    # --- base identity ---
    base = get_application_base(application_id)

    # --- analytics views ---
    metrics_rows = application_metrics_view()
    state_rows = application_state_view()
    narratives = application_narratives_view()

    metrics = {
        r["application_id"]: r
        for r in metrics_rows
    }

    states = {
        r["application_id"]: r["application_state"]
        for r in state_rows
    }

    if not base or application_id not in metrics:
        return None

    return {
        "application_id": application_id,
        "base": base,
        "state": states.get(application_id),
        "metrics": metrics.get(application_id),
        "narratives": narratives.get(application_id, []),
    }


# ==================================================
# D.4 — Global Orchestration & Suppression
# ==================================================

# --------------------------------------------------
# D.4.1 — Display Limits
# --------------------------------------------------

MAX_APPLICATIONS_DISPLAYED = 5
MAX_APPLICATION_SENTENCES = 2
MAX_CHANNEL_SENTENCES = 1
MAX_PORTFOLIO_SENTENCES = 3


# --------------------------------------------------
# D.4.2 — Cross-Level Suppression Rules
# --------------------------------------------------

def _filter_active_applications(application_rows):
    """
    Closed applications may not surface alongside active ones.
    """
    active = [r for r in application_rows if r.get("application_state") != "closed"]
    return active if active else application_rows


def _filter_low_signal_channels(channel_rows):
    """
    Suppresses channels with no meaningful signal.
    """
    return [
        r for r in channel_rows
        if r.get("channel_signal_state") not in {"no_signal", "insufficient_data"}
    ]


# --------------------------------------------------
# D.4.3 — Assembly Engine
# --------------------------------------------------

def assemble_insight_bundle(
    *,
    application_rows,
    channel_rows,
    portfolio_row,
    include_channels=True,
):
    """
    Orchestrates D.1, D.2, and D.3 outputs into a single,
    human-readable insight bundle with strict suppression.
    """

    bundle = {
        "portfolio": [],
        "applications": [],
        "channels": [],
    }

    # ---- Portfolio first ----
    bundle["portfolio"] = describe_portfolio(portfolio_row)[:MAX_PORTFOLIO_SENTENCES]

    # ---- Applications ----
    filtered_apps = _filter_active_applications(application_rows)

    for r in filtered_apps[:MAX_APPLICATIONS_DISPLAYED]:
        narrative = describe_application(
            application_state=r.get("application_state"),
            no_follow_up_flag=r.get("has_no_follow_up", False),
            responded_flag=r.get("responded_flag", False),
        )

        if narrative:
            bundle["applications"].append({
                "application_id": r.get("application_id"),
                "sentences": narrative[:MAX_APPLICATION_SENTENCES],
            })

    # ---- Channels ----
    if include_channels:
        filtered_channels = _filter_low_signal_channels(channel_rows)

        for r in filtered_channels:
            summary = describe_channel(r)
            if summary:
                bundle["channels"].append({
                    "channel_name": r.get("channel_name"),
                    "sentences": summary[:MAX_CHANNEL_SENTENCES],
                })

    return bundle

# ==================================================
# TEST RUNNER — Manual Sanity Checks
# ==================================================

if __name__ == "__main__":

    print("\n==============================")
    print("APPLICATION METRICS (Pillar B)")
    print("==============================")

    app_metrics = application_metrics_view()
    for r in app_metrics:
        print(r)

    print("\n==============================")
    print("APPLICATION STATES (Pillar C)")
    print("==============================")

    app_states = application_state_view()
    for r in app_states:
        print(
            f"Application {r['application_id']}: {r['application_state']}"
        )

    print("\n==============================")
    print("APPLICATION NARRATIVES (D.1)")
    print("==============================")

    for r in app_states:
        narrative = describe_application(
            application_state=r["application_state"],
            no_follow_up_flag=r.get("has_no_follow_up", False),
            responded_flag=r.get("responded_flag", False),
        )

        print(f"\nApplication {r['application_id']}:")
        for s in narrative:
            print("  -", s)

    print("\n==============================")
    print("CHANNEL METRICS (Pillar B)")
    print("==============================")

    channel_rows = channel_metrics_view()
    for r in channel_rows:
        print(r)

    print("\n==============================")
    print("CHANNEL SIGNAL STATES (Pillar C.2)")
    print("==============================")

    channel_states = channel_signal_state_view()
    for r in channel_states:
        print(
            r["channel_name"],
            r["channel_signal_state"],
            r["channel_flags"],
        )

    print("\n==============================")
    print("CHANNEL SUMMARIES (D.2)")
    print("==============================")

    for r in channel_states:
        summary = describe_channel(r)
        print(f"\n{r['channel_name']}:")
        for s in summary:
            print("  -", s)

    print("\n==============================")
    print("PORTFOLIO METRICS (Pillar B)")
    print("==============================")

    portfolio_metrics = portfolio_metrics_view()
    print(portfolio_metrics)

    print("\n==============================")
    print("PORTFOLIO PATTERN (Pillar C.3)")
    print("==============================")

    portfolio_row = portfolio_pattern_view()
    print(
        portfolio_row.get("portfolio_pattern"),
        {
            "high_idle": portfolio_row.get("high_idle_portfolio"),
            "low_follow_up": portfolio_row.get("low_follow_up_portfolio"),
            "channel_dependency": portfolio_row.get("channel_dependency_flag"),
            "low_signal_environment": portfolio_row.get("low_signal_environment_flag"),
        },
    )

    print("\n==============================")
    print("PORTFOLIO SUMMARY (D.3)")
    print("==============================")

    for s in describe_portfolio(portfolio_row):
        print("  -", s)

    print("\n==============================")
    print("FINAL INSIGHT BUNDLE (D.4)")
    print("==============================")

    bundle = assemble_insight_bundle(
        application_rows=app_states,
        channel_rows=channel_states,
        portfolio_row=portfolio_row,
    )

    print("\nPORTFOLIO:")
    for s in bundle["portfolio"]:
        print("  -", s)

    print("\nAPPLICATIONS:")
    for app in bundle["applications"]:
        print(f"  Application {app['application_id']}:")
        for s in app["sentences"]:
            print("    -", s)

    print("\nCHANNELS:")
    for ch in bundle["channels"]:
        print(f"  {ch['channel_name']}:")
        for s in ch["sentences"]:
            print("    -", s)

