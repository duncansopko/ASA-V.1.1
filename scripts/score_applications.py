import sqlite3
from pathlib import Path
from datetime import datetime, timezone

# ==================================================
# Pillar B — Thresholds (v1.1)
# ==================================================

IDLE_DAYS_THRESHOLD = 7
MIN_CHANNEL_SAMPLE_SIZE = 5
HIGH_IDLE_RATE_THRESHOLD = 0.30
LOW_FOLLOW_UP_RATE_THRESHOLD = 0.50
STABLE_RESPONSE_COUNT_THRESHOLD = 3

# ==================================================
# Configuration
# ==================================================

DB_PATH = Path(__file__).resolve().parents[1] / "data" / "asa.db"

# ==================================================
# Database connection
# ==================================================

def get_connection():
    return sqlite3.connect(DB_PATH)

# ==================================================
# Pillar A — Core write functions
# ==================================================

def add_application(company, role, application_link=None):
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
    app_id = cursor.lastrowid
    conn.close()
    return app_id


def add_outreach(application_id, channel, outreach_type="initial"):
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

# ==================================================
# Pillar B — Metric helpers (application level)
# ==================================================

def days_since_last_action(application_id):
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT MAX(timestamp) FROM (
            SELECT timestamp FROM status_history WHERE application_id = ?
            UNION ALL
            SELECT timestamp FROM outreach_events WHERE application_id = ?
            UNION ALL
            SELECT created_at AS timestamp FROM applications WHERE application_id = ?
        )
        """,
        (application_id, application_id, application_id),
    )

    result = cursor.fetchone()[0]
    conn.close()

    if result is None:
        return None

    last_action_time = datetime.fromisoformat(result).replace(tzinfo=timezone.utc)
    return (datetime.now(timezone.utc) - last_action_time).days


def total_outreach_count(application_id):
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT COUNT(*)
        FROM outreach_events
        WHERE application_id = ?
        """,
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


def has_follow_up(application_id):
    return follow_up_count(application_id) >= 1


def status_change_count(application_id):
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT COUNT(*)
        FROM status_history
        WHERE application_id = ?
        """,
        (application_id,),
    )

    count = cursor.fetchone()[0]
    conn.close()
    return count


def total_action_count(application_id):
    return total_outreach_count(application_id) + status_change_count(application_id)


def effort_score_raw(application_id):
    return total_action_count(application_id)


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

    if row is None:
        return "open"

    return row[0]

# ==================================================
# Pillar B — Application Metrics View
# ==================================================

def application_metrics_view():
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("SELECT application_id FROM applications")
    app_ids = [row[0] for row in cursor.fetchall()]
    conn.close()

    rows = []

    for app_id in app_ids:
        total_outreach = total_outreach_count(app_id)
        follow_ups = follow_up_count(app_id)
        days_idle = days_since_last_action(app_id)

        row = {
            "application_id": app_id,
            "current_status": current_status(app_id),
            "days_since_last_action": days_idle,
            "total_outreach_count": total_outreach,
            "follow_up_count": follow_ups,
            "has_follow_up": follow_ups >= 1,
            "total_action_count": total_action_count(app_id),
            "effort_score_raw": effort_score_raw(app_id),
            "is_idle_application": (
                days_idle is not None and days_idle > IDLE_DAYS_THRESHOLD
            ),
            "has_zero_outreach": total_outreach == 0,
            "has_no_follow_up": total_outreach >= 1 and follow_ups == 0,
        }

        rows.append(row)

    return rows

# ==================================================
# Pillar B — Channel Metrics View
# ==================================================

def channel_metrics_view():
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("SELECT DISTINCT channel FROM outreach_events")
    channels = [row[0] for row in cursor.fetchall()]

    rows = []
    for ch in channels:
        cursor.execute(
            "SELECT COUNT(*) FROM outreach_events WHERE channel = ?",
            (ch,),
        )
        outreach_count = cursor.fetchone()[0]

        cursor.execute(
            "SELECT COUNT(DISTINCT application_id) FROM outreach_events WHERE channel = ?",
            (ch,),
        )
        app_coverage = cursor.fetchone()[0]

        row = {
            "channel_name": ch,
            "outreach_count_by_channel": outreach_count,
            "application_coverage_by_channel": app_coverage,
            "response_count_by_channel": 0,
            "response_rate_by_channel": None,
            "median_response_time_by_channel": None,
            "is_low_sample_channel": outreach_count < MIN_CHANNEL_SAMPLE_SIZE,
        }
        rows.append(row)

    conn.close()
    return rows

# ==================================================
# Pillar C.2 — Channel Signal States
# ==================================================

def channel_signal_state(channel_row):
    outreach_count = channel_row["outreach_count_by_channel"]
    response_count = channel_row["response_count_by_channel"]

    flags = {"no_response_flag": response_count == 0}

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
# Pillar D.2 — Channel-Level Summaries
# ==================================================

# ----------------------
# D.2.1 — Phrase Templates
# ----------------------

CHANNEL_SIGNAL_TEMPLATES = {
    "no_signal": "This channel has not produced any responses yet.",
    "insufficient_data": "This channel has produced some responses, but there is not yet enough data to interpret patterns.",
    "emerging_signal": "This channel is beginning to show response patterns, though the signal is still forming.",
    "stable_signal": "This channel has produced enough activity to support reliable pattern observation.",
}

CHANNEL_FLAG_TEMPLATES = {
    "fast_response_flag": "Responses on this channel tend to arrive relatively quickly.",
    "slow_response_flag": "Responses on this channel tend to arrive more slowly.",
    "volatile_response_rate_flag": "Response patterns on this channel have been variable over time.",
}

# ----------------------
# D.2.2 — Eligibility Logic
# ----------------------

def _eligible_channel_sentences(channel_row):
    """
    Determines eligible sentences for a channel summary.
    Returns:
      base_sentence (str)
      optional_sentences (list[str])
    """

    state = channel_row.get("channel_signal_state")
    flags = channel_row

    base_sentence = CHANNEL_SIGNAL_TEMPLATES.get(state)
    optional_sentences = []

    # Safety fallback
    if base_sentence is None:
        return None, []

    # Low-signal suppression
    if state in {"no_signal", "insufficient_data"}:
        return base_sentence, []

    # Response existence suppression
    if flags.get("no_response_flag"):
        return base_sentence, []

    # Collect eligible flag sentences
    for flag_name, sentence in CHANNEL_FLAG_TEMPLATES.items():
        if flags.get(flag_name) is True:
            optional_sentences.append(sentence)

    return base_sentence, optional_sentences

# ----------------------
# D.2.3 — Assembly Rules
# ----------------------

def _assemble_channel_summary(channel_row):
    """
    Assembles up to 2 sentences:
      - Required primary signal sentence
      - Optional single descriptive modifier
    """

    base_sentence, optional_sentences = _eligible_channel_sentences(channel_row)

    if base_sentence is None:
        return []

    if not optional_sentences:
        return [base_sentence]

    # Truncation priority:
    # 1) volatile_response_rate_flag
    # 2) fast_response_flag / slow_response_flag
    priority_order = [
        CHANNEL_FLAG_TEMPLATES.get("volatile_response_rate_flag"),
        CHANNEL_FLAG_TEMPLATES.get("fast_response_flag"),
        CHANNEL_FLAG_TEMPLATES.get("slow_response_flag"),
    ]

    for preferred in priority_order:
        if preferred in optional_sentences:
            return [base_sentence, preferred]

    # Fallback (should rarely trigger)
    return [base_sentence, optional_sentences[0]]

# ----------------------
# D.2 — Public Interface
# ----------------------

def describe_channel(channel_row):
    """
    Returns a list of 1–2 neutral, descriptive sentences
    summarizing the channel's current signal characteristics.
    """

    return _assemble_channel_summary(channel_row)

# ==================================================
# Pillar D.3 — Portfolio-Level Insights
# ==================================================

# ----------------------
# D.3.1 — Phrase Templates
# ----------------------

PORTFOLIO_PATTERN_TEMPLATES = {
    "inactive": "Overall activity across applications has been limited recently.",
    "unstructured_bursting": "Applications are being submitted, but engagement across them has been uneven.",
    "steady_engagement": "Applications are being submitted and engaged with consistently.",
    "overconcentrated_effort": "A small number of applications are receiving a large share of overall effort.",
    "stalled": "Earlier activity occurred, but many applications have since become inactive.",
}

PORTFOLIO_FLAG_TEMPLATES = {
    "low_follow_up_portfolio_flag": "Follow-up activity has been limited across applications.",
    "high_idle_portfolio_flag": "A sizable portion of applications have not been touched recently.",
    "channel_dependency_flag": "Most responses have come from a single outreach channel.",
    "low_signal_environment_flag": "Across channels, response signals remain limited.",
}

# ----------------------
# D.3.2 — Eligibility Logic
# ----------------------

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

    # Collect eligible secondary flags
    for flag_name, sentence in PORTFOLIO_FLAG_TEMPLATES.items():
        if portfolio_row.get(flag_name) is True:
            secondary_sentences.append((flag_name, sentence))

    # Redundancy suppression
    if pattern == "stalled":
        secondary_sentences = [
            (f, s) for (f, s) in secondary_sentences
            if f != "high_idle_portfolio_flag"
        ]

    return primary_sentence, secondary_sentences

# ----------------------
# D.3.3 — Assembly Rules
# ----------------------

def _assemble_portfolio_summary(portfolio_row):
    """
    Assembles up to 3 sentences:
      - 1 required primary pattern sentence
      - Up to 2 secondary flag sentences
    """

    primary_sentence, secondary_sentences = _eligible_portfolio_sentences(portfolio_row)

    if primary_sentence is None:
        return []

    # Truncation priority (highest first)
    priority_order = [
        "channel_dependency_flag",
        "low_follow_up_portfolio_flag",
        "high_idle_portfolio_flag",
        "low_signal_environment_flag",
    ]

    ordered_secondary = []
    for flag in priority_order:
        for f, sentence in secondary_sentences:
            if f == flag:
                ordered_secondary.append(sentence)

    return [primary_sentence] + ordered_secondary[:2]

# ----------------------
# D.3 — Public Interface
# ----------------------

def describe_portfolio(portfolio_row):
    """
    Returns 1–3 neutral, descriptive sentences
    summarizing overall portfolio posture.
    """

    return _assemble_portfolio_summary(portfolio_row)

# ==================================================
# Pillar D.4 — Global Orchestration & Suppression
# ==================================================

# ----------------------
# D.4.1 — Display Limits
# ----------------------

MAX_APPLICATIONS_DISPLAYED = 5
MAX_APPLICATION_SENTENCES = 2
MAX_CHANNEL_SENTENCES = 1
MAX_PORTFOLIO_SENTENCES = 3

# ----------------------
# D.4.2 — Cross-Level Suppression Rules
# ----------------------

def _filter_active_applications(application_rows):
    """
    Closed applications may not surface alongside active ones.
    """
    active = [r for r in application_rows if r.get("application_state") != "closed"]
    return active if active else application_rows


def _filter_low_signal_channels(channel_rows):
    """
    Channels with no_signal or insufficient_data may be hidden
    unless explicitly requested.
    """
    return [
        r for r in channel_rows
        if r.get("channel_signal_state") not in {"no_signal", "insufficient_data"}
    ]


# ----------------------
# D.4.3 — Assembly Engine
# ----------------------

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
        "applications": [],
        "channels": [],
        "portfolio": [],
    }

    # ---- Portfolio takes precedence ----
    portfolio_sentences = describe_portfolio(portfolio_row)
    bundle["portfolio"] = portfolio_sentences[:MAX_PORTFOLIO_SENTENCES]

    # ---- Application-level insights ----
    filtered_apps = _filter_active_applications(application_rows)

    for r in filtered_apps[:MAX_APPLICATIONS_DISPLAYED]:
        narrative = describe_application(
            application_state=r.get("application_state"),
            no_follow_up_flag=r.get("has_no_follow_up", False),
            responded_flag=r.get("responded_flag", False),
            slow_response_flag=r.get("slow_response_flag", False),
            reactivated_flag=r.get("reactivated_flag", False),
            stagnation_flag=r.get("stagnation_flag", False),
        )

        if narrative:
            bundle["applications"].append({
                "application_id": r.get("application_id"),
                "sentences": narrative[:MAX_APPLICATION_SENTENCES],
            })

    # ---- Channel-level insights ----
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
# Pillar C.3 — Portfolio Pattern
# ==================================================

ACTIVITY_RATE_THRESHOLD = 1.0
INACTIVITY_RATE_THRESHOLD = 0.5
CHANNEL_DEPENDENCY_THRESHOLD = 0.60
STABLE_CHANNEL_RATIO_THRESHOLD = 0.30


def portfolio_pattern(portfolio_row):
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
    row = portfolio_metrics_view()
    row["portfolio_pattern"] = portfolio_pattern(row)

    channel_rows = channel_signal_state_view()
    stable_count = sum(1 for c in channel_rows if c["channel_signal_state"] == "stable_signal")
    total_channels = max(1, len(channel_rows))

    row["low_signal_environment_flag"] = (
        stable_count / total_channels
    ) < STABLE_CHANNEL_RATIO_THRESHOLD

    total_responses = sum(c["response_count_by_channel"] for c in channel_rows)
    max_responses = max((c["response_count_by_channel"] for c in channel_rows), default=0)

    if total_responses == 0:
        row["channel_dependency_flag"] = False
    else:
        row["channel_dependency_flag"] = (max_responses / total_responses) >= CHANNEL_DEPENDENCY_THRESHOLD

    return row

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
    zero_outreach_rate = sum(1 for r in app_rows if r["total_outreach_count"] == 0) / applications_total
    idle_application_rate = sum(1 for r in app_rows if r["is_idle_application"]) / applications_total

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
    if metrics_row["current_status"] == "closed":
        return "closed"

    if metrics_row["total_outreach_count"] == 0:
        return "unengaged"

    if metrics_row["days_since_last_action"] > IDLE_DAYS_THRESHOLD:
        return "engaged_idle"

    return "active"


def application_state_view():
    rows = application_metrics_view()
    for r in rows:
        r["application_state"] = application_state(r)
    return rows

# ==================================================
# Pillar D.1 — Application-Level Narratives
# ==================================================

# ----------------------
# D.1.1 — Phrase Templates
# ----------------------

APPLICATION_STATE_TEMPLATES = {
    "unengaged": {
        "base": "This application has been submitted, but no outreach has been logged yet.",
        "flags": {},
    },
    "engaged_idle": {
        "base": "Outreach has occurred, but there has been no recent activity on this application.",
        "flags": {
            "responded_flag": "A response was received earlier.",
            "slow_response_flag": "Responses on this application arrived more slowly than typical.",
        },
    },
    "active": {
        "base": "This application has ongoing outreach activity.",
        "flags": {},
    },
    "closed": {
        "base": "This application is marked as closed.",
        "flags": {},
    },
}

TRANSITION_TEMPLATES = {
    "reactivated_flag": "This application was recently reactivated after a period of inactivity.",
    "stagnation_flag": "This application has remained inactive for an extended period.",
}

# ----------------------
# D.1.2 — Eligibility Logic
# ----------------------

def _eligible_application_templates(application_state, flags):
    """
    Determines which narrative templates are eligible.
    Returns:
      base_sentence (str)
      optional_sentences (list[str])
    """

    state_block = APPLICATION_STATE_TEMPLATES.get(application_state)

    if state_block is None:
        return None, []

    base_sentence = state_block["base"]
    optional_sentences = []

    # Closed state suppression
    if application_state == "closed":
        return base_sentence, []

    # State-level flag templates
    for flag_name, sentence in state_block.get("flags", {}).items():
        if flags.get(flag_name) is True:
            optional_sentences.append(sentence)

    # Transition templates (added later with priority)
    for flag_name, sentence in TRANSITION_TEMPLATES.items():
        if flags.get(flag_name) is True:
            optional_sentences.append(sentence)

    return base_sentence, optional_sentences

# ----------------------
# D.1.3 — Assembly Rules
# ----------------------

def _assemble_application_narrative(application_state, flags):
    """
    Assembles up to 2 sentences:
      - Required base sentence
      - Optional secondary sentence (flag or transition)
    """

    base_sentence, optional_sentences = _eligible_application_templates(
        application_state,
        flags,
    )

    if base_sentence is None:
        return []

    if not optional_sentences:
        return [base_sentence]

    # Prefer transition-based templates
    transition_sentences = [
        s for s in optional_sentences
        if s in TRANSITION_TEMPLATES.values()
    ]

    if transition_sentences:
        chosen_optional = transition_sentences[-1]
    else:
        chosen_optional = optional_sentences[-1]

    return [base_sentence, chosen_optional]

# ----------------------
# D.1 — Public Interface
# ----------------------

def describe_application(
    *,
    application_state,
    no_follow_up_flag=False,
    responded_flag=False,
    slow_response_flag=False,
    reactivated_flag=False,
    stagnation_flag=False,
):
    """
    Returns a list of 1–2 neutral, descriptive sentences
    describing the current application state.
    """

    flags = {
        "no_follow_up_flag": no_follow_up_flag,
        "responded_flag": responded_flag,
        "slow_response_flag": slow_response_flag,
        "reactivated_flag": reactivated_flag,
        "stagnation_flag": stagnation_flag,
    }

    return _assemble_application_narrative(application_state, flags)


# ==================================================
# Test Runner
# ==================================================

if __name__ == "__main__":

    print("\n=== APPLICATION METRICS ===")
    app_metrics = application_metrics_view()
    for r in app_metrics:
        print(r)

    print("\n=== APPLICATION STATES ===")
    app_states = application_state_view()
    for r in app_states:
        print(r["application_id"], r["application_state"])

    print("\n=== APPLICATION NARRATIVES (PILLAR D.1) ===")
    for r in app_metrics:
        narrative = describe_application(
            application_state=application_state(r),
            no_follow_up_flag=r["has_no_follow_up"],
            responded_flag=False,
            slow_response_flag=False,
            reactivated_flag=False,
            stagnation_flag=False,
        )

        print(f"Application {r['application_id']}:")
        for sentence in narrative:
            print("  -", sentence)

    print("\n=== CHANNEL METRICS ===")
    channel_rows = channel_metrics_view()
    for r in channel_rows:
        print(r)

    print("\n=== CHANNEL SIGNAL STATES ===")
    channel_states = channel_signal_state_view()
    for r in channel_states:
        print(
            r["channel_name"],
            r["channel_signal_state"],
            r["channel_flags"],
        )

    print("\n=== PORTFOLIO METRICS ===")
    pm = portfolio_metrics_view()
    print(pm)

    print("\n=== PORTFOLIO PATTERN ===")
    pp = portfolio_pattern_view()
    print(
        pp["portfolio_pattern"],
        {
            "high_idle": pp["high_idle_portfolio"],
            "low_follow_up": pp["low_follow_up_portfolio"],
            "channel_dependency": pp["channel_dependency_flag"],
            "low_signal_environment": pp["low_signal_environment_flag"],
        },
    )

print("\n=== CHANNEL SUMMARIES (PILLAR D.2) ===")
for r in channel_signal_state_view():
    summary = describe_channel(r)
    print(r["channel_name"] + ":")
    for sentence in summary:
        print("  -", sentence)

print("\n=== PORTFOLIO INSIGHTS (PILLAR D.3) ===")
portfolio_row = portfolio_pattern_view()
summary = describe_portfolio(portfolio_row)

for sentence in summary:
    print("  -", sentence)

print("\n=== INSIGHT BUNDLE (PILLAR D.4) ===")

# Prepare inputs
app_rows = application_state_view()
channel_rows = channel_signal_state_view()
portfolio_row = portfolio_pattern_view()

bundle = assemble_insight_bundle(
    application_rows=app_rows,
    channel_rows=channel_rows,
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

