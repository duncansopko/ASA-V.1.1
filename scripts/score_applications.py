import sqlite3
from pathlib import Path
from datetime import datetime, timezone

# --------------------
# Pillar B — Thresholds (v1.1)
# --------------------

IDLE_DAYS_THRESHOLD = 7
MIN_CHANNEL_SAMPLE_SIZE = 5
HIGH_IDLE_RATE_THRESHOLD = 0.30
LOW_FOLLOW_UP_RATE_THRESHOLD = 0.50

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
# Core write functions (Pillar A)
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
# Metric helpers (Pillar B — application level)
# ==================================================

def days_since_last_action(application_id):
    """
    Returns number of days since the most recent action
    (application creation, outreach, or status change).
    """
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


def time_to_first_outreach(application_id):
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT MIN(o.timestamp), a.created_at
        FROM outreach_events o
        JOIN applications a ON a.application_id = o.application_id
        WHERE o.application_id = ?
        """,
        (application_id,),
    )

    row = cursor.fetchone()
    conn.close()

    if row[0] is None:
        return None

    first_outreach = datetime.fromisoformat(row[0]).replace(tzinfo=timezone.utc)
    created_at = datetime.fromisoformat(row[1]).replace(tzinfo=timezone.utc)
    return (first_outreach - created_at).days


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
    """
    One row per application.
    Canonical per-application metrics table.
    """
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

# ----------------------
# Pillar B — Channel Metrics View
# ----------------------

def channel_metrics_view():
    """
    One row per outreach channel.
    """
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT DISTINCT channel
        FROM outreach_events
        """
    )
    channels = [row[0] for row in cursor.fetchall()]

    rows = []
    for ch in channels:
        # outreach_count_by_channel
        cursor.execute(
            """
            SELECT COUNT(*)
            FROM outreach_events
            WHERE channel = ?
            """,
            (ch,)
        )
        outreach_count = cursor.fetchone()[0]

        # application_coverage_by_channel
        cursor.execute(
            """
            SELECT COUNT(DISTINCT application_id)
            FROM outreach_events
            WHERE channel = ?
            """,
            (ch,)
        )
        app_coverage = cursor.fetchone()[0]

        # v1.1 placeholder (responses not implemented yet)
        response_count = 0
        response_rate = None
        median_response_time = None

        row = {
            "channel_name": ch,
            "outreach_count_by_channel": outreach_count,
            "application_coverage_by_channel": app_coverage,
            "response_count_by_channel": response_count,
            "response_rate_by_channel": response_rate,
            "median_response_time_by_channel": median_response_time,
            "is_low_sample_channel": outreach_count < MIN_CHANNEL_SAMPLE_SIZE,
        }
        rows.append(row)

    conn.close()
    return rows


# ----------------------
# Pillar B — Portfolio Metrics View
# ----------------------

def portfolio_metrics_view():
    """
    One row for the entire job search.
    """
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

    # follow_up_rate
    apps_with_follow_up = sum(1 for r in app_rows if r["has_follow_up"])
    follow_up_rate = apps_with_follow_up / applications_total

    # zero_outreach_rate
    apps_zero_outreach = sum(1 for r in app_rows if r["total_outreach_count"] == 0)
    zero_outreach_rate = apps_zero_outreach / applications_total

    # idle_application_rate
    apps_idle = sum(1 for r in app_rows if r["is_idle_application"])
    idle_application_rate = apps_idle / applications_total

    high_idle_portfolio = idle_application_rate > HIGH_IDLE_RATE_THRESHOLD
    low_follow_up_portfolio = follow_up_rate < LOW_FOLLOW_UP_RATE_THRESHOLD

    # applications_per_week (simple version based on created_at range)
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT MIN(created_at), MAX(created_at) FROM applications")
    min_ts, max_ts = cursor.fetchone()
    conn.close()

    if min_ts is None or max_ts is None:
        applications_per_week = None
    else:
        start = datetime.fromisoformat(min_ts)
        end = datetime.fromisoformat(max_ts)
        days = max(1, (end - start).days)
        weeks_active = max(1, (days + 6) // 7)
        applications_per_week = applications_total / weeks_active

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
# Pillar C — Application States (primary only)
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
    for row in rows:
        row["application_state"] = application_state(row)
    return rows

# ==================================================
# Test runner
# ==================================================

if __name__ == "__main__":
    print("Running test…")

    app_id = add_application(
        company="OpenAI",
        role="Research Intern",
        application_link="https://example.com/job",
    )

    print(f"Inserted application_id: {app_id}")

    add_outreach(application_id=app_id, channel="LinkedIn")
    print("Logged outreach event.")

    print("\nApplication states:")
    for row in application_state_view():
        print(row["application_id"], row["application_state"])

    print("\nApplication metrics view:")
    for row in application_metrics_view():
        print(row)

    print("\nChannel metrics view:")
    for row in channel_metrics_view():
        print(row)

    print("\nPortfolio metrics view:")
    print(portfolio_metrics_view())

