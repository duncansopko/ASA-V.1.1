import sqlite3
from pathlib import Path
from datetime import datetime, timezone

# --------------------
# Database connection
# --------------------

DB_PATH = Path(__file__).resolve().parents[1] / "data" / "asa.db"

def get_connection():
    return sqlite3.connect(DB_PATH)


# --------------------
# Core write functions
# --------------------

def add_application(company, role, application_link=None):
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute(
        """
        INSERT INTO applications (company, role, application_link)
        VALUES (?, ?, ?)
        """,
        (company, role, application_link)
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
        (application_id, channel, outreach_type)
    )

    conn.commit()
    conn.close()


# --------------------
# Metric functions
# --------------------

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
        (application_id, application_id, application_id)
    )

    result = cursor.fetchone()[0]
    conn.close()

    if result is None:
        return None

    last_action_time = datetime.fromisoformat(result).replace(tzinfo=timezone.utc)
    return (datetime.now(timezone.utc) - last_action_time).days


def list_applications():
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT application_id, company, role, created_at
        FROM applications
        """
    )

    rows = cursor.fetchall()
    conn.close()
    return rows


# --------------------
# Test runner
# --------------------

if __name__ == "__main__":
    print("Running test…")

    app_id = add_application(
        company="OpenAI",
        role="Research Intern",
        application_link="https://example.com/job"
    )

    print(f"Inserted application_id: {app_id}")

    add_outreach(
        application_id=app_id,
        channel="LinkedIn"
    )

    print("Logged outreach event.")

    days_idle = days_since_last_action(app_id)
    print(f"Days since last action: {days_idle}")

    print("Current applications:")
    for row in list_applications():
        print(row)
if __name__ == "__main__":
    print("Running test…")

    app_id = add_application(
        company="OpenAI",
        role="Research Intern",
        application_link="https://example.com/job"
    )

    print(f"Inserted application_id: {app_id}")

    add_outreach(
        application_id=app_id,
        channel="LinkedIn",
        outreach_type="initial"
    )

    print("Logged outreach event.")

    days_idle = days_since_last_action(app_id)
    print(f"Days since last action: {days_idle}")

