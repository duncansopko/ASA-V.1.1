import argparse

from scripts.score_applications import (
    add_application,
    add_customization,
    add_outreach,
    get_application_snapshot,
)

print("CLI FILE EXECUTING")


# --------------------
# add application
# --------------------
def add_application_cmd(args):
    application_id = add_application(
        company=args.company,
        role=args.role,
        application_link=args.link,
    )

    if args.resume_customized or args.cover_letter_customized:
        add_customization(
            application_id=application_id,
            resume_customized=args.resume_customized,
            cover_letter_customized=args.cover_letter_customized,
        )

    print(f"Application {application_id} added successfully.")


# --------------------
# outreach
# --------------------
def outreach_cmd(args):
    add_outreach(
        application_id=args.application_id,
        channel=args.channel,
        outreach_type=args.type,
    )

    print(
        f"Outreach logged for application {args.application_id} via {args.channel}."
    )


# --------------------
# status
# --------------------
def status_cmd(args):
    snapshot = get_application_snapshot(args.application_id)

    if not snapshot:
        print(f"No application found with ID {args.application_id}")
        return

    m = snapshot["metrics"]

    print(f"\nApplication {args.application_id}")
    print(f"Company: {m['company']}")
    print(f"Role: {m['role']}\n")

    print(f"State: {snapshot['state']}")
    print(f"Outreach: {m['total_outreach_count']}")
    print(f"Follow-ups: {m['follow_up_count']}")
    print(
        f"Customization: resume {'✓' if m['resume_customized'] else '✗'} | "
        f"cover letter {'✓' if m['cover_letter_customized'] else '✗'}\n"
    )

    print("Insights:")
    for line in snapshot["narratives"]:
        print(f"- {line}")


def main():
    parser = argparse.ArgumentParser(
        description="ASA v1.1 — Job Application CLI"
    )

    subparsers = parser.add_subparsers(dest="command", required=True)

    # add
    add_parser = subparsers.add_parser("add", help="Add a new application")
    add_parser.add_argument("--company", required=True)
    add_parser.add_argument("--role", required=True)
    add_parser.add_argument("--link")
    add_parser.add_argument("--resume-customized", action="store_true")
    add_parser.add_argument("--cover-letter-customized", action="store_true")
    add_parser.set_defaults(func=add_application_cmd)

    # outreach
    outreach_parser = subparsers.add_parser(
        "outreach", help="Log outreach activity"
    )
    outreach_parser.add_argument(
        "--application-id", type=int, required=True
    )
    outreach_parser.add_argument("--channel", required=True)
    outreach_parser.add_argument(
        "--type",
        choices=["initial", "follow_up"],
        default="initial",
    )
    outreach_parser.set_defaults(func=outreach_cmd)

    # status
    status_parser = subparsers.add_parser(
        "status", help="View application status and insights"
    )
    status_parser.add_argument(
        "--application-id", type=int, required=True
    )
    status_parser.set_defaults(func=status_cmd)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()

