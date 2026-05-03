import os
from google.cloud import bigquery
from google.oauth2 import service_account

BQ_PROJECT = "project-16d9dcf7-d9a8-4c74-b35"
BQ_DATASET = "cart_pipeline"
BQ_TABLE   = "abandoned_carts"
KEY_PATH   = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "credentials/gcp-key.json")

# ── Recovery logic ─────────────────────────────────────────────────────────────
# cart_value > $100  → 10% discount email      (high-value, worth the incentive)
# cart_value < $30   → urgency push            (low-value, cheap to nudge)
# $30–$100           → standard recovery email (mid-range, no discount needed)

def classify(cart_value: float) -> tuple[str, str]:
    if cart_value > 100:
        return "email", "10% discount — complete your order"
    elif cart_value < 30:
        return "push",  "Your cart is waiting — items selling fast!"
    else:
        return "email", "You left something behind"


def run():
    credentials = service_account.Credentials.from_service_account_file(KEY_PATH)
    client      = bigquery.Client(project=BQ_PROJECT, credentials=credentials)

    # ── Fetch unactioned abandoned carts ──────────────────────────────────────
    query = f"""
        SELECT session_id, user_id, cart_value, minutes_inactive, abandoned_at
        FROM `{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}`
        WHERE recovery_sent = false
        ORDER BY cart_value DESC
    """
    rows  = list(client.query(query).result())
    total = len(rows)

    if total == 0:
        print("[recovery] No unactioned abandoned carts found.")
        return

    print(f"[recovery] Processing {total:,} abandoned carts...\n")

    email_count = 0
    push_count  = 0

    for row in rows:
        channel, message = classify(row.cart_value)

        if channel == "email":
            email_count += 1
        else:
            push_count += 1

        print(
            f"[recovery] ${row.cart_value:<8.2f} "
            f"→ {channel:<5}  \"{message}\""
            f"  user={str(row.user_id)[:8]}"
        )

    # ── Mark all processed rows as sent ───────────────────────────────────────
    update = f"""
        UPDATE `{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}`
        SET recovery_sent = true
        WHERE recovery_sent = false
    """
    client.query(update).result()

    print(f"\n[recovery] Marked {total:,} rows as recovery_sent=true in BigQuery")
    print(f"\n[recovery] ── Summary ─────────────────────────────────")
    print(f"[recovery]   Total actioned:       {total:,}")
    print(f"[recovery]   Discount emails:      {email_count:,}")
    print(f"[recovery]   Urgency push notifs:  {push_count:,}")
    print(f"[recovery] ────────────────────────────────────────────────")


if __name__ == "__main__":
    run()
