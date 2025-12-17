import argparse
from pathlib import Path
import pandas as pd
import redshift_connector


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run a Redshift query using an IAM profile and save results to CSV.")
    parser.add_argument("sql_path", help="Path to a SQL file to execute.")
    parser.add_argument("--cluster-id", default="coco-delivery-platform-data-warehouse", help="Redshift cluster identifier.")
    parser.add_argument("--database", default="dw", help="Database name.")
    parser.add_argument("--db-user", default="coco", help="Database user to assume for IAM auth.")
    parser.add_argument("--region", default="us-west-2", help="AWS region of the cluster.")
    parser.add_argument("--profile", default=None, help="AWS profile name for credentials (default: use environment).")
    parser.add_argument("-o", "--output", default="redshift_results.csv", help="Path to write CSV output.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    sql = Path(args.sql_path).read_text().strip()
    if not sql:
        raise SystemExit("SQL file is empty.")

    conn = redshift_connector.connect(
        iam=True,
        cluster_identifier=args.cluster_id,
        database=args.database,
        db_user=args.db_user,
        region=args.region,
        profile=args.profile,
    )
    try:
        cursor = conn.cursor()
        try:
            cursor.execute(sql)
            df = cursor.fetch_dataframe()
            df.to_csv(args.output, index=False)
        finally:
            cursor.close()
    finally:
        conn.close()


if __name__ == "__main__":
    main()
