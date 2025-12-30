import argparse
from pathlib import Path

from tqdm import tqdm
import pyarrow as pa
import pyarrow.parquet as pq
import redshift_connector


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run a Redshift query using an IAM profile and stream results to Parquet.")
    parser.add_argument("sql_path", help="Path to a SQL file to execute.")
    parser.add_argument("--cluster-id", default="coco-delivery-platform-data-warehouse", help="Redshift cluster identifier.")
    parser.add_argument("--database", default="dw", help="Database name.")
    parser.add_argument("--db-user", default="coco", help="Database user to assume for IAM auth.")
    parser.add_argument("--region", default="us-west-2", help="AWS region of the cluster.")
    parser.add_argument("--profile", default=None, help="AWS profile name for credentials (default: use environment).")
    parser.add_argument("-o", "--output", default="redshift_results.parquet", help="Path to write Parquet output.")
    parser.add_argument("--batch-size", type=int, default=10_000, help="Number of rows to fetch per batch.")
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
            output_path = Path(args.output)
            writer = None
            progress = tqdm(unit="rows", desc="Streaming rows")
            try:
                column_names = [col[0] for col in cursor.description]
                while True:
                    rows = cursor.fetchmany(args.batch_size)
                    if not rows:
                        break
                    columns = list(zip(*rows))
                    batch = {name: list(values) if columns else [] for name, values in zip(column_names, columns, strict=False)}
                    table = pa.Table.from_pydict(batch)
                    if writer is None:
                        writer = pq.ParquetWriter(output_path, table.schema)
                    writer.write_table(table)
                    progress.update(len(rows))
            finally:
                if writer is not None:
                    writer.close()
                progress.close()
        finally:
            cursor.close()
    finally:
        conn.close()


if __name__ == "__main__":
    main()
