#!/usr/bin/env python3
import argparse
import json
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlsplit, urlunsplit
from urllib.request import Request, urlopen

import pandas as pd
from tqdm import tqdm


DEFAULT_BASE_URL = "https://maps.delivery.cocodelivery.com/route/f74ea4a0-ec40-4282-b538-97ee59c389fa"
FLUSH_SIZE = 10_000


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch route data for route IDs in a parquet file.")
    parser.add_argument("--base-url", default=DEFAULT_BASE_URL, help="Base URL for route fetches.")
    parser.add_argument("--limit", type=int, default=None, help="Limit number of routes to fetch.")
    parser.add_argument("--input-file", required=True, help="Input parquet file with route_id column.")
    parser.add_argument("--output-file", required=True, help="Output directory for parquet parts.")
    parser.add_argument("--workers", type=int, default=8, help="Number of routes to fetch in parallel.")
    return parser.parse_args()


def normalize_base_url(base_url: str) -> str:
    base_url = base_url.strip()
    if not base_url.startswith(("http://", "https://")):
        base_url = f"https://{base_url}"
    return base_url


def build_route_url(base_url: str, route_id: str) -> str:
    if "{route_id}" in base_url:
        return base_url.format(route_id=route_id)
    parts = urlsplit(base_url)
    path = parts.path or "/"
    if "/route/" in path:
        prefix = path.rsplit("/route/", 1)[0] + "/route/"
        new_path = prefix + route_id
    elif path.rstrip("/").endswith("/route"):
        new_path = path.rstrip("/") + "/" + route_id
    else:
        new_path = path.rstrip("/") + "/" + route_id
    return urlunsplit((parts.scheme, parts.netloc, new_path, parts.query, parts.fragment))


def decode_body(raw: bytes) -> str:
    return raw.decode("utf-8", errors="replace")


def parse_body(text: str) -> Any:
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return text


def fetch_url(url: str) -> dict[str, Any]:
    req = Request(url, headers={"Accept": "application/json"})
    try:
        with urlopen(req, timeout=30) as resp:
            body_text = decode_body(resp.read())
            return {
                "status": resp.status,
                "http_ok": 200 <= resp.status < 300,
                "body": parse_body(body_text),
            }
    except HTTPError as exc:
        body_text = decode_body(exc.read())
        return {
            "status": exc.code,
            "http_ok": False,
            "error": str(exc),
            "body": parse_body(body_text),
        }
    except URLError as exc:
        return {
            "status": None,
            "http_ok": False,
            "error": str(exc),
        }


def load_route_ids(input_file: str, limit: int | None) -> list[str]:
    df = pd.read_parquet(input_file, columns=["route_id"])
    if "route_id" not in df.columns:
        raise SystemExit("Input parquet does not include a route_id column.")
    route_ids = [str(rid) for rid in df["route_id"].dropna().tolist()]
    if limit is not None:
        route_ids = route_ids[:limit]
    return route_ids


def extract_coordinates(body: Any) -> Any | None:
    if not isinstance(body, dict):
        return None
    geojson = body.get("geojson")
    if not isinstance(geojson, dict):
        return None
    geometry = geojson.get("geometry")
    if not isinstance(geometry, dict):
        return None
    return geometry.get("coordinates")


def fetch_route(base_url: str, route_id: str) -> dict[str, Any]:
    url = build_route_url(base_url, route_id)
    payload = fetch_url(url)
    coords = extract_coordinates(payload.get("body"))
    ok = bool(payload.get("http_ok")) and coords is not None
    return {
        "route_id": route_id,
        "ok": ok,
        "coordinates": coords,
        "status": payload.get("status"),
    }


def load_existing_route_ids(output_dir: Path) -> set[str]:
    if not output_dir.exists():
        return set()
    existing_ids: set[str] = set()
    for path in sorted(output_dir.glob("part-*.parquet")):
        try:
            df = pd.read_parquet(path, columns=["route_id"])
        except Exception:
            continue
        if "route_id" in df.columns:
            existing_ids.update(str(rid) for rid in df["route_id"].dropna().tolist())
    return existing_ids


def next_part_index(output_dir: Path) -> int:
    parts = sorted(output_dir.glob("part-*.parquet"))
    if not parts:
        return 0
    last = parts[-1].stem
    try:
        return int(last.split("-", 1)[1]) + 1
    except (IndexError, ValueError):
        return len(parts)


def main() -> None:
    args = parse_args()
    base_url = normalize_base_url(args.base_url)
    route_ids = load_route_ids(args.input_file, args.limit)
    output_dir = Path(args.output_file)
    output_dir.mkdir(parents=True, exist_ok=True)
    existing_ids = load_existing_route_ids(output_dir)
    pending_ids = [rid for rid in route_ids if rid not in existing_ids]
    rows: list[dict[str, Any]] = []
    part_index = next_part_index(output_dir)

    def flush_rows() -> None:
        nonlocal rows, part_index
        if not rows:
            return
        new_df = pd.DataFrame(rows)
        tmp_path = output_dir / f"part-{part_index:06d}.parquet.tmp"
        final_path = output_dir / f"part-{part_index:06d}.parquet"
        new_df.to_parquet(tmp_path, index=False)
        os.replace(tmp_path, final_path)
        part_index += 1
        rows = []
    bar = tqdm(total=len(pending_ids), unit="route", dynamic_ncols=True)
    executor = ThreadPoolExecutor(max_workers=args.workers)
    futures: list[Any] = []
    interrupted = False
    try:
        try:
            for route_id in pending_ids:
                futures.append(executor.submit(fetch_route, base_url, route_id))
        except KeyboardInterrupt:
            interrupted = True
        if not interrupted:
            for future in as_completed(futures):
                rows.append(future.result())
                bar.update(1)
                if len(rows) >= FLUSH_SIZE:
                    flush_rows()
    except KeyboardInterrupt:
        interrupted = True
        for future in futures:
            future.cancel()
    finally:
        bar.close()
        executor.shutdown(wait=not interrupted, cancel_futures=True)
        flush_rows()
        if interrupted:
            raise SystemExit("Interrupted; saved partial results.")


if __name__ == "__main__":
    main()
