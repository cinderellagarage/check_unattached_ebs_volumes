#!/usr/bin/env python3
"""
Scan all (or selected) AWS regions for unattached EBS volumes and
estimate monthly storage cost. Outputs JSON/NDJSON/CSV/TSV/Markdown/Table.

Usage examples:
  # default profile, table output
  ./ebs-unused.py

  # JSON to file
  ./ebs-unused.py --format json --out unused_ebs.json

  # Limit to two regions and CSV
  ./ebs-unused.py -r us-west-2 -r us-east-2 --format csv

  # Use env creds, verbose, filter to gp3 volumes >= 50 GiB
  ./ebs-unused.py -e -v --type gp3 --min-size 50
"""
from __future__ import annotations

import argparse
import csv
import json
import logging
import os
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from typing import Dict, Iterable, List, Optional, Tuple

import boto3
from botocore.exceptions import BotoCoreError, ClientError

# ---- Region -> Pricing "location" mapping (non-exhaustive, extend as needed) ----
PRICING_LOCATION_BY_REGION = {
    "us-east-1": "US East (N. Virginia)",
    "us-east-2": "US East (Ohio)",
    "us-west-1": "US West (N. California)",
    "us-west-2": "US West (Oregon)",
    "ca-central-1": "Canada (Central)",
    "eu-central-1": "EU (Frankfurt)",
    "eu-west-1": "EU (Ireland)",
    "eu-west-2": "EU (London)",
    "eu-west-3": "EU (Paris)",
    "eu-north-1": "EU (Stockholm)",
    "eu-south-1": "Europe (Milan)",
    "eu-central-2": "Europe (Zurich)",
    "ap-south-1": "Asia Pacific (Mumbai)",
    "ap-south-2": "Asia Pacific (Hyderabad)",
    "ap-southeast-1": "Asia Pacific (Singapore)",
    "ap-southeast-2": "Asia Pacific (Sydney)",
    "ap-southeast-3": "Asia Pacific (Jakarta)",
    "ap-southeast-4": "Asia Pacific (Melbourne)",
    "ap-northeast-1": "Asia Pacific (Tokyo)",
    "ap-northeast-2": "Asia Pacific (Seoul)",
    "ap-northeast-3": "Asia Pacific (Osaka)",
    "ap-east-1": "Asia Pacific (Hong Kong)",
    "sa-east-1": "South America (Sao Paulo)",
    "af-south-1": "Africa (Cape Town)",
    "me-south-1": "Middle East (Bahrain)",
    "me-central-1": "Middle East (UAE)",
}

# Pricing "volumeType" names expected by the AWS Pricing API
PRICING_EBS_TYPES = {
    "gp3": "General Purpose",
    "gp2": "General Purpose",
    "io1": "Provisioned IOPS",
    "io2": "Provisioned IOPS",
    "sc1": "Cold HDD",
    "st1": "Throughput Optimized HDD",
    "standard": "Magnetic",
}

# ---- Data model ----------------------------------------------------------------
@dataclass
class UnusedVolume:
    region: str
    volume_id: str
    az: str
    volume_type: str
    size_gib: int
    encrypted: bool
    kms_key_id: Optional[str]
    create_time: str  # ISO
    snapshot_count: int
    tags: Dict[str, str]
    est_monthly_cost_usd: float

# ---- Argument parsing -----------------------------------------------------------
def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Detect and estimate cost of unattached (available) EBS volumes.")
    p.add_argument("-p", "--profile", default="default", help="AWS profile to use (default: default)")
    p.add_argument("-e", "--env", action="store_true", help="Use AWS creds from environment (AWS_ACCESS_KEY_ID/SECRET)")
    p.add_argument("-i", "--aws-id", help="AWS access key id")
    p.add_argument("-k", "--aws-secret-key", help="AWS secret access key")
    p.add_argument("-r", "--regions", action="append", help="Limit to specific region(s). Repeatable.")
    p.add_argument("--min-size", type=int, default=0, help="Filter: minimum size GiB (default: 0)")
    p.add_argument("--max-size", type=int, help="Filter: maximum size GiB")
    p.add_argument("--type", dest="types", action="append", help="Filter: volume type(s) (e.g., gp3). Repeatable.")
    p.add_argument("-f", "--format", choices=["json", "ndjson", "csv", "tsv", "markdown", "table"], default="table",
                   help="Output format (default: table)")
    p.add_argument("-o", "--out", help="Write output to file path instead of stdout")
    p.add_argument("-v", "--verbose", action="store_true", help="Verbose logging")
    return p.parse_args()

# ---- Logging & session helpers --------------------------------------------------
def setup_logging(verbose: bool) -> None:
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

def make_session(args: argparse.Namespace) -> boto3.session.Session:
    try:
        if args.env:
            # trust environment variables
            return boto3.Session()
        if args.aws_id and args.aws_secret_key:
            return boto3.Session(aws_access_key_id=args.aws_id, aws_secret_access_key=args.aws_secret_key)
        if args.aws_id or args.aws_secret_key:
            raise ValueError("If specifying static creds, provide BOTH --aws-id and --aws-secret-key.")
        return boto3.Session(profile_name=args.profile)
    except Exception as e:
        logging.error(f"Failed to create AWS session: {e}")
        raise

def list_ec2_regions(sess: boto3.session.Session) -> List[str]:
    try:
        ec2 = sess.client("ec2")
        regions = [r["RegionName"] for r in ec2.describe_regions(AllRegions=False)["Regions"]]
        regions.sort()
        return regions
    except (BotoCoreError, ClientError) as e:
        logging.error(f"Error fetching regions: {e}")
        raise

# ---- Pricing -------------------------------------------------------------------
def _parse_price_usd(price_list_item: str) -> Optional[float]:
    """Extract the USD pricePerUnit from a single Pricing API PriceList JSON string."""
    try:
        obj = json.loads(price_list_item)
        on_demand_terms = next(iter(obj["terms"]["OnDemand"].values()))
        price_dims = next(iter(on_demand_terms["priceDimensions"].values()))
        return float(price_dims["pricePerUnit"]["USD"])
    except Exception:
        return None

def get_pricing_gb_month(sess: boto3.session.Session, region: str) -> Dict[str, float]:
    """
    Return a mapping of EBS volume types (gp3, io2, etc.) to $/GB-month for the given region.
    Pricing service is global but queried via us-east-1.
    """
    pricing = sess.client("pricing", region_name="us-east-1")
    location = PRICING_LOCATION_BY_REGION.get(region)
    price_map: Dict[str, float] = {}
    if not location:
        logging.warning(f"No Pricing location mapping for region {region}; skipping pricing (cost=0).")
        return price_map

    for ebs_code, volume_name in PRICING_EBS_TYPES.items():
        try:
            resp = pricing.get_products(
                ServiceCode="AmazonEC2",
                Filters=[
                    {"Type": "TERM_MATCH", "Field": "productFamily", "Value": "Storage"},
                    {"Type": "TERM_MATCH", "Field": "volumeType", "Value": volume_name},
                    {"Type": "TERM_MATCH", "Field": "location", "Value": location},
                ],
                MaxResults=100,
            )
            # Choose the first parsable price entry.
            price: Optional[float] = None
            for item in resp.get("PriceList", []):
                price = _parse_price_usd(item)
                if price is not None:
                    break
            if price is not None:
                price_map[ebs_code] = price
            else:
                logging.debug(f"No parsable price for {ebs_code} in {region} ({location})")
        except (BotoCoreError, ClientError) as e:
            logging.debug(f"Pricing API error for {ebs_code} in {region}: {e}")
            continue

    return price_map

# ---- Core scan -----------------------------------------------------------------
def scan_region_unattached(
    sess: boto3.session.Session,
    region: str,
    size_range: Tuple[int, Optional[int]],
    types: Optional[Iterable[str]],
    price_gb_month: Dict[str, float],
) -> List[UnusedVolume]:
    min_size, max_size = size_range
    ec2 = sess.resource("ec2", region_name=region)
    vols = ec2.volumes.filter(Filters=[{"Name": "status", "Values": ["available"]}])

    results: List[UnusedVolume] = []
    for v in vols:
        v_type = (v.volume_type or "").lower()
        if types and v_type not in {t.lower() for t in types}:
            continue
        size = int(v.size)
        if size < min_size:
            continue
        if max_size is not None and size > max_size:
            continue

        tags = {t["Key"]: t["Value"] for t in (v.tags or []) if "Key" in t and "Value" in t}
        price = price_gb_month.get(v_type, 0.0)
        cost = round(price * size, 4)

        # Count snapshots referencing this volume (best-effort; safe and cheap)
        snap_count = 0
        try:
            snap_count = sum(1 for _ in ec2.snapshots.filter(Filters=[{"Name": "volume-id", "Values": [v.id]}]))
        except (BotoCoreError, ClientError):
            pass

        create_time_iso = v.create_time.astimezone(timezone.utc).isoformat() if isinstance(v.create_time, datetime) else str(v.create_time)

        results.append(
            UnusedVolume(
                region=region,
                volume_id=v.id,
                az=v.availability_zone,
                volume_type=v_type,
                size_gib=size,
                encrypted=bool(getattr(v, "encrypted", False)),
                kms_key_id=getattr(v, "kms_key_id", None),
                create_time=create_time_iso,
                snapshot_count=snap_count,
                tags=tags,
                est_monthly_cost_usd=cost,
            )
        )
    return results

# ---- Output formatting ----------------------------------------------------------
CSV_FIELDS = [
    "region",
    "volume_id",
    "az",
    "volume_type",
    "size_gib",
    "encrypted",
    "kms_key_id",
    "create_time",
    "snapshot_count",
    "est_monthly_cost_usd",
    "tags",
]

def to_table(rows: List[UnusedVolume]) -> str:
    if not rows:
        return "No unattached EBS volumes found."
    # Simple pretty table without extra deps
    headers = ["Region", "VolumeId", "AZ", "Type", "GiB", "Enc", "Cost($/mo)"]
    data = [
        [r.region, r.volume_id, r.az, r.volume_type, r.size_gib, "Y" if r.encrypted else "N", f"{r.est_monthly_cost_usd:.4f}"]
        for r in rows
    ]
    colw = [max(len(str(x)) for x in col) for col in zip(*([headers] + data))]
    def fmt(row): return " | ".join(str(val).ljust(w) for val, w in zip(row, colw))
    sep = "-+-".join("-"*w for w in colw)
    out = [fmt(headers), sep]
    out.extend(fmt(r) for r in data)
    total = sum(r.est_monthly_cost_usd for r in rows)
    out.append(sep)
    out.append(f"TOTAL EST. MONTHLY COST: ${total:.2f} across {len(rows)} volumes")
    return "\n".join(out)

def to_markdown(rows: List[UnusedVolume]) -> str:
    if not rows:
        return "_No unattached EBS volumes found._"
    headers = ["Region", "VolumeId", "AZ", "Type", "GiB", "Encrypted", "Cost($/mo)"]
    lines = ["| " + " | ".join(headers) + " |", "| " + " | ".join("---" for _ in headers) + " |"]
    for r in rows:
        lines.append(
            f"| {r.region} | {r.volume_id} | {r.az} | {r.volume_type} | {r.size_gib} | {'Yes' if r.encrypted else 'No'} | {r.est_monthly_cost_usd:.4f} |"
        )
    total = sum(r.est_monthly_cost_usd for r in rows)
    lines.append(f"\n**Total est. monthly cost:** ${total:.2f} across {len(rows)} volumes")
    return "\n".join(lines)

def write_structured(
    rows: List[UnusedVolume],
    fmt: str,
    out_path: Optional[str],
) -> None:
    sink = open(out_path, "w", newline="", encoding="utf-8") if out_path else sys.stdout
    close = (sink is not sys.stdout)

    try:
        if fmt == "json":
            payload = {
                "generated_at": datetime.utcnow().isoformat() + "Z",
                "total_est_monthly_cost_usd": round(sum(r.est_monthly_cost_usd for r in rows), 4),
                "count": len(rows),
                "volumes": [asdict(r) for r in rows],
            }
            json.dump(payload, sink, indent=2)
            sink.write("\n")
        elif fmt == "ndjson":
            for r in rows:
                sink.write(json.dumps(asdict(r)) + "\n")
        elif fmt in ("csv", "tsv"):
            delim = "," if fmt == "csv" else "\t"
            w = csv.DictWriter(sink, fieldnames=CSV_FIELDS, delimiter=delim)
            w.writeheader()
            for r in rows:
                row = asdict(r)
                # flatten tags as key1=val1;key2=val2
                row["tags"] = ";".join(f"{k}={v}" for k, v in sorted(r["tags"].items()))
                w.writerow(row)
        elif fmt == "markdown":
            sink.write(to_markdown(rows) + "\n")
        elif fmt == "table":
            sink.write(to_table(rows) + "\n")
        else:
            raise ValueError(f"Unsupported format: {fmt}")
    finally:
        if close:
            sink.close()

# ---- Main ----------------------------------------------------------------------
def main() -> int:
    args = parse_args()
    setup_logging(args.verbose)

    try:
        sess = make_session(args)
        regions = args.regions or list_ec2_regions(sess)
        logging.info(f"Scanning {len(regions)} region(s): {', '.join(regions)}")
    except Exception:
        return 2

    # Pricing cache per region
    pricing_cache: Dict[str, Dict[str, float]] = {}

    # Parallel scan per region
    futures = {}
    results: List[UnusedVolume] = []
    errors = 0

    with ThreadPoolExecutor(max_workers=min(16, max(4, len(regions)))) as pool:
        for region in regions:
            # fetch pricing once per region
            if region not in pricing_cache:
                pricing_cache[region] = get_pricing_gb_month(sess, region)

            futures[pool.submit(
                scan_region_unattached,
                sess,
                region,
                (args.min_size, args.max_size),
                args.types,
                pricing_cache[region],
            )] = region

        for fut in as_completed(futures):
            region = futures[fut]
            try:
                rows = fut.result()
                if rows:
                    logging.info(f"{region}: {len(rows)} unattached volume(s)")
                else:
                    logging.info(f"{region}: none found")
                results.extend(rows)
            except Exception as e:
                errors += 1
                logging.error(f"{region}: scan failed: {e}")

    # Sort results: highest cost first
    results.sort(key=lambda r: r.est_monthly_cost_usd, reverse=True)

    # Emit
    try:
        write_structured(results, args.format, args.out)
    except Exception as e:
        logging.error(f"Failed to write output: {e}")
        return 3

    if errors:
        logging.warning(f"Completed with {errors} region error(s).")
        return 1 if results else 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
