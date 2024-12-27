#!/usr/bin/env python3

import boto3
import os
import argparse
import json
import logging
from botocore.exceptions import BotoCoreError, ClientError


def resolve_arguments():
    """Parse and resolve CLI arguments."""
    parser = argparse.ArgumentParser(description="Detect and analyze unattached EBS volumes.")
    parser.add_argument(
        "-p", "--profile", default="default", help="AWS profile to use (default: 'default')."
    )
    parser.add_argument(
        "-i", "--aws-id", default=None, help="AWS access key ID."
    )
    parser.add_argument(
        "-k", "--aws-secret-key", default=None, help="AWS secret access key."
    )
    parser.add_argument(
        "-e", "--env", action="store_true", help="Use AWS credentials from environment variables."
    )
    parser.add_argument(
        "-v", "--verbose", action="store_true", help="Enable verbose logging."
    )
    parser.add_argument(
        "-j", "--json", action="store_true", help="Output results in JSON format."
    )

    args = parser.parse_args()

    aws_id = None
    aws_secret_key = None

    if args.env:
        aws_id = os.getenv("AWS_ACCESS_KEY_ID")
        aws_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
    else:
        aws_id = args.aws_id
        aws_secret_key = args.aws_secret_key

    if aws_id and not aws_secret_key:
        parser.error("--aws-id requires --aws-secret-key")
    elif aws_secret_key and not aws_id:
        parser.error("--aws-secret-key requires --aws-id")

    return {
        "profile": args.profile,
        "aws_access_key_id": aws_id,
        "aws_secret_access_key": aws_secret_key,
    }, args.json, args.verbose


def setup_logging(verbose):
    """Configure logging."""
    log_level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(level=log_level, format="%(asctime)s [%(levelname)s] %(message)s")


def create_session(credentials):
    """Create a boto3 session."""
    try:
        if credentials["aws_access_key_id"]:
            return boto3.Session(
                aws_access_key_id=credentials["aws_access_key_id"],
                aws_secret_access_key=credentials["aws_secret_access_key"],
            )
        return boto3.Session(profile_name=credentials["profile"])
    except (BotoCoreError, ClientError) as e:
        logging.error(f"Failed to create AWS session: {e}")
        raise


def fetch_regions(session):
    """Retrieve all available AWS regions for EC2."""
    try:
        ec2_client = session.client("ec2")
        regions = ec2_client.describe_regions()["Regions"]
        return [region["RegionName"] for region in regions]
    except ClientError as e:
        logging.error(f"Error fetching regions: {e}")
        raise


def get_unused_volumes(ec2_resource):
    """Retrieve unused EBS volumes."""
    try:
        volumes = ec2_resource.volumes.filter(
            Filters=[{"Name": "status", "Values": ["available"]}]
        )
        unused_volumes = []
        for volume in volumes:
            unused_volumes.append({
                "id": volume.id,
                "type": volume.volume_type,
                "size": volume.size,
            })
        return unused_volumes
    except ClientError as e:
        logging.error(f"Error retrieving volumes: {e}")
        raise


def calculate_storage_cost(unused_volumes, pricing):
    """Calculate the storage cost of unused volumes."""
    total_cost = 0
    volume_costs = {}
    for volume in unused_volumes:
        volume_type = volume["type"]
        size = volume["size"]
        cost_per_gb = pricing.get(volume_type, 0)
        cost = size * cost_per_gb
        total_cost += cost
        volume_costs[volume["id"]] = cost
    return total_cost, volume_costs


def get_pricing(session, region):
    """Retrieve EBS pricing for a given region."""
    pricing_client = session.client("pricing", region_name="us-east-1")
    price_map = {}
    ebs_types = {
        "gp2": "General Purpose",
        "io1": "Provisioned IOPS",
        "sc1": "Cold HDD",
        "st1": "Throughput Optimized HDD",
        "standard": "Magnetic",
    }
    for ebs_code, ebs_name in ebs_types.items():
        try:
            response = pricing_client.get_products(
                ServiceCode="AmazonEC2",
                Filters=[
                    {"Type": "TERM_MATCH", "Field": "volumeType", "Value": ebs_name},
                    {"Type": "TERM_MATCH", "Field": "location", "Value": region},
                ],
            )
            price_list = json.loads(response["PriceList"][0])
            terms = list(price_list["terms"]["OnDemand"].values())[0]
            price_dimensions = list(terms["priceDimensions"].values())[0]
            price_per_gb = float(price_dimensions["pricePerUnit"]["USD"])
            price_map[ebs_code] = price_per_gb
        except Exception as e:
            logging.warning(f"Failed to fetch price for {ebs_code} in {region}: {e}")
    return price_map


def main():
    credentials, json_mode, verbose_mode = resolve_arguments()
    setup_logging(verbose_mode)

    session = create_session(credentials)
    regions = fetch_regions(session)
    overall_unused = []
    total_cost = 0

    for region in regions:
        logging.info(f"Processing region: {region}")
        ec2_resource = session.resource("ec2", region_name=region)
        unused_volumes = get_unused_volumes(ec2_resource)
        if not unused_volumes:
            logging.info(f"No unused volumes found in {region}.")
            continue
        pricing = get_pricing(session, region)
        region_cost, volume_costs = calculate_storage_cost(unused_volumes, pricing)
        total_cost += region_cost
        overall_unused.extend(unused_volumes)

        if verbose_mode:
            for volume in unused_volumes:
                logging.info(f"Unused Volume - ID: {volume['id']}, Type: {volume['type']}, Size: {volume['size']} GB")
            logging.info(f"Region Cost: ${region_cost:.2f}")

    if json_mode:
        print(json.dumps({"total_cost": total_cost, "unused_volumes": overall_unused}, indent=4))
    else:
        print(f"Total Unused Volume Cost: ${total_cost:.2f}")


if __name__ == "__main__":
    main()
