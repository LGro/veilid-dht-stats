import argparse
import asyncio
import json
import os
import random
import time
from pathlib import Path

import veilid


async def simple_update_callback(update: veilid.VeilidUpdate):
    pass


def generate_random_byte_string(min_length=1, max_length=32000):
    # Generate a random length between min_length and max_length
    length = random.randint(min_length, max_length)

    # Generate a random byte string of the specified length
    random_byte_string = os.urandom(length)

    return random_byte_string


async def create_experiment(rc) -> dict:
    payload = generate_random_byte_string()

    async with rc:
        record = await rc.create_dht_record(veilid.DHTSchema.dflt(1))
        await rc.set_dht_value(record.key, veilid.ValueSubkey(0), payload)

        # Wait for record to settle
        while True:
            report = await rc.inspect_dht_record(
                record.key, subkeys=[], scope=veilid.types.DHTReportScope.LOCAL
            )
            if not report.offline_subkeys:
                break
            time.sleep(1)

        await rc.close_dht_record(record.key)

    return {
        "payload_size_b": len(payload),
        "evaluation_time_interval_h": random.choice([1, 12, 24, 168, 672]),
        "dht_record_key": str(record.key),
        "next_evaluation_unixtime": time.time(),
        "evaluation_start_unixtimes": [],
        "evaluation_durations_s": [],
    }


async def run_experiment(rc, experiment: dict) -> dict:
    exp = experiment.copy()
    start_evaluation = time.time()
    try:
        async with rc:
            record = await rc.open_dht_record(exp["dht_record_key"])
            content = await rc.get_dht_value(
                record.key, veilid.ValueSubkey(0), force_refresh=True
            )
            await rc.close_dht_record(record.key)
            exp["next_evaluation_unixtime"] += (
                exp["evaluation_time_interval_h"] * 60 * 60
            )
            if exp["payload_size_b"] != len(content.data):
                raise ValueError(
                    f"Expected payload size {exp["payload_size_b"]} "
                    f"but got {len(content.data)}"
                )
    except Exception as e:
        exp["exception"] = str(e)
        exp["next_evaluation_unixtime"] = None
    exp["evaluation_start_unixtimes"].append(start_evaluation)
    exp["evaluation_durations_s"].append(time.time() - start_evaluation)
    return exp


async def main(result: Path):
    num_max_experiments = 100
    experiments = json.loads(result.read_text()) if result.exists() else {}

    try:
        api = await veilid.api_connector(simple_update_callback)
    except veilid.VeilidConnectionError:
        print("Unable to connect to veilid-server.")
        return

    async with api:
        # purge routes and DHT records to ensure we start fresh
        await api.debug("purge routes")
        await api.debug("record purge local")
        await api.debug("record purge remote")

        rc = await api.new_routing_context()
        async with rc:
            # Select all experiments that are due for evaluation
            pending_experiments = {
                k: v
                for k, v in experiments.items()
                if v["next_evaluation_unixtime"] is not None
                and v["next_evaluation_unixtime"] < time.time()
            }
            # Run those pending experiments and update their data
            updated_experiments = await asyncio.gather(
                *[run_experiment(rc, e) for e in pending_experiments.values()]
            )
            for e in updated_experiments:
                experiments[e["dht_record_key"]] = e

            # Select all experiments that have not ended
            active_experiments = [
                e
                for e in experiments.values()
                if e["next_evaluation_unixtime"] is not None
            ]
            # Ensure that the maxmimum number of experiments are running by adding more
            num_new_experiments = max(0, num_max_experiments - len(active_experiments))
            new_experiments = await asyncio.gather(
                *[create_experiment(rc) for _ in range(num_new_experiments)]
            )
            for e in new_experiments:
                experiments[e["dht_record_key"]] = e

    result.write_text(json.dumps(experiments))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Run Veilid DHT availability experiments."
    )
    parser.add_argument(
        "target_path",
        nargs="?",
        type=str,
        default="/var/www/html/veilid-dht-stats.json",
        help="The target path for the JSON results file.",
    )
    args = parser.parse_args()

    asyncio.run(main(Path(args.target_path)))
