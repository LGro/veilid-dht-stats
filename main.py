from typing import Awaitable, Callable
import asyncio
import time
import os
from pathlib import Path
import json
import random
import argparse

import veilid
from veilid import ValueSubkey


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
        rec = await rc.create_dht_record(veilid.DHTSchema.dflt(1))
        vd = await rc.set_dht_value(rec.key, veilid.ValueSubkey(0), payload)

        # Wait for record to settle
        while True:
            rr = await rc.inspect_dht_record(rec.key, [])
            left = 0
            [left := left + (x[1] - x[0] + 1) for x in rr.offline_subkeys]
            if left == 0:
                break
            time.sleep(1)

    return {
        "payload_size": len(payload),
        "fetch_time_interval_hours": random.choice([1, 12, 24]),
        "dht_record_key": str(rec.key),
        "next_evaluation": time.time(),
    }


async def run_experiment(rc, experiment: dict) -> dict:
    exp = {**experiment}
    exp["start_fetch"] = time.time()
    try:
        async with rc:
            content = await rc.get_dht_value(
                exp["dht_record_key"], veilid.ValueSubkey(0), force_refresh=True
            )
            exp["next_evaluation"] += exp["fetch_time_interval_hours"] * 60 * 60
            exp["payload_size"] = len(content.data)
    except Exception as e:
        exp["exception"] = str(e)
        exp["next_evaluation"] = None
        exp["payload_size"] = None
    exp["end_fetch"] = time.time()
    return exp


async def main(result: Path):
    num_max_experiments = 10
    experiments = json.loads(result.read_text()) if result.exists() else []

    try:
        api = await veilid.api_connector(simple_update_callback)
    except veilid.VeilidConnectionError:
        print("Unable to connect to veilid-server.")

    async with api:
        # purge routes to ensure we start fresh
        await api.debug("purge routes")

        rc = await api.new_routing_context()
        async with rc:
            pending_experiments = filter(
                lambda e: e["next_evaluation"] is not None
                and e["next_evaluation"] < time.time(),
                experiments,
            )
            updated_experiments = await asyncio.gather(
                *[run_experiment(rc, e) for e in pending_experiments]
            )
            experiments.extend(updated_experiments)

            active_experiments = filter(
                lambda e: e["next_evaluation"] is not None, experiments
            )
            num_new_experiments = max(
                0, num_max_experiments - len(list(active_experiments))
            )
            new_experiments = await asyncio.gather(
                *[create_experiment(rc) for _ in range(num_new_experiments)]
            )
            experiments.extend(new_experiments)

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
