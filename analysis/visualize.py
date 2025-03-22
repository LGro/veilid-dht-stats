import argparse
import json
import math
from pathlib import Path

import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
import requests


def main(stats_json_uri: str, out_directory: str):
    # Initialize directory to save visualizations to
    out_directory = Path(out_directory)
    out_directory.mkdir(exist_ok=True)

    # Load JSON with experimental results from HTTP or disk
    if stats_json_uri.startswith("http"):
        r = requests.get(stats_json_uri)
        experiments = json.loads(r.content.decode()).values()
    else:
        experiments = json.loads(Path(stats_json_uri).read_text()).values()
    df = pd.DataFrame(experiments)

    # Calculate current experiment lifetime from first and last evaluation
    df["lifetime_s"] = df.apply(
        lambda r: (
            r["evaluation_start_unixtimes"][-1]
            - r["evaluation_start_unixtimes"][0]
            + r["evaluation_durations_s"][-1]
            if len(r["evaluation_start_unixtimes"]) > 1
            else None
        ),
        axis=1,
    )
    df["lifetime_h"] = df["lifetime_s"] / 60 / 60
    df["lifetime_d"] = df["lifetime_h"] / 24
    df["payload_size_kib"] = df["payload_size_b"] / 1024

    # Split into ongoing (successful) and stopped (failed) experiments
    df_successes = df[~df["next_evaluation_unixtime"].isna()]
    df_failures = df[df["next_evaluation_unixtime"].isna()]

    # Visualize record lifetime for successes
    f, ax = plt.subplots()
    bins = np.arange(
        math.floor(df_successes["lifetime_d"].dropna().min()),
        math.ceil(df_successes["lifetime_d"].dropna().max()) + 1,
        1,
    )
    for time_interval, group in df_successes.groupby("evaluation_time_interval_h"):
        if len(group["lifetime_d"].dropna()) == 0:
            continue
        ax.hist(
            group["lifetime_d"],
            bins=bins,
            label=f"{time_interval}h ({len(group)})",
            alpha=0.3,
        )
    ax.set_ylabel("Count")
    ax.set_xlabel("DHT record lifetime\n(days, 1 day bins)")
    ax.set_title(
        f"Successes ({len(df_successes["lifetime_d"].dropna())})\n"
        "for different time intervals"
    )
    ax.legend()
    f.tight_layout()
    f.savefig(out_directory / "lifetimes_successes.png")

    # Visualize record lifetime for failures
    if len(df_failures) > 0:
        f, ax = plt.subplots()
        bins = np.arange(
            math.floor(df_failures["lifetime_d"].dropna().min()),
            math.ceil(df_failures["lifetime_d"].dropna().max()) + 1,
            1,
        )
        for time_interval, group in df_failures.groupby("evaluation_time_interval_h"):
            if len(group["lifetime_d"].dropna()) == 0:
                continue
            ax.hist(
                group["lifetime_d"],
                bins=bins,
                label=f"{time_interval}h ({len(group)})",
                alpha=0.3,
            )
        ax.set_ylabel("Count")
        ax.set_xlabel("DHT record lifetime\n(days, 1 day bins)")
        ax.set_title(
            f"Failures ({len(df_failures["lifetime_d"].dropna())})\n"
            "for different time intervals"
        )
        ax.legend()
        f.tight_layout()
        f.savefig(out_directory / "lifetimes_failures.png")

    # Visualize payload size distributions for successes vs failures
    f, ax = plt.subplots()
    bins = range(0, math.ceil(df["payload_size_kib"].dropna().max()) + 1, 1)
    ax.hist(
        df_successes["payload_size_kib"],
        bins=bins,
        label=f"Successes ({len(df_successes)})",
        alpha=0.6,
    )
    if len(df_failures) > 0:
        ax.hist(
            df_failures["payload_size_kib"],
            bins=bins,
            label=f"Failures ({len(df_failures)})",
            alpha=0.6,
        )
    ax.set_ylabel("Count")
    ax.set_xlabel("Payload size\n(KiB, 1KiB bin size)")
    ax.set_title("Payload size\nsuccesses vs failures")
    ax.legend()
    f.tight_layout()
    f.savefig(out_directory / "payload_size_bs_success_vs_failure.png")

    print("done")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Visualize the Veilid DHT availability experiments."
    )
    parser.add_argument(
        "stats_json_uri",
        nargs="?",
        type=str,
        default="http://65.108.215.98/veilid-dht-stats.json",
        help="The URL or local file system path for the JSON results file.",
    )
    parser.add_argument(
        "out_directory",
        nargs="?",
        type=str,
        default=str(Path(__file__).parent.parent),
        help="The local file path to save visualizations to.",
    )
    args = parser.parse_args()
    main(args.stats_json_uri, args.out_directory)
