import argparse
import os

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd
from matplotlib.ticker import MaxNLocator

IMAGE_SIZES_MB = {"light": 0.08, "size-50": 52, "size-100": 105, "size-250": 262, "size-500": 525}
B2_LABELS = {"new": "New objective\n(CPU + memory + cache)", "old": "Old objective\n(free CPU + 500 bonus)"}
COMPLETED = "#2b7bba"
REJECTED = "#e8833a"


def load(path):
    df = pd.read_csv(path, keep_default_na=False)
    df["ok"] = (df["http_status"] == 200) & (df["status"] == "Succeeded")
    df["image_cached"] = df["image_cached"].astype(str).str.lower() == "true"
    df["tag"] = df["image"].str.split(":").str[-1]
    return df


def show(table, outdir, name):
    table.to_csv(os.path.join(outdir, name + ".csv"))
    try:
        print(table.to_markdown())
    except ImportError:
        print(table.to_string())


def save(fig, outdir, name, note=None):
    if note:
        fig.text(0.5, 0.01, note, ha="center", fontsize=9, color="dimgray")
        fig.tight_layout(rect=(0, 0.05, 1, 1))
    else:
        fig.tight_layout()
    path = os.path.join(outdir, name + ".png")
    fig.savefig(path, dpi=150)
    print("saved:", path)


def label_bars(ax, bars, fmt="{:.0f}", errors=None):
    for i, bar in enumerate(bars):
        height = bar.get_height()
        top = bar.get_y() + height + (errors[i] if errors is not None else 0)
        ax.annotate(fmt.format(height), (bar.get_x() + bar.get_width() / 2, top),
                    xytext=(0, 3), textcoords="offset points", ha="center", va="bottom", fontsize=9)


def outcome_panel(ax, table, labels):
    x = list(range(len(table)))
    done = ax.bar(x, table["succeeded"], color=COMPLETED, label="Completed")
    rejected = ax.bar(x, table["rejected_or_failed"], bottom=table["succeeded"], color=REJECTED, label="Rejected")
    for bars in (done, rejected):
        for bar in bars:
            if bar.get_height():
                ax.text(bar.get_x() + bar.get_width() / 2, bar.get_y() + bar.get_height() / 2, f"{bar.get_height():.0f}",
                        ha="center", va="center", color="white", fontweight="bold")
    ax.set_xticks(x)
    ax.set_xticklabels(labels)
    ax.set_ylabel("Number of tasks")
    ax.set_ylim(0, table["tasks"].max() * 1.25)
    ax.yaxis.set_major_locator(MaxNLocator(integer=True))
    ax.set_title("Task outcome")
    ax.legend(loc="upper center", ncol=2)


def b1(df, outdir):
    ok = df[df["ok"]]
    table = ok.groupby(["tag", "label"])["pod_ms"].agg(["mean", "std", "count"]).unstack("label")
    table = table.reindex(sorted(table.index, key=lambda t: IMAGE_SIZES_MB.get(t, 0)))
    table.columns = ["_".join(c) for c in table.columns]
    table["pull_ms"] = table["mean_cold"] - table["mean_warm"]
    table["failed"] = df[~df["ok"]].groupby("tag").size()
    show(table.fillna(0).round(0), outdir, "b1_table")

    wrong = df[df["ok"] & (df["label"] == "cold") & df["image_cached"]]
    if len(wrong):
        print(f"Note: {len(wrong)} cold starts had image_cached=true in the bid. Before the fix of isImageCachedOnNode "
              "an image with a longer name (e.g. size-500 for size-50) counted as cached, the pod time still includes the image pull.")

    fig, ax = plt.subplots(figsize=(8, 4.8))
    x = list(range(len(table)))
    cold = ax.bar([i - 0.2 for i in x], table["mean_cold"] / 1000, width=0.4, yerr=table["std_cold"] / 1000, capsize=4,
                  color=COMPLETED, label="Cold start (image pulled from Docker Hub)")
    warm = ax.bar([i + 0.2 for i in x], table["mean_warm"] / 1000, width=0.4, yerr=table["std_warm"] / 1000, capsize=4,
                  color=REJECTED, label="Warm start (image already on node)")
    label_bars(ax, cold, "{:.1f}", list(table["std_cold"].fillna(0) / 1000))
    label_bars(ax, warm, "{:.1f}", list(table["std_warm"].fillna(0) / 1000))
    ax.set_xticks(x)
    ax.set_xticklabels([f"{tag}\n({IMAGE_SIZES_MB.get(tag, 0):g} MB)" for tag in table.index])
    ax.set_xlabel("Container image (compressed download size)")
    ax.set_ylabel("Pod run time incl. image pull [s]")
    ax.set_ylim(0, (table["mean_cold"] + table["std_cold"]).max() / 1000 * 1.15)
    ax.set_title("B1: Task start time vs. image size")
    ax.legend(loc="upper left")
    runs = int(table["count_cold"].min())
    save(fig, outdir, "b1_chart", f"Mean of {runs} runs per bar, error bars show standard deviation.")


def b2(df, outdir):
    ok = df[df["ok"]]
    table = ok.groupby("label")["total_ms"].agg(
        mean="mean", median="median", p95=lambda s: s.quantile(0.95)
    )
    table["tasks"] = df.groupby("label").size()
    table["succeeded"] = ok.groupby("label").size()
    table["rejected_or_failed"] = table["tasks"] - table["succeeded"]
    table["warm_starts_pct"] = ok.groupby("label")["image_cached"].mean() * 100
    table["pod_ms_mean"] = ok.groupby("label")["pod_ms"].mean()
    show(table.fillna(0).round(0), outdir, "b2_table")

    labels = [B2_LABELS.get(label, label) for label in table.index]
    x = list(range(len(table)))
    fig, (ax1, ax2, ax3) = plt.subplots(1, 3, figsize=(14, 5))
    outcome_panel(ax1, table, labels)

    mean = ax2.bar([i - 0.2 for i in x], table["mean"] / 1000, width=0.4, color=COMPLETED, label="Mean")
    p95 = ax2.bar([i + 0.2 for i in x], table["p95"] / 1000, width=0.4, color=REJECTED, label="95th percentile")
    label_bars(ax2, mean, "{:.1f}")
    label_bars(ax2, p95, "{:.1f}")
    ax2.set_xticks(x)
    ax2.set_xticklabels(labels)
    ax2.set_ylabel("Time from submission to result [s]")
    ax2.set_ylim(0, table["p95"].max() / 1000 * 1.3)
    ax2.set_title("Latency of completed tasks")
    ax2.legend(loc="upper center", ncol=2)

    warm = ax3.bar(x, table["warm_starts_pct"], color=COMPLETED)
    label_bars(ax3, warm, "{:.0f}%")
    ax3.set_xticks(x)
    ax3.set_xticklabels(labels)
    ax3.set_ylabel("Completed tasks run on a node with the image [%]")
    ax3.set_ylim(0, 110)
    ax3.set_title("Warm starts (image cache hits)")

    fig.suptitle("B2: Cluster selection – new vs. old objective function", fontsize=13)
    series = df.groupby("label")["task_id"].apply(lambda s: s.str.rsplit("-", n=2).str[1].nunique()).max()
    per_series = len(df) // len(table) // series
    save(fig, outdir, "b2_chart",
         f"{series} series × {per_series} tasks per variant, 4 images (Zipf popularity), "
         "each image pre-pulled on one of the two nodes before every series.")


def max_concurrent(rows):
    events = []
    for _, r in rows.iterrows():
        if r["ack_at"]:
            events.append((pd.Timestamp(r["ack_at"]), 1))
            events.append((pd.Timestamp(r["done_at"]), -1))
    current = best = 0
    for _, step in sorted(events):
        current += step
        best = max(best, current)
    return best


def b3(df, outdir):
    ok = df[df["ok"]]
    table = pd.DataFrame(index=sorted(df["label"].unique()))
    table["tasks"] = df.groupby("label").size()
    table["succeeded"] = ok.groupby("label").size()
    table["rejected_or_failed"] = table["tasks"] - table["succeeded"]
    table["resignations"] = df.groupby("label")["resignations"].sum()
    table["dispatches_per_task"] = df.groupby("label")["dispatches"].mean()
    table["ack_ms_mean"] = ok.groupby("label")["ack_ms"].mean()
    table["ack_ms_max"] = ok.groupby("label")["ack_ms"].max()
    table["rejected_ms_mean"] = df[~df["ok"]].groupby("label")["total_ms"].mean()
    table["max_tasks_on_one_cluster"] = [
        max([max_concurrent(g) for _, g in df[df["label"] == label].groupby("cluster")] or [0])
        for label in table.index
    ]
    show(table.fillna(0).round(1), outdir, "b3_table")

    labels = [f"{label} strategy" for label in table.index]
    x = list(range(len(table)))
    fig, (ax1, ax2, ax3) = plt.subplots(1, 3, figsize=(13, 4.8))
    outcome_panel(ax1, table, labels)

    ack = ax2.bar(x, table["ack_ms_mean"] / 1000, color=COMPLETED)
    label_bars(ax2, ack, "{:.2f} s")
    ax2.set_xticks(x)
    ax2.set_xticklabels(labels)
    ax2.set_ylabel("Time from submission to ACK [s]")
    ax2.set_ylim(0, table["ack_ms_mean"].max() / 1000 * 1.25)
    ax2.set_title("Time to assign a completed task")

    rejected = ax3.bar(x, table["rejected_ms_mean"] / 1000, color=REJECTED)
    label_bars(ax3, rejected, "{:.2f} s")
    ax3.set_xticks(x)
    ax3.set_xticklabels(labels)
    ax3.set_ylabel("Time from submission to rejection [s]")
    ax3.set_ylim(0, table["rejected_ms_mean"].max() / 1000 * 1.25)
    ax3.set_title("Time to reject a task that does not fit")

    fig.suptitle("B3: Overload – more simultaneous tasks than the clusters can run", fontsize=13)
    series = df.groupby("label")["task_id"].apply(lambda s: s.str.rsplit("-", n=2).str[1].nunique()).max()
    burst = len(df) // len(table) // series
    save(fig, outdir, "b3_chart",
         f"{series} bursts × {burst} simultaneous tasks per strategy, each task needs 60% of a Raspberry Pi CPU, so a cluster fits one at a time. "
         f"Max tasks running at once on one cluster: {int(table['max_tasks_on_one_cluster'].max())}.")


def main():
    parser = argparse.ArgumentParser(description="Tables and charts from benchmark results")
    parser.add_argument("test", choices=["b1", "b2", "b3"])
    parser.add_argument("csv", help="CSV file from loadgen, e.g. b1.csv")
    parser.add_argument("--outdir", default="results", help="directory for tables and charts")
    args = parser.parse_args()

    os.makedirs(args.outdir, exist_ok=True)
    df = load(args.csv)
    {"b1": b1, "b2": b2, "b3": b3}[args.test](df, args.outdir)


if __name__ == "__main__":
    main()
