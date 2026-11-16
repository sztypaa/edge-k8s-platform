import argparse
import os

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd


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


def save(fig, outdir, name):
    fig.tight_layout()
    path = os.path.join(outdir, name + ".png")
    fig.savefig(path, dpi=150)
    print("zapisano:", path)


def image_size_mb(tag):
    if tag.startswith("size-"):
        return int(tag.split("-")[1])
    return 0


def b1(df, outdir):
    ok = df[df["ok"]]
    table = ok.groupby(["tag", "label"])["pod_ms"].agg(["mean", "std", "count"]).unstack("label")
    table = table.reindex(sorted(table.index, key=image_size_mb))
    table["pull_ms"] = table[("mean", "cold")] - table[("mean", "warm")]
    table.columns = ["_".join(c).strip("_") for c in table.columns]
    table["failed"] = df[~df["ok"]].groupby("tag").size()
    show(table.fillna(0).round(0), outdir, "b1_tabela")

    wrong = df[df["ok"] & (df["label"] == "cold") & df["image_cached"]]
    if len(wrong):
        print(f"UWAGA: {len(wrong)} zimnych startów zgłosiło obraz na węźle, sprawdź usuwanie obrazów")

    fig, ax = plt.subplots(figsize=(7, 4))
    x = range(len(table))
    ax.bar([i - 0.2 for i in x], table["mean_cold"] / 1000, width=0.4, yerr=table["std_cold"] / 1000, capsize=4, label="zimny start")
    ax.bar([i + 0.2 for i in x], table["mean_warm"] / 1000, width=0.4, yerr=table["std_warm"] / 1000, capsize=4, label="ciepły start")
    ax.set_xticks(list(x))
    ax.set_xticklabels(table.index)
    ax.set_xlabel("obraz")
    ax.set_ylabel("czas działania poda [s]")
    ax.set_title("Zimny i ciepły start")
    ax.legend()
    save(fig, outdir, "b1_start")


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
    show(table.fillna(0).round(0), outdir, "b2_tabela")

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(9, 4))
    x = range(len(table))
    ax1.bar([i - 0.2 for i in x], table["mean"] / 1000, width=0.4, label="średnia")
    ax1.bar([i + 0.2 for i in x], table["p95"] / 1000, width=0.4, label="p95")
    ax1.set_xticks(list(x))
    ax1.set_xticklabels(table.index)
    ax1.set_ylabel("czas od zlecenia do wyniku [s]")
    ax1.set_title("Czas wykonania")
    ax1.legend()
    ax2.bar(list(x), table["warm_starts_pct"])
    ax2.set_xticks(list(x))
    ax2.set_xticklabels(table.index)
    ax2.set_ylabel("% zadań z obrazem na węźle")
    ax2.set_ylim(0, 100)
    ax2.set_title("Ciepłe starty")
    save(fig, outdir, "b2_funkcja_celu")


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
    table["max_tasks_on_one_cluster"] = [
        max([max_concurrent(g) for _, g in df[df["label"] == label].groupby("cluster")] or [0])
        for label in table.index
    ]
    show(table.fillna(0).round(1), outdir, "b3_tabela")

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(9, 4))
    x = list(range(len(table)))
    ax1.bar(x, table["succeeded"], label="wykonane")
    ax1.bar(x, table["rejected_or_failed"], bottom=table["succeeded"], label="odrzucone")
    ax1.set_xticks(x)
    ax1.set_xticklabels(table.index)
    ax1.set_ylabel("liczba zadań")
    ax1.set_title("Wykonane i odrzucone zadania")
    ax1.legend()
    ax2.bar(x, table["ack_ms_mean"] / 1000)
    ax2.set_xticks(x)
    ax2.set_xticklabels(table.index)
    ax2.set_ylabel("czas od zlecenia do ACK [s]")
    ax2.set_title("Czas przydziału")
    save(fig, outdir, "b3_strategie")


def main():
    parser = argparse.ArgumentParser(description="Tabele i wykresy z wyników benchmarków")
    parser.add_argument("test", choices=["b1", "b2", "b3"])
    parser.add_argument("csv", help="plik CSV z loadgen, np. results/b1.csv")
    parser.add_argument("--outdir", default="wyniki", help="katalog na tabele i wykresy")
    args = parser.parse_args()

    os.makedirs(args.outdir, exist_ok=True)
    df = load(args.csv)
    {"b1": b1, "b2": b2, "b3": b3}[args.test](df, args.outdir)


if __name__ == "__main__":
    main()
