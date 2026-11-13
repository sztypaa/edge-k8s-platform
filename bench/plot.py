import argparse
import os

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd


def load(files):
    df = pd.concat([pd.read_csv(f) for f in files], ignore_index=True)
    df["ok"] = (df["http_status"] == 200) & (df["status"] == "Succeeded")
    df["image_cached"] = df["image_cached"].astype(str).str.lower() == "true"
    return df


def summary(df):
    ok = df[df["ok"]]
    stats = ok.groupby("label")["total_ms"].agg(
        mean="mean",
        std="std",
        median="median",
        p95=lambda s: s.quantile(0.95),
    )
    stats["tasks"] = df.groupby("label").size()
    stats["succeeded"] = ok.groupby("label").size()
    stats["rejected_or_failed"] = stats["tasks"] - stats["succeeded"]
    stats["cold_starts"] = ok[~ok["image_cached"]].groupby("label").size()
    stats["dispatch_ms_mean"] = ok.groupby("label")["dispatch_ms"].mean()
    stats["pod_ms_mean"] = ok.groupby("label")["pod_ms"].mean()
    return stats.fillna(0).round(0).astype(int, errors="ignore")


def plot_times(stats, outdir):
    fig, ax = plt.subplots(figsize=(7, 4))
    x = range(len(stats))
    ax.bar([i - 0.2 for i in x], stats["mean"], width=0.4, yerr=stats["std"], capsize=4, label="średnia")
    ax.bar([i + 0.2 for i in x], stats["p95"], width=0.4, label="p95")
    ax.set_xticks(list(x))
    ax.set_xticklabels(stats.index)
    ax.set_ylabel("czas od zlecenia do wyniku [ms]")
    ax.set_title("Czas wykonania zadań")
    ax.legend()
    fig.tight_layout()
    path = os.path.join(outdir, "czas_wykonania.png")
    fig.savefig(path, dpi=150)
    return path


def plot_clusters(df, outdir):
    counts = df[df["ok"]].groupby(["label", "cluster"]).size().unstack(fill_value=0)
    fig, ax = plt.subplots(figsize=(7, 4))
    counts.plot(kind="bar", stacked=True, ax=ax)
    ax.legend(title="klaster")
    ax.set_ylabel("liczba zadań")
    ax.set_xlabel("")
    ax.set_title("Rozkład zadań na klastry")
    ax.tick_params(axis="x", rotation=0)
    fig.tight_layout()
    path = os.path.join(outdir, "rozklad_klastry.png")
    fig.savefig(path, dpi=150)
    return path


def main():
    parser = argparse.ArgumentParser(description="Tabela i wykresy z plików CSV generatora loadgen")
    parser.add_argument("files", nargs="+", help="pliki CSV z loadgen")
    parser.add_argument("--outdir", default="wyniki", help="katalog na wykresy i tabelę")
    args = parser.parse_args()

    os.makedirs(args.outdir, exist_ok=True)
    df = load(args.files)
    stats = summary(df)

    try:
        print(stats.to_markdown())
    except ImportError:
        print(stats.to_string())
    stats.to_csv(os.path.join(args.outdir, "podsumowanie.csv"))
    print("zapisano:", plot_times(stats, args.outdir))
    print("zapisano:", plot_clusters(df, args.outdir))


if __name__ == "__main__":
    main()
