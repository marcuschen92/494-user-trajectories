import marimo

__generated_with = "0.23.1"
app = marimo.App(width="medium")


@app.cell
def _():
    from pathlib import Path

    import polars as pl

    return Path, pl


@app.cell
def _(Path):
    DATA_DIR = Path(".").absolute().parent.parent / "data"
    return (DATA_DIR,)


@app.cell
def _(pl):
    def enrich_with_tweet_author_ids(
        notes: pl.DataFrame,
    ) -> pl.DataFrame:
        tweet_authors = (
            pl.scan_parquet("/data/cn_archive/derivatives/20260227_raw_posts.parquet")
            .select("post_id", "author_id")
            .filter(pl.col("author_id").is_not_null())
            .unique()
            .collect()
        )

        notes = notes.join(
            tweet_authors.rename({"post_id": "tweetId", "author_id": "tweet_author_id"}),
            on="tweetId",
            how="left",
            coalesce=True,
            validate="m:1"
        )
        return notes

    return (enrich_with_tweet_author_ids,)


@app.cell
def _(DATA_DIR, enrich_with_tweet_author_ids, pl):
    _raw_notes=pl.read_parquet(DATA_DIR / "2026-02-03/notes.parquet")
    _raw_ratings=pl.read_parquet(DATA_DIR / "2026-02-03/noteRatings.parquet")

    notes = _raw_notes.with_columns(
        tweetId=pl.col("tweetId").cast(pl.String),
        noteId=pl.col("noteId").cast(pl.String),
    )

    notes = enrich_with_tweet_author_ids(notes=notes)
    return (notes,)


@app.cell
def _(DATA_DIR, notes):
    (
        notes
        .select("tweet_author_id")
        .unique()
        .sample(fraction=1, shuffle=True)
        .write_csv(DATA_DIR / "tweet_author_ids.csv")
    )
    return


if __name__ == "__main__":
    app.run()
