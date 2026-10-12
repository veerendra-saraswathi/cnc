import polars as pl
from datetime import datetime, timezone
from pathlib import Path

LAKE_ROOT = Path("data/lake")
LAKE_ROOT.mkdir(parents=True, exist_ok=True)

def append_telemetry(records: list[dict]) -> str | None:
    if not records:
        return None
    df = pl.DataFrame(records)
    if "ts" not in df.columns:
        df = df.with_columns(pl.lit(datetime.now(timezone.utc).isoformat()).alias("ts"))

    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    out_dir = LAKE_ROOT / "telemetry" / f"date={today}"
    out_dir.mkdir(parents=True, exist_ok=True)

    fname = out_dir / f"part-{datetime.now(timezone.utc).strftime('%H%M%S%f')}.parquet"
    df.write_parquet(fname)
    return str(fname)

def query_recent(platform_id: str | None = None, limit: int = 100):
    import duckdb
    con = duckdb.connect()
    path = str(LAKE_ROOT / "telemetry" / "**" / "*.parquet")
    q = f"SELECT * FROM read_parquet('{path}', union_by_name=true)"
    if platform_id:
        q += f" WHERE platform_id = '{platform_id}'"
    q += f" ORDER BY ts DESC LIMIT {limit}"
    return con.execute(q).pl()
