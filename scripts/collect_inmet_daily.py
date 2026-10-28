#!/usr/bin/env python3
"""Coleta observações diárias das estações automáticas do INMET."""

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import shutil
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import requests

CATALOG_URL = "https://apitempo.inmet.gov.br/estacoes/T"
OBSERVATION_URL = "https://apitempo.inmet.gov.br/estacao/{day}/{day}/{station}"
DATA_ROOT = Path("data/inmet")
HEADERS = {
    "Accept": "application/json",
    "User-Agent": "xpe-painel-desastres/0.1 (projeto academico)",
}


def get_json(url: str, attempts: int = 3) -> object:
    """Obtém JSON com repetição curta para erros transitórios."""
    last_error: Exception | None = None
    for attempt in range(1, attempts + 1):
        try:
            response = requests.get(url, headers=HEADERS, timeout=60)
            if response.status_code == 204:
                return []
            response.raise_for_status()
            return response.json()
        except (requests.RequestException, ValueError) as exc:
            last_error = exc
            if attempt < attempts:
                time.sleep(2 ** (attempt - 1))
    raise RuntimeError(f"Falha após {attempts} tentativas: {url}") from last_error


def collect_station(day: date, station: dict[str, object]) -> tuple[str, list[dict[str, object]]]:
    code = str(station["CD_ESTACAO"])
    url = OBSERVATION_URL.format(day=day.isoformat(), station=code)
    payload = get_json(url)
    if not isinstance(payload, list):
        raise TypeError(f"Resposta inesperada para {code}: {type(payload).__name__}")

    rows: list[dict[str, object]] = []
    for item in payload:
        if isinstance(item, dict):
            row = dict(item)
            row["_source_url"] = url
            row["_collected_at_utc"] = datetime.now(timezone.utc).isoformat()
            rows.append(row)
    return code, rows


def prune_old_partitions(root: Path, cutoff: date) -> list[str]:
    removed: list[str] = []
    for path in root.glob("observation_date=*"):
        if not path.is_dir():
            continue
        try:
            partition_date = date.fromisoformat(path.name.split("=", 1)[1])
        except (IndexError, ValueError):
            continue
        if partition_date < cutoff:
            shutil.rmtree(path)
            removed.append(path.as_posix())
    return sorted(removed)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", type=date.fromisoformat)
    parser.add_argument("--max-stations", type=int, default=0)
    parser.add_argument("--workers", type=int, default=8)
    args = parser.parse_args()

    sao_paulo = ZoneInfo("America/Sao_Paulo")
    target_day = args.date or (datetime.now(sao_paulo).date() - timedelta(days=1))

    catalog = get_json(CATALOG_URL)
    if not isinstance(catalog, list):
        raise TypeError("O catálogo do INMET não retornou uma lista.")

    stations = [
        station
        for station in catalog
        if isinstance(station, dict) and station.get("CD_ESTACAO")
    ]
    stations.sort(key=lambda item: str(item["CD_ESTACAO"]))
    if args.max_stations > 0:
        stations = stations[: args.max_stations]

    records: list[dict[str, object]] = []
    failures: list[dict[str, str]] = []
    with ThreadPoolExecutor(max_workers=max(1, args.workers)) as executor:
        futures = {
            executor.submit(collect_station, target_day, station): station
            for station in stations
        }
        for future in as_completed(futures):
            station = futures[future]
            code = str(station["CD_ESTACAO"])
            try:
                _, rows = future.result()
                records.extend(rows)
            except Exception as exc:
                failures.append({"station": code, "error": str(exc)})

    if not records:
        raise RuntimeError(
            f"Nenhuma observação recebida para {target_day}; "
            f"falhas={len(failures)}, estações={len(stations)}"
        )

    records.sort(
        key=lambda row: (
            str(row.get("CD_ESTACAO", "")),
            str(row.get("DT_MEDICAO", "")),
            str(row.get("HR_MEDICAO", "")),
        )
    )

    partition = DATA_ROOT / f"observation_date={target_day.isoformat()}"
    partition.mkdir(parents=True, exist_ok=True)
    data_path = partition / "inmet_automatic.jsonl.gz"
    with gzip.open(data_path, "wt", encoding="utf-8") as stream:
        for row in records:
            stream.write(json.dumps(row, ensure_ascii=False, sort_keys=True))
            stream.write(chr(10))

    checksum = hashlib.sha256(data_path.read_bytes()).hexdigest()
    cutoff = target_day - timedelta(days=89)
    removed = prune_old_partitions(DATA_ROOT, cutoff)
    manifest = {
        "status": "success",
        "source": "INMET API de estações automáticas",
        "catalog_url": CATALOG_URL,
        "observation_date": target_day.isoformat(),
        "collected_at_utc": datetime.now(timezone.utc).isoformat(),
        "stations_attempted": len(stations),
        "stations_failed": len(failures),
        "records": len(records),
        "file": data_path.as_posix(),
        "bytes": data_path.stat().st_size,
        "sha256": checksum,
        "retention_days": 90,
        "removed_partitions": removed,
        "failure_sample": sorted(failures, key=lambda item: item["station"])[:20],
    }
    manifest_text = json.dumps(manifest, ensure_ascii=False, indent=2) + chr(10)
    (partition / "manifest.json").write_text(manifest_text, encoding="utf-8")
    print(json.dumps(manifest, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
