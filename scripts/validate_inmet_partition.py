#!/usr/bin/env python3
"""Valida a partição WIS2 publicada pelo coletor diário do INMET."""

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
from collections import Counter
from datetime import date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo


DATA_ROOT = Path("data/inmet")
EXPECTED_VARIABLES = {
    "air_temperature",
    "relative_humidity",
    "total_precipitation_or_total_water_equivalent",
    "wind_speed",
    "maximum_wind_gust_speed",
    "pressure_reduced_to_mean_sea_level",
}


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while chunk := stream.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def validate_partition(target_day: date, root: Path = DATA_ROOT) -> dict[str, object]:
    partition = root / f"observation_date={target_day.isoformat()}"
    manifest_path = partition / "manifest.json"
    if not manifest_path.is_file():
        raise FileNotFoundError(f"Manifesto não encontrado: {manifest_path}")

    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if manifest.get("status") != "success":
        raise ValueError("O manifesto não registra status=success.")
    if manifest.get("observation_date_utc") != target_day.isoformat():
        raise ValueError("A data do manifesto diverge da partição.")
    if manifest.get("retention_days") != 90:
        raise ValueError("A retenção esperada é de 90 dias.")

    data_path = Path(str(manifest.get("file", "")))
    if not data_path.is_file():
        raise FileNotFoundError(f"Arquivo de dados não encontrado: {data_path}")
    if data_path.stat().st_size != manifest.get("bytes"):
        raise ValueError("O tamanho do arquivo diverge do manifesto.")

    actual_sha256 = sha256_file(data_path)
    if actual_sha256 != manifest.get("sha256"):
        raise ValueError("O SHA-256 do arquivo diverge do manifesto.")

    feature_ids: set[str] = set()
    duplicate_count = 0
    station_ids: set[str] = set()
    variable_counts: Counter[str] = Counter()
    record_count = 0

    with gzip.open(data_path, "rt", encoding="utf-8") as stream:
        for line_number, line in enumerate(stream, start=1):
            row = json.loads(line)
            feature_id = str(row.get("feature_id") or "")
            if not feature_id:
                raise ValueError(f"Registro sem feature_id na linha {line_number}.")
            if feature_id in feature_ids:
                duplicate_count += 1
            feature_ids.add(feature_id)

            station = row.get("station_wigos")
            if station:
                station_ids.add(str(station))
            variable_counts[str(row.get("variable"))] += 1
            record_count += 1

    if duplicate_count:
        raise ValueError(f"Foram encontrados {duplicate_count} registros duplicados.")
    if record_count != manifest.get("records"):
        raise ValueError("A quantidade de registros diverge do manifesto.")
    if len(station_ids) != manifest.get("stations"):
        raise ValueError("A quantidade de estações diverge do manifesto.")
    if set(variable_counts) != EXPECTED_VARIABLES:
        raise ValueError("O conjunto de variáveis está incompleto ou inesperado.")
    if dict(sorted(variable_counts.items())) != manifest.get("variable_counts"):
        raise ValueError("As contagens por variável divergem do manifesto.")

    return {
        "status": "quality_passed",
        "observation_date_utc": target_day.isoformat(),
        "records": record_count,
        "stations": len(station_ids),
        "variables": len(variable_counts),
        "duplicates": duplicate_count,
        "bytes": data_path.stat().st_size,
        "sha256_verified": actual_sha256,
        "retention_days": manifest["retention_days"],
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--date", type=date.fromisoformat)
    return parser


def main() -> int:
    args = build_parser().parse_args()
    sao_paulo = ZoneInfo("America/Sao_Paulo")
    target_day = args.date or (
        datetime.now(sao_paulo).date() - timedelta(days=1)
    )
    result = validate_partition(target_day)
    print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
