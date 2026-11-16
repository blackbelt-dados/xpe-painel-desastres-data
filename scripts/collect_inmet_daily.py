#!/usr/bin/env python3
"""Coleta observações SYNOP do INMET pela API oficial WIS2/OGC."""

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import shutil
import time
from collections import Counter
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Iterator
from urllib.parse import urlencode
from zoneinfo import ZoneInfo

import requests

ITEMS_URL = (
    "https://wis2bra.inmet.gov.br/oapi/collections/"
    "urn:wmo:md:br-inmet:synop/items"
)
DATA_ROOT = Path("data/inmet")
PAGE_SIZE = 10_000
VARIABLES = (
    "air_temperature",
    "relative_humidity",
    "total_precipitation_or_total_water_equivalent",
    "wind_speed",
    "maximum_wind_gust_speed",
    "pressure_reduced_to_mean_sea_level",
)
HEADERS = {
    "Accept": "application/geo+json, application/json",
    "User-Agent": "xpe-painel-desastres/0.2 (projeto academico)",
}


def get_json(url: str, attempts: int = 4) -> dict[str, object]:
    """Obtém JSON com espera exponencial para erros transitórios."""
    last_error: Exception | None = None
    for attempt in range(1, attempts + 1):
        try:
            response = requests.get(url, headers=HEADERS, timeout=120)
            response.raise_for_status()
            payload = response.json()
            if not isinstance(payload, dict):
                raise TypeError(
                    f"Resposta inesperada: {type(payload).__name__}"
                )
            return payload
        except (requests.RequestException, ValueError, TypeError) as exc:
            last_error = exc
            if attempt < attempts:
                time.sleep(2 ** (attempt - 1))
    raise RuntimeError(
        f"Falha após {attempts} tentativas: {url}"
    ) from last_error


def first_page_url(day: date, variable: str) -> str:
    """Monta a primeira página para uma variável e um dia UTC."""
    next_day = day + timedelta(days=1)
    params = {
        "f": "json",
        "datetime": (
            f"{day.isoformat()}T00:00:00Z/"
            f"{next_day.isoformat()}T00:00:00Z"
        ),
        "name": variable,
        "limit": str(PAGE_SIZE),
    }
    return f"{ITEMS_URL}?{urlencode(params)}"


def next_page_url(payload: dict[str, object]) -> str | None:
    """Extrai do GeoJSON o link da próxima página, quando houver."""
    links = payload.get("links", [])
    if not isinstance(links, list):
        return None
    for link in links:
        if (
            isinstance(link, dict)
            and link.get("rel") == "next"
            and isinstance(link.get("href"), str)
        ):
            return str(link["href"])
    return None


def normalize_feature(
    feature: dict[str, object], collected_at: str
) -> dict[str, object]:
    """Converte uma feature OGC em uma linha analítica compacta."""
    properties = feature.get("properties", {})
    geometry = feature.get("geometry", {})
    if not isinstance(properties, dict):
        properties = {}
    if not isinstance(geometry, dict):
        geometry = {}

    coordinates = geometry.get("coordinates", [])
    if not isinstance(coordinates, list):
        coordinates = []

    return {
        "feature_id": feature.get("id") or properties.get("id"),
        "station_wigos": properties.get("wigos_station_identifier"),
        "report_id": properties.get("reportId"),
        "report_time_utc": properties.get("reportTime"),
        "phenomenon_time_utc": properties.get("phenomenonTime"),
        "variable": properties.get("name"),
        "value": properties.get("value"),
        "unit": properties.get("units"),
        "description": properties.get("description"),
        "longitude": coordinates[0] if len(coordinates) > 0 else None,
        "latitude": coordinates[1] if len(coordinates) > 1 else None,
        "elevation_m": coordinates[2] if len(coordinates) > 2 else None,
        "collected_at_utc": collected_at,
    }


def collect_variable(
    day: date, variable: str, collected_at: str
) -> Iterator[dict[str, object]]:
    """Percorre todas as páginas de uma variável."""
    url: str | None = first_page_url(day, variable)
    seen_urls: set[str] = set()
    page = 0

    while url:
        if url in seen_urls:
            raise RuntimeError(
                f"Ciclo de paginação detectado para {variable}: {url}"
            )
        seen_urls.add(url)
        page += 1

        payload = get_json(url)
        features = payload.get("features", [])
        if not isinstance(features, list):
            raise TypeError(
                f"A API não retornou uma lista de features para {variable}."
            )

        print(
            json.dumps(
                {
                    "variable": variable,
                    "page": page,
                    "number_matched": payload.get("numberMatched"),
                    "number_returned": len(features),
                },
                ensure_ascii=False,
            )
        )

        for feature in features:
            if isinstance(feature, dict):
                yield normalize_feature(feature, collected_at)

        url = next_page_url(payload)


def prune_old_partitions(root: Path, cutoff: date) -> list[str]:
    """Remove partições locais anteriores à janela de retenção."""
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
    parser.add_argument(
        "--variables",
        nargs="+",
        choices=VARIABLES,
        default=list(VARIABLES),
    )
    args = parser.parse_args()

    sao_paulo = ZoneInfo("America/Sao_Paulo")
    target_day = args.date or (
        datetime.now(sao_paulo).date() - timedelta(days=1)
    )
    collected_at = datetime.now(timezone.utc).isoformat()

    records_by_id: dict[str, dict[str, object]] = {}
    for variable in args.variables:
        for row in collect_variable(target_day, variable, collected_at):
            feature_id = str(row.get("feature_id") or "")
            if not feature_id:
                raise RuntimeError(
                    f"Feature sem identificador recebida para {variable}."
                )
            records_by_id[feature_id] = row

    records = sorted(
        records_by_id.values(),
        key=lambda row: (
            str(row.get("station_wigos", "")),
            str(row.get("report_time_utc", "")),
            str(row.get("variable", "")),
            str(row.get("feature_id", "")),
        ),
    )
    if not records:
        raise RuntimeError(
            f"Nenhuma observação WIS2 recebida para {target_day}."
        )

    partition = DATA_ROOT / f"observation_date={target_day.isoformat()}"
    partition.mkdir(parents=True, exist_ok=True)
    data_path = partition / "inmet_synop.jsonl.gz"
    with gzip.open(data_path, "wt", encoding="utf-8") as stream:
        for row in records:
            stream.write(json.dumps(row, ensure_ascii=False, sort_keys=True))
            stream.write("\n")

    checksum = hashlib.sha256(data_path.read_bytes()).hexdigest()
    cutoff = target_day - timedelta(days=89)
    removed = prune_old_partitions(DATA_ROOT, cutoff)
    station_count = len(
        {
            str(row["station_wigos"])
            for row in records
            if row.get("station_wigos")
        }
    )
    variable_counts = Counter(
        str(row.get("variable")) for row in records
    )
    manifest = {
        "status": "success",
        "source": "INMET WIS2 OGC API Features - SYNOP Brasil",
        "items_url": ITEMS_URL,
        "observation_date_utc": target_day.isoformat(),
        "collected_at_utc": collected_at,
        "variables": list(args.variables),
        "variable_counts": dict(sorted(variable_counts.items())),
        "stations": station_count,
        "records": len(records),
        "file": data_path.as_posix(),
        "bytes": data_path.stat().st_size,
        "sha256": checksum,
        "retention_days": 90,
        "retention_cutoff": cutoff.isoformat(),
        "removed_partitions": removed,
    }
    manifest_text = json.dumps(
        manifest, ensure_ascii=False, indent=2, sort_keys=True
    ) + "\n"
    (partition / "manifest.json").write_text(
        manifest_text, encoding="utf-8"
    )
    print(json.dumps(manifest, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
