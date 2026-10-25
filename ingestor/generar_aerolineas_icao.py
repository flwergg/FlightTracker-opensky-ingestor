"""Genera aerolineas_icao.txt, la lista del filtro de aviacion general (T-160).

Toma todos los ICAO de 3 letras de OpenFlights airlines.dat, en un commit
fijo, sin mirar la marca de activa: en la medicion V1 (T-157) PSA Airlines
(JIA), que vuela para American, figuraba inactiva. Agrega las aerolineas de
EE. UU. que OpenFlights no tiene con su ICAO actual.

    python generar_aerolineas_icao.py
"""

from __future__ import annotations

import csv
import io
import re
from pathlib import Path

import requests

OPENFLIGHTS_COMMIT = "5d623a6969a1adee7961cf1c9a8a212c4a784713"
AIRLINES_URL = f"https://raw.githubusercontent.com/jpatokal/openflights/{OPENFLIGHTS_COMMIT}/data/airlines.dat"
OUTPUT = Path(__file__).with_name("aerolineas_icao.txt")

# ICAO actual -> aerolinea (IATA). OpenFlights las omite o les asigna el ICAO de
# una aerolinea que ya no existe (YX figura como MEP, MQ como EGF, OH como COM).
CORRECTIONS = {
    "EDV": "Endeavor Air (9E)",
    "ENY": "Envoy Air (MQ)",
    "JIA": "PSA Airlines (OH)",
    "MXY": "Breeze Airways (MX)",
    "RPA": "Republic Airways (YX)",
    "VXP": "Avelo Airlines (XP)",
}


def icao_codes(airlines_dat: str) -> set[str]:
    """ICAO de 3 letras de airlines.dat (columna 5), activas o no."""
    return {row[4] for row in csv.reader(io.StringIO(airlines_dat)) if len(row) >= 8 and re.fullmatch(r"[A-Z]{3}", row[4] or "")}


def main() -> None:
    response = requests.get(AIRLINES_URL, timeout=30)
    response.raise_for_status()
    codes = icao_codes(response.text) | set(CORRECTIONS)
    header = [
        "# ICAO de aerolineas para el filtro de aviacion general del ingestor (T-160).",
        f"# Todos los ICAO de OpenFlights airlines.dat (commit {OPENFLIGHTS_COMMIT}), sin mirar la marca",
        "# de activa, que no es confiable (PSA figura inactiva), mas estas correcciones de",
        "# aerolineas de EE. UU. que OpenFlights no tiene con su ICAO actual:",
        *[f"#   {code} {name}" for code, name in sorted(CORRECTIONS.items())],
        "# Se regenera con: python generar_aerolineas_icao.py (ver README).",
    ]
    OUTPUT.write_text("\n".join(header + sorted(codes)) + "\n", encoding="utf-8")
    print(f"{len(codes)} codigos en {OUTPUT.name}")


if __name__ == "__main__":
    main()
