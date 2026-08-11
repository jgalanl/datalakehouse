#!/usr/bin/env python3
"""Envía un payload dataframe_split a un modelo de MLflow servido en local e imprime las predicciones.
Uso: python src/ml/api/client.py [fare|duration|demand] [--json FILE] [--host H] [--port P]"""
import argparse
import json
import os
import sys
import urllib.error
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
EXAMPLES = {
    "fare": "examples/fare_request.json",
    "duration": "examples/duration_request.json",
    "demand": "examples/demand_request.json",
}


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("use_case", nargs="?", default="fare", choices=sorted(EXAMPLES),
                    help="qué payload de ejemplo enviar (por defecto: fare)")
    ap.add_argument("--json", help="payload dataframe_split propio (anula use_case)")
    ap.add_argument("--host", default="localhost")
    ap.add_argument("--port", type=int, default=5001)
    args = ap.parse_args()

    payload_path = args.json or os.path.join(HERE, EXAMPLES[args.use_case])
    with open(payload_path, "rb") as fh:
        payload = fh.read()

    url = f"http://{args.host}:{args.port}/invocations"
    req = urllib.request.Request(url, data=payload,
                                 headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            print(json.dumps(json.load(resp), indent=2))
    except urllib.error.HTTPError as e:
        # MLflow devuelve el motivo del fallo en el cuerpo de la respuesta.
        sys.exit(f"HTTP {e.code} desde {url}: {e.read().decode(errors='replace')}")
    except urllib.error.URLError as e:
        sys.exit(f"No se pudo conectar con {url}: {e.reason} (¿está activo el perfil `api`?)")


if __name__ == "__main__":
    main()
