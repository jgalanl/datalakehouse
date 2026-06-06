#!/usr/bin/env python3
"""Send a prediction request to a locally served MLflow model.

The model is served by the `api` compose profile (`mlflow models serve`, see
https://mlflow.org/docs/latest/ml/deployment/deploy-model-locally/). This POSTs a
`dataframe_split` JSON payload to the /invocations endpoint and prints the
predictions. Standard library only — runs on the host.

  python src/ml/api/client.py                                  # fare example, port 5001
  python src/ml/api/client.py demand --port 5003
  python src/ml/api/client.py --json path/to/payload.json
"""
import argparse
import json
import os
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
                    help="which example payload to send (default: fare)")
    ap.add_argument("--json", help="custom dataframe_split payload (overrides use_case)")
    ap.add_argument("--host", default="localhost")
    ap.add_argument("--port", type=int, default=5001)
    args = ap.parse_args()

    payload_path = args.json or os.path.join(HERE, EXAMPLES[args.use_case])
    with open(payload_path, "rb") as fh:
        payload = fh.read()

    url = f"http://{args.host}:{args.port}/invocations"
    req = urllib.request.Request(url, data=payload,
                                 headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=60) as resp:
        print(json.load(resp))


if __name__ == "__main__":
    main()
