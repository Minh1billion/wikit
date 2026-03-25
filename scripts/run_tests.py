"""
scripts/run_tests.py

Test runner for the wikit ingestion pipeline.

Usage:
    python scripts/run_tests.py            # unit tests only (default)
    python scripts/run_tests.py --all      # unit + integration (needs docker)
    python scripts/run_tests.py --integration
    python scripts/run_tests.py --verbose
"""

import argparse
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
TEST_FILE = ROOT / "tests" / "test_ingestion.py"

KAFKA_BOOTSTRAP = "localhost:29092"


def _run(cmd: list[str]) -> int:
    print(f"\n$ {' '.join(cmd)}\n{'' * 60}")
    result = subprocess.run(cmd, cwd=ROOT)
    return result.returncode


def _check_kafka() -> bool:
    """Return True if Kafka is reachable on localhost:29092."""
    try:
        from kafka import KafkaProducer
        from kafka.errors import NoBrokersAvailable

        p = KafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP, request_timeout_ms=3000)
        p.close()
        return True
    except Exception:
        return False


def _wait_for_kafka(timeout: int = 30) -> bool:
    print(f"⏳  Waiting for Kafka at {KAFKA_BOOTSTRAP} (up to {timeout}s)…")
    deadline = time.time() + timeout
    while time.time() < deadline:
        if _check_kafka():
            print("✅  Kafka is up.\n")
            return True
        time.sleep(2)
    return False


def main() -> None:
    parser = argparse.ArgumentParser(description="wikit test runner")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--all",         action="store_true", help="Run unit + integration tests")
    group.add_argument("--integration", action="store_true", help="Run integration tests only")
    group.add_argument("--unit",        action="store_true", help="Run unit tests only (default)")
    parser.add_argument("--verbose", "-v", action="store_true", help="Verbose pytest output")
    args = parser.parse_args()

    # Default to unit if nothing specified
    run_unit        = args.unit or args.all or not (args.integration or args.all)
    run_integration = args.integration or args.all

    base_cmd = [sys.executable, "-m", "pytest", str(TEST_FILE)]
    if args.verbose:
        base_cmd.append("-v")

    exit_code = 0

    # Unit tests 
    if run_unit and not args.integration:
        print("=" * 60)
        print("🧪  Running UNIT tests (no external deps required)")
        print("=" * 60)
        code = _run(base_cmd + ["-m", "unit"])
        exit_code = exit_code or code

    # Integration tests 
    if run_integration:
        print("=" * 60)
        print("🔗  Running INTEGRATION tests (requires docker compose up)")
        print("=" * 60)

        if not _wait_for_kafka():
            print("❌  Kafka unreachable — skipping integration tests.")
            print("   Start the stack with:  docker compose up -d\n")
            exit_code = exit_code or 1
        else:
            code = _run(base_cmd + ["-m", "integration"])
            exit_code = exit_code or code

    # All 
    if args.all:
        print("=" * 60)
        print("🧪  Running ALL tests")
        print("=" * 60)
        code = _run(base_cmd)
        exit_code = exit_code or code

    # Summary 
    print("\n" + "=" * 60)
    if exit_code == 0:
        print("✅  All tests passed.")
    else:
        print("❌  Some tests failed. See output above.")
    print("=" * 60)

    sys.exit(exit_code)


if __name__ == "__main__":
    main()