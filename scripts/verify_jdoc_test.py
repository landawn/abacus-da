#!/usr/bin/env python3
"""
verify_jdoc_test.py
===================

Maven-free, fully-isolated runner for a single throwaway JUnit 5 test file. Compiles
ONLY the given test (against the already-built target/classes plus the project's test
dependencies) into a private temp dir and runs it via scripts/JdocTestRunner. Because
nothing is written into Maven's shared target/test-classes and no other test files are
compiled, many agents can verify their own throwaway tests fully in parallel without
clobbering each other.

Prerequisites (the orchestrator sets these up once, while no agents are running):
  * target/classes is up to date  (mvn -o compile)
  * scripts/.testcp.txt exists    (mvn -o dependency:build-classpath -Dmdep.outputFile=scripts/.testcp.txt)
  * scripts/runner-classes/JdocTestRunner.class exists
      (javac -cp "@scripts/.testcp.txt-as-cp" -d scripts/runner-classes scripts/JdocTestRunner.java)
  verify_jdoc_test.py will auto-build the last two if missing.

Usage:
  python scripts/verify_jdoc_test.py path/to/SomeThrowawayTest.java

Exit code 0 = all tests passed; non-zero = compile error or test failure (details printed).
"""
from __future__ import annotations

import os
import re
import subprocess
import sys
import tempfile
import shutil

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _find_module_target() -> str:
    """Locate the Maven build dir holding the compiled classes.

    Works for both single-module repos (``ROOT/target/classes``) and
    aggregator-pom multi-module repos where all sources live in one child
    module (e.g. ``ROOT/abacus-da-all/target/classes``). An explicit
    ``JDOC_MODULE`` env var (a child directory name) overrides detection.
    Returns the ``.../target`` directory path.
    """
    override = os.environ.get("JDOC_MODULE")
    if override:
        return os.path.join(ROOT, override, "target")
    root_classes = os.path.join(ROOT, "target", "classes")
    if os.path.isdir(root_classes):
        return os.path.join(ROOT, "target")
    best, best_count = None, -1
    for name in sorted(os.listdir(ROOT)):
        cand = os.path.join(ROOT, name, "target", "classes")
        if os.path.isdir(cand):
            count = sum(len(files) for _, _, files in os.walk(cand))
            if count > best_count:
                best, best_count = os.path.join(ROOT, name, "target"), count
    return best or os.path.join(ROOT, "target")


MODULE_TARGET = _find_module_target()
MODULE_DIR = os.path.dirname(MODULE_TARGET)
# Maven reactor module name for ``-pl`` (None when the classes are at the repo root).
MODULE_NAME = os.path.basename(MODULE_DIR) if os.path.abspath(MODULE_DIR) != os.path.abspath(ROOT) else None
CP_FILE = os.path.join(ROOT, "scripts", ".testcp.txt")
RUNNER_SRC = os.path.join(ROOT, "scripts", "JdocTestRunner.java")
RUNNER_OUT = os.path.join(ROOT, "scripts", "runner-classes")
TARGET_CLASSES = os.path.join(MODULE_TARGET, "classes")
TARGET_TEST_CLASSES = os.path.join(MODULE_TARGET, "test-classes")
SEP = ";" if os.name == "nt" else ":"


def deps_classpath() -> str:
    if not os.path.exists(CP_FILE):
        print("scripts/.testcp.txt missing; generating (one-time)...", file=sys.stderr)
        cmd = ["mvn", "-o", "dependency:build-classpath", f"-Dmdep.outputFile={CP_FILE}", "-q"]
        if MODULE_NAME:  # restrict to the module that owns the sources in a reactor build
            cmd[1:1] = ["-pl", MODULE_NAME]
        subprocess.run(cmd, cwd=ROOT, check=True, shell=(os.name == "nt"))
    with open(CP_FILE, encoding="utf-8") as fh:
        return fh.read().strip()


def ensure_runner(deps: str) -> None:
    cls = os.path.join(RUNNER_OUT, "JdocTestRunner.class")
    if os.path.exists(cls) and os.path.getmtime(cls) >= os.path.getmtime(RUNNER_SRC):
        return
    os.makedirs(RUNNER_OUT, exist_ok=True)
    print("compiling scripts/JdocTestRunner.java (one-time)...", file=sys.stderr)
    subprocess.run(["javac", "-cp", deps, "-d", RUNNER_OUT, RUNNER_SRC], check=True)


def parse_fqcn(test_path: str) -> str:
    with open(test_path, encoding="utf-8") as fh:
        src = fh.read()
    pkg_m = re.search(r"^\s*package\s+([\w.]+)\s*;", src, re.M)
    cls_m = re.search(r"\b(?:public\s+)?(?:final\s+)?class\s+(\w+)", src)
    if not cls_m:
        raise SystemExit(f"could not find a class declaration in {test_path}")
    cls = cls_m.group(1)
    return f"{pkg_m.group(1)}.{cls}" if pkg_m else cls


def main(argv=None):
    argv = argv if argv is not None else sys.argv[1:]
    if len(argv) != 1:
        print(__doc__)
        return 2
    test_path = os.path.abspath(argv[0])
    if not os.path.isfile(test_path):
        raise SystemExit(f"not a file: {test_path}")

    deps = deps_classpath()
    ensure_runner(deps)
    fqcn = parse_fqcn(test_path)

    base_cp = TARGET_CLASSES + SEP + deps
    if os.path.isdir(TARGET_TEST_CLASSES):
        base_cp = TARGET_TEST_CLASSES + SEP + base_cp

    tmp = tempfile.mkdtemp(prefix="jdocverify_")
    try:
        # 1) compile ONLY this test, in isolation
        cc = subprocess.run(
            ["javac", "-encoding", "UTF-8", "-cp", base_cp, "-d", tmp, test_path],
            capture_output=True, text=True)
        if cc.returncode != 0:
            print("COMPILE FAILED:")
            print(cc.stderr or cc.stdout)
            return 1
        # 2) run it
        run_cp = RUNNER_OUT + SEP + tmp + SEP + base_cp
        rr = subprocess.run(["java", "-cp", run_cp, "JdocTestRunner", fqcn],
                            capture_output=True, text=True)
        sys.stdout.write(rr.stdout)
        if rr.stderr.strip():
            sys.stderr.write(rr.stderr)
        return rr.returncode
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


if __name__ == "__main__":
    sys.exit(main())
