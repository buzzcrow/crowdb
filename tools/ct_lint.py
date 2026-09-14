#!/usr/bin/env python3
import os
import platform
import re
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

# Each entry: (source dirs, build dir for compile_commands.json).
# The build dir must have CMAKE_EXPORT_COMPILE_COMMANDS=ON so
# clang-tidy can find the compile flags.
SOURCE_TREES = [
    (
        [
            Path("lib/crowdb-tree/src"),
            Path("lib/crowdb-tree/include"),
            Path("lib/crowdb-tree/tests"),
            Path("lib/crowdb-tree/bench"),
            Path("lib/crowdb-common/cpp"),
        ],
        "lib/crowdb-tree/build",
    ),
    (
        [
            Path("lib/crowdb-rpc/src"),
            Path("lib/crowdb-rpc/include"),
            Path("lib/crowdb-rpc/tests"),
        ],
        "lib/crowdb-rpc/build",
    ),
]
EXTENSIONS = {".cpp", ".h"}
DEFAULT_BATCH_SIZE = 3
DEFAULT_JOBS = 10
EXCLUDE_HEADER_FILTER = r"(^|.*/)(\.pixi|usr/include|usr/local/include|third-party)/.*"
TEST_PATH_MARKER = "/tests/"
DIAGNOSTIC_RE = re.compile(r"^.*:\d+:\d+: (?:fatal error|error|warning|remark):")

# Files that are Linux-only (guarded by CROWDB_TREE_HAVE_LIBURING). clang-tidy
# cannot process them on macOS (reactor.h has a #error when liburing is
# absent), so they are skipped when liburing is not found by CMake.
LIBURING_GATED_FILES = {
    "lib/crowdb-tree/include/crowdb-tree/reactor.h",
    "lib/crowdb-tree/src/reactor.cpp",
    "lib/crowdb-tree/src/backend/local/block_async_page_store.cpp",
    "lib/crowdb-tree/tests/unit/reactor_test.cpp",
}

# Files that are Linux-only (guarded by CROWDB_RPC_HAVE_RDMA). clang-tidy
# cannot process them on macOS (ibverbs headers absent), so they are
# skipped when RDMA is not found by CMake.
RDMA_GATED_FILES = {
    "lib/crowdb-rpc/include/crowdb-rpc/rdma_transport.h",
    "lib/crowdb-rpc/src/rdma_buffer_pool.cpp",
    "lib/crowdb-rpc/src/rdma_transport.cpp",
}

# Platform-only transport engines (lib/crowdb-rpc/CMakeLists.txt removes them
# from the build on the wrong platform). clang-tidy cannot process them when
# the kernel headers are absent, so skip them off their native platform:
# kqueue is macOS/BSD-only; epoll is Linux-only.
KQUEUE_GATED_FILES = {
    "lib/crowdb-rpc/src/transport/kqueue/kqueue_engine.cpp",
}
EPOLL_GATED_FILES = {
    "lib/crowdb-rpc/src/transport/epoll/epoll_engine.cpp",
}


def liburing_available() -> bool:
    """Check the CMake cache for liburing (set by lib/crowdb-tree/CMakeLists.txt)."""
    cache = Path("lib/crowdb-tree/build/CMakeCache.txt")
    if not cache.exists():
        return True  # no build dir — don't skip (let clang-tidy report the real error)
    text = cache.read_text()
    return "LIBURING_INCLUDE_DIR:PATH=LIBURING_INCLUDE_DIR-NOTFOUND" not in text


def rdma_available() -> bool:
    """Check the CMake cache for RDMA (set by lib/crowdb-rpc/CMakeLists.txt)."""
    cache = Path("lib/crowdb-rpc/build/CMakeCache.txt")
    if not cache.exists():
        return True  # no build dir — don't skip
    text = cache.read_text()
    return "CROWDB_RPC_HAVE_RDMA:INTERNAL=TRUE" in text


def collect_files(selected: set[str] | None = None) -> list[tuple[str, str]]:
    """Return list of (filepath, build_dir) pairs."""
    skip_liburing = not liburing_available()
    skip_rdma = not rdma_available()
    # kqueue is macOS/BSD-only; epoll is Linux-only. Mirror the CMake
    # CMAKE_SYSTEM_NAME gating in lib/crowdb-rpc/CMakeLists.txt.
    skip_kqueue = platform.system() != "Darwin"
    skip_epoll = platform.system() != "Linux"
    files: list[tuple[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for dirs, build_dir in SOURCE_TREES:
        for root in dirs:
            if not root.exists():
                continue
            for path in root.rglob("*"):
                if path.is_file() and path.suffix in EXTENSIONS:
                    posix = path.as_posix()
                    if selected is not None and posix not in selected:
                        continue
                    if skip_liburing and posix in LIBURING_GATED_FILES:
                        continue
                    if skip_rdma and posix in RDMA_GATED_FILES:
                        continue
                    if skip_kqueue and posix in KQUEUE_GATED_FILES:
                        continue
                    if skip_epoll and posix in EPOLL_GATED_FILES:
                        continue
                    item = (posix, build_dir)
                    if item not in seen:
                        seen.add(item)
                        files.append(item)
    files.sort()
    return files


def run_batch(batch: list[tuple[str, str]]) -> subprocess.CompletedProcess[str]:
    # All files in a batch share the same build dir (grouped by caller).
    build_dir = batch[0][1]
    filepaths = [f for f, _ in batch]
    # Restrict header diagnostics to the files being linted in this batch.
    # Shared project headers are otherwise reported once for every source
    # file that includes them, while each header is still linted directly in
    # its own batch.
    batch_header_filter = r"^(?:" + "|".join(re.escape(str(Path(f).resolve())) for f in filepaths) + r")$"
    extra_args = ["--extra-arg=-Wno-unknown-warning-option"]
    if any(TEST_PATH_MARKER in f"/{filepath}/" for filepath in filepaths):
        # GoogleTest assertions expand into several nested macros. Keep the
        # test source diagnostic, but avoid dumping the whole third-party
        # macro backtrace for every analyzer warning.
        extra_args.append("--extra-arg=-fmacro-backtrace-limit=1")
    cache = Path(build_dir) / "CMakeCache.txt"
    if cache.exists():
        for line in cache.read_text().splitlines():
            if line.startswith("LIBURING_INCLUDE_DIR:PATH="):
                include_dir = line.partition("=")[2]
                if include_dir and not include_dir.endswith("-NOTFOUND"):
                    extra_args.append(f"--extra-arg=-isystem{include_dir}")
                break
    # compile_commands.json is generated by the conda gcc build, which can
    # carry GCC-only -Wno-<flag> options (e.g. -Wno-stringop-overflow) that
    # clang does not recognize. Under the build's -Werror, clang-tidy would
    # promote the "unknown warning option" diagnostic to a hard error.
    # -Wno-unknown-warning-option silences it so clang-tidy can proceed.
    return subprocess.run(
        [
            "clang-tidy",
            "-p",
            build_dir,
            "--quiet",
            f"--header-filter={batch_header_filter}",
            f"--exclude-header-filter={EXCLUDE_HEADER_FILTER}",
            *extra_args,
            *filepaths,
        ],
        text=True,
        capture_output=True,
    )


def format_output(output: str, compact_tests: bool) -> str:
    output = re.sub(
        r"^\[\d+/\d+\](?: \(\d+/\d+\))? Processing file .*?\r?\n",
        "",
        output,
        flags=re.MULTILINE,
    )
    if not compact_tests:
        return output

    # Analyzer notes from GoogleTest assertions are mostly macro expansion
    # backtraces. Keep the warning's source and caret, but omit only notes
    # attached to warnings; errors retain their complete diagnostic context.
    compacted: list[str] = []
    warning_tail = ""
    for line in output.splitlines(keepends=True):
        if DIAGNOSTIC_RE.match(line):
            warning_tail = "warning" if " warning:" in line else "other"
            compacted.append(line)
        elif warning_tail == "warning" and (" note:" in line or line.lstrip().startswith("note:")):
            warning_tail = "notes"
        elif warning_tail != "notes":
            compacted.append(line)
    return "".join(compacted)


def main() -> int:
    batch_size = max(1, int(os.environ.get("CT_LINT_BATCH_SIZE", str(DEFAULT_BATCH_SIZE))))
    jobs = max(1, int(os.environ.get("CT_LINT_JOBS", str(DEFAULT_JOBS))))
    selected = {str(Path(path)) for path in sys.argv[1:]} or None
    files = collect_files(selected)
    if selected is not None:
        found = {path for path, _ in files}
        missing = sorted(selected - found)
        if missing:
            sys.stderr.write(f"tree-lint: unsupported or missing files: {', '.join(missing)}\n")
            return 2
    if not files:
        return 0

    # Group by build dir so each batch uses the correct compile_commands.json.
    by_build: dict[str, list[tuple[str, str]]] = {}
    for f in files:
        by_build.setdefault(f[1], []).append(f)

    batches: list[list[tuple[str, str]]] = []
    for file_list in by_build.values():
        for i in range(0, len(file_list), batch_size):
            batches.append(file_list[i : i + batch_size])

    exit_code = 0

    with ThreadPoolExecutor(max_workers=jobs) as executor:
        futures = [executor.submit(run_batch, batch) for batch in batches]
        future_batches = dict(zip(futures, batches))
        for future in as_completed(futures):
            result = future.result()
            compact_tests = any(TEST_PATH_MARKER in f"/{filepath}/" for filepath, _ in future_batches[future])
            if result.stdout:
                sys.stdout.write(format_output(result.stdout, compact_tests))
            if result.stderr:
                sys.stderr.write(format_output(result.stderr, compact_tests))
            if result.returncode != 0 and exit_code == 0:
                exit_code = result.returncode

    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
