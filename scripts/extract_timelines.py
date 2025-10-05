import re
import sys
from pathlib import Path

if len(sys.argv) < 2:
    print("Usage: extract_timelines.py <debug_trace.log> [key_high ...]")
    sys.exit(2)

log_path = Path(sys.argv[1])
if not log_path.exists():
    print(f"Log file not found: {log_path}")
    sys.exit(1)

# Default keys to extract if none provided
default_keys = ["7081773136864629903", "16642596542309617938"]
keys = sys.argv[2:] or default_keys

# Precompile regexes
key_regexes = {k: re.compile(re.escape(k)) for k in keys}
dispatch_re = re.compile(r"DISPATCH_BATCH")
apply_re = re.compile(r"_apply_insert_sync")
ensure_re = re.compile(r"_ensure_mem_index_sync")
find_re = re.compile(r"find_entry_in_bucket_sync")
getvals_re = re.compile(r"get_values_sync")

lines = log_path.read_text(encoding="utf-8", errors="replace").splitlines()

# Build per-key lists
per_key_events = {k: [] for k in keys}
other_events = []

for i, line in enumerate(lines, start=1):
    line_stripped = line.strip()
    # Check dispatch/apply/ensure/find/get lines first
    if any(
        r.search(line_stripped)
        for r in (dispatch_re, apply_re, ensure_re, find_re, getvals_re)
    ):
        tagged = False
        for k, kre in key_regexes.items():
            if kre.search(line_stripped):
                per_key_events[k].append((i, line_stripped))
                tagged = True
        if not tagged:
            # Some dispatch/apply lines include wal_time tuples but not numeric key; still include in other
            other_events.append((i, line_stripped))
    else:
        # Also capture lines that explicitly contain the key numbers anywhere
        for k, kre in key_regexes.items():
            if kre.search(line_stripped):
                per_key_events[k].append((i, line_stripped))
                break

# Print simple report
print(f"Parsed {len(lines)} lines from {log_path}\n")
for k in keys:
    events = per_key_events.get(k, [])
    print(f"Key {k}: {len(events)} matching lines")
    if not events:
        print("  (no events found)")
        continue
    print("  Timeline (line_number: text snippet)")
    for ln, txt in events:
        # shorten long lines
        out = txt if len(txt) < 300 else (txt[:300] + "...")
        print(f"   {ln}: {out}")
    print()

print("Other relevant dispatch/apply/ensure/find/get events (not matched to keys):")
for ln, txt in other_events[:200]:
    out = txt if len(txt) < 300 else (txt[:300] + "...")
    print(f" {ln}: {out}")

print("\nDone.")
