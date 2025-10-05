import re
import sys
from pathlib import Path

if len(sys.argv) < 2:
    print("Usage: condense_timelines.py <debug_trace.log> [key_high ...]")
    sys.exit(2)

log_path = Path(sys.argv[1])
if not log_path.exists():
    print(f"Log file not found: {log_path}")
    sys.exit(1)

# Default keys to extract if none provided
default_keys = ["7081773136864629903", "16642596542309617938"]
keys = sys.argv[2:] or default_keys

# Regexes
wkr_re = re.compile(r"WKR_(START|ENQUEUE|ENQUEUED|FINISH): (.*)")
dispatch_re = re.compile(r"DISPATCH_BATCH: (.*)")

lines = log_path.read_text(encoding="utf-8", errors="replace").splitlines()

per_key = {k: [] for k in keys}
other = []

for i, line in enumerate(lines, start=1):
    s = line.strip()
    if not s:
        continue
    m = wkr_re.search(s)
    if m:
        evt = m.group(1)
        body = m.group(2)
        # Try to extract wal_time tuple if present
        wal_match = re.search(r"wal_time=\(?([0-9, ]+)\)?", s)
        wal = wal_match.group(1).strip() if wal_match else None
        # shorten body
        short = body
        if len(short) > 150:
            short = short[:150] + "..."
        tagged = False
        for k in keys:
            if k in s:
                per_key[k].append((i, evt, short, wal))
                tagged = True
        if not tagged:
            other.append((i, "WKR", evt, short, wal))
        continue
    md = dispatch_re.search(s)
    if md:
        body = md.group(1)
        # Extract tags array
        tags = re.findall(r"\(\"([^\"]*)\", ([^\)]+)\)", body)
        # tags is list of (tagstr, walstr)
        tagged = False
        for k in keys:
            if k in s:
                per_key[k].append((i, "DISPATCH", body[:200]))
                tagged = True
        if not tagged:
            other.append((i, "DISPATCH", body[:200]))
        continue
    # catch lines that contain key numbers casually
    for k in keys:
        if k in s:
            per_key[k].append((i, "LINE", s[:200]))
            break

# Now print condensed timelines
for k in keys:
    events = per_key.get(k, [])
    print(f"Key {k}: {len(events)} events")
    if not events:
        print("  (no events)")
        continue
    # Only keep first 20 relevant events, but try to show sequence with apply/dispatch/ensure/find/get
    filtered = []
    for ev in events:
        if ev[1] in ("DISPATCH", "START", "ENQUEUE", "ENQUEUED", "FINISH", "LINE"):
            filtered.append(ev)
    # take a window around the first apply for clarity
    idx_apply = None
    for idx, ev in enumerate(filtered):
        if isinstance(ev[2], str) and "_apply_insert_sync" in (ev[2] or ""):
            idx_apply = idx
            break
    if idx_apply is not None:
        start = max(0, idx_apply - 6)
        end = min(len(filtered), idx_apply + 14)
        window = filtered[start:end]
    else:
        window = filtered[:20]
    for ln, typ, *rest in window:
        if typ == "DISPATCH":
            print(f"  {ln}: DISPATCH {rest[0]}")
        elif typ == "LINE":
            print(f"  {ln}: {rest[0]}")
        else:
            evt = typ
            info = rest[0]
            wal = rest[1] if len(rest) > 1 else None
            if wal:
                print(f"  {ln}: WKR_{evt} {info} wal_time=({wal})")
            else:
                print(f"  {ln}: WKR_{evt} {info}")
    print("\n")

print("Done.")
