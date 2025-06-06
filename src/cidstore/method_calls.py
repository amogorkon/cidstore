import ast
import os
from collections import Counter

# Directory to scan
SRC_DIR = os.path.dirname(__file__)

# Find all .py files in this directory
py_files = [
    f
    for f in os.listdir(SRC_DIR)
    if f.endswith(".py") and f != os.path.basename(__file__)
]

method_counter = Counter()

for fname in py_files:
    with open(os.path.join(SRC_DIR, fname), "r", encoding="utf-8") as f:
        try:
            tree = ast.parse(f.read(), filename=fname)
        except Exception:
            continue
        func_names = {
            node.name for node in ast.walk(tree) if isinstance(node, ast.FunctionDef)
        }
        # Count all function/method calls
        for node in ast.walk(tree):
            if isinstance(node, ast.Call):
                if isinstance(node.func, ast.Attribute):
                    method_counter[node.func.attr] += 1
                elif isinstance(node.func, ast.Name):
                    method_counter[node.func.id] += 1


print("Method/function call counts in src/cidstore/*.py:")
for name, count in method_counter.most_common():
    print(f"{name}: {count}")
