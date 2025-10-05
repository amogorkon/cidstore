import tomllib
from pathlib import Path

p = Path(__file__).resolve().parents[1] / "pyproject.toml"
print("Validating:", p)
with p.open("rb") as f:
    data = tomllib.load(f)

deps = data.get("project", {}).get("dependencies")
opt = data.get("project", {}).get("optional-dependencies")

print("\nproject.dependencies:")
print(deps)
print("\nproject.optional-dependencies:")
print(opt)

# basic checks
assert deps is not None, "project.dependencies missing"
assert "numpy" in deps[2] or any("numpy" in str(d) for d in deps), "numpy missing"
assert opt is not None and "dev" in opt, "dev optional-dependencies missing"
print("\npyproject.toml looks sane")
