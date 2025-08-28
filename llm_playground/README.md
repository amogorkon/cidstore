LLM Playground for CIDStore

This folder contains a small demo harness that starts the CIDStore service, performs a set of primary operations (INSERT, LOOKUP, DELETE) over ZMQ and queries the control HTTP API.

Files:
- `run_demo.py` - main demo script. Run from repository root.

Prerequisites:
- Install the project's Python dependencies (see `requirements.txt`). Using a virtual environment is recommended.

Quick run (PowerShell):

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
python llm_playground/run_demo.py
```

Notes:
- The demo uses the same `main.py` entrypoint — it will create/operate on `cidstore.h5` in the repository root.
- If ports `8000`, `5557`, `5558`, `5559` are in use, stop processes using them first.
