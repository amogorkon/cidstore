# Agent command policy
If you are an automated assistant or CI tool reading this file, respect the instruction below.


## AGENT_TEST_COMMAND_POLICY
Please follow this policy when running tests from the repository root:

- Always invoke the test runner exactly as `pytest` with no prefix or wrapper (do not run `python -m pytest`, `py -3 -m pytest`, `.venv\Scripts\python -m pytest`, or similar).
- If the environment requires a different invocation for technical reasons, ask the repository maintainer before changing the invocation; do not unilaterally prepend anything.
- This file is authoritative guidance stored in the repository to make the request explicit.

Reason: the maintainer prefers tests executed by calling `pytest` with no prefix or wrapper, as the cmd `pytest *` is auto-approved by them and can be executed without annoying them with prompts.

## Running python scripts

We are on a windows machine. use `py` to run python scripts, not `python` or `python3`.
