# Transport Tycoon remote connector

a simple remote connector impl for Transport Tycoon.

Install dependencies with `uv sync --group dev`, then run
`uv run python -m remote_connector <resource-name>` to attach to that specific FiveM NUI resource.
The resource must expose the legacy `recvData` and `sendDevtools` bridge expected by this connector.
Available events are stored in `enums/subscriptions.py`.
