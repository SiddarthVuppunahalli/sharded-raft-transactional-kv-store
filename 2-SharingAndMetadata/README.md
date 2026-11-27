# Sharing & Metadata - Pranav

Python 3.10+, no packages to install. Run everything from this folder.

Backend (terminal 1): `py -m backend.cmd.metadata`

Frontend (terminal 2): `py frontend/dev_server.py`

Open http://localhost:3000/. The backend is http://localhost:8080.
The frontend shows the range map and lets you look up a key.

Metadata uses its own Raft group and SQLite database in `data/metadata/`.
The default is one replica for local development. See [URLs.txt](URLs.txt) for three replicas.

[URLs.txt](URLs.txt) lists the seven supported API calls. Keys, boundaries and values are plain UTF-8 text.

Range stats, registration commands, and deployment integration notes are in [URLs.txt](URLs.txt).

Checks: `py -B -m unittest discover -s tests -v`
