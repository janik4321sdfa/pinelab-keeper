# pinelab-keeper

Exact-time, PC-independent trigger for a private GitHub Actions workflow (R25, 2026-10-05).

GitHub drops or delays scheduled runs (`on: schedule`) under load; `workflow_dispatch` starts promptly. One keeper run
waits inside an Actions job and dispatches the target workflows at fixed New York times (`keeper.py` → `plan()`):

| action | New York time | target |
|---|---|---|
| tsy-1 / tsy-2 (days 21-31, 1-7) | 10:15 / 14:15 | `tsy_monthend.yml` |
| gold-1 / gold-2 | 15:08 / 15:26 | `signal.yml` (`force=0`) |
| gold-deadman | 16:25 | `signal.yml` (`force=0`) |

The keeper computes nothing and posts nothing except a failure notice when a dispatch fails. All targets are idempotent.
Crons only need to start one keeper per weekday; extra runs exit within a minute; a run hands over to a successor
before the 6 h job limit. Secrets: `TARGET_TOKEN` (actions:write on the target repo), `DISCORD_WEBHOOK` (optional).
Manual check: run the workflow with `test=1`.
