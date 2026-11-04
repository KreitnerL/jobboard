# Job Board

Sidebar view of the jobs tracked by [`jobrun`](https://github.com/KreitnerL/jobboard): running and finished
jobs and groups with progress, rate, ETA and elapsed time; open or follow logs; stop or remove jobs.

Requires the `jobrun` CLI (`pipx install git+https://github.com/KreitnerL/jobboard`). Settings: `jobboard.command`
(path to `jobrun`), `jobboard.registry` (`JOBRUN_HOME`), `jobboard.refreshSeconds`, `jobboard.finishedDays`,
`jobboard.notifyOnFinish`, `jobboard.updateWhileUnfocused` (redraw the list while VS Code is not focused, e.g. on a second monitor; off by default).
