# Job Board

Sidebar view of the jobs tracked by [`jobrun`](https://github.com/KreitnerL/jobboard): running and finished
jobs and groups with progress, rate, ETA and elapsed time; open or follow logs; stop or remove jobs.

Brings its own copy of the `jobrun` CLI, so it works right after installing: `jobrun` is available in VS Code's
terminals, and GitHub Copilot gets the jobrun skill. To use `jobrun` in the terminal
and let your coding agents register their jobs, click **Set Up Terminal and Agents** in the empty Jobs view (or run
**Job Board: Set Up jobrun for Terminal and Agents**). **Run Demo Jobs** starts a few short example jobs. Settings: `jobboard.command`
(path to `jobrun`), `jobboard.registry` (`JOBRUN_HOME`), `jobboard.refreshSeconds`, `jobboard.finishedDays`,
`jobboard.notifyOnFinish`, `jobboard.updateWhileUnfocused` (redraw the list while VS Code is not focused, e.g. on a second monitor; off by default).
