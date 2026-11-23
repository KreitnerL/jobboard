# Job Board

Sidebar view of the jobs tracked by [`jobrun`](https://github.com/KreitnerL/jobboard): running and finished
jobs and groups with progress, rate, ETA and elapsed time; open or follow logs; stop or remove jobs.

Brings its own copy of the `jobrun` CLI, so it works right after installing: `jobrun` is available in VS Code's
terminals, and GitHub Copilot gets the jobrun skill. To use `jobrun` in the terminal
and let your coding agents register their jobs, click **Set Up Terminal and Agents** in the empty Jobs view (or run
**Job Board: Set Up jobrun for Terminal and Agents**). **Run Demo Jobs** starts a few short example jobs. Settings (search for "Job Board"): which events notify you (`jobboard.notifyOn`: finished, failed
or stalled jobs and problems a job reports by default), how long finished jobs stay in the list, ETA format, row details, grouping by status or machine,
the status bar count, a low-disk warning, and the settings jobrun itself uses: stall detection, log cap and
auto-clean. See [Settings](https://github.com/KreitnerL/jobboard#settings).
