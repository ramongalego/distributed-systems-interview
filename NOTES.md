# Notes

Time spent: 2 hours

AI assistance: I used Claude Code to reason through the problems, but I did most of the work myself, and then used Claude Code to try to break it.

## How to check it

Beyond `./scripts/acceptance.sh`:

```bash
uv run pytest                                   # the driver and the record's rules, no stack needed
uv run python scripts/load.py --for 60 --every 3 --cancel 4 &   # keeps runs going
./scripts/chaos.sh random --seed 7 --for 60
uv run python scripts/truth.py --settled        # the record against the instruments' ledgers
```

`load.py` keeps runs going while `chaos.sh` breaks things. `truth.py` then compares
the record with what the instruments actually did and with the files in storage.

## 1. Topology

```
        Postgres                                      ledger, one file per instrument
           | what should run, and what we were told       | what physically happened
      +----+-----+   "tell me about job J"            +---+----+
      | executor | ---- lab.jobs.<device_id> -------> | driver | --> instrument
      |          | <--- "J is ..." (the reply) ------ |        | --> object storage
      +----------+                                    +--------+     <job_id>.csv
```

- Core NATS request/reply, one subject per device, like `lab.jobs.incubator-1`.
  **No JetStream streams or consumers, so NATS stores nothing.**
- One message each way (`services/common/protocol.py`):
  - request: `{job_id, run_id, step_name, start_by, wait}`
  - reply: `{state, started_at, finished_at, error, retryable, data_ref, data_sha256}`,
    where `state` is `unknown`, `running`, `completed` or `failed`.
- When a driver gets a request:
  - if the job is in its ledger, it answers from the ledger
  - if the job is still running, it waits up to `wait` seconds for it to finish
    before answering
  - if it has never seen the job and `start_by` hasn't passed, it starts it and
    answers `running` with the ledger's start time
  - otherwise it answers `unknown`, which means the job never ran.
- **The reply is the only acknowledgement.** If no reply comes, the executor asks
  again, and keeps asking until the job is finished. `job_id` is
  `run/step/attempt` and is never reused, so asking twice is always safe.
- Where state lives:
  - Postgres, used only by the executor: `runs`, `steps`, and `jobs` with one
    row per attempt. A job row is committed before anything is sent.
  - The instrument's ledger: what actually happened. The driver answers from it.
  - Object storage: each measurement, at `<job_id>.csv`. The driver stores it
    before it reports the job completed.
  - Memory only: the executor's tasks (rebuilt from Postgres on start), the
    driver's list of jobs in flight (rebuilt from the ledger), and a measurement
    between the end of a read and its upload. That last one is the only thing a
    restart can lose (see section 5).

One step, end to end:

```
executor                                    driver                   instrument
INSERT job (pending), commit
-- {J, start_by: now+1.5s, wait: 0} ------> not in the ledger, in time
                                            -- run(J) -------------> ledger: J running
<-- {running, started_at} ----------------
UPDATE job and step: running
-- {J, start_by: null, wait: 5} ----------> holds the reply
                                            <-- outcome ----------- ledger: J completed
                                            PUT J.csv, retried until it is stored
<-- {completed, times, data_ref, sha256} --
UPDATE job and step: completed
```

The executor (`service.py`) runs two kinds of task. One loop reads the database
and acts on it: it finishes runs that have nothing left to do, and opens a job
for each step that's ready to start. Then there's one task per open job, which
keeps asking that job's driver until the job is finished. Neither keeps any state
of its own. Everything they need is in the database.

## 2. Why this topology

**Request/reply, not JetStream.** Both ends already store what matters: Postgres
has what should run, and the ledger has what did run. So a message doesn't need
to survive. It just needs to be safe to send again. A stream would be a third
copy of the state that could disagree with the other two. A redelivery timer
could fire in the middle of a 30 second hang, a dedup window could expire, or a
queued command could still be waiting for a run that has since been cancelled.
With one repeatable question, every fault is handled the same way: ask again.
I'd use JetStream if drivers had no ledger to answer from, or if something other
than the executor needed to hear about results.

**The executor asks, the driver doesn't announce.** The executor knows what
should happen, so it's the one that retries. That makes a driver restart, an
executor restart and a lost reply the same case. If drivers published events
instead, they'd need to remember what they had already published, and they have
nowhere safe to keep that.

**Start requests have a deadline (`start_by`) that's shorter than the time the
executor waits for a reply.** A request held up by a network partition must not
fill a plate for a run that was cancelled in the meantime. Once the executor
stops waiting for a reply, that request can't start anything any more, so a later
`unknown` really does mean "never ran". The alternative was to rely on message
order, which a reconnect doesn't guarantee. The cost is an assumption that the
two clocks agree within half a second.

**A step is only `running` once the instrument confirms it.** A step sent to a
driver that's cut off stays `pending`. All times come from the ledger, not the
executor. Marking a step running when the request is sent would claim work that
may never have started.

**Hangs come from the instrument, not from a timeout.** The driver watches the
instrument's status and reports the job failed when the ledger says `hung`. No
single timeout fits both a 2 second step and a slow one.

**A step is only retried when the instrument says that's safe, and at most 3
times.** Liquid handler steps are never repeated, whether after a failure, a hang
or a restart. A hang or a restart doesn't give me an `Outcome` to check, so I use
the instrument's `failures_retryable` setting, which says the same thing about
the device. A retry is a new job with a new id.

**The driver stores the measurement before it reports the job completed.** Only
the reference and the instrument's hash go over NATS. Sending the bytes through
NATS for the executor to store would limit how big a measurement can be, and give
the data a second place to get lost.

**A run only ends when no instrument is still working for it.** A cancel, or a
failure that can't be retried, stops new work. Steps that haven't been sent
become `cancelled` straight away, and a request that hasn't been confirmed is
withdrawn and then checked. Steps already on an instrument keep their real
outcome, and the run gets its final status when the last one reports. So
`POST /cancel` can return `running`. Returning `cancelled` straight away would
mean the record calls a run over while a plate is still being filled.

**One open job per device is enforced by a unique index, not by convention.**
The scheduler works per run, but two runs can want the same liquid handler at
once. The database refuses a second open job on a device, whoever asks.

**The connection settings are about recovery speed, not correctness.** After a
40 second partition, the executor took 20 seconds to recover, waiting on TCP.
More frequent NATS pings, `tcp_user_timeout`, and a Postgres
`idle_in_transaction_session_timeout` (so a transaction cut off half way doesn't
keep holding its row locks) brought that down to 3 seconds. `Executor` and
`serve` don't rely on them, in case the checks pass in their own connections.

## 3. The GraphQL schema

```graphql
type Query {
  run(id: ID!): Run                          # null if there is no such run
  runs(status: RunStatus, limit: Int! = 20): [Run!]!   # newest first, at most 100
  workflows: [Workflow!]!
}
type Mutation {
  createRun(workflowName: String): Run!      # the default workflow, if none
  startRun(id: ID!): Run!
  cancelRun(id: ID!): Run!
}
type Subscription {
  run(id: ID!): Run!                         # now, then on every change, until it is over
}
type Run  { id workflowName status createdAt startedAt finishedAt steps: [Step!]! }
type Step { name deviceId dependsOn status startedAt finishedAt error data attempts: [Attempt!]! }
type Attempt { jobId number status startedAt finishedAt error retryable data }
type Data { ref sha256 }
```

- **It's the same record as the HTTP API, not a second model of it.** Same field
  names and statuses (GraphQL writes the statuses in capitals), so the two can't
  disagree.
- **`attempts` is what HTTP can't show.** A step that ran twice shows both
  attempts, each under the `jobId` the instrument's ledger uses.
  `scripts/truth.py` relies on this to match every attempt on record with an
  execution on an instrument.
- **`data` includes the hash with the reference**, so a reader can check the file
  is the one the instrument produced.
- **Null means "not known to have happened".** For example, `startedAt` stays
  null until the instrument confirms the start.
- **Mutations return the `Run`**, so a tool gets the new state in one round trip.
  Errors carry a `code` (`RUN_NOT_FOUND`, `UNKNOWN_WORKFLOW`). `startRun` on a run
  that isn't pending changes nothing and returns it as it is.
- **Following a run is a subscription that sends the whole run each time**, not
  just what changed, so a client that misses an update doesn't lose anything. It
  ends when the run ends.
- Left out on purpose: pagination cursors (there's just a `limit` and a status
  filter), any way to act on a single step, and authentication.

## 4. What I did to break it

| What | Command | What happened |
| --- | --- | --- |
| Happy path | `acceptance.sh`, both workflows | 7/7, in 10 s and 6 s |
| Executor killed mid-run | `chaos.sh kill executor` | The run completed after the restart, and nothing ran twice |
| Driver cut off for 8 s and 40 s mid-step | `chaos.sh disconnect incubator-1 40` | The instrument finished on its own. The record caught up 7 s after the driver came back, with the ledger's times |
| Executor cut off for 8 s and 40 s | `chaos.sh disconnect executor 40` | Same, 3 s after it came back |
| Storage down as a read ended | `chaos.sh storage-down 10` | The step stayed `running` until the file was stored, then went `completed` |
| Cancel while the first step's driver is cut off | disconnect, start, cancel | The step never started after the network came back. The ledger stayed empty |
| Cancel mid-step | | Steps already running ended `completed`, the rest `cancelled`, then the run |
| Driver restarted mid-job | `docker compose restart <driver>` | Incubator: retried. Liquid handler: the step failed and was never repeated |
| Measurement lost | kill storage, let the read end, restart the reader's driver | Attempt 1 recorded as failed, the plate read again, and the record points at attempt 2 |
| NATS restarted, then Postgres, mid-run | `docker compose restart nats` | Runs completed within 10 s |
| Random faults under load | `chaos.sh random --seed 7`, `11`, `23`, 60 s each | Record true |
| The same with instrument faults | seeds `7` and `42`, with `LH_FAIL_PCT=15 INC_HANG_PCT=30 INC_FAIL_PCT=20 PR_FAIL_PCT=40 HANG_DURATION=6s CHAOS_SEED=3` | Record true. Incubator steps went `failed`, then `hung`, then `completed`. No liquid handler step ran twice |

Load was `load.py --for 60 --every 3 --cancel 4`. Across the five random
sessions and the cases above, that's 87 runs and 357 executions, and
`truth.py --settled` passed all twelve of its checks.

What I changed because of it:

- **The acceptance check caught one of my bugs.** `read_plate` ran three times.
  The instrument writes `completed` to its ledger just before `run()` returns,
  and my status check (every second, on a 2 second job) landed in that gap and
  mistook a finished read for a hang. Now only the ledger's `hung` counts.
  `test_a_job_that_ends_just_as_it_is_checked_is_not_taken_for_a_hang`
  reproduces it.
- Recovery after a long partition went from 20 seconds to 3, with the connection
  settings above.
- The time a driver holds a reply went from 10 seconds to 5, since that's how
  long a lost reply costs.
- The executor used to exit on start if object storage was down, even though it
  never uses it. After `storage-down`, SeaweedFS takes about 20 seconds before it
  accepts writes again, so a restart in that window kept looping. It now retries
  in the background.
- I wrote `truth.py` because the acceptance script can't be used with faults
  switched on.

## 5. What's still not true, or not proven

On purpose:

- **A measurement can be lost.** Between the end of a read and its upload, it
  only exists in the driver's memory. If the driver dies at that moment, the
  ledger says completed but the file doesn't exist. The record marks that
  attempt failed, with the reason, and the plate is read again, because the
  reader says that's safe. So an instrument can have completed something the
  record calls failed.
- **The record is only as fresh as the last answer.** While a driver is cut off,
  its step stays `pending` or `running` whatever the instrument has done since,
  and a cancelled run stays `running` until that driver answers. Between sending
  a job and getting confirmation, the step reads `pending` even if the instrument
  has already started.
- **`start_by` assumes the clocks agree** within half a second. Here they're the
  same clock.
- A hung job holds its instrument until the instrument lets go.

Not proven:

- That a file object storage has acknowledged is never lost. I killed storage
  straight after 5 writes and all 5 survived, but I don't read files back later.
- Two executors running at once. The unique index and row locks should keep the
  record consistent, but I didn't try it.
- A ledger that loses a job the driver had started. There's a path for it (the
  job fails and isn't repeated), but only a unit test covers it.
- More than three devices, long histories (`status()` copies every execution on
  each call), or a device id that isn't a plain NATS subject token.

## What I thought of the exercise

- The acceptance script is a good floor, and check 5 caught a real bug. But
  nothing provided can check a run with faults switched on, which is where the
  exercise says the work is.
- Cancel isn't specified. Whether a run is `cancelled` while an instrument is
  still working for it decides what `POST /cancel` returns, and I had to pick.
- `retryable` is defined for a failure. A hang, the more interesting case, gives
  nothing, and the README doesn't say whether a step can be run again after a
  hang or a restart.
- `chaos.sh` never restarts a driver. That only happens as a side effect of
  changing `*_FAIL_PCT`, and then the instrument restarts with it, which a real
  instrument wouldn't.
- `storage-down N` is really N plus about 20 seconds before SeaweedFS accepts
  writes again. `disconnect` leaves TCP connections half open, so with default
  client settings a long partition takes nearly as long again to recover. Both
  took time to tell apart from my own bugs.
- It doesn't say whether the checks create `Executor` and call `serve`
  themselves. I assumed they might, and kept everything in `main.py` optional.
- The scheduler works per run, and nothing mentions that runs share instruments.
  A fair trap, but an easy one to miss.
- Three hours is tight for both ends of a protocol, recovery, a GraphQL API and a
  note this detailed.
