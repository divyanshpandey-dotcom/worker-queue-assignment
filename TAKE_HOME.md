# Backend Engineer take-home

Three parts, one for each thing the job needs. Build a small piece of infrastructure against an unreliable third party. Review code an agent wrote. Then defend and extend your work live. Budget 3 to 5 hours for part 1 and about an hour for part 2. We send part 2 once you hand in part 1. Part 3 is a 75-minute call.

## Ground rules

- Use whatever you use at work, including AI coding agents. We will ask you to explain any line of your submission, and in part 3 we change the requirements. Understanding what you hand in is the whole test.
- Python 3.12 and PostgreSQL. Any libraries. We run `docker compose up`, then `make seed` and `make worker`, both yours. `README.md` has the exact contract.
- For part 1, hand in a repository link containing the code and `DECISIONS.md` of at most one page. For part 2, add `REVIEW.md` to the same repository.
- If something in the brief is wrong or impossible, say so in `DECISIONS.md` and build the sensible version. That is a valid answer, and sometimes the only correct one.
- The work is paid at [rate] regardless of outcome.

## Part 1: the dispatcher

We schedule outreach actions, one email to one contact, and a third-party provider sends them. Build the worker that drains scheduled actions and sends them through the provider.

### What we give you

- `provider/`, a mock provider HTTP server with three endpoints.
    - `POST /send` with `{action_id, recipient, body}` returns `200 {receipt_id}`. `receipt_id` identifies that one send, and every accepted call produces a new one. The provider stores `action_id` and echoes it on the receipt but does not enforce uniqueness. Send the same `action_id` twice and it sends two emails and produces two receipts.
    - `GET /sent?since=<cursor>` returns `{receipts: [{receipt_id, action_id, recipient, sent_at}], next}` in send order, paginated. There is no lookup by `action_id` or `recipient`.
    - `GET /stats` returns request counts, including `violations`, in total and per `Retry-After` window: requests that arrived while a window the provider had issued was still open.
- The provider misbehaves on purpose. Some requests get `429` with a `Retry-After` header. Some get `500`, and nothing was sent. Some the provider records and then never answers. The connection hangs until your client times out. Some the provider processes and then drops the connection. Environment variables set the fault rates and a random seed. The same seed replays the same fault sequence in request-arrival order; with several workers, which request gets which fault still varies from run to run.
- `docker-compose.yml` with PostgreSQL.
- `seed/actions.json`, 500 actions with `id`, `recipient` and `body`. Load them into whatever schema you design. Your database is yours; nothing of ours reads it.
- `verify.py`, the report we run on your submission. It reads `seed/actions.json` and the provider, nothing else.

### What it has to do

1. No contact receives the same action twice.
2. No scheduled action is lost. If a worker dies mid-send, another worker finishes the job.
3. Three worker processes run at the same time without stepping on each other.
4. The worker respects the provider's rate limit.
5. Every action ends in a terminal state, `SENT` with its receipt or `FAILED` with a reason.

The first requirement is harder than it looks with this provider. That is the point.

### How we check it

We run three workers for two minutes with faults on, `kill -9` one of them at the one-minute mark, and then run `verify.py`. Each worker is started with `make worker`, in the foreground and in its own process group, with `PROVIDER_URL` and `DATABASE_URL` in the environment. The kill goes to the whole process group.

`verify.py` compares the 500 seeded actions against the provider's `/sent` and `/stats`, reports what reached the provider, and warns about anything out of bounds:

1. An `action_id` appears in `/sent` more than once.
2. Any of the 500 is missing from `/sent`, or appears with a recipient other than the seeded one. Every fault the provider injects is recoverable, so with two minutes of retries nothing should be missing.
3. More than three requests arrive inside one `Retry-After` window, or more than three over the whole run arrive more than 100 ms into a window. A request that was already on its way when the window opened lands within milliseconds.

It is not a pass/fail gate. We read its output together with your `DECISIONS.md`. A warning you saw coming and can explain is worth more than a clean run you can't account for.

Your own state table is not part of the report. Whether you call something `FAILED` or `SENDING` is your bookkeeping; what reached the provider, and how many times, is what we measure.

The run is two minutes and the kill comes at one. Whatever the dead worker had in flight has to reach `/sent` in the minute that is left. Size your leases for that.

Run `verify.py` yourself before you hand in. If it warns about something, say why in `DECISIONS.md`.

### DECISIONS.md, one page

- The state machine and what guards each transition.
- What you do when the provider times out after possibly sending. Be precise about what you can and cannot guarantee with this provider API.
- What you would ask the provider to add to their API, and why that specific thing.
- What `verify.py` cannot catch, and what you would check instead.
- What breaks first at 50,000 actions an hour, and what you would change.
- What you deliberately did not build.

A framework that solves this for you is not a substitute for the state machine. We want to see the transitions.

## Part 2: the review

We send part 2 after you hand in part 1. It is a code review: a pull request of about 250 lines that a coding agent wrote against some of our code, from a two-line prompt. Nobody has reviewed it. The instructions come with it, and you write the result in `REVIEW.md`.

Ranking matters as much as finding. A review that flags everything at the same level is no use to whoever has to merge it.

## Part 3: live session, 75 minutes

1. You walk us through your state machine and the worst failure you handled. 15 minutes.
2. We change a requirement and you adapt the design with us, on a whiteboard or in code. 25 minutes.
3. We go through your review and pick one thing to dig into. 20 minutes.
4. Your questions. 15 minutes.

Nothing in this session needs preparation beyond having done parts 1 and 2 yourself.
