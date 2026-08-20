# Study Intake Preprocessor

This repository is the version-controlled canonical source for the shared
math, 408, and English preprocessing center. It contains code, schemas, tests,
the read-only dashboard, and a configuration template only.

Runtime data is deliberately separate. Never copy `logs/`, `state/`,
`packages/`, `private/`, or `receipts/` into this repository or an immutable
release.

## English contract

- Canonical state directory: `/Users/xiazhibin/Documents/kaoyan-english/intake`
- Event input: `events/YYYY-MM-DD/`
- Candidate output: `candidates/YYYY-MM-DD/`
- Microbatch eligibility: five unconsumed captures, 180 seconds of article
  silence, or an `article_completed` event.
- Each subject dispatcher rescans for newly frozen work at least once per
  second. This is a discovery-latency ceiling only; it does not limit how many
  independent tasks may be submitted by the scan.
- Luna performs two ordered structured stages: analysis first, then critical
  review of that frozen draft. Both stages are fixed to `gpt-5.6-luna` with
  `reasoning=max`. The host overwrites every source, evidence, runtime-identity
  and zero-write binding before the repository-owned English pipeline CLI
  validates and renders the final candidate.
- A completed analysis may be retained as a recovery checkpoint, but it is not
  consumable unless the critical-review stage also completes successfully.
- Requested and observed runtime identity remain separate. A requested runtime
  may be published as `requested_unverified`; only verifiable runtime metadata
  may be displayed as `confirmed`.
- Foreground capture and Luna preprocessing keep `formal_write_count=0`.

The bounded nightly backfill command is:

```sh
/Users/xiazhibin/.codex/study-intake-preprocessor/current/bin/preprocess_worker.py \
  --config /Users/xiazhibin/.codex/study-intake-preprocessor/current/config.json \
  run-once --subject english --date YYYY-MM-DD
```

Its `study-intake-english-run-once-receipt-v1` receipt is returned on stdout;
it is not a formal writer receipt and is not persisted by this worker.

## Tests

```sh
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s tests -p 'test_*.py'
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s dashboard/tests -p 'test_*.py'
```

The four blocking historical tests require an explicit, content-addressed,
read-only external test-input manifest. Missing or drifting inputs fail instead
of skipping:

```sh
STUDY_PREPROCESSOR_HISTORICAL_TEST_INPUT_MANIFEST=/absolute/path/sha256.json \
  python3 -m unittest \
  tests.test_math_shadow_backaudit.RealAugustFourthBackauditTests
```

## Immutable release workflow

Release v2 binds the canonical source hashes, sealed executable modes, and the
canonical runtime-data root into the release ID.  `config.json` is rebuilt
deterministically from the bound template and roots.  Verification requires an
exact regular-file and directory closure, rejects symlinks and undeclared
bytecode, and checks all release directories and files are sealed read-only.
The current target contract is
`study-intake-model-request-contract-v2`: `model=gpt-5.6-luna`,
`reasoning_effort=max`, no `service_tier` key in config or the actual argv,
`requested_service_tier=null`, `fast_mode_requested=false`, and
`fast_mode_effective=not_requested`. Historical priority releases and the
pre-plugin no-tier release remain byte-for-byte reopenable for explicit
rollback, but cannot pass a target build or activation gate.

Build and verify the default `concurrent_v2` profile without changing the active
runtime or LaunchAgents. A non-skipped build requires the formal-surface guard:

```sh
python3 scripts/release_manager.py build \
  --source-root /absolute/path/to/three-subject-successor \
  --release-base /absolute/path/to/runtime-root \
  --runtime-data-root /absolute/path/to/runtime-root \
  --formal-config /absolute/path/to/formal-surface-config.json \
  --formal-baseline /absolute/path/to/formal-surface-baseline.json \
  --historical-test-input-manifest /absolute/path/to/sha256.json
```

The rollback artifact for the currently active legacy monolith must be derived
from that exact release rather than rebuilt from mutable staging source. The
builder checks `current` before and after the copy, captures the two installed
legacy LaunchAgent definitions into the immutable rollback artifact, excludes
caches and bytecode, verifies the sealed closure, and runs the old worker in an
isolated temporary runtime with adapters disabled, a bounded timeout, and no
model or formal writes:

```sh
python3 scripts/release_manager.py build-legacy-rollback \
  --expected-current c3ff664d99b79249bd637478093f04a5fdba6247076c08b9189017567a000d63
```

The resulting verification receipt is stored under
`rollback-verifications/`. Building or verifying this artifact does not switch
`current` and does not invoke `launchctl`.

The formal baseline gate is content-based rather than wall-clock-based: the
configured baseline manifest hash must exactly equal the freshly re-read
pre-build formal-surface manifest. A reported write counter cannot substitute
for that comparison. The final operator stop point is after build, strict
target verification, content-addressed canary-manifest preparation, and the
read-only canary preview. At that point `current`, LaunchAgents, runtime state,
and TCP 8767 must still match the pre-preview snapshot; report this result
before any `--apply` command.

Preview an activation:

```sh
python3 scripts/release_manager.py activate \
  --release-id RELEASE_ID \
  --expected-current CURRENT_RELEASE_ID
```

Create the HMAC-bound drain receipt from the new immutable release. This waits
for the live legacy `worker.lock`, proves the old claim gate and process
snapshot, and does not switch `current`:

```sh
python3 /absolute/path/to/releases/RELEASE_ID/scripts/release_manager.py \
  prepare-drain \
  --release-base /absolute/path/to/runtime-root \
  --active-link /absolute/path/to/runtime-root/current \
  --target-release-id RELEASE_ID \
  --output /absolute/path/to/drain-receipt.json
```

The receipt is accepted for at most 15 minutes, allows at most 60 seconds of
clock skew into the future, and must match the same live claim-gate device and
inode while apply holds that gate. Generate it after the final preview and as
close as practical to the explicit apply decision.

Activate only after the preview, tests, formal guard, rollback verification,
and operational review succeed:

```sh
python3 scripts/release_manager.py activate \
  --release-id RELEASE_ID \
  --expected-current CURRENT_RELEASE_ID \
  --drain-receipt /absolute/path/to/drain-receipt.json \
  --apply
```

Rollback is an explicit switch to a previously verified release and is also a
dry run unless `--apply` is supplied:

```sh
python3 scripts/release_manager.py rollback \
  --release-id PREVIOUS_RELEASE_ID \
  --expected-current CURRENT_RELEASE_ID
python3 scripts/release_manager.py rollback \
  --release-id PREVIOUS_RELEASE_ID \
  --expected-current CURRENT_RELEASE_ID \
  --drain-receipt /absolute/path/to/drain-receipt.json \
  --apply
```

When preparing a receipt for an explicit rollback target, add
`--operation rollback` to `prepare-drain`. A preview never needs a drain
receipt. An applied activation or rollback that replaces an existing active
release always does.

Use `--expected-current absent` only for the first activation. Applied changes
write prepare and postcommit receipts and hold the activation lock. When an old
release is active, activation reacquires the exact claim-gate inode recorded by
the authenticated drain receipt and keeps it locked while the old services are
disabled, stopped, and checked for legacy workers or Luna children. It then
atomically switches `current` and installs fully rendered target plists. In the
concurrent profile, cs408, English, and Dashboard are enabled and bootstrapped;
cs408 and English preserve their drain markers and emit paused heartbeats until
the replay acceptance gate clears them. Math remains explicitly disabled and
booted out with its drain marker intact.
Every prior plist is retained in a deployment-specific backup.
`legacy_monolith` owns the old worker plus Dashboard;
`concurrent_v2` owns the three subject Dispatchers plus Dashboard. Before a
failed concurrent activation can roll back, all four target services are first
disabled to prevent KeepAlive restart; the three Dispatchers are then drained
and booted out. Only after that proof succeeds may the previous symlink, plist
bytes, and service topology be restored. `launchagents/` contains the four
concurrent-v2 templates. Dry-run activation never writes plists, switches the
link, or invokes `launchctl`.

Controlled replay does not clear a subject drain or open normal claims. It
first inspects an exact, release-bound capture allowlist, then executes only
the content-addressed tasks authorized by a one-shot HMAC receipt. The exact
allowlist is applied before adapter evidence parsing, so an unrelated damaged
capture cannot poison a replay. For English it may rebuild an exact set of
already-consumed source events as one deterministic microbatch; controlled
replay bypasses an old ready-package reuse decision but never bypasses evidence,
hash, schema, or publication-generation gates:

```sh
python3 scripts/controlled_replay_lane.py \
  --config /absolute/path/to/current/config.json \
  --spec /absolute/path/to/replay-spec.json \
  inspect

python3 scripts/controlled_replay_lane.py \
  --config /absolute/path/to/current/config.json \
  --spec /absolute/path/to/replay-spec.json \
  run --output-root /absolute/path/to/replay-reports
```

The run report includes verified model-submission event receipts, task
authority, before/after subject control state, and `formal_write_count=0`.
408 knowledge snapshots use a v2 binding identity over the evidence manifest,
query, complete source hashes, and subject processing contract. Legacy binding
files remain untouched; a semantic or source change creates a new immutable
binding instead of rewriting historical evidence.

### Transactional successor canary activation

`activate-canary` is the zero-model successor activation path. It does not
require any real Luna acceptance receipt. The release manager derives the
content-addressed `study-intake-three-subject-canary-activation-v2` manifest
from the exact immutable release and its three model-free producer authorities:

```sh
python3 scripts/release_manager.py prepare-canary-manifest \
  --release-base /absolute/path/to/runtime-root \
  --release-id RELEASE_ID \
  --output-dir /absolute/path/to/canary-manifests
```

The resulting file is named `SHA256.json`, has one producer-authority
fingerprint for each of math, cs408, and English, and binds the v2 target
contract. Producer authorities and config contain no `service_tier` key. The
manifest evidence explicitly records `requested_service_tier=null`,
`fast_mode_requested=false`, and `fast_mode_effective=not_requested`. It does
not pre-bind a timestamp or high-watermark. Every slot starts as `planned`, is
post-activation only, has `initial_canary_inflight_limit=1`, and binds
`continuous_concurrency_limit=20`. The target runtime must advertise the same
persistent capability; a manifest declaration alone cannot pass that gate.

Preview first. Preview verifies the immutable release, formal zero-model gate,
runtime canary capability, all three drained/claim-free subject states, and a
read-only historical-backlog snapshot. Existing backlog is allowed; it must be
frozen behind the later activation high-watermark. Preview neither switches
`current`, changes a drain marker, invokes launchctl, nor arms a runtime slot:

```sh
python3 scripts/release_manager.py activate-canary \
  --release-id RELEASE_ID \
  --canary-manifest /absolute/path/to/SHA256.json \
  --preview
```

The default preview invokes only each target dispatcher's read-only `audit`
surface. It snapshots the dispatch runtime tree, active symlink, relevant
LaunchAgent plist bytes, and TCP 8767 listener owners before and after the
audits, and fails if any surface changes. It does not call the writable
`status` path, start a service, or require a drain receipt.

Only after an explicit Go decision may the same manifest be applied. Replacing
an active release still requires the normal HMAC drain receipt and exact
`--expected-current` compare-and-swap value:

```sh
python3 scripts/release_manager.py activate-canary \
  --release-id RELEASE_ID \
  --canary-manifest /absolute/path/to/SHA256.json \
  --expected-current PREVIOUS_RELEASE_ID \
  --drain-receipt /absolute/path/to/drain-receipt.json \
  --apply
```

Apply rechecks the historical backlog while holding the activation/claim gates
and switches `current` only through the release transaction. Immediately after
that atomic switch, the transaction creates one UTC `activated_at` boundary;
all three runtimes independently derive and verify their producer
high-watermarks against that same boundary before any daemon is bootstrapped.
The zero-model audit then classifies every older item as
`pre_activation_frozen`, records the excluded count, and proves that no older
item entered the Canary queue. Captures in the switch-to-boundary interval are
therefore conservatively frozen as historical. The transaction then enables
and bootstraps the three dispatchers and Dashboard. Success
requires four running services bound to the same release plus three fresh
release-matched heartbeats/status projections whose canary gates are `armed`,
post-activation-only, and at or below the one-slot first-task limit. After a
subject's first complete success, its independent continuous limit is 20. The
public v2 HMAC receipt is content-addressed under `deployments/`; it exposes
hashes, not private absolute evidence paths, and reports
`status=production_canary_active`, `production_accepted=false`, zero activation
model/Provider calls, `formal_write_count=0`, and `sol_enabled=false`.

Any arm, enable, bootstrap, launchctl, heartbeat, status, or receipt-seal
failure disables and drains the target, rolls back its canary gates without
deleting producer queues or immutable receipts, boots out all four target
services, restores the previous plists and `current`, and restarts only the
previous paused topology. A normal task failure after a committed activation is
subject-local: that subject's consumer becomes `failed_drained` while its
producer and the other subjects continue. It is not a global service rollback.

Canary activation is not final acceptance. Each subject's first Canary success
independently moves that subject to `continuous_concurrent_unlocked`; it does
not wait for the other two subjects. The legacy commands
`seal-three-subject-resume-acceptance` and non-rollback `resume-all` remain
available only for reopening the historical v1 priority recovery topology.
They fail closed for every current v2 no-fast-mode release and cannot control,
validate, or resume a new successor. They are not a prerequisite for
production-Canary activation, and they are not a later gate for per-subject
first-success unlock.

### Current v2 Canary campaign projection

The current campaign publisher follows the asynchronous subject lifecycle; it
does not require a three-subject barrier. It derives each subject from the HMAC
Canary state, terminal index, terminal receipt, task-supervisor identity and
exit receipts, processing completion, MCP grounding, and content-addressed
publication artifacts. Mutable concurrency telemetry is accepted only as a
non-authoritative cross-check after the publisher has independently reopened
those receipts and recomputed half-open task intervals. If two or more subject
lifecycle intervals really overlap, the projection reports that observed
overlap and the recomputed peak. It never infers overlap merely because three
dispatchers are enabled.

The v2 request contract contains no `service_tier` in config or argv and records
`requested_service_tier=null`, `fast_mode_requested=false`, and
`fast_mode_effective=not_requested`. The first-task limit is one per subject;
the independently unlocked continuous limit is 20. A subject may advance while
the other two still await their first post-activation capture. A runtime chain
whose terminal, processing receipt, and package reopen but whose independent
report binding is unavailable remains partial evidence and cannot make the
campaign `production_verified`.

Publication requires an explicit evidence scope:

```sh
python3 scripts/publish_concurrency_campaign.py \
  --processing-runtime-root /absolute/path/to/processing-runtime \
  --dispatch-authority-key /absolute/path/to/dispatch-authority.key \
  --candidate-release-root /absolute/path/to/runtime-root/releases/RELEASE_ID \
  --candidate-release-id RELEASE_ID \
  --deployment-authority-key /absolute/path/to/deployment-authority.key \
  --canary-activation-receipt /absolute/path/to/activation-receipt.json \
  --evidence-scope real_production_hmac_v2 \
  --evidence-root /absolute/path/to/campaign-evidence \
  --state-file /absolute/path/to/three-subject-concurrency-campaign.json
```

`real_production_hmac_v2` requires the real task-supervisor and Provider-stage
HMAC closures. Before accepting them, the publisher runs the immutable release
verifier, hashes the configured `model.codex_path`, and hashes the same-release
`bin/preprocess_task_runner.py`. Both Analysis and fresh Critical Review must
reopen their Provider identity and exit receipts. The publisher independently
recomputes the canonical argv and environment-key-name hashes, rejects every
service-tier or priority token, requires the no-fast argv/environment policy,
and binds both executable hashes to the verified release. The public projection
exposes only `provider_execution_contract_status`, the two stage identity/exit
SHA-256 values, and executable SHA-256 values; argv, environment key names, and
local paths remain private evidence.

`zero_model_fixture_v2` is reserved for model-free tests: its actual model,
Provider, and MCP counters remain zero, its Provider contract status is
`zero_model_fixture_not_applicable`, its status is `test_evidence_only`, and
`current_release_usable=false`. It cannot be relabeled as production evidence.
Legacy v1 priority campaign receipts remain byte-stable historical evidence
only; they cannot validate or activate the current v2 release.

### Legacy transactional three-subject resume and rollback

This compatibility path applies only to an integrity-verified historical v1
priority release that an operator deliberately left drained and disabled. It
is separate from `activate-canary`. A release carrying the target v2 model
request contract, including the no-`service_tier` successor, is rejected before
the acceptance manifest is opened. For the historical priority topology,
resume all three only after creating the content-addressed
`study-intake-three-subject-resume-acceptance-v1` manifest. The manifest binds
the active immutable release manifest, one normalized single-subject evidence
receipt for each of math, cs408, and English, the successful three-subject
concurrency receipt, all three MCP authority fingerprints, and the release's
unchanged formal-surface hashes. Sealing and previewing are zero-model
operations and require `formal_write_count=0` and `sol_enabled=false`.

Each normalized subject evidence receipt must use
`study-intake-subject-resume-evidence-v1` and prove a real MCP read, completed
Analysis, a content-addressed report, at least one real Luna/Provider request,
and requested priority service tier. The concurrency receipt must use
`study-intake-three-subject-concurrency-evidence-v1`, bind those exact three
receipt hashes and MCP fingerprints, and prove a 3/3 barrier, global peak of at
least three, one active run per subject, and three generated reports. An
effective tier of `requested_unverified` remains labeled as such; it is never
promoted to provider-attested priority.

Seal the HMAC acceptance receipt, then run the default preview. Preview checks
the exact current release, sealed receipt closure, installed plists, all three
drain states, and zero-model backlog audits without invoking launchctl:

```sh
python3 scripts/release_manager.py seal-three-subject-resume-acceptance \
  --release-id RELEASE_ID \
  --acceptance-manifest /absolute/path/to/SHA256.json \
  --output /absolute/path/to/three-subject-resume-receipt.json

python3 scripts/release_manager.py resume-all \
  --release-id RELEASE_ID \
  --acceptance-receipt /absolute/path/to/three-subject-resume-receipt.json
```

Apply only after reviewing the preview:

```sh
python3 scripts/release_manager.py resume-all \
  --release-id RELEASE_ID \
  --acceptance-receipt /absolute/path/to/three-subject-resume-receipt.json \
  --apply
```

`resume-all --apply` clears all three drain markers, then enables, bootstraps,
and verifies all three LaunchAgents as one transaction. Success requires a new
post-resume heartbeat from every subject whose `release_id` matches the exact
active release. A stale heartbeat from the same or an older release cannot
pass. If any drain clear, enable, bootstrap, launchctl verification, or
heartbeat check fails, all three subjects are drained, all three LaunchAgents
are disabled and booted out, and a sealed failure proof is recorded.

Global emergency stop uses the legacy `resume-all --rollback` spelling for
backward compatibility. Unlike the non-rollback v1 resume chain, this safety
operation remains valid for a verified target v2 release as well as an
integrity-verified historical release. It has a no-mutation preview and an
apply mode, and it does not require any acceptance receipt. Apply first disables KeepAlive and
boots out all three dispatchers so SIGTERM performs bounded hard-cancel and
late-result fencing. It then requires zero active/claimed leases, changes every
active Canary gate to `paused_drained`, and verifies
`luna_consumer_enabled=false`. The pause preserves each original activation id, producer
high-watermark, append-only queue, receipt, and
`producer_capture_enabled=true`; it never deactivates or re-arms the Canary and
never creates a new high-watermark:

```sh
python3 scripts/release_manager.py resume-all \
  --release-id RELEASE_ID \
  --rollback

python3 scripts/release_manager.py resume-all \
  --release-id RELEASE_ID \
  --rollback \
  --apply
```

Resume and emergency stop never rewrite or switch `current`; release changes
remain exclusive to the authenticated `activate` and `rollback` commands
above.

### Subject-scoped Canary pause and resume

The top-level `pause-canary` and `resume-canary` controls accept one subject or
`all`. Both default to a no-mutation preview:

```sh
python3 scripts/release_manager.py pause-canary \
  --release-id RELEASE_ID \
  --subject math

python3 scripts/release_manager.py resume-canary \
  --release-id RELEASE_ID \
  --subject math
```

After an explicit decision, add `--apply`. Subject pause first disables
KeepAlive and boots out only the selected dispatcher. SIGTERM invokes the
runtime's bounded hard-cancel path; if a Luna task was active, apply requires a
content-addressed `emergency_hard_cancel` terminal, `daemon_shutdown`, a sealed
late-result fence, and zero active/claimed leases before writing
`paused_drained`. It then restarts that same-release dispatcher as a
materialize-only watcher. The producer remains enabled, new post-high-watermark
captures continue entering the durable queue, and no paused consumer submits a
model request. Other subjects are untouched.

`resume-canary --apply` verifies the selected gate is `paused_drained` or
`failed_drained`, preserves its activation id, producer authority,
high-watermark, and queue, invokes the runtime `resume-canary`, then
enables/bootstraps the selected LaunchAgent and requires a fresh
release-matched heartbeat. `--subject all` is transactional over all three
subjects; a failure re-pauses only that target set. A single-subject failure
never pauses or restarts either of the other subjects. Neither command admits
frozen pre-activation backlog or rewrites `current`.

New pause and resume control receipts use
`study-intake-canary-pause-proof-v2` and
`study-intake-canary-resume-proof-v2`. Their public control bindings carry the
one-task initial limit, the continuous limit of 20, null requested tier, and
false/not-requested fast-mode evidence. Historical v1 activation and recovery
schemas and their sealed receipts are not rewritten.

### Legacy math-only resume

The immutable config keeps the math adapter resume-capable, while activation
still disables and boots out the math LaunchAgent and preserves its drain
marker. After the frozen replay inventory, new-intake checks, business goldens,
Dashboard generation checks, and formal-surface guard pass, seal the
content-addressed acceptance manifest and then resume math. `resume-math`
revalidates the HMAC receipt, exact current release, installed plist, zero
eligible backlog, drain state, and current formal-write boundary before it can
enable the service. It fails back to disabled/drained if startup or heartbeat
verification fails:

```sh
python3 scripts/release_manager.py seal-math-resume-acceptance \
  --release-id RELEASE_ID \
  --acceptance-manifest /absolute/path/to/SHA256.json \
  --output /absolute/path/to/math-resume-receipt.json

python3 scripts/release_manager.py resume-math \
  --release-id RELEASE_ID \
  --acceptance-receipt /absolute/path/to/math-resume-receipt.json \
  --apply
```

## Isolated real Luna Max validation

The validation manifest contains exactly the independent read-only tasks to run;
there is no concurrency-count option. Every selected task is released together
into its own process group and output directory. The command records requested
Luna Max identity honestly as `requested_unverified` when the runtime does not
provide independent attestation, and verifies the formal surfaces before and
after:

```sh
python3 scripts/read_only_luna_validation.py \
  --selection /absolute/path/to/selection.json \
  --output-root /absolute/path/to/empty-validation-directory \
  --formal-config config.json \
  --formal-baseline /absolute/path/to/formal-surface-baseline.json
```
