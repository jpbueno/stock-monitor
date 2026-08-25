# Evidence Authority Workflow Design

**Date:** 2026-08-24

**Status:** Approved for implementation with the existing fail-closed trading and scheduling boundaries unchanged.

## Outcome

Stock Monitor gains a reproducible, GET-only workflow for collecting exact official-source bytes, preparing an unreviewed proposal, compiling and inspecting a reviewer-authored candidate release, and installing that release only after its digest has been independently pinned in source control.

This work makes daily evidence review operationally repeatable. It does not make the current public sources exhaustive. The existing broad `BINARY_EVENT` and `ETF_ACTION` gates therefore remain blocked unless an exact source is later demonstrated to provide complete coverage or the user separately approves a narrower policy definition. No external schedule may be created from this implementation alone.

## Verified constraint

The current event definitions cover more than any reviewed public feed can exhaustively enumerate:

- Stock binary events include earnings, merger votes, regulatory decisions, known court rulings, and other material events during the intended holding window.
- ETF actions include sponsor and index-provider actions.

The SEC submissions API is authoritative for disseminated EDGAR submissions and is updated throughout the day, but Form 8-K generally permits reporting after an event occurs. Issuer investor-relations pages and RSS feeds are curated publication channels, not completeness contracts. Public ETF sponsor and index pages likewise provide no exhaustive negative result for every action class. Consequently, absence from these sources remains `UNKNOWN`; it must not mint `CONFIRMED_CLEAR`.

Primary references:

- [SEC EDGAR APIs](https://www.sec.gov/search-filings/edgar-application-programming-interfaces)
- [SEC Form 8-K instructions](https://www.sec.gov/about/forms/form8-k.pdf)
- [NVIDIA FY27 Q2 results event on August 26, 2026](https://investor.nvidia.com/events-and-presentations/events-and-presentations/event-details/2026/NVIDIA-2nd-Quarter-FY27-Financial-Results/default.aspx)

The NVIDIA event is a current positive finding. Any NVDA holding window containing August 26, 2026 must be blocked as an overlap. It is not evidence that the full stock event class is otherwise clear.

## Non-negotiable boundaries

- All external retrieval is credential-free HTTPS `GET` through the existing proxy-free, redirect-bounded transport.
- The workflow never accesses a brokerage, requests order capability, or places, routes, modifies, or cancels an order.
- `evidence prepare` never writes `data/evidence/**`, changes a release pin, labels material reviewed, issues a `ReviewedEvidenceBundle`, or emits clear coverage.
- `evidence inspect` is network-free and cannot activate a candidate.
- `evidence install` is network-free and refuses a candidate whose exact release digest is not already equal to `CURRENT_EVIDENCE_RELEASE_SHA256`.
- Updating `CURRENT_EVIDENCE_RELEASE_SHA256` remains an independent human-reviewed source-control change. No CLI flag or unsigned approval file substitutes for it.
- Review validity remains at most 24 hours and is bounded by every underlying source and coverage validity.
- Scheduled workflows never prepare, inspect, install, refresh, extend, or repin evidence.
- Manual-only, cash-only, paper-validation, drawdown, and options gates remain unchanged.

## Command surface

The CLI adds a top-level `evidence` command with three subcommands.

### `evidence prepare --json`

`prepare` loads the currently pinned eligible universe, records the current installed release digest as its compare-and-swap parent, and retrieves every required public discovery source from a compiled exact-source catalog. It writes a private proposal below:

```text
<STOCK_MONITOR_HOME>/.stock-monitor/evidence-proposals/<proposal_sha256>/
```

The directory contains canonical `proposal.json`, content-addressed raw source envelopes, and a reviewer-input template. The proposal has kind `UNREVIEWED_EVIDENCE_PROPOSAL`. It contains retrieval metadata and source failures, but no `reviewed_at`, `review_by`, `CONFIRMED_CLEAR`, `complete=true` for a relevant event class, or runtime authority.

`prepare` may persist a partial proposal when a source is unavailable. Its safe result is then `PREPARED_BLOCKED`, with allowlisted reason codes and no candidate authority. Re-running with identical canonical inputs is idempotent; a proposal identifier can never be rebound to different bytes.

### `evidence inspect --proposal <sha256> --review-input <path> --json`

`inspect` performs no network access. It securely reads the named proposal and a reviewer-authored structured input, verifies every referenced observation against the proposal bytes, builds a candidate release tree below:

```text
<STOCK_MONITOR_HOME>/.stock-monitor/evidence-candidates/<candidate_sha256>/
```

The reviewer input is not itself proof of human review. It may classify exact observed records, adverse tags, conflicts, known dates, and incomplete coverage. It cannot request `CONFIRMED_CLEAR` unless the compiled policy names an exact clear-capable authority bundle for the subject and event class. The initial policy names none.

`inspect` validates the resulting release with the existing `load_evidence_release()` invariants using the candidate's own digest, but it never changes the compiled current digest or active evidence tree. Its output includes the proposal digest, review-input digest, candidate digest, intended release digest, review window, symbols, coverage summary, and blocking conditions. This exact output is what the human reviews before independently repinning the release digest.

### `evidence install --candidate <sha256> --json`

`install` performs no network access. It refuses unless all of these conditions hold:

1. the candidate wrapper, inventory, and release tree are canonical, confined regular files with exact hashes;
2. the candidate release digest equals the already compiled `CURRENT_EVIDENCE_RELEASE_SHA256`;
3. the current installed release digest equals the candidate's recorded parent digest;
4. the eligible universe still equals the candidate's universe digest and subject partition;
5. `load_evidence_release()` validates the candidate at the actual installation time;
6. the release remains valid after installation begins and for no more than 24 hours;
7. no source, subject, or manifest target is a symlink, FIFO, device, multiply linked file, or oversized input.

Installation copies immutable content-addressed source artifacts first, then subject children, and replaces `current.json` last. A crash before the final replacement leaves verification failed closed. Installation preserves original retrieval, check, review, and expiration timestamps. It never extends them. A repeated install of the same already-active release is an idempotent readback; a stale-parent attempt is rejected.

## Source authority catalog

A focused `evidence_authorities.py` module becomes the single policy source for subject-scoped evidence URLs. Each immutable entry binds:

- symbol and, for stocks, exact ten-digit issuer CIK;
- exact requested URL and any exact same-origin canonical redirect target;
- exact publisher identity;
- source role;
- event class;
- purpose, initially `FACT_DISCOVERY` only;
- timestamp derivation rule;
- whether the source is clear-capable, initially `False` for every entry.

The existing FAQ and general product URLs are not clear authorities. The first catalog may collect these official observed-event surfaces:

- AAPL: [SEC submissions](https://data.sec.gov/submissions/CIK0000320193.json), [Apple Investor Relations](https://investor.apple.com/investor-relations/default.aspx), and [Apple Newsroom RSS](https://www.apple.com/newsroom/rss-feed.rss).
- AMD: [SEC submissions](https://data.sec.gov/submissions/CIK0000002488.json), the [AMD investor calendar](https://ir.amd.com/news-events/ir-calendar), and [AMD press-release RSS](https://ir.amd.com/news-events/press-releases/rss).
- NVDA: [SEC submissions](https://data.sec.gov/submissions/CIK0001045810.json), [NVIDIA event RSS](https://investor.nvidia.com/rss/Event.aspx?LanguageId=1), and [NVIDIA press-release RSS](https://nvidianews.nvidia.com/cats/press_release.xml).
- QQQ: [Invesco QQQ](https://www.invesco.com/qqq-etf/en/home.html) and the [Invesco newsroom](https://www.invesco.com/us/en/newsroom.html).
- SPY: the [State Street SPY page](https://www.ssga.com/us/en/intermediary/etfs/state-street-spdr-sp-500-etf-trust-spy) and [State Street authorized-participant resources](https://www.ssga.com/us/en/intermediary/resources/authorized-participants).
- VTI: the [Vanguard VTI page](https://investor.vanguard.com/investment-products/etfs/profile/vti) and [Vanguard pressroom](https://corporate.vanguard.com/content/corporatesite/us/en/corp/who-we-are/pressroom/index.html.html).
- XLK: the [State Street XLK page](https://www.ssga.com/us/en/intermediary/etfs/state-street-technology-select-sector-spdr-etf-xlk) and [State Street authorized-participant resources](https://www.ssga.com/us/en/intermediary/resources/authorized-participants).

Public index-provider pages may be added as observed-event sources only after exact URL, publisher, identifier, response-shape, and timestamp rules are fixture-tested. Entitled SFTP, POST-authenticated, email-only, or API-key feeds are outside this GET-only implementation.

The active loader continues to reject clear coverage unless the subject/event class requires an exact complete authority bundle. Membership of one allowlisted source is not enough. Identical URL/content bytes cannot be reused under separate observation IDs to pretend that facts and complete coverage came from independent sources.

## Provider boundary

A separate `providers/evidence_sources.py` adapter fetches compiled evidence catalog entries. It does not widen `ReferenceClient`, whose configured roles are for operational calendar and halt checks.

The adapter:

- accepts only an exact catalog entry, exact `HttpGetClient`, and injected clock;
- constructs an `EgressPolicy` from the catalog's exact official hosts;
- calls only `get_with_redirects()` with bounded attempts, response size, content type, and exact-target validation;
- does not accept caller-supplied publisher, source role, publication time, or source type;
- derives a content-addressed observation ID from immutable source identity, retrieval time, and bytes;
- returns an immutable proposal observation, not `EvidenceSourceBinding` or reviewed authority;
- redacts URL query values and response details from public errors.

SEC collection continues through `SecClient` and its shared fair-access governor. Its source labels are corrected so submissions emit `SEC_SUBMISSIONS_METADATA` and archive documents emit `SEC_FILING_METADATA`, matching the evidence loader. This is a compatibility correction, not new authority: a single SEC filing can never attest exhaustive event coverage.

## Proposal and candidate schemas

All JSON is canonical UTF-8 with sorted keys, compact separators, duplicate-key rejection, integer schema versions, and lowercase SHA-256 digests.

`proposal.json` includes exactly:

- schema version and kind;
- creation time;
- parent release digest;
- universe digest;
- ordered subject identities;
- ordered source observation metadata and artifact paths;
- ordered source failure codes;
- a digest of the reviewer-input template.

The template defaults the relevant event class to incomplete `UNKNOWN` and the opposite product class to complete `NOT_APPLICABLE`. It contains no reviewer identity or approval claim.

The reviewer input may add only closed-taxonomy fields already accepted by `EvidenceRecord` and `EvidenceCoverageAttestation`. Every fact references exact proposal observations. Unknown coverage may use a healthy observation without asserting completeness. A known event inside the hold is preserved as a dated record and resolves to `OVERLAP` even while overall source completeness remains unknown.

The candidate wrapper includes exactly:

- schema version and kind `EVIDENCE_RELEASE_CANDIDATE`;
- proposal digest;
- review-input digest;
- parent release digest;
- universe digest;
- intended release digest;
- canonical inventory of every release-tree path and digest.

Candidate directories are immutable once addressed. Any collision with different bytes is an integrity error.

## File-system safety

Proposal, review-input, candidate, and install readers follow the existing evidence loader's secure-read discipline:

- resolve from an absolute operator root;
- reject parent or leaf symlinks;
- open with `O_NOFOLLOW` where available;
- require one-link regular files owned by the current user;
- enforce per-file and aggregate byte limits before parsing;
- reject absolute paths, backslashes, empty components, `.` and `..`;
- use private `0700` directories and `0600` files;
- write temporary files in the destination directory, `fsync`, and replace atomically;
- never follow a candidate-provided destination path.

The installer owns the fixed destination mapping. Candidate metadata cannot select repository files.

## Safe output and failures

CLI JSON exposes only allowlisted status, digests, timestamps, symbols, coverage states, reason codes, and local proposal/candidate identifiers. It never prints source bodies, provider payloads, environment values, API credentials, SEC contact information, arbitrary exception text, or reviewer-input free text.

Expected collection or integrity failures map to existing data/verification exits. Unexpected exceptions retain the generic `INTERNAL ERROR` boundary.

## Testing

Implementation is test-driven. Each production behavior is preceded by a failing test that is observed to fail for the intended reason.

Required coverage includes:

- exact subject/purpose/source catalog validation and an empty clear-capable set;
- SEC timestamp-label compatibility with `EvidenceSourceBinding`;
- exact-host GET-only source retrieval, redirect containment, immutable metadata, and caller-injection rejection;
- deterministic proposal hashing, private atomic writes, collision detection, partial-source blocking, and proof that prepare cannot touch active evidence or pins;
- review-input closed schema, proposal observation binding, no-clear enforcement, overlap preservation, and candidate validation;
- secure candidate reads, compare-and-swap parent checks, pin-required offline install, manifest-last behavior, idempotent readback, and timestamp preservation;
- FIFO, symlink, hardlink, traversal, oversized input, duplicate JSON key, and race/tamper adversarial cases;
- safe CLI JSON and redaction;
- permanent no-brokerage/no-order, network, scheduled-authority, and unattended-environment regressions.

## Activation consequence

After this workflow is implemented, the expected current result is still blocked:

- public sources can contribute observed events and explicit overlaps;
- public-source silence remains `UNKNOWN` for the broad event classes;
- `_CLEAR_COVERAGE_AUTHORITIES` remains empty;
- Phase 1 and external scheduling remain unactivated;
- external schedule count remains zero.

Activation requires a separate, explicit decision supported by new evidence: integrate proven exhaustive authorities for every mandatory event scope, or narrow the event policy and approve that changed risk meaning. Only after that decision, a human-reviewed release repin, provider `READY`, Phase 1 activation, and reviewed manual and scheduled-mode smoke reports may the three schedules be created atomically.
