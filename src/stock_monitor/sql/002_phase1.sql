CREATE TABLE phase1_validation_windows (
    id INTEGER PRIMARY KEY CHECK(typeof(id) = 'integer' AND id > 0),
    window_id TEXT NOT NULL COLLATE BINARY UNIQUE
        CHECK(length(window_id) = 64 AND window_id NOT GLOB '*[^0-9a-f]*'),
    started_session TEXT NOT NULL
        CHECK(length(started_session) = 10 AND started_session = strftime('%Y-%m-%d', started_session)),
    starting_capital_micros INTEGER NOT NULL
        CHECK(typeof(starting_capital_micros) = 'integer' AND starting_capital_micros = 5000000000),
    started_at TEXT NOT NULL
        CHECK(length(started_at) = 27 AND substr(started_at, 1, 19) = strftime('%Y-%m-%dT%H:%M:%S', started_at) AND substr(started_at, 20, 1) = '.' AND substr(started_at, 21, 6) NOT GLOB '*[^0-9]*' AND substr(started_at, 27, 1) = 'Z'),
    received_at TEXT NOT NULL
        CHECK(length(received_at) = 27 AND substr(received_at, 1, 19) = strftime('%Y-%m-%dT%H:%M:%S', received_at) AND substr(received_at, 20, 1) = '.' AND substr(received_at, 21, 6) NOT GLOB '*[^0-9]*' AND substr(received_at, 27, 1) = 'Z'),
    calendar_digest TEXT NOT NULL COLLATE BINARY
        CHECK(length(calendar_digest) = 64 AND calendar_digest NOT GLOB '*[^0-9a-f]*'),
    source_digest TEXT NOT NULL COLLATE BINARY
        CHECK(length(source_digest) = 64 AND source_digest NOT GLOB '*[^0-9a-f]*'),
    singleton_key INTEGER NOT NULL DEFAULT 1 UNIQUE
        CHECK(typeof(singleton_key) = 'integer' AND singleton_key = 1),
    CHECK(started_at <= received_at)
) STRICT;

CREATE TABLE phase1_source_payloads (
    id INTEGER PRIMARY KEY CHECK(typeof(id) = 'integer' AND id > 0),
    source_observation_id INTEGER NOT NULL UNIQUE
        CHECK(typeof(source_observation_id) = 'integer' AND source_observation_id > 0),
    payload_sha256 TEXT NOT NULL COLLATE BINARY
        CHECK(length(payload_sha256) = 64 AND payload_sha256 NOT GLOB '*[^0-9a-f]*'),
    source_payload BLOB NOT NULL
        CHECK(typeof(source_payload) = 'blob' AND length(source_payload) > 0),
    recorded_at TEXT NOT NULL
        CHECK(length(recorded_at) = 27 AND substr(recorded_at, 1, 19) = strftime('%Y-%m-%dT%H:%M:%S', recorded_at) AND substr(recorded_at, 20, 1) = '.' AND substr(recorded_at, 21, 6) NOT GLOB '*[^0-9]*' AND substr(recorded_at, 27, 1) = 'Z'),
    record_sha256 TEXT NOT NULL COLLATE BINARY
        CHECK(length(record_sha256) = 64 AND record_sha256 NOT GLOB '*[^0-9a-f]*'),
    UNIQUE(source_observation_id, payload_sha256),
    FOREIGN KEY(source_observation_id) REFERENCES source_observations(id)
        ON UPDATE RESTRICT ON DELETE RESTRICT
) STRICT;

CREATE TABLE phase1_publication_manifests (
    id INTEGER PRIMARY KEY CHECK(typeof(id) = 'integer' AND id > 0),
    publication_report_id INTEGER NOT NULL UNIQUE
        CHECK(typeof(publication_report_id) = 'integer' AND publication_report_id > 0),
    manifest_digest TEXT NOT NULL COLLATE BINARY
        CHECK(length(manifest_digest) = 64 AND manifest_digest NOT GLOB '*[^0-9a-f]*'),
    candidate_source_observation_ids_json TEXT NOT NULL
        CHECK(length(candidate_source_observation_ids_json) > 2),
    candidate_context_digests_json TEXT NOT NULL
        CHECK(length(candidate_context_digests_json) > 2),
    candidate_subjects_json TEXT NOT NULL
        CHECK(length(candidate_subjects_json) > 2),
    source_observation_ids_json TEXT NOT NULL
        CHECK(length(source_observation_ids_json) > 2),
    record_sha256 TEXT NOT NULL COLLATE BINARY
        CHECK(length(record_sha256) = 64 AND record_sha256 NOT GLOB '*[^0-9a-f]*'),
    FOREIGN KEY(publication_report_id) REFERENCES reports(id)
        ON UPDATE RESTRICT ON DELETE RESTRICT
) STRICT;

CREATE TABLE phase1_publication_fetch_pages (
    id INTEGER PRIMARY KEY CHECK(typeof(id) = 'integer' AND id > 0),
    publication_report_id INTEGER NOT NULL
        CHECK(typeof(publication_report_id) = 'integer' AND publication_report_id > 0),
    fetch_manifest_ordinal INTEGER NOT NULL
        CHECK(typeof(fetch_manifest_ordinal) = 'integer' AND fetch_manifest_ordinal > 0),
    fetch_manifest_digest TEXT NOT NULL COLLATE BINARY
        CHECK(length(fetch_manifest_digest) = 64 AND fetch_manifest_digest NOT GLOB '*[^0-9a-f]*'),
    collection_name TEXT NOT NULL CHECK(collection_name IN ('bars', 'quotes', 'trades')),
    requested_symbols_json TEXT NOT NULL CHECK(length(requested_symbols_json) > 2),
    request_digest TEXT NOT NULL COLLATE BINARY
        CHECK(length(request_digest) = 64 AND request_digest NOT GLOB '*[^0-9a-f]*'),
    page_ordinal INTEGER NOT NULL
        CHECK(typeof(page_ordinal) = 'integer' AND page_ordinal > 0),
    external_source_observation_id TEXT NOT NULL COLLATE BINARY
        CHECK(length(external_source_observation_id) > 0),
    source_type TEXT NOT NULL COLLATE BINARY
        CHECK(length(source_type) > 0),
    request_url TEXT NOT NULL COLLATE BINARY
        CHECK(length(request_url) > 0),
    request_page_token TEXT COLLATE BINARY
        CHECK(request_page_token IS NULL OR length(request_page_token) > 0),
    next_page_token TEXT COLLATE BINARY
        CHECK(next_page_token IS NULL OR length(next_page_token) > 0),
    payload_sha256 TEXT NOT NULL COLLATE BINARY
        CHECK(length(payload_sha256) = 64 AND payload_sha256 NOT GLOB '*[^0-9a-f]*'),
    terminal INTEGER NOT NULL CHECK(typeof(terminal) = 'integer' AND terminal IN (0, 1)),
    record_sha256 TEXT NOT NULL COLLATE BINARY
        CHECK(length(record_sha256) = 64 AND record_sha256 NOT GLOB '*[^0-9a-f]*'),
    UNIQUE(publication_report_id, fetch_manifest_ordinal, page_ordinal),
    UNIQUE(publication_report_id, fetch_manifest_digest, page_ordinal),
    UNIQUE(publication_report_id, external_source_observation_id),
    UNIQUE(
        publication_report_id,
        external_source_observation_id,
        fetch_manifest_digest,
        page_ordinal
    ),
    FOREIGN KEY(publication_report_id)
        REFERENCES phase1_publication_manifests(publication_report_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT
) STRICT;

CREATE TABLE phase1_publication_facts (
    id INTEGER PRIMARY KEY CHECK(typeof(id) = 'integer' AND id > 0),
    publication_report_id INTEGER NOT NULL
        CHECK(typeof(publication_report_id) = 'integer' AND publication_report_id > 0),
    fact_ordinal INTEGER NOT NULL
        CHECK(typeof(fact_ordinal) = 'integer' AND fact_ordinal > 0),
    candidate_symbol TEXT NOT NULL COLLATE BINARY
        CHECK(length(candidate_symbol) > 0 AND candidate_symbol = upper(candidate_symbol)),
    observation_kind TEXT NOT NULL CHECK(observation_kind IN ('BAR', 'QUOTE', 'TRADE')),
    symbol TEXT NOT NULL COLLATE BINARY
        CHECK(length(symbol) > 0 AND symbol = upper(symbol)),
    feed TEXT NOT NULL COLLATE BINARY CHECK(length(feed) > 0),
    external_source_observation_id TEXT NOT NULL COLLATE BINARY
        CHECK(length(external_source_observation_id) > 0),
    page_ordinal INTEGER NOT NULL
        CHECK(typeof(page_ordinal) = 'integer' AND page_ordinal > 0),
    source_item_ordinal INTEGER NOT NULL
        CHECK(typeof(source_item_ordinal) = 'integer' AND source_item_ordinal > 0),
    source_item_path TEXT NOT NULL COLLATE BINARY
        CHECK(length(source_item_path) > 0 AND length(source_item_path) <= 512
            AND source_item_path GLOB '$.*'),
    page_payload_sha256 TEXT NOT NULL COLLATE BINARY
        CHECK(length(page_payload_sha256) = 64 AND page_payload_sha256 NOT GLOB '*[^0-9a-f]*'),
    normalized_fields_digest TEXT NOT NULL COLLATE BINARY
        CHECK(length(normalized_fields_digest) = 64 AND normalized_fields_digest NOT GLOB '*[^0-9a-f]*'),
    fetch_manifest_digest TEXT NOT NULL COLLATE BINARY
        CHECK(length(fetch_manifest_digest) = 64 AND fetch_manifest_digest NOT GLOB '*[^0-9a-f]*'),
    record_sha256 TEXT NOT NULL COLLATE BINARY
        CHECK(length(record_sha256) = 64 AND record_sha256 NOT GLOB '*[^0-9a-f]*'),
    UNIQUE(publication_report_id, fact_ordinal),
    UNIQUE(publication_report_id, candidate_symbol, external_source_observation_id, source_item_path),
    FOREIGN KEY(publication_report_id)
        REFERENCES phase1_publication_manifests(publication_report_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
    FOREIGN KEY(
        publication_report_id,
        external_source_observation_id,
        fetch_manifest_digest,
        page_ordinal
    ) REFERENCES phase1_publication_fetch_pages(
        publication_report_id,
        external_source_observation_id,
        fetch_manifest_digest,
        page_ordinal
    )
        ON UPDATE RESTRICT ON DELETE RESTRICT
) STRICT;

CREATE TABLE phase1_signals (
    id INTEGER PRIMARY KEY CHECK(typeof(id) = 'integer' AND id > 0),
    signal_id TEXT NOT NULL COLLATE BINARY UNIQUE,
    validation_window_id TEXT NOT NULL COLLATE BINARY,
    symbol TEXT NOT NULL COLLATE BINARY
        CHECK(length(symbol) > 0 AND symbol = upper(symbol)),
    subject_kind TEXT NOT NULL CHECK(subject_kind IN ('STOCK', 'ETF')),
    issuer_cik TEXT COLLATE BINARY,
    role TEXT NOT NULL CHECK(role IN ('PRIMARY', 'WATCHLIST_SHADOW')),
    publication_session TEXT NOT NULL
        CHECK(length(publication_session) = 10 AND publication_session = strftime('%Y-%m-%d', publication_session)),
    maximum_entry_micros INTEGER NOT NULL
        CHECK(typeof(maximum_entry_micros) = 'integer' AND maximum_entry_micros > 0),
    recommended_stop_micros INTEGER NOT NULL
        CHECK(typeof(recommended_stop_micros) = 'integer' AND recommended_stop_micros > 0),
    target_micros INTEGER NOT NULL
        CHECK(typeof(target_micros) = 'integer' AND target_micros > 0),
    planned_shares INTEGER NOT NULL
        CHECK(typeof(planned_shares) = 'integer' AND planned_shares >= 0),
    tick_size_micros INTEGER NOT NULL
        CHECK(typeof(tick_size_micros) = 'integer' AND tick_size_micros > 0),
    trigger_price_micros INTEGER NOT NULL
        CHECK(typeof(trigger_price_micros) = 'integer' AND trigger_price_micros > 0),
    publication_report_id INTEGER NOT NULL
        CHECK(typeof(publication_report_id) = 'integer' AND publication_report_id > 0),
    publication_rank INTEGER NOT NULL
        CHECK(typeof(publication_rank) = 'integer' AND publication_rank BETWEEN 1 AND 3),
    publication_source_digest TEXT NOT NULL COLLATE BINARY
        CHECK(length(publication_source_digest) = 64 AND publication_source_digest NOT GLOB '*[^0-9a-f]*'),
    publication_state_digest TEXT NOT NULL COLLATE BINARY
        CHECK(length(publication_state_digest) = 64 AND publication_state_digest NOT GLOB '*[^0-9a-f]*'),
    publication_content_digest TEXT NOT NULL COLLATE BINARY
        CHECK(length(publication_content_digest) = 64 AND publication_content_digest NOT GLOB '*[^0-9a-f]*'),
    publication_observation_set_digest TEXT NOT NULL COLLATE BINARY
        CHECK(length(publication_observation_set_digest) = 64 AND publication_observation_set_digest NOT GLOB '*[^0-9a-f]*'),
    publication_decision_digest TEXT NOT NULL COLLATE BINARY
        CHECK(length(publication_decision_digest) = 64 AND publication_decision_digest NOT GLOB '*[^0-9a-f]*'),
    primary_plan_digest TEXT NOT NULL COLLATE BINARY
        CHECK(length(primary_plan_digest) = 64 AND primary_plan_digest NOT GLOB '*[^0-9a-f]*'),
    policy_digest TEXT NOT NULL COLLATE BINARY
        CHECK(length(policy_digest) = 64 AND policy_digest NOT GLOB '*[^0-9a-f]*'),
    calendar_digest TEXT NOT NULL COLLATE BINARY
        CHECK(length(calendar_digest) = 64 AND calendar_digest NOT GLOB '*[^0-9a-f]*'),
    published_at TEXT NOT NULL
        CHECK(length(published_at) = 27 AND substr(published_at, 1, 19) = strftime('%Y-%m-%dT%H:%M:%S', published_at) AND substr(published_at, 20, 1) = '.' AND substr(published_at, 21, 6) NOT GLOB '*[^0-9]*' AND substr(published_at, 27, 1) = 'Z'),
    received_at TEXT NOT NULL
        CHECK(length(received_at) = 27 AND substr(received_at, 1, 19) = strftime('%Y-%m-%dT%H:%M:%S', received_at) AND substr(received_at, 20, 1) = '.' AND substr(received_at, 21, 6) NOT GLOB '*[^0-9]*' AND substr(received_at, 27, 1) = 'Z'),
    record_sha256 TEXT NOT NULL COLLATE BINARY
        CHECK(length(record_sha256) = 64 AND record_sha256 NOT GLOB '*[^0-9a-f]*'),
    CHECK(recommended_stop_micros < maximum_entry_micros),
    CHECK(trigger_price_micros <= maximum_entry_micros),
    CHECK((role = 'PRIMARY' AND planned_shares > 0)
        OR (role = 'WATCHLIST_SHADOW' AND planned_shares = 0)),
    CHECK((subject_kind = 'STOCK' AND issuer_cik IS NOT NULL
            AND length(issuer_cik) = 10
            AND issuer_cik NOT GLOB '*[^0-9]*')
        OR (subject_kind = 'ETF' AND issuer_cik IS NULL)),
    CHECK(published_at <= received_at),
    FOREIGN KEY(validation_window_id) REFERENCES phase1_validation_windows(window_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
    FOREIGN KEY(publication_report_id) REFERENCES reports(id)
        ON UPDATE RESTRICT ON DELETE RESTRICT
) STRICT;

CREATE INDEX phase1_signals_symbol_session
ON phase1_signals(symbol, publication_session, received_at);

CREATE UNIQUE INDEX phase1_signals_one_primary_per_session
ON phase1_signals(publication_session)
WHERE role = 'PRIMARY';

CREATE TABLE phase1_signal_evidence_reviews (
    id INTEGER PRIMARY KEY CHECK(typeof(id) = 'integer' AND id > 0),
    evidence_id TEXT NOT NULL COLLATE BINARY UNIQUE
        CHECK(length(evidence_id) = 64 AND evidence_id NOT GLOB '*[^0-9a-f]*'),
    signal_id TEXT NOT NULL COLLATE BINARY,
    review_at TEXT NOT NULL
        CHECK(length(review_at) = 27 AND substr(review_at, 1, 19) = strftime('%Y-%m-%dT%H:%M:%S', review_at) AND substr(review_at, 20, 1) = '.' AND substr(review_at, 21, 6) NOT GLOB '*[^0-9]*' AND substr(review_at, 27, 1) = 'Z'),
    registry_source_row_id INTEGER NOT NULL
        CHECK(typeof(registry_source_row_id) = 'integer' AND registry_source_row_id > 0),
    manifest_digest TEXT NOT NULL COLLATE BINARY
        CHECK(length(manifest_digest) = 64 AND manifest_digest NOT GLOB '*[^0-9a-f]*'),
    manifest_bytes BLOB NOT NULL
        CHECK(typeof(manifest_bytes) = 'blob' AND length(manifest_bytes) > 0),
    registry_id TEXT NOT NULL COLLATE BINARY CHECK(length(registry_id) > 0),
    registry_content_hash TEXT NOT NULL COLLATE BINARY
        CHECK(length(registry_content_hash) = 64 AND registry_content_hash NOT GLOB '*[^0-9a-f]*'),
    registry_release_pin TEXT NOT NULL COLLATE BINARY
        CHECK(length(registry_release_pin) = 64 AND registry_release_pin NOT GLOB '*[^0-9a-f]*'),
    bundle_digest TEXT NOT NULL COLLATE BINARY
        CHECK(length(bundle_digest) = 64 AND bundle_digest NOT GLOB '*[^0-9a-f]*'),
    decision_digest TEXT NOT NULL COLLATE BINARY
        CHECK(length(decision_digest) = 64 AND decision_digest NOT GLOB '*[^0-9a-f]*'),
    calendar_digest TEXT NOT NULL COLLATE BINARY
        CHECK(length(calendar_digest) = 64 AND calendar_digest NOT GLOB '*[^0-9a-f]*'),
    source_observation_highwater INTEGER NOT NULL
        CHECK(typeof(source_observation_highwater) = 'integer' AND source_observation_highwater > 0),
    expected_source_observation_count INTEGER NOT NULL
        CHECK(typeof(expected_source_observation_count) = 'integer' AND expected_source_observation_count > 0),
    recorded_at TEXT NOT NULL
        CHECK(length(recorded_at) = 27 AND substr(recorded_at, 1, 19) = strftime('%Y-%m-%dT%H:%M:%S', recorded_at) AND substr(recorded_at, 20, 1) = '.' AND substr(recorded_at, 21, 6) NOT GLOB '*[^0-9]*' AND substr(recorded_at, 27, 1) = 'Z'),
    source_digest TEXT NOT NULL COLLATE BINARY
        CHECK(length(source_digest) = 64 AND source_digest NOT GLOB '*[^0-9a-f]*'),
    record_sha256 TEXT NOT NULL COLLATE BINARY
        CHECK(length(record_sha256) = 64 AND record_sha256 NOT GLOB '*[^0-9a-f]*'),
    UNIQUE(signal_id, review_at),
    CHECK(registry_content_hash = registry_release_pin),
    CHECK(recorded_at = review_at),
    FOREIGN KEY(signal_id) REFERENCES phase1_signals(signal_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
    FOREIGN KEY(registry_source_row_id) REFERENCES source_observations(id)
        ON UPDATE RESTRICT ON DELETE RESTRICT
) STRICT;

CREATE TABLE phase1_signal_evidence_bindings (
    id INTEGER PRIMARY KEY CHECK(typeof(id) = 'integer' AND id > 0),
    evidence_id TEXT NOT NULL COLLATE BINARY,
    binding_ordinal INTEGER NOT NULL
        CHECK(typeof(binding_ordinal) = 'integer' AND binding_ordinal > 0),
    source_observation_row_id INTEGER NOT NULL
        CHECK(typeof(source_observation_row_id) = 'integer' AND source_observation_row_id > 0),
    external_source_observation_id TEXT NOT NULL COLLATE BINARY
        CHECK(length(external_source_observation_id) > 0),
    source_digest TEXT NOT NULL COLLATE BINARY
        CHECK(length(source_digest) = 64 AND source_digest NOT GLOB '*[^0-9a-f]*'),
    record_sha256 TEXT NOT NULL COLLATE BINARY
        CHECK(length(record_sha256) = 64 AND record_sha256 NOT GLOB '*[^0-9a-f]*'),
    UNIQUE(evidence_id, binding_ordinal),
    UNIQUE(evidence_id, source_observation_row_id),
    UNIQUE(evidence_id, external_source_observation_id),
    FOREIGN KEY(evidence_id) REFERENCES phase1_signal_evidence_reviews(evidence_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
    FOREIGN KEY(source_observation_row_id) REFERENCES source_observations(id)
        ON UPDATE RESTRICT ON DELETE RESTRICT
) STRICT;

CREATE TABLE phase1_observation_fetch_manifests (
    id INTEGER PRIMARY KEY CHECK(typeof(id) = 'integer' AND id > 0),
    cohort_id TEXT NOT NULL COLLATE BINARY UNIQUE
        CHECK(length(cohort_id) = 64 AND cohort_id NOT GLOB '*[^0-9a-f]*'),
    signal_id TEXT NOT NULL COLLATE BINARY,
    session_date TEXT NOT NULL
        CHECK(length(session_date) = 10 AND session_date = strftime('%Y-%m-%d', session_date)),
    purpose TEXT NOT NULL CHECK(purpose = 'SIGNAL_LIFECYCLE'),
    collection_name TEXT NOT NULL CHECK(collection_name IN ('bars', 'quotes', 'trades')),
    requested_symbols_json TEXT NOT NULL CHECK(length(requested_symbols_json) > 2),
    request_digest TEXT NOT NULL COLLATE BINARY
        CHECK(length(request_digest) = 64 AND request_digest NOT GLOB '*[^0-9a-f]*'),
    manifest_digest TEXT NOT NULL COLLATE BINARY
        CHECK(length(manifest_digest) = 64 AND manifest_digest NOT GLOB '*[^0-9a-f]*'),
    semantic_manifest_digest TEXT NOT NULL COLLATE BINARY
        CHECK(length(semantic_manifest_digest) = 64 AND semantic_manifest_digest NOT GLOB '*[^0-9a-f]*'),
    terminal INTEGER NOT NULL CHECK(typeof(terminal) = 'integer' AND terminal = 1),
    request_start TEXT NOT NULL
        CHECK(length(request_start) = 27 AND substr(request_start, 1, 19) = strftime('%Y-%m-%dT%H:%M:%S', request_start) AND substr(request_start, 20, 1) = '.' AND substr(request_start, 21, 6) NOT GLOB '*[^0-9]*' AND substr(request_start, 27, 1) = 'Z'),
    request_end TEXT NOT NULL
        CHECK(length(request_end) = 27 AND substr(request_end, 1, 19) = strftime('%Y-%m-%dT%H:%M:%S', request_end) AND substr(request_end, 20, 1) = '.' AND substr(request_end, 21, 6) NOT GLOB '*[^0-9]*' AND substr(request_end, 27, 1) = 'Z'),
    calendar_digest TEXT NOT NULL COLLATE BINARY
        CHECK(length(calendar_digest) = 64 AND calendar_digest NOT GLOB '*[^0-9a-f]*'),
    received_through TEXT NOT NULL
        CHECK(length(received_through) = 27 AND substr(received_through, 1, 19) = strftime('%Y-%m-%dT%H:%M:%S', received_through) AND substr(received_through, 20, 1) = '.' AND substr(received_through, 21, 6) NOT GLOB '*[^0-9]*' AND substr(received_through, 27, 1) = 'Z'),
    source_digest TEXT NOT NULL COLLATE BINARY
        CHECK(length(source_digest) = 64 AND source_digest NOT GLOB '*[^0-9a-f]*'),
    record_sha256 TEXT NOT NULL COLLATE BINARY
        CHECK(length(record_sha256) = 64 AND record_sha256 NOT GLOB '*[^0-9a-f]*'),
    CHECK(request_start < request_end),
    UNIQUE(signal_id, purpose, manifest_digest),
    UNIQUE(signal_id, purpose, semantic_manifest_digest),
    UNIQUE(signal_id, session_date, purpose, collection_name),
    FOREIGN KEY(signal_id) REFERENCES phase1_signals(signal_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT
) STRICT;

CREATE TABLE phase1_observation_fetch_pages (
    id INTEGER PRIMARY KEY CHECK(typeof(id) = 'integer' AND id > 0),
    cohort_id TEXT NOT NULL COLLATE BINARY,
    page_ordinal INTEGER NOT NULL
        CHECK(typeof(page_ordinal) = 'integer' AND page_ordinal > 0),
    source_observation_id INTEGER NOT NULL
        CHECK(typeof(source_observation_id) = 'integer' AND source_observation_id > 0),
    external_source_observation_id TEXT NOT NULL COLLATE BINARY
        CHECK(length(external_source_observation_id) > 0),
    source_type TEXT NOT NULL COLLATE BINARY CHECK(length(source_type) > 0),
    request_url TEXT NOT NULL COLLATE BINARY CHECK(length(request_url) > 0),
    request_page_token TEXT COLLATE BINARY
        CHECK(request_page_token IS NULL OR length(request_page_token) > 0),
    next_page_token TEXT COLLATE BINARY
        CHECK(next_page_token IS NULL OR length(next_page_token) > 0),
    payload_sha256 TEXT NOT NULL COLLATE BINARY
        CHECK(length(payload_sha256) = 64 AND payload_sha256 NOT GLOB '*[^0-9a-f]*'),
    semantic_page_digest TEXT NOT NULL COLLATE BINARY
        CHECK(length(semantic_page_digest) = 64 AND semantic_page_digest NOT GLOB '*[^0-9a-f]*'),
    record_sha256 TEXT NOT NULL COLLATE BINARY
        CHECK(length(record_sha256) = 64 AND record_sha256 NOT GLOB '*[^0-9a-f]*'),
    UNIQUE(cohort_id, page_ordinal),
    UNIQUE(cohort_id, source_observation_id),
    UNIQUE(cohort_id, external_source_observation_id),
    UNIQUE(cohort_id, page_ordinal, source_observation_id),
    FOREIGN KEY(cohort_id) REFERENCES phase1_observation_fetch_manifests(cohort_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
    FOREIGN KEY(source_observation_id) REFERENCES source_observations(id)
        ON UPDATE RESTRICT ON DELETE RESTRICT
) STRICT;

CREATE TABLE phase1_observations (
    id INTEGER PRIMARY KEY CHECK(typeof(id) = 'integer' AND id > 0),
    observation_id TEXT NOT NULL COLLATE BINARY UNIQUE,
    signal_id TEXT NOT NULL COLLATE BINARY,
    source_observation_id INTEGER NOT NULL
        CHECK(typeof(source_observation_id) = 'integer' AND source_observation_id > 0),
    source_item_ordinal INTEGER NOT NULL
        CHECK(typeof(source_item_ordinal) = 'integer' AND source_item_ordinal > 0),
    source_item_path TEXT NOT NULL COLLATE BINARY
        CHECK(length(source_item_path) > 0 AND length(source_item_path) <= 512
            AND (source_item_path = '$' OR source_item_path GLOB '$.*')),
    source_payload_sha256 TEXT NOT NULL COLLATE BINARY
        CHECK(length(source_payload_sha256) = 64 AND source_payload_sha256 NOT GLOB '*[^0-9a-f]*'),
    source_payload BLOB NOT NULL
        CHECK(typeof(source_payload) = 'blob' AND length(source_payload) > 0),
    stream_id TEXT NOT NULL COLLATE BINARY,
    feed TEXT NOT NULL COLLATE BINARY,
    observation_kind TEXT NOT NULL
        CHECK(observation_kind IN ('TRADE', 'QUOTE', 'BAR')),
    session_date TEXT NOT NULL
        CHECK(length(session_date) = 10 AND session_date = strftime('%Y-%m-%d', session_date)),
    source_time TEXT NOT NULL
        CHECK(length(source_time) = 27 AND substr(source_time, 1, 19) = strftime('%Y-%m-%dT%H:%M:%S', source_time) AND substr(source_time, 20, 1) = '.' AND substr(source_time, 21, 6) NOT GLOB '*[^0-9]*' AND substr(source_time, 27, 1) = 'Z'),
    received_at TEXT NOT NULL
        CHECK(length(received_at) = 27 AND substr(received_at, 1, 19) = strftime('%Y-%m-%dT%H:%M:%S', received_at) AND substr(received_at, 20, 1) = '.' AND substr(received_at, 21, 6) NOT GLOB '*[^0-9]*' AND substr(received_at, 27, 1) = 'Z'),
    provider_sequence INTEGER
        CHECK(provider_sequence IS NULL OR (typeof(provider_sequence) = 'integer' AND provider_sequence >= 0)),
    source_ordinal INTEGER NOT NULL
        CHECK(typeof(source_ordinal) = 'integer' AND source_ordinal > 0),
    cohort_ordinal INTEGER NOT NULL
        CHECK(typeof(cohort_ordinal) = 'integer' AND cohort_ordinal > 0),
    trade_price_micros INTEGER
        CHECK(trade_price_micros IS NULL OR (typeof(trade_price_micros) = 'integer' AND trade_price_micros > 0)),
    bid_micros INTEGER
        CHECK(bid_micros IS NULL OR (typeof(bid_micros) = 'integer' AND bid_micros > 0)),
    ask_micros INTEGER
        CHECK(ask_micros IS NULL OR (typeof(ask_micros) = 'integer' AND ask_micros > 0)),
    open_micros INTEGER CHECK(open_micros IS NULL OR (typeof(open_micros) = 'integer' AND open_micros > 0)),
    high_micros INTEGER CHECK(high_micros IS NULL OR (typeof(high_micros) = 'integer' AND high_micros > 0)),
    low_micros INTEGER CHECK(low_micros IS NULL OR (typeof(low_micros) = 'integer' AND low_micros > 0)),
    close_micros INTEGER CHECK(close_micros IS NULL OR (typeof(close_micros) = 'integer' AND close_micros > 0)),
    volume INTEGER CHECK(volume IS NULL OR (typeof(volume) = 'integer' AND volume >= 0)),
    fresh INTEGER NOT NULL CHECK(typeof(fresh) = 'integer' AND fresh IN (0, 1)),
    fetch_cohort_id TEXT NOT NULL COLLATE BINARY
        CHECK(length(fetch_cohort_id) = 64 AND fetch_cohort_id NOT GLOB '*[^0-9a-f]*'),
    fetch_page_ordinal INTEGER NOT NULL
        CHECK(typeof(fetch_page_ordinal) = 'integer' AND fetch_page_ordinal > 0),
    source_digest TEXT NOT NULL COLLATE BINARY
        CHECK(length(source_digest) = 64 AND source_digest NOT GLOB '*[^0-9a-f]*'),
    details_json TEXT NOT NULL,
    CHECK(source_time <= received_at),
    CHECK(length(source_payload) > 0),
    CHECK(
        (observation_kind = 'TRADE' AND trade_price_micros IS NOT NULL
            AND bid_micros IS NULL AND ask_micros IS NULL
            AND open_micros IS NULL AND high_micros IS NULL
            AND low_micros IS NULL AND close_micros IS NULL
            AND volume IS NULL)
        OR (observation_kind = 'QUOTE' AND trade_price_micros IS NULL
            AND bid_micros IS NOT NULL AND ask_micros IS NOT NULL
            AND ask_micros >= bid_micros
            AND open_micros IS NULL AND high_micros IS NULL
            AND low_micros IS NULL AND close_micros IS NULL
            AND volume IS NULL)
        OR (observation_kind = 'BAR' AND trade_price_micros IS NULL
            AND open_micros IS NOT NULL AND high_micros IS NOT NULL
            AND low_micros IS NOT NULL AND close_micros IS NOT NULL
            AND high_micros >= open_micros AND high_micros >= close_micros
            AND low_micros <= open_micros AND low_micros <= close_micros
            AND high_micros >= low_micros
            AND ((bid_micros IS NULL AND ask_micros IS NULL)
                OR (bid_micros IS NOT NULL AND ask_micros IS NOT NULL
                    AND ask_micros >= bid_micros)))
    ),
    UNIQUE(signal_id, session_date, cohort_ordinal),
    UNIQUE(stream_id, source_ordinal),
    UNIQUE(source_observation_id, source_item_ordinal),
    UNIQUE(source_observation_id, source_item_path),
    FOREIGN KEY(signal_id) REFERENCES phase1_signals(signal_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
    FOREIGN KEY(source_observation_id) REFERENCES source_observations(id)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
    FOREIGN KEY(fetch_cohort_id) REFERENCES phase1_observation_fetch_manifests(cohort_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
    FOREIGN KEY(fetch_cohort_id, fetch_page_ordinal, source_observation_id)
        REFERENCES phase1_observation_fetch_pages(cohort_id, page_ordinal, source_observation_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT
) STRICT;

CREATE TABLE phase1_session_completions (
    id INTEGER PRIMARY KEY CHECK(typeof(id) = 'integer' AND id > 0),
    completion_id TEXT NOT NULL COLLATE BINARY UNIQUE
        CHECK(length(completion_id) = 64 AND completion_id NOT GLOB '*[^0-9a-f]*'),
    signal_id TEXT NOT NULL COLLATE BINARY,
    session_date TEXT NOT NULL
        CHECK(length(session_date) = 10 AND session_date = strftime('%Y-%m-%d', session_date)),
    cohort_through_ordinal INTEGER NOT NULL
        CHECK(typeof(cohort_through_ordinal) = 'integer' AND cohort_through_ordinal >= 0),
    expected_observation_count INTEGER NOT NULL
        CHECK(typeof(expected_observation_count) = 'integer' AND expected_observation_count >= 0),
    received_through TEXT NOT NULL
        CHECK(length(received_through) = 27 AND substr(received_through, 1, 19) = strftime('%Y-%m-%dT%H:%M:%S', received_through) AND substr(received_through, 20, 1) = '.' AND substr(received_through, 21, 6) NOT GLOB '*[^0-9]*' AND substr(received_through, 27, 1) = 'Z'),
    completed_at TEXT NOT NULL
        CHECK(length(completed_at) = 27 AND substr(completed_at, 1, 19) = strftime('%Y-%m-%dT%H:%M:%S', completed_at) AND substr(completed_at, 20, 1) = '.' AND substr(completed_at, 21, 6) NOT GLOB '*[^0-9]*' AND substr(completed_at, 27, 1) = 'Z'),
    source_digest TEXT NOT NULL COLLATE BINARY
        CHECK(length(source_digest) = 64 AND source_digest NOT GLOB '*[^0-9a-f]*'),
    CHECK(received_through <= completed_at),
    UNIQUE(signal_id, session_date),
    FOREIGN KEY(signal_id) REFERENCES phase1_signals(signal_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT
) STRICT;

CREATE TABLE phase1_session_late_evidence (
    id INTEGER PRIMARY KEY CHECK(typeof(id) = 'integer' AND id > 0),
    late_evidence_id TEXT NOT NULL COLLATE BINARY UNIQUE
        CHECK(length(late_evidence_id) = 64 AND late_evidence_id NOT GLOB '*[^0-9a-f]*'),
    completion_id TEXT NOT NULL COLLATE BINARY,
    signal_id TEXT NOT NULL COLLATE BINARY,
    session_date TEXT NOT NULL
        CHECK(length(session_date) = 10 AND session_date = strftime('%Y-%m-%d', session_date)),
    collection_name TEXT NOT NULL CHECK(collection_name IN ('trades', 'quotes')),
    request_start TEXT NOT NULL
        CHECK(length(request_start) = 27 AND substr(request_start, 1, 19) = strftime('%Y-%m-%dT%H:%M:%S', request_start) AND substr(request_start, 20, 1) = '.' AND substr(request_start, 21, 6) NOT GLOB '*[^0-9]*' AND substr(request_start, 27, 1) = 'Z'),
    request_end TEXT NOT NULL
        CHECK(length(request_end) = 27 AND substr(request_end, 1, 19) = strftime('%Y-%m-%dT%H:%M:%S', request_end) AND substr(request_end, 20, 1) = '.' AND substr(request_end, 21, 6) NOT GLOB '*[^0-9]*' AND substr(request_end, 27, 1) = 'Z'),
    request_digest TEXT NOT NULL COLLATE BINARY
        CHECK(length(request_digest) = 64 AND request_digest NOT GLOB '*[^0-9a-f]*'),
    manifest_digest TEXT NOT NULL COLLATE BINARY
        CHECK(length(manifest_digest) = 64 AND manifest_digest NOT GLOB '*[^0-9a-f]*'),
    semantic_manifest_digest TEXT NOT NULL COLLATE BINARY
        CHECK(length(semantic_manifest_digest) = 64 AND semantic_manifest_digest NOT GLOB '*[^0-9a-f]*'),
    received_through TEXT NOT NULL
        CHECK(length(received_through) = 27 AND substr(received_through, 1, 19) = strftime('%Y-%m-%dT%H:%M:%S', received_through) AND substr(received_through, 20, 1) = '.' AND substr(received_through, 21, 6) NOT GLOB '*[^0-9]*' AND substr(received_through, 27, 1) = 'Z'),
    invalidated_at TEXT NOT NULL
        CHECK(length(invalidated_at) = 27 AND substr(invalidated_at, 1, 19) = strftime('%Y-%m-%dT%H:%M:%S', invalidated_at) AND substr(invalidated_at, 20, 1) = '.' AND substr(invalidated_at, 21, 6) NOT GLOB '*[^0-9]*' AND substr(invalidated_at, 27, 1) = 'Z'),
    source_digest TEXT NOT NULL COLLATE BINARY
        CHECK(length(source_digest) = 64 AND source_digest NOT GLOB '*[^0-9a-f]*'),
    record_sha256 TEXT NOT NULL COLLATE BINARY
        CHECK(length(record_sha256) = 64 AND record_sha256 NOT GLOB '*[^0-9a-f]*'),
    CHECK(request_start < request_end),
    CHECK(received_through <= invalidated_at),
    UNIQUE(completion_id, semantic_manifest_digest),
    FOREIGN KEY(completion_id) REFERENCES phase1_session_completions(completion_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
    FOREIGN KEY(signal_id) REFERENCES phase1_signals(signal_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT
) STRICT;

CREATE TABLE phase1_session_late_evidence_pages (
    id INTEGER PRIMARY KEY CHECK(typeof(id) = 'integer' AND id > 0),
    late_evidence_id TEXT NOT NULL COLLATE BINARY,
    page_ordinal INTEGER NOT NULL
        CHECK(typeof(page_ordinal) = 'integer' AND page_ordinal > 0),
    source_observation_id INTEGER NOT NULL
        CHECK(typeof(source_observation_id) = 'integer' AND source_observation_id > 0),
    external_source_observation_id TEXT NOT NULL COLLATE BINARY
        CHECK(length(external_source_observation_id) > 0),
    source_type TEXT NOT NULL COLLATE BINARY CHECK(length(source_type) > 0),
    request_url TEXT NOT NULL COLLATE BINARY CHECK(length(request_url) > 0),
    request_page_token TEXT COLLATE BINARY
        CHECK(request_page_token IS NULL OR length(request_page_token) > 0),
    next_page_token TEXT COLLATE BINARY
        CHECK(next_page_token IS NULL OR length(next_page_token) > 0),
    payload_sha256 TEXT NOT NULL COLLATE BINARY
        CHECK(length(payload_sha256) = 64 AND payload_sha256 NOT GLOB '*[^0-9a-f]*'),
    semantic_page_digest TEXT NOT NULL COLLATE BINARY
        CHECK(length(semantic_page_digest) = 64 AND semantic_page_digest NOT GLOB '*[^0-9a-f]*'),
    record_sha256 TEXT NOT NULL COLLATE BINARY
        CHECK(length(record_sha256) = 64 AND record_sha256 NOT GLOB '*[^0-9a-f]*'),
    UNIQUE(late_evidence_id, page_ordinal),
    UNIQUE(late_evidence_id, source_observation_id),
    UNIQUE(late_evidence_id, external_source_observation_id),
    FOREIGN KEY(late_evidence_id)
        REFERENCES phase1_session_late_evidence(late_evidence_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
    FOREIGN KEY(source_observation_id) REFERENCES source_observations(id)
        ON UPDATE RESTRICT ON DELETE RESTRICT
) STRICT;

CREATE TABLE phase1_expiry_deadlines (
    id INTEGER PRIMARY KEY CHECK(typeof(id) = 'integer' AND id > 0),
    expiry_source_id TEXT NOT NULL COLLATE BINARY UNIQUE
        CHECK(length(expiry_source_id) = 64 AND expiry_source_id NOT GLOB '*[^0-9a-f]*'),
    signal_id TEXT NOT NULL COLLATE BINARY,
    deadline_session TEXT NOT NULL
        CHECK(length(deadline_session) = 10 AND deadline_session = strftime('%Y-%m-%d', deadline_session)),
    deadline_at TEXT NOT NULL
        CHECK(length(deadline_at) = 27 AND substr(deadline_at, 1, 19) = strftime('%Y-%m-%dT%H:%M:%S', deadline_at) AND substr(deadline_at, 20, 1) = '.' AND substr(deadline_at, 21, 6) NOT GLOB '*[^0-9]*' AND substr(deadline_at, 27, 1) = 'Z'),
    observed_at TEXT NOT NULL
        CHECK(length(observed_at) = 27 AND substr(observed_at, 1, 19) = strftime('%Y-%m-%dT%H:%M:%S', observed_at) AND substr(observed_at, 20, 1) = '.' AND substr(observed_at, 21, 6) NOT GLOB '*[^0-9]*' AND substr(observed_at, 27, 1) = 'Z'),
    calendar_digest TEXT NOT NULL COLLATE BINARY
        CHECK(length(calendar_digest) = 64 AND calendar_digest NOT GLOB '*[^0-9a-f]*'),
    deadline_session_manifest_json TEXT NOT NULL
        CHECK(length(deadline_session_manifest_json) > 2
            AND json_valid(deadline_session_manifest_json)),
    completion_terminal_cursor INTEGER
        CHECK(completion_terminal_cursor IS NULL OR (typeof(completion_terminal_cursor) = 'integer' AND completion_terminal_cursor > 0)),
    completion_source_highwater INTEGER NOT NULL
        CHECK(typeof(completion_source_highwater) = 'integer' AND completion_source_highwater >= 0),
    expected_completion_count INTEGER NOT NULL
        CHECK(typeof(expected_completion_count) = 'integer' AND expected_completion_count = 0),
    evidence_review_highwater INTEGER NOT NULL
        CHECK(typeof(evidence_review_highwater) = 'integer' AND evidence_review_highwater >= 0),
    expected_positive_evidence_count INTEGER NOT NULL
        CHECK(typeof(expected_positive_evidence_count) = 'integer'
            AND expected_positive_evidence_count = 0),
    source_digest TEXT NOT NULL COLLATE BINARY
        CHECK(length(source_digest) = 64 AND source_digest NOT GLOB '*[^0-9a-f]*'),
    record_sha256 TEXT NOT NULL COLLATE BINARY
        CHECK(length(record_sha256) = 64 AND record_sha256 NOT GLOB '*[^0-9a-f]*'),
    UNIQUE(signal_id, observed_at),
    CHECK(deadline_at <= observed_at),
    FOREIGN KEY(signal_id) REFERENCES phase1_signals(signal_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT
) STRICT;

CREATE TABLE phase1_exit_reviews (
    id INTEGER PRIMARY KEY CHECK(typeof(id) = 'integer' AND id > 0),
    review_id TEXT NOT NULL COLLATE BINARY UNIQUE
        CHECK(length(review_id) = 64 AND review_id NOT GLOB '*[^0-9a-f]*'),
    signal_id TEXT NOT NULL COLLATE BINARY,
    review_session TEXT NOT NULL
        CHECK(length(review_session) = 10 AND review_session = strftime('%Y-%m-%d', review_session)),
    calendar_digest TEXT NOT NULL COLLATE BINARY
        CHECK(length(calendar_digest) = 64 AND calendar_digest NOT GLOB '*[^0-9a-f]*'),
    query_cutoff TEXT NOT NULL
        CHECK(length(query_cutoff) = 27 AND substr(query_cutoff, 1, 19) = strftime('%Y-%m-%dT%H:%M:%S', query_cutoff) AND substr(query_cutoff, 20, 1) = '.' AND substr(query_cutoff, 21, 6) NOT GLOB '*[^0-9]*' AND substr(query_cutoff, 27, 1) = 'Z'),
    expected_manifest_count INTEGER NOT NULL
        CHECK(typeof(expected_manifest_count) = 'integer' AND expected_manifest_count = 3),
    expected_fact_count INTEGER NOT NULL
        CHECK(typeof(expected_fact_count) = 'integer' AND expected_fact_count >= 15),
    source_observation_highwater INTEGER NOT NULL
        CHECK(typeof(source_observation_highwater) = 'integer' AND source_observation_highwater > 0),
    recorded_at TEXT NOT NULL
        CHECK(length(recorded_at) = 27 AND substr(recorded_at, 1, 19) = strftime('%Y-%m-%dT%H:%M:%S', recorded_at) AND substr(recorded_at, 20, 1) = '.' AND substr(recorded_at, 21, 6) NOT GLOB '*[^0-9]*' AND substr(recorded_at, 27, 1) = 'Z'),
    source_digest TEXT NOT NULL COLLATE BINARY
        CHECK(length(source_digest) = 64 AND source_digest NOT GLOB '*[^0-9a-f]*'),
    record_sha256 TEXT NOT NULL COLLATE BINARY
        CHECK(length(record_sha256) = 64 AND record_sha256 NOT GLOB '*[^0-9a-f]*'),
    UNIQUE(signal_id, review_session),
    CHECK(query_cutoff = recorded_at),
    FOREIGN KEY(signal_id) REFERENCES phase1_signals(signal_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT
) STRICT;

CREATE TABLE phase1_exit_review_manifests (
    id INTEGER PRIMARY KEY CHECK(typeof(id) = 'integer' AND id > 0),
    review_id TEXT NOT NULL COLLATE BINARY,
    purpose TEXT NOT NULL
        CHECK(purpose IN ('DAILY_BAR', 'EXECUTION_BAR', 'QUOTE')),
    collection_name TEXT NOT NULL
        CHECK((purpose IN ('DAILY_BAR', 'EXECUTION_BAR') AND collection_name = 'bars')
            OR (purpose = 'QUOTE' AND collection_name = 'quotes')),
    requested_symbols_json TEXT NOT NULL CHECK(length(requested_symbols_json) > 2),
    request_start TEXT NOT NULL
        CHECK(length(request_start) = 27 AND substr(request_start, 1, 19) = strftime('%Y-%m-%dT%H:%M:%S', request_start) AND substr(request_start, 20, 1) = '.' AND substr(request_start, 21, 6) NOT GLOB '*[^0-9]*' AND substr(request_start, 27, 1) = 'Z'),
    request_end TEXT NOT NULL
        CHECK(length(request_end) = 27 AND substr(request_end, 1, 19) = strftime('%Y-%m-%dT%H:%M:%S', request_end) AND substr(request_end, 20, 1) = '.' AND substr(request_end, 21, 6) NOT GLOB '*[^0-9]*' AND substr(request_end, 27, 1) = 'Z'),
    request_digest TEXT NOT NULL COLLATE BINARY
        CHECK(length(request_digest) = 64 AND request_digest NOT GLOB '*[^0-9a-f]*'),
    manifest_digest TEXT NOT NULL COLLATE BINARY
        CHECK(length(manifest_digest) = 64 AND manifest_digest NOT GLOB '*[^0-9a-f]*'),
    semantic_manifest_digest TEXT NOT NULL COLLATE BINARY
        CHECK(length(semantic_manifest_digest) = 64
            AND semantic_manifest_digest NOT GLOB '*[^0-9a-f]*'),
    terminal INTEGER NOT NULL CHECK(typeof(terminal) = 'integer' AND terminal = 1),
    received_through TEXT NOT NULL
        CHECK(length(received_through) = 27 AND substr(received_through, 1, 19) = strftime('%Y-%m-%dT%H:%M:%S', received_through) AND substr(received_through, 20, 1) = '.' AND substr(received_through, 21, 6) NOT GLOB '*[^0-9]*' AND substr(received_through, 27, 1) = 'Z'),
    expected_page_count INTEGER NOT NULL
        CHECK(typeof(expected_page_count) = 'integer' AND expected_page_count > 0),
    expected_fact_count INTEGER NOT NULL
        CHECK(typeof(expected_fact_count) = 'integer' AND expected_fact_count >= 0),
    source_digest TEXT NOT NULL COLLATE BINARY
        CHECK(length(source_digest) = 64 AND source_digest NOT GLOB '*[^0-9a-f]*'),
    record_sha256 TEXT NOT NULL COLLATE BINARY
        CHECK(length(record_sha256) = 64 AND record_sha256 NOT GLOB '*[^0-9a-f]*'),
    CHECK(request_start < request_end),
    UNIQUE(review_id, purpose),
    UNIQUE(review_id, manifest_digest),
    UNIQUE(review_id, semantic_manifest_digest),
    FOREIGN KEY(review_id) REFERENCES phase1_exit_reviews(review_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT
) STRICT;

CREATE TABLE phase1_exit_review_pages (
    id INTEGER PRIMARY KEY CHECK(typeof(id) = 'integer' AND id > 0),
    review_id TEXT NOT NULL COLLATE BINARY,
    purpose TEXT NOT NULL,
    page_ordinal INTEGER NOT NULL
        CHECK(typeof(page_ordinal) = 'integer' AND page_ordinal > 0),
    source_observation_id INTEGER NOT NULL
        CHECK(typeof(source_observation_id) = 'integer' AND source_observation_id > 0),
    external_source_observation_id TEXT NOT NULL COLLATE BINARY
        CHECK(length(external_source_observation_id) > 0),
    source_type TEXT NOT NULL COLLATE BINARY
        CHECK(source_type IN ('ALPACA_DAILY_BARS', 'ALPACA_INTRADAY_BARS', 'ALPACA_HISTORICAL_QUOTES')),
    request_url TEXT NOT NULL COLLATE BINARY CHECK(length(request_url) > 0),
    request_page_token TEXT COLLATE BINARY
        CHECK(request_page_token IS NULL OR length(request_page_token) > 0),
    next_page_token TEXT COLLATE BINARY
        CHECK(next_page_token IS NULL OR length(next_page_token) > 0),
    payload_sha256 TEXT NOT NULL COLLATE BINARY
        CHECK(length(payload_sha256) = 64 AND payload_sha256 NOT GLOB '*[^0-9a-f]*'),
    source_digest TEXT NOT NULL COLLATE BINARY
        CHECK(length(source_digest) = 64 AND source_digest NOT GLOB '*[^0-9a-f]*'),
    record_sha256 TEXT NOT NULL COLLATE BINARY
        CHECK(length(record_sha256) = 64 AND record_sha256 NOT GLOB '*[^0-9a-f]*'),
    CHECK((purpose = 'DAILY_BAR' AND source_type = 'ALPACA_DAILY_BARS')
        OR (purpose = 'EXECUTION_BAR' AND source_type = 'ALPACA_INTRADAY_BARS')
        OR (purpose = 'QUOTE' AND source_type = 'ALPACA_HISTORICAL_QUOTES')),
    UNIQUE(review_id, purpose, page_ordinal),
    UNIQUE(review_id, purpose, source_observation_id),
    UNIQUE(review_id, purpose, external_source_observation_id),
    UNIQUE(review_id, purpose, page_ordinal, source_observation_id),
    FOREIGN KEY(review_id, purpose)
        REFERENCES phase1_exit_review_manifests(review_id, purpose)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
    FOREIGN KEY(source_observation_id) REFERENCES source_observations(id)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
    FOREIGN KEY(source_observation_id, payload_sha256)
        REFERENCES phase1_source_payloads(source_observation_id, payload_sha256)
        ON UPDATE RESTRICT ON DELETE RESTRICT
) STRICT;

CREATE TABLE phase1_exit_review_facts (
    id INTEGER PRIMARY KEY CHECK(typeof(id) = 'integer' AND id > 0),
    fact_id TEXT NOT NULL COLLATE BINARY UNIQUE
        CHECK(length(fact_id) = 64 AND fact_id NOT GLOB '*[^0-9a-f]*'),
    review_id TEXT NOT NULL COLLATE BINARY,
    purpose TEXT NOT NULL,
    fact_ordinal INTEGER NOT NULL
        CHECK(typeof(fact_ordinal) = 'integer' AND fact_ordinal > 0),
    source_observation_id INTEGER NOT NULL
        CHECK(typeof(source_observation_id) = 'integer' AND source_observation_id > 0),
    external_source_observation_id TEXT NOT NULL COLLATE BINARY
        CHECK(length(external_source_observation_id) > 0),
    page_ordinal INTEGER NOT NULL
        CHECK(typeof(page_ordinal) = 'integer' AND page_ordinal > 0),
    source_item_ordinal INTEGER NOT NULL
        CHECK(typeof(source_item_ordinal) = 'integer' AND source_item_ordinal > 0),
    source_item_path TEXT NOT NULL COLLATE BINARY
        CHECK(length(source_item_path) > 0 AND source_item_path GLOB '$.*'),
    fact_kind TEXT NOT NULL CHECK(fact_kind IN ('BAR', 'QUOTE')),
    symbol TEXT NOT NULL COLLATE BINARY
        CHECK(length(symbol) > 0 AND symbol = upper(symbol)),
    feed TEXT NOT NULL COLLATE BINARY CHECK(lower(feed) = 'sip'),
    source_time TEXT NOT NULL
        CHECK(length(source_time) = 27 AND substr(source_time, 1, 19) = strftime('%Y-%m-%dT%H:%M:%S', source_time) AND substr(source_time, 20, 1) = '.' AND substr(source_time, 21, 6) NOT GLOB '*[^0-9]*' AND substr(source_time, 27, 1) = 'Z'),
    received_at TEXT NOT NULL
        CHECK(length(received_at) = 27 AND substr(received_at, 1, 19) = strftime('%Y-%m-%dT%H:%M:%S', received_at) AND substr(received_at, 20, 1) = '.' AND substr(received_at, 21, 6) NOT GLOB '*[^0-9]*' AND substr(received_at, 27, 1) = 'Z'),
    provider_sequence INTEGER
        CHECK(provider_sequence IS NULL OR (typeof(provider_sequence) = 'integer' AND provider_sequence >= 0)),
    payload_sha256 TEXT NOT NULL COLLATE BINARY
        CHECK(length(payload_sha256) = 64 AND payload_sha256 NOT GLOB '*[^0-9a-f]*'),
    normalized_fields_digest TEXT NOT NULL COLLATE BINARY
        CHECK(length(normalized_fields_digest) = 64 AND normalized_fields_digest NOT GLOB '*[^0-9a-f]*'),
    values_json TEXT NOT NULL CHECK(length(values_json) > 2),
    source_digest TEXT NOT NULL COLLATE BINARY
        CHECK(length(source_digest) = 64 AND source_digest NOT GLOB '*[^0-9a-f]*'),
    record_sha256 TEXT NOT NULL COLLATE BINARY
        CHECK(length(record_sha256) = 64 AND record_sha256 NOT GLOB '*[^0-9a-f]*'),
    CHECK(source_time <= received_at),
    CHECK((purpose IN ('DAILY_BAR', 'EXECUTION_BAR') AND fact_kind = 'BAR')
        OR (purpose = 'QUOTE' AND fact_kind = 'QUOTE')),
    UNIQUE(review_id, purpose, fact_ordinal),
    UNIQUE(review_id, purpose, source_observation_id, source_item_ordinal),
    UNIQUE(review_id, purpose, source_observation_id, source_item_path),
    FOREIGN KEY(review_id, purpose, page_ordinal, source_observation_id)
        REFERENCES phase1_exit_review_pages(review_id, purpose, page_ordinal, source_observation_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT
) STRICT;

CREATE TABLE phase1_equity_mark_sets (
    id INTEGER PRIMARY KEY CHECK(typeof(id) = 'integer' AND id > 0),
    mark_set_id TEXT NOT NULL COLLATE BINARY UNIQUE
        CHECK(length(mark_set_id) = 64 AND mark_set_id NOT GLOB '*[^0-9a-f]*'),
    session_date TEXT NOT NULL UNIQUE
        CHECK(length(session_date) = 10 AND session_date = strftime('%Y-%m-%d', session_date)),
    calendar_digest TEXT NOT NULL COLLATE BINARY
        CHECK(length(calendar_digest) = 64 AND calendar_digest NOT GLOB '*[^0-9a-f]*'),
    query_cutoff TEXT NOT NULL
        CHECK(length(query_cutoff) = 27 AND substr(query_cutoff, 1, 19) = strftime('%Y-%m-%dT%H:%M:%S', query_cutoff) AND substr(query_cutoff, 20, 1) = '.' AND substr(query_cutoff, 21, 6) NOT GLOB '*[^0-9]*' AND substr(query_cutoff, 27, 1) = 'Z'),
    sealed_at TEXT NOT NULL
        CHECK(length(sealed_at) = 27 AND substr(sealed_at, 1, 19) = strftime('%Y-%m-%dT%H:%M:%S', sealed_at) AND substr(sealed_at, 20, 1) = '.' AND substr(sealed_at, 21, 6) NOT GLOB '*[^0-9]*' AND substr(sealed_at, 27, 1) = 'Z'),
    requested_symbols_json TEXT NOT NULL CHECK(length(requested_symbols_json) > 2),
    expected_manifest_count INTEGER NOT NULL
        CHECK(typeof(expected_manifest_count) = 'integer' AND expected_manifest_count = 2),
    expected_fact_count INTEGER NOT NULL
        CHECK(typeof(expected_fact_count) = 'integer' AND expected_fact_count > 0),
    source_observation_highwater INTEGER NOT NULL
        CHECK(typeof(source_observation_highwater) = 'integer' AND source_observation_highwater > 0),
    source_digest TEXT NOT NULL COLLATE BINARY
        CHECK(length(source_digest) = 64 AND source_digest NOT GLOB '*[^0-9a-f]*'),
    record_sha256 TEXT NOT NULL COLLATE BINARY
        CHECK(length(record_sha256) = 64 AND record_sha256 NOT GLOB '*[^0-9a-f]*'),
    CHECK(query_cutoff <= sealed_at)
) STRICT;

CREATE TABLE phase1_equity_mark_manifests (
    id INTEGER PRIMARY KEY CHECK(typeof(id) = 'integer' AND id > 0),
    mark_set_id TEXT NOT NULL COLLATE BINARY,
    purpose TEXT NOT NULL CHECK(purpose IN ('QUOTE', 'DAILY_BAR')),
    collection_name TEXT NOT NULL CHECK(collection_name IN ('quotes', 'bars')),
    requested_symbols_json TEXT NOT NULL CHECK(length(requested_symbols_json) > 2),
    request_start TEXT NOT NULL
        CHECK(length(request_start) = 27 AND substr(request_start, 1, 19) = strftime('%Y-%m-%dT%H:%M:%S', request_start) AND substr(request_start, 20, 1) = '.' AND substr(request_start, 21, 6) NOT GLOB '*[^0-9]*' AND substr(request_start, 27, 1) = 'Z'),
    request_end TEXT NOT NULL
        CHECK(length(request_end) = 27 AND substr(request_end, 1, 19) = strftime('%Y-%m-%dT%H:%M:%S', request_end) AND substr(request_end, 20, 1) = '.' AND substr(request_end, 21, 6) NOT GLOB '*[^0-9]*' AND substr(request_end, 27, 1) = 'Z'),
    request_digest TEXT NOT NULL COLLATE BINARY
        CHECK(length(request_digest) = 64 AND request_digest NOT GLOB '*[^0-9a-f]*'),
    manifest_digest TEXT NOT NULL COLLATE BINARY
        CHECK(length(manifest_digest) = 64 AND manifest_digest NOT GLOB '*[^0-9a-f]*'),
    semantic_manifest_digest TEXT NOT NULL COLLATE BINARY
        CHECK(length(semantic_manifest_digest) = 64 AND semantic_manifest_digest NOT GLOB '*[^0-9a-f]*'),
    terminal INTEGER NOT NULL CHECK(typeof(terminal) = 'integer' AND terminal = 1),
    received_through TEXT NOT NULL
        CHECK(length(received_through) = 27 AND substr(received_through, 1, 19) = strftime('%Y-%m-%dT%H:%M:%S', received_through) AND substr(received_through, 20, 1) = '.' AND substr(received_through, 21, 6) NOT GLOB '*[^0-9]*' AND substr(received_through, 27, 1) = 'Z'),
    expected_page_count INTEGER NOT NULL
        CHECK(typeof(expected_page_count) = 'integer' AND expected_page_count > 0),
    expected_fact_count INTEGER NOT NULL
        CHECK(typeof(expected_fact_count) = 'integer' AND expected_fact_count >= 0),
    source_digest TEXT NOT NULL COLLATE BINARY
        CHECK(length(source_digest) = 64 AND source_digest NOT GLOB '*[^0-9a-f]*'),
    record_sha256 TEXT NOT NULL COLLATE BINARY
        CHECK(length(record_sha256) = 64 AND record_sha256 NOT GLOB '*[^0-9a-f]*'),
    CHECK(request_start < request_end AND request_end <= received_through),
    CHECK((purpose = 'QUOTE' AND collection_name = 'quotes')
        OR (purpose = 'DAILY_BAR' AND collection_name = 'bars')),
    UNIQUE(mark_set_id, purpose),
    UNIQUE(mark_set_id, purpose, manifest_digest),
    FOREIGN KEY(mark_set_id) REFERENCES phase1_equity_mark_sets(mark_set_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT
) STRICT;

CREATE TABLE phase1_equity_mark_pages (
    id INTEGER PRIMARY KEY CHECK(typeof(id) = 'integer' AND id > 0),
    mark_set_id TEXT NOT NULL COLLATE BINARY,
    purpose TEXT NOT NULL CHECK(purpose IN ('QUOTE', 'DAILY_BAR')),
    page_ordinal INTEGER NOT NULL
        CHECK(typeof(page_ordinal) = 'integer' AND page_ordinal > 0),
    source_observation_id INTEGER NOT NULL
        CHECK(typeof(source_observation_id) = 'integer' AND source_observation_id > 0),
    external_source_observation_id TEXT NOT NULL COLLATE BINARY
        CHECK(length(external_source_observation_id) > 0),
    source_type TEXT NOT NULL
        CHECK(source_type IN ('ALPACA_HISTORICAL_QUOTES', 'ALPACA_DAILY_BARS')),
    request_url TEXT NOT NULL CHECK(length(request_url) > 0),
    request_page_token TEXT COLLATE BINARY,
    next_page_token TEXT COLLATE BINARY,
    payload_sha256 TEXT NOT NULL COLLATE BINARY
        CHECK(length(payload_sha256) = 64 AND payload_sha256 NOT GLOB '*[^0-9a-f]*'),
    source_digest TEXT NOT NULL COLLATE BINARY
        CHECK(length(source_digest) = 64 AND source_digest NOT GLOB '*[^0-9a-f]*'),
    record_sha256 TEXT NOT NULL COLLATE BINARY
        CHECK(length(record_sha256) = 64 AND record_sha256 NOT GLOB '*[^0-9a-f]*'),
    CHECK((purpose = 'QUOTE' AND source_type = 'ALPACA_HISTORICAL_QUOTES')
        OR (purpose = 'DAILY_BAR' AND source_type = 'ALPACA_DAILY_BARS')),
    UNIQUE(mark_set_id, purpose, page_ordinal),
    UNIQUE(mark_set_id, purpose, source_observation_id),
    UNIQUE(mark_set_id, purpose, external_source_observation_id),
    UNIQUE(mark_set_id, purpose, page_ordinal, source_observation_id),
    FOREIGN KEY(mark_set_id, purpose) REFERENCES phase1_equity_mark_manifests(mark_set_id, purpose)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
    FOREIGN KEY(source_observation_id) REFERENCES source_observations(id)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
    FOREIGN KEY(source_observation_id, payload_sha256)
        REFERENCES phase1_source_payloads(source_observation_id, payload_sha256)
        ON UPDATE RESTRICT ON DELETE RESTRICT
) STRICT;

CREATE TABLE phase1_equity_mark_facts (
    id INTEGER PRIMARY KEY CHECK(typeof(id) = 'integer' AND id > 0),
    fact_id TEXT NOT NULL COLLATE BINARY UNIQUE
        CHECK(length(fact_id) = 64 AND fact_id NOT GLOB '*[^0-9a-f]*'),
    mark_set_id TEXT NOT NULL COLLATE BINARY,
    purpose TEXT NOT NULL CHECK(purpose IN ('QUOTE', 'DAILY_BAR')),
    fact_ordinal INTEGER NOT NULL
        CHECK(typeof(fact_ordinal) = 'integer' AND fact_ordinal > 0),
    source_observation_id INTEGER NOT NULL
        CHECK(typeof(source_observation_id) = 'integer' AND source_observation_id > 0),
    external_source_observation_id TEXT NOT NULL COLLATE BINARY
        CHECK(length(external_source_observation_id) > 0),
    page_ordinal INTEGER NOT NULL
        CHECK(typeof(page_ordinal) = 'integer' AND page_ordinal > 0),
    source_item_ordinal INTEGER NOT NULL
        CHECK(typeof(source_item_ordinal) = 'integer' AND source_item_ordinal > 0),
    source_item_path TEXT NOT NULL COLLATE BINARY
        CHECK(length(source_item_path) > 0 AND source_item_path GLOB '$.*'),
    fact_kind TEXT NOT NULL CHECK(fact_kind IN ('BAR', 'QUOTE')),
    symbol TEXT NOT NULL COLLATE BINARY
        CHECK(length(symbol) > 0 AND symbol = upper(symbol)),
    feed TEXT NOT NULL COLLATE BINARY CHECK(lower(feed) = 'sip'),
    source_time TEXT NOT NULL
        CHECK(length(source_time) = 27 AND substr(source_time, 1, 19) = strftime('%Y-%m-%dT%H:%M:%S', source_time) AND substr(source_time, 20, 1) = '.' AND substr(source_time, 21, 6) NOT GLOB '*[^0-9]*' AND substr(source_time, 27, 1) = 'Z'),
    received_at TEXT NOT NULL
        CHECK(length(received_at) = 27 AND substr(received_at, 1, 19) = strftime('%Y-%m-%dT%H:%M:%S', received_at) AND substr(received_at, 20, 1) = '.' AND substr(received_at, 21, 6) NOT GLOB '*[^0-9]*' AND substr(received_at, 27, 1) = 'Z'),
    provider_sequence INTEGER
        CHECK(provider_sequence IS NULL OR (typeof(provider_sequence) = 'integer' AND provider_sequence >= 0)),
    payload_sha256 TEXT NOT NULL COLLATE BINARY
        CHECK(length(payload_sha256) = 64 AND payload_sha256 NOT GLOB '*[^0-9a-f]*'),
    normalized_fields_digest TEXT NOT NULL COLLATE BINARY
        CHECK(length(normalized_fields_digest) = 64 AND normalized_fields_digest NOT GLOB '*[^0-9a-f]*'),
    values_json TEXT NOT NULL CHECK(length(values_json) > 2),
    source_digest TEXT NOT NULL COLLATE BINARY
        CHECK(length(source_digest) = 64 AND source_digest NOT GLOB '*[^0-9a-f]*'),
    record_sha256 TEXT NOT NULL COLLATE BINARY
        CHECK(length(record_sha256) = 64 AND record_sha256 NOT GLOB '*[^0-9a-f]*'),
    CHECK(source_time <= received_at),
    CHECK((purpose = 'QUOTE' AND fact_kind = 'QUOTE')
        OR (purpose = 'DAILY_BAR' AND fact_kind = 'BAR')),
    UNIQUE(mark_set_id, purpose, fact_ordinal),
    UNIQUE(mark_set_id, purpose, source_observation_id, source_item_ordinal),
    UNIQUE(mark_set_id, purpose, source_observation_id, source_item_path),
    FOREIGN KEY(mark_set_id, purpose, page_ordinal, source_observation_id)
        REFERENCES phase1_equity_mark_pages(mark_set_id, purpose, page_ordinal, source_observation_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT
) STRICT;

CREATE TABLE phase1_equity_mark_invalidations (
    id INTEGER PRIMARY KEY CHECK(typeof(id) = 'integer' AND id > 0),
    invalidation_id TEXT NOT NULL COLLATE BINARY UNIQUE
        CHECK(length(invalidation_id) = 64 AND invalidation_id NOT GLOB '*[^0-9a-f]*'),
    mark_set_id TEXT NOT NULL COLLATE BINARY,
    session_date TEXT NOT NULL
        CHECK(length(session_date) = 10 AND session_date = strftime('%Y-%m-%d', session_date)),
    semantic_manifest_digests_json TEXT NOT NULL CHECK(length(semantic_manifest_digests_json) > 2),
    received_through TEXT NOT NULL
        CHECK(length(received_through) = 27 AND substr(received_through, 1, 19) = strftime('%Y-%m-%dT%H:%M:%S', received_through) AND substr(received_through, 20, 1) = '.' AND substr(received_through, 21, 6) NOT GLOB '*[^0-9]*' AND substr(received_through, 27, 1) = 'Z'),
    invalidated_at TEXT NOT NULL
        CHECK(length(invalidated_at) = 27 AND substr(invalidated_at, 1, 19) = strftime('%Y-%m-%dT%H:%M:%S', invalidated_at) AND substr(invalidated_at, 20, 1) = '.' AND substr(invalidated_at, 21, 6) NOT GLOB '*[^0-9]*' AND substr(invalidated_at, 27, 1) = 'Z'),
    expected_page_count INTEGER NOT NULL
        CHECK(typeof(expected_page_count) = 'integer' AND expected_page_count > 0),
    source_digest TEXT NOT NULL COLLATE BINARY
        CHECK(length(source_digest) = 64 AND source_digest NOT GLOB '*[^0-9a-f]*'),
    record_sha256 TEXT NOT NULL COLLATE BINARY
        CHECK(length(record_sha256) = 64 AND record_sha256 NOT GLOB '*[^0-9a-f]*'),
    CHECK(received_through <= invalidated_at),
    FOREIGN KEY(mark_set_id) REFERENCES phase1_equity_mark_sets(mark_set_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT
) STRICT;

CREATE TABLE phase1_equity_mark_invalidation_pages (
    id INTEGER PRIMARY KEY CHECK(typeof(id) = 'integer' AND id > 0),
    invalidation_id TEXT NOT NULL COLLATE BINARY,
    purpose TEXT NOT NULL CHECK(purpose IN ('QUOTE', 'DAILY_BAR')),
    page_ordinal INTEGER NOT NULL
        CHECK(typeof(page_ordinal) = 'integer' AND page_ordinal > 0),
    source_observation_id INTEGER NOT NULL
        CHECK(typeof(source_observation_id) = 'integer' AND source_observation_id > 0),
    external_source_observation_id TEXT NOT NULL COLLATE BINARY CHECK(length(external_source_observation_id) > 0),
    source_type TEXT NOT NULL CHECK(source_type IN ('ALPACA_HISTORICAL_QUOTES', 'ALPACA_DAILY_BARS')),
    request_url TEXT NOT NULL CHECK(length(request_url) > 0),
    request_page_token TEXT COLLATE BINARY,
    next_page_token TEXT COLLATE BINARY,
    payload_sha256 TEXT NOT NULL COLLATE BINARY
        CHECK(length(payload_sha256) = 64 AND payload_sha256 NOT GLOB '*[^0-9a-f]*'),
    semantic_manifest_digest TEXT NOT NULL COLLATE BINARY
        CHECK(length(semantic_manifest_digest) = 64 AND semantic_manifest_digest NOT GLOB '*[^0-9a-f]*'),
    source_digest TEXT NOT NULL COLLATE BINARY
        CHECK(length(source_digest) = 64 AND source_digest NOT GLOB '*[^0-9a-f]*'),
    record_sha256 TEXT NOT NULL COLLATE BINARY
        CHECK(length(record_sha256) = 64 AND record_sha256 NOT GLOB '*[^0-9a-f]*'),
    UNIQUE(invalidation_id, purpose, page_ordinal),
    UNIQUE(invalidation_id, purpose, source_observation_id),
    FOREIGN KEY(invalidation_id) REFERENCES phase1_equity_mark_invalidations(invalidation_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
    FOREIGN KEY(source_observation_id) REFERENCES source_observations(id)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
    FOREIGN KEY(source_observation_id, payload_sha256)
        REFERENCES phase1_source_payloads(source_observation_id, payload_sha256)
        ON UPDATE RESTRICT ON DELETE RESTRICT
) STRICT;

CREATE TABLE phase1_signal_events (
    id INTEGER PRIMARY KEY CHECK(typeof(id) = 'integer' AND id > 0),
    lifecycle_event_id TEXT NOT NULL COLLATE BINARY UNIQUE,
    signal_id TEXT NOT NULL COLLATE BINARY,
    event_ordinal INTEGER NOT NULL
        CHECK(typeof(event_ordinal) = 'integer' AND event_ordinal >= 0),
    event_kind TEXT NOT NULL
        CHECK(event_kind IN ('PUBLISHED', 'TRIGGER_OBSERVED', 'PAPER_FILL', 'SHADOW_FILL', 'LIVE_CONFIRM', 'LIVE_SKIP', 'FINALIZE_NOT_TRIGGERED', 'FINALIZE_NOT_FILLED', 'FINALIZE_UNRESOLVED', 'EXPIRE', 'INVALIDATE', 'PARTIAL_EXIT', 'CLOSE')),
    from_status TEXT
        CHECK(from_status IS NULL OR from_status IN ('PUBLISHED', 'TRIGGERED_AWAITING_LIMIT', 'TRIGGERED_PAPER', 'LIVE_CONFIRMED', 'SKIPPED_LIVE_TRACKED_PAPER', 'SHADOW_FILLED_INFORMATIONAL', 'NOT_TRIGGERED', 'NOT_FILLED_LIMIT', 'UNRESOLVED', 'EXPIRED', 'INVALIDATED', 'CLOSED')),
    to_status TEXT NOT NULL
        CHECK(to_status IN ('PUBLISHED', 'TRIGGERED_AWAITING_LIMIT', 'TRIGGERED_PAPER', 'LIVE_CONFIRMED', 'SKIPPED_LIVE_TRACKED_PAPER', 'SHADOW_FILLED_INFORMATIONAL', 'NOT_TRIGGERED', 'NOT_FILLED_LIMIT', 'UNRESOLVED', 'EXPIRED', 'INVALIDATED', 'CLOSED')),
    event_time TEXT NOT NULL
        CHECK(length(event_time) = 27 AND substr(event_time, 1, 19) = strftime('%Y-%m-%dT%H:%M:%S', event_time) AND substr(event_time, 20, 1) = '.' AND substr(event_time, 21, 6) NOT GLOB '*[^0-9]*' AND substr(event_time, 27, 1) = 'Z'),
    message_time TEXT NOT NULL
        CHECK(length(message_time) = 27 AND substr(message_time, 1, 19) = strftime('%Y-%m-%dT%H:%M:%S', message_time) AND substr(message_time, 20, 1) = '.' AND substr(message_time, 21, 6) NOT GLOB '*[^0-9]*' AND substr(message_time, 27, 1) = 'Z'),
    received_at TEXT NOT NULL
        CHECK(length(received_at) = 27 AND substr(received_at, 1, 19) = strftime('%Y-%m-%dT%H:%M:%S', received_at) AND substr(received_at, 20, 1) = '.' AND substr(received_at, 21, 6) NOT GLOB '*[^0-9]*' AND substr(received_at, 27, 1) = 'Z'),
    confirmation_execution_event_id INTEGER
        CHECK(confirmation_execution_event_id IS NULL OR (typeof(confirmation_execution_event_id) = 'integer' AND confirmation_execution_event_id > 0)),
    trigger_observation_id TEXT COLLATE BINARY,
    quote_observation_id TEXT COLLATE BINARY,
    session_completion_id TEXT COLLATE BINARY,
    exit_observation_id TEXT COLLATE BINARY,
    exit_authority_digest TEXT COLLATE BINARY
        CHECK(exit_authority_digest IS NULL OR (length(exit_authority_digest) = 64 AND exit_authority_digest NOT GLOB '*[^0-9a-f]*')),
    shares INTEGER CHECK(shares IS NULL OR (typeof(shares) = 'integer' AND shares > 0)),
    price_micros INTEGER CHECK(price_micros IS NULL OR (typeof(price_micros) = 'integer' AND price_micros > 0)),
    recommended_stop_micros INTEGER
        CHECK(recommended_stop_micros IS NULL OR (typeof(recommended_stop_micros) = 'integer' AND recommended_stop_micros > 0)),
    source_digest TEXT NOT NULL COLLATE BINARY
        CHECK(length(source_digest) = 64 AND source_digest NOT GLOB '*[^0-9a-f]*'),
    details_json TEXT NOT NULL,
    signal_evidence_id TEXT COLLATE BINARY,
    expiry_source_id TEXT COLLATE BINARY,
    CHECK(event_time <= message_time AND message_time <= received_at),
    CHECK((event_ordinal = 0 AND from_status IS NULL AND event_kind = 'PUBLISHED' AND to_status = 'PUBLISHED') OR event_ordinal > 0),
    CHECK(
        (event_kind = 'PUBLISHED'
            AND confirmation_execution_event_id IS NULL
            AND trigger_observation_id IS NULL AND quote_observation_id IS NULL
            AND session_completion_id IS NULL AND exit_observation_id IS NULL
            AND exit_authority_digest IS NULL AND shares IS NULL
            AND price_micros IS NULL AND recommended_stop_micros IS NULL)
        OR (event_kind = 'TRIGGER_OBSERVED'
            AND confirmation_execution_event_id IS NULL
            AND trigger_observation_id IS NOT NULL AND quote_observation_id IS NULL
            AND session_completion_id IS NOT NULL AND exit_observation_id IS NULL
            AND exit_authority_digest IS NULL AND shares IS NULL
            AND price_micros IS NULL AND recommended_stop_micros IS NULL)
        OR (event_kind = 'PAPER_FILL'
            AND confirmation_execution_event_id IS NULL
            AND trigger_observation_id IS NOT NULL AND quote_observation_id IS NOT NULL
            AND session_completion_id IS NOT NULL AND exit_observation_id IS NULL
            AND exit_authority_digest IS NULL AND shares IS NOT NULL
            AND price_micros IS NOT NULL AND recommended_stop_micros IS NULL)
        OR (event_kind = 'SHADOW_FILL'
            AND confirmation_execution_event_id IS NULL
            AND trigger_observation_id IS NOT NULL AND quote_observation_id IS NOT NULL
            AND session_completion_id IS NOT NULL AND exit_observation_id IS NULL
            AND exit_authority_digest IS NULL AND shares IS NULL
            AND price_micros IS NOT NULL AND recommended_stop_micros IS NULL)
        OR (event_kind IN ('LIVE_CONFIRM', 'LIVE_SKIP')
            AND confirmation_execution_event_id IS NOT NULL AND (
            (from_status = 'TRIGGERED_AWAITING_LIMIT'
                AND trigger_observation_id IS NOT NULL
                AND quote_observation_id IS NOT NULL
                AND shares IS NOT NULL AND price_micros IS NOT NULL)
            OR (from_status = 'TRIGGERED_PAPER'
                AND trigger_observation_id IS NULL
                AND quote_observation_id IS NULL
                AND shares IS NULL AND price_micros IS NULL))
            AND session_completion_id IS NOT NULL AND exit_observation_id IS NULL
            AND exit_authority_digest IS NULL AND recommended_stop_micros IS NULL)
        OR (event_kind IN ('FINALIZE_NOT_TRIGGERED', 'FINALIZE_NOT_FILLED', 'FINALIZE_UNRESOLVED')
            AND confirmation_execution_event_id IS NULL
            AND trigger_observation_id IS NULL AND quote_observation_id IS NULL
            AND session_completion_id IS NOT NULL AND exit_observation_id IS NULL
            AND exit_authority_digest IS NULL AND shares IS NULL
            AND price_micros IS NULL AND recommended_stop_micros IS NULL)
        OR (event_kind = 'EXPIRE'
            AND confirmation_execution_event_id IS NULL
            AND trigger_observation_id IS NULL AND quote_observation_id IS NULL
            AND session_completion_id IS NULL AND exit_observation_id IS NULL
            AND exit_authority_digest IS NULL AND shares IS NULL
            AND price_micros IS NULL AND recommended_stop_micros IS NULL)
        OR (event_kind = 'INVALIDATE'
            AND confirmation_execution_event_id IS NULL
            AND trigger_observation_id IS NULL AND quote_observation_id IS NULL
            AND session_completion_id IS NULL AND exit_observation_id IS NULL
            AND exit_authority_digest IS NULL AND shares IS NULL
            AND price_micros IS NULL AND recommended_stop_micros IS NULL)
        OR (event_kind = 'PARTIAL_EXIT'
            AND confirmation_execution_event_id IS NULL
            AND trigger_observation_id IS NULL AND quote_observation_id IS NULL
            AND session_completion_id IS NULL AND exit_observation_id IS NOT NULL
            AND exit_authority_digest IS NOT NULL AND shares IS NOT NULL
            AND price_micros IS NOT NULL AND recommended_stop_micros IS NOT NULL)
        OR (event_kind = 'CLOSE'
            AND confirmation_execution_event_id IS NULL
            AND trigger_observation_id IS NULL AND quote_observation_id IS NULL
            AND session_completion_id IS NULL AND exit_observation_id IS NOT NULL
            AND exit_authority_digest IS NOT NULL AND shares IS NOT NULL
            AND price_micros IS NOT NULL AND recommended_stop_micros IS NULL)
    ),
    CHECK((event_kind = 'INVALIDATE' AND signal_evidence_id IS NOT NULL
            AND expiry_source_id IS NULL)
        OR (event_kind = 'EXPIRE' AND signal_evidence_id IS NULL
            AND expiry_source_id IS NOT NULL)
        OR (event_kind NOT IN ('INVALIDATE', 'EXPIRE')
            AND signal_evidence_id IS NULL AND expiry_source_id IS NULL)),
    UNIQUE(signal_id, event_ordinal),
    FOREIGN KEY(signal_id) REFERENCES phase1_signals(signal_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
    FOREIGN KEY(confirmation_execution_event_id) REFERENCES execution_events(id)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
    FOREIGN KEY(trigger_observation_id) REFERENCES phase1_observations(observation_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
    FOREIGN KEY(quote_observation_id) REFERENCES phase1_observations(observation_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
    FOREIGN KEY(session_completion_id) REFERENCES phase1_session_completions(completion_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
    FOREIGN KEY(exit_observation_id) REFERENCES phase1_exit_review_facts(fact_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
    FOREIGN KEY(signal_evidence_id) REFERENCES phase1_signal_evidence_reviews(evidence_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
    FOREIGN KEY(expiry_source_id) REFERENCES phase1_expiry_deadlines(expiry_source_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT
) STRICT;

CREATE UNIQUE INDEX phase1_signal_events_one_confirmation_action
ON phase1_signal_events(confirmation_execution_event_id)
WHERE confirmation_execution_event_id IS NOT NULL;

CREATE TABLE phase1_canonical_postings (
    id INTEGER PRIMARY KEY CHECK(typeof(id) = 'integer' AND id > 0),
    posting_key TEXT NOT NULL COLLATE BINARY UNIQUE,
    lifecycle_event_id TEXT NOT NULL COLLATE BINARY,
    signal_id TEXT NOT NULL COLLATE BINARY,
    entry_kind TEXT NOT NULL CHECK(entry_kind IN ('BUY', 'SALE', 'FEE')),
    account_name TEXT NOT NULL CHECK(account_name IN ('CASH', 'POSITION', 'FEE')),
    amount_micros INTEGER NOT NULL CHECK(typeof(amount_micros) = 'integer'),
    shares_delta INTEGER CHECK(shares_delta IS NULL OR (typeof(shares_delta) = 'integer' AND shares_delta != 0)),
    unit_price_micros INTEGER CHECK(unit_price_micros IS NULL OR (typeof(unit_price_micros) = 'integer' AND unit_price_micros > 0)),
    occurred_at TEXT NOT NULL
        CHECK(length(occurred_at) = 27 AND substr(occurred_at, 1, 19) = strftime('%Y-%m-%dT%H:%M:%S', occurred_at) AND substr(occurred_at, 20, 1) = '.' AND substr(occurred_at, 21, 6) NOT GLOB '*[^0-9]*' AND substr(occurred_at, 27, 1) = 'Z'),
    received_at TEXT NOT NULL
        CHECK(length(received_at) = 27 AND substr(received_at, 1, 19) = strftime('%Y-%m-%dT%H:%M:%S', received_at) AND substr(received_at, 20, 1) = '.' AND substr(received_at, 21, 6) NOT GLOB '*[^0-9]*' AND substr(received_at, 27, 1) = 'Z'),
    settlement_available_session TEXT NOT NULL
        CHECK(length(settlement_available_session) = 10 AND settlement_available_session = strftime('%Y-%m-%d', settlement_available_session) AND CAST(substr(settlement_available_session, 1, 4) AS INTEGER) BETWEEN 1 AND 9999 AND strftime('%Y-%m-%d', settlement_available_session) IS NOT NULL),
    fee_schedule_version TEXT NOT NULL COLLATE BINARY
        CHECK(length(fee_schedule_version) > 0),
    fee_schedule_digest TEXT NOT NULL COLLATE BINARY
        CHECK(length(fee_schedule_digest) = 64 AND fee_schedule_digest NOT GLOB '*[^0-9a-f]*'),
    source_digest TEXT NOT NULL COLLATE BINARY
        CHECK(length(source_digest) = 64 AND source_digest NOT GLOB '*[^0-9a-f]*'),
    record_sha256 TEXT NOT NULL COLLATE BINARY
        CHECK(length(record_sha256) = 64 AND record_sha256 NOT GLOB '*[^0-9a-f]*'),
    details_json TEXT NOT NULL,
    CHECK(occurred_at <= received_at),
    CHECK(
        (entry_kind = 'BUY' AND account_name = 'CASH' AND amount_micros < 0 AND shares_delta > 0 AND unit_price_micros IS NOT NULL)
        OR (entry_kind = 'SALE' AND account_name = 'CASH' AND amount_micros > 0 AND shares_delta < 0 AND unit_price_micros IS NOT NULL)
        OR (entry_kind = 'FEE' AND account_name = 'FEE' AND amount_micros < 0 AND shares_delta IS NULL AND unit_price_micros IS NULL)
    ),
    FOREIGN KEY(lifecycle_event_id) REFERENCES phase1_signal_events(lifecycle_event_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
    FOREIGN KEY(signal_id) REFERENCES phase1_signals(signal_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT
) STRICT;

CREATE INDEX phase1_canonical_postings_time
ON phase1_canonical_postings(received_at, id);

CREATE UNIQUE INDEX phase1_canonical_postings_one_buy_per_signal
ON phase1_canonical_postings(signal_id)
WHERE entry_kind = 'BUY';

CREATE UNIQUE INDEX phase1_canonical_postings_one_kind_per_lifecycle
ON phase1_canonical_postings(lifecycle_event_id, entry_kind);

CREATE TABLE phase1_equity_points (
    id INTEGER PRIMARY KEY CHECK(typeof(id) = 'integer' AND id > 0),
    point_id TEXT NOT NULL COLLATE BINARY UNIQUE,
    validation_window_id TEXT NOT NULL COLLATE BINARY,
    ledger_name TEXT NOT NULL CHECK(ledger_name IN ('CANONICAL', 'ACTUAL')),
    session_date TEXT NOT NULL
        CHECK(length(session_date) = 10 AND session_date = strftime('%Y-%m-%d', session_date)),
    equity_micros INTEGER NOT NULL CHECK(typeof(equity_micros) = 'integer'),
    cash_micros INTEGER NOT NULL CHECK(typeof(cash_micros) = 'integer'),
    positions_value_micros INTEGER NOT NULL CHECK(typeof(positions_value_micros) = 'integer' AND positions_value_micros >= 0),
    external_cash_flow_micros INTEGER NOT NULL CHECK(typeof(external_cash_flow_micros) = 'integer'),
    source_cursor INTEGER NOT NULL CHECK(typeof(source_cursor) = 'integer' AND source_cursor >= 0),
    mark_source_digest TEXT NOT NULL COLLATE BINARY
        CHECK(length(mark_source_digest) = 64 AND mark_source_digest NOT GLOB '*[^0-9a-f]*'),
    at TEXT NOT NULL
        CHECK(length(at) = 27 AND substr(at, 1, 19) = strftime('%Y-%m-%dT%H:%M:%S', at) AND substr(at, 20, 1) = '.' AND substr(at, 21, 6) NOT GLOB '*[^0-9]*' AND substr(at, 27, 1) = 'Z'),
    message_time TEXT NOT NULL
        CHECK(length(message_time) = 27 AND substr(message_time, 1, 19) = strftime('%Y-%m-%dT%H:%M:%S', message_time) AND substr(message_time, 20, 1) = '.' AND substr(message_time, 21, 6) NOT GLOB '*[^0-9]*' AND substr(message_time, 27, 1) = 'Z'),
    received_at TEXT NOT NULL
        CHECK(length(received_at) = 27 AND substr(received_at, 1, 19) = strftime('%Y-%m-%dT%H:%M:%S', received_at) AND substr(received_at, 20, 1) = '.' AND substr(received_at, 21, 6) NOT GLOB '*[^0-9]*' AND substr(received_at, 27, 1) = 'Z'),
    source_digest TEXT NOT NULL COLLATE BINARY
        CHECK(length(source_digest) = 64 AND source_digest NOT GLOB '*[^0-9a-f]*'),
    CHECK(at <= message_time AND message_time <= received_at),
    CHECK(equity_micros = cash_micros + positions_value_micros),
    UNIQUE(validation_window_id, ledger_name, session_date),
    FOREIGN KEY(validation_window_id) REFERENCES phase1_validation_windows(window_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT
) STRICT;

CREATE TABLE phase1_equity_point_marks (
    id INTEGER PRIMARY KEY CHECK(typeof(id) = 'integer' AND id > 0),
    equity_point_id TEXT NOT NULL COLLATE BINARY,
    observation_id TEXT NOT NULL COLLATE BINARY,
    symbol TEXT NOT NULL COLLATE BINARY
        CHECK(length(symbol) > 0 AND symbol = upper(symbol)),
    mark_ordinal INTEGER NOT NULL
        CHECK(typeof(mark_ordinal) = 'integer' AND mark_ordinal > 0),
    method TEXT NOT NULL
        CHECK(method IN ('CONSOLIDATED_BID', 'CLOSE_MINUS_0.10_PERCENT')),
    derived_price_micros INTEGER NOT NULL
        CHECK(typeof(derived_price_micros) = 'integer' AND derived_price_micros > 0),
    mark_at TEXT NOT NULL
        CHECK(length(mark_at) = 27 AND substr(mark_at, 1, 19) = strftime('%Y-%m-%dT%H:%M:%S', mark_at) AND substr(mark_at, 20, 1) = '.' AND substr(mark_at, 21, 6) NOT GLOB '*[^0-9]*' AND substr(mark_at, 27, 1) = 'Z'),
    source_digest TEXT NOT NULL COLLATE BINARY
        CHECK(length(source_digest) = 64 AND source_digest NOT GLOB '*[^0-9a-f]*'),
    UNIQUE(equity_point_id, mark_ordinal),
    UNIQUE(equity_point_id, symbol),
    UNIQUE(equity_point_id, observation_id),
    FOREIGN KEY(equity_point_id) REFERENCES phase1_equity_points(point_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
    FOREIGN KEY(observation_id) REFERENCES phase1_equity_mark_facts(fact_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT
) STRICT;

CREATE TABLE phase1_closed_trades (
    id INTEGER PRIMARY KEY CHECK(typeof(id) = 'integer' AND id > 0),
    trade_id TEXT NOT NULL COLLATE BINARY UNIQUE,
    validation_window_id TEXT NOT NULL COLLATE BINARY,
    ledger_name TEXT NOT NULL CHECK(ledger_name IN ('CANONICAL', 'ACTUAL')),
    signal_id TEXT NOT NULL COLLATE BINARY,
    lifecycle_event_id TEXT NOT NULL COLLATE BINARY,
    session_date TEXT NOT NULL
        CHECK(length(session_date) = 10 AND session_date = strftime('%Y-%m-%d', session_date)),
    shares INTEGER NOT NULL CHECK(typeof(shares) = 'integer' AND shares > 0),
    entry_value_micros INTEGER NOT NULL CHECK(typeof(entry_value_micros) = 'integer' AND entry_value_micros > 0),
    exit_value_micros INTEGER NOT NULL CHECK(typeof(exit_value_micros) = 'integer' AND exit_value_micros >= 0),
    fee_micros INTEGER NOT NULL CHECK(typeof(fee_micros) = 'integer' AND fee_micros >= 0),
    pnl_micros INTEGER NOT NULL CHECK(typeof(pnl_micros) = 'integer'),
    initial_risk_micros INTEGER NOT NULL
        CHECK(typeof(initial_risk_micros) = 'integer' AND initial_risk_micros > 0),
    net_r_numerator_micros INTEGER NOT NULL
        CHECK(typeof(net_r_numerator_micros) = 'integer'),
    at TEXT NOT NULL
        CHECK(length(at) = 27 AND substr(at, 1, 19) = strftime('%Y-%m-%dT%H:%M:%S', at) AND substr(at, 20, 1) = '.' AND substr(at, 21, 6) NOT GLOB '*[^0-9]*' AND substr(at, 27, 1) = 'Z'),
    message_time TEXT NOT NULL
        CHECK(length(message_time) = 27 AND substr(message_time, 1, 19) = strftime('%Y-%m-%dT%H:%M:%S', message_time) AND substr(message_time, 20, 1) = '.' AND substr(message_time, 21, 6) NOT GLOB '*[^0-9]*' AND substr(message_time, 27, 1) = 'Z'),
    received_at TEXT NOT NULL
        CHECK(length(received_at) = 27 AND substr(received_at, 1, 19) = strftime('%Y-%m-%dT%H:%M:%S', received_at) AND substr(received_at, 20, 1) = '.' AND substr(received_at, 21, 6) NOT GLOB '*[^0-9]*' AND substr(received_at, 27, 1) = 'Z'),
    source_digest TEXT NOT NULL COLLATE BINARY
        CHECK(length(source_digest) = 64 AND source_digest NOT GLOB '*[^0-9a-f]*'),
    CHECK(pnl_micros = exit_value_micros - entry_value_micros - fee_micros),
    CHECK(net_r_numerator_micros = pnl_micros),
    CHECK(at <= message_time AND message_time <= received_at),
    FOREIGN KEY(validation_window_id) REFERENCES phase1_validation_windows(window_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
    FOREIGN KEY(signal_id) REFERENCES phase1_signals(signal_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
    FOREIGN KEY(lifecycle_event_id) REFERENCES phase1_signal_events(lifecycle_event_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT
) STRICT;

CREATE UNIQUE INDEX phase1_closed_trade_one_signal
ON phase1_closed_trades(validation_window_id, ledger_name, signal_id);

CREATE TABLE phase1_adherence_checks (
    id INTEGER PRIMARY KEY CHECK(typeof(id) = 'integer' AND id > 0),
    check_id TEXT NOT NULL COLLATE BINARY UNIQUE,
    validation_window_id TEXT NOT NULL COLLATE BINARY,
    signal_id TEXT NOT NULL COLLATE BINARY,
    check_name TEXT NOT NULL COLLATE BINARY
        CHECK(check_name IN (
            'DATA_CALENDAR_UNIVERSE_FRESHNESS',
            'HARD_ELIGIBILITY_GATES',
            'SCORE_ARITHMETIC_AND_PRIMARY_SELECTION',
            'VALID_TRIGGER_TIMING',
            'ENTRY_AND_SPREAD_COMPLIANCE',
            'POSITION_SIZE_EXPOSURE_AND_RISK',
            'STOP_STATE',
            'EXIT_RULE',
            'CIRCUIT_BREAKER_BEHAVIOR',
            'RECORD_COMPLETENESS'
        )),
    applicable INTEGER NOT NULL CHECK(typeof(applicable) = 'integer' AND applicable IN (0, 1)),
    passed INTEGER NOT NULL CHECK(typeof(passed) = 'integer' AND passed IN (0, 1)),
    hard_breach INTEGER NOT NULL CHECK(typeof(hard_breach) = 'integer' AND hard_breach IN (0, 1)),
    evaluated_at TEXT NOT NULL
        CHECK(length(evaluated_at) = 27 AND substr(evaluated_at, 1, 19) = strftime('%Y-%m-%dT%H:%M:%S', evaluated_at) AND substr(evaluated_at, 20, 1) = '.' AND substr(evaluated_at, 21, 6) NOT GLOB '*[^0-9]*' AND substr(evaluated_at, 27, 1) = 'Z'),
    received_at TEXT NOT NULL
        CHECK(length(received_at) = 27 AND substr(received_at, 1, 19) = strftime('%Y-%m-%dT%H:%M:%S', received_at) AND substr(received_at, 20, 1) = '.' AND substr(received_at, 21, 6) NOT GLOB '*[^0-9]*' AND substr(received_at, 27, 1) = 'Z'),
    terminal_lifecycle_event_id TEXT NOT NULL COLLATE BINARY,
    evidence_digest TEXT NOT NULL COLLATE BINARY
        CHECK(length(evidence_digest) = 64 AND evidence_digest NOT GLOB '*[^0-9a-f]*'),
    review_source_digest TEXT NOT NULL COLLATE BINARY
        CHECK(length(review_source_digest) = 64 AND review_source_digest NOT GLOB '*[^0-9a-f]*'),
    authority_digest TEXT NOT NULL COLLATE BINARY
        CHECK(length(authority_digest) = 64 AND authority_digest NOT GLOB '*[^0-9a-f]*'),
    source_digest TEXT NOT NULL COLLATE BINARY
        CHECK(length(source_digest) = 64 AND source_digest NOT GLOB '*[^0-9a-f]*'),
    details_json TEXT NOT NULL,
    record_sha256 TEXT NOT NULL COLLATE BINARY
        CHECK(length(record_sha256) = 64 AND record_sha256 NOT GLOB '*[^0-9a-f]*'),
    CHECK(evaluated_at <= received_at),
    CHECK(applicable = 1 OR (passed = 0 AND hard_breach = 0)),
    CHECK(hard_breach = 0 OR (applicable = 1 AND passed = 0)),
    UNIQUE(validation_window_id, signal_id, check_name),
    FOREIGN KEY(validation_window_id) REFERENCES phase1_validation_windows(window_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
    FOREIGN KEY(signal_id) REFERENCES phase1_signals(signal_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
    FOREIGN KEY(terminal_lifecycle_event_id)
        REFERENCES phase1_signal_events(lifecycle_event_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT
) STRICT;

CREATE TRIGGER phase1_source_payloads_validate_lineage
BEFORE INSERT ON phase1_source_payloads
WHEN NOT EXISTS (
    SELECT 1 FROM source_observations
    WHERE id = NEW.source_observation_id
      AND payload_sha256 = NEW.payload_sha256 COLLATE BINARY
      AND retrieved_at = NEW.recorded_at
)
BEGIN
    SELECT RAISE(ABORT, 'phase1 source payload conflicts with core source');
END;

CREATE TRIGGER phase1_equity_point_marks_validate_lineage
BEFORE INSERT ON phase1_equity_point_marks
WHEN NOT EXISTS (
    SELECT 1
    FROM phase1_equity_points AS point
    JOIN phase1_equity_mark_facts AS fact
      ON fact.fact_id = NEW.observation_id COLLATE BINARY
    JOIN phase1_equity_mark_sets AS mark_set
      ON mark_set.mark_set_id = fact.mark_set_id COLLATE BINARY
    WHERE point.point_id = NEW.equity_point_id
      AND mark_set.session_date = point.session_date
      AND fact.symbol = NEW.symbol COLLATE BINARY
      AND fact.source_time <= NEW.mark_at
      AND NEW.mark_at <= point.at
      AND fact.received_at <= point.received_at
      AND ((NEW.method = 'CONSOLIDATED_BID' AND fact.fact_kind = 'QUOTE')
        OR (NEW.method = 'CLOSE_MINUS_0.10_PERCENT' AND fact.fact_kind = 'BAR'))
)
BEGIN
    SELECT RAISE(ABORT, 'phase1 equity mark conflicts with point lineage');
END;

CREATE TRIGGER phase1_session_completions_validate_manifest
BEFORE INSERT ON phase1_session_completions
WHEN NEW.expected_observation_count != NEW.cohort_through_ordinal
    OR 2 != (
        SELECT COUNT(*) FROM phase1_observation_fetch_manifests
        WHERE signal_id = NEW.signal_id
          AND session_date = NEW.session_date
          AND purpose = 'SIGNAL_LIFECYCLE'
    )
    OR 1 != (
        SELECT COUNT(*)
        FROM phase1_observation_fetch_manifests AS manifest
        JOIN phase1_signals AS signal ON signal.signal_id = manifest.signal_id
        WHERE manifest.signal_id = NEW.signal_id
          AND manifest.session_date = NEW.session_date
          AND manifest.purpose = 'SIGNAL_LIFECYCLE'
          AND manifest.collection_name = 'trades'
          AND manifest.terminal = 1
          AND manifest.requested_symbols_json = json_array(signal.symbol)
    )
    OR 1 != (
        SELECT COUNT(*)
        FROM phase1_observation_fetch_manifests AS manifest
        JOIN phase1_signals AS signal ON signal.signal_id = manifest.signal_id
        WHERE manifest.signal_id = NEW.signal_id
          AND manifest.session_date = NEW.session_date
          AND manifest.purpose = 'SIGNAL_LIFECYCLE'
          AND manifest.collection_name = 'quotes'
          AND manifest.terminal = 1
          AND manifest.requested_symbols_json = json_array(signal.symbol)
    )
    OR NEW.expected_observation_count != (
        SELECT COUNT(*) FROM phase1_observations
        WHERE signal_id = NEW.signal_id
          AND session_date = NEW.session_date
          AND cohort_ordinal <= NEW.cohort_through_ordinal
    )
    OR NEW.cohort_through_ordinal != COALESCE((
        SELECT MAX(cohort_ordinal) FROM phase1_observations
        WHERE signal_id = NEW.signal_id
          AND session_date = NEW.session_date
    ), 0)
    OR EXISTS (
        SELECT 1 FROM phase1_observations
        WHERE signal_id = NEW.signal_id
          AND session_date = NEW.session_date
          AND received_at > NEW.received_through
    )
BEGIN
    SELECT RAISE(ABORT, 'phase1 completion requires its exact observation manifest');
END;

CREATE TRIGGER phase1_observations_reject_completed_session
BEFORE INSERT ON phase1_observations
WHEN EXISTS (
    SELECT 1 FROM phase1_session_completions
    WHERE signal_id = NEW.signal_id
      AND session_date = NEW.session_date
)
BEGIN
    SELECT RAISE(ABORT, 'phase1 completed session rejects later normalized observations');
END;

CREATE TRIGGER phase1_observation_fetch_manifests_reject_completed_session
BEFORE INSERT ON phase1_observation_fetch_manifests
WHEN EXISTS (
    SELECT 1 FROM phase1_session_completions
    WHERE signal_id = NEW.signal_id
      AND session_date = NEW.session_date
)
BEGIN
    SELECT RAISE(ABORT, 'phase1 completed session rejects later fetch manifests');
END;

CREATE TRIGGER phase1_observation_fetch_pages_reject_completed_session
BEFORE INSERT ON phase1_observation_fetch_pages
WHEN EXISTS (
    SELECT 1
    FROM phase1_observation_fetch_manifests AS manifest
    JOIN phase1_session_completions AS completion
      ON completion.signal_id = manifest.signal_id
     AND completion.session_date = manifest.session_date
    WHERE manifest.cohort_id = NEW.cohort_id
)
BEGIN
    SELECT RAISE(ABORT, 'phase1 completed session rejects later fetch pages');
END;

CREATE TRIGGER phase1_session_late_evidence_validate_lineage
BEFORE INSERT ON phase1_session_late_evidence
WHEN NOT EXISTS (
    SELECT 1 FROM phase1_session_completions
    WHERE completion_id = NEW.completion_id
      AND signal_id = NEW.signal_id
      AND session_date = NEW.session_date
      AND completed_at < NEW.invalidated_at
      AND NEW.received_through <= NEW.invalidated_at
)
BEGIN
    SELECT RAISE(ABORT, 'phase1 late evidence requires an earlier exact completion');
END;

CREATE TRIGGER phase1_session_late_evidence_pages_validate_lineage
BEFORE INSERT ON phase1_session_late_evidence_pages
WHEN NOT EXISTS (
    SELECT 1
    FROM phase1_session_late_evidence AS late
    JOIN source_observations AS source
      ON source.id = NEW.source_observation_id
    WHERE late.late_evidence_id = NEW.late_evidence_id
      AND source.source_type = NEW.source_type
      AND source.source_uri = NEW.request_url
      AND source.payload_sha256 = NEW.payload_sha256
      AND json_extract(source.details_json, '$.source_observation_id')
          = NEW.external_source_observation_id
)
BEGIN
    SELECT RAISE(ABORT, 'phase1 late evidence page conflicts with core source');
END;

CREATE TRIGGER phase1_signal_evidence_reviews_validate_lineage
BEFORE INSERT ON phase1_signal_evidence_reviews
WHEN NOT EXISTS (
    SELECT 1
    FROM phase1_signals AS signal
    JOIN source_observations AS registry
      ON registry.id = NEW.registry_source_row_id
    JOIN phase1_source_payloads AS payload
      ON payload.source_observation_id = registry.id
    WHERE signal.signal_id = NEW.signal_id
      AND signal.calendar_digest = NEW.calendar_digest
      AND signal.received_at <= NEW.review_at
      AND registry.source_type = 'REVIEWED_EVIDENCE_REGISTRY'
      AND registry.provider = 'operator-reviewed'
      AND registry.feed IS NULL
      AND registry.provider_sequence IS NULL
      AND registry.delay_seconds = 0
      AND registry.health_result = 'REVIEWED'
      AND registry.payload_sha256 = NEW.registry_content_hash
      AND payload.payload_sha256 = NEW.registry_content_hash
      AND json_extract(registry.details_json, '$.registry_id') = NEW.registry_id
      AND json_extract(registry.details_json, '$.content_hash')
          = NEW.registry_content_hash
)
BEGIN
    SELECT RAISE(ABORT, 'phase1 signal evidence review conflicts with raw lineage');
END;

CREATE TRIGGER phase1_signal_evidence_bindings_validate_lineage
BEFORE INSERT ON phase1_signal_evidence_bindings
WHEN NOT EXISTS (
    SELECT 1
    FROM phase1_signal_evidence_reviews AS review
    JOIN source_observations AS source
      ON source.id = NEW.source_observation_row_id
    JOIN phase1_source_payloads AS payload
      ON payload.source_observation_id = source.id
    WHERE review.evidence_id = NEW.evidence_id
      AND source.retrieved_at <= review.review_at
      AND payload.payload_sha256 = source.payload_sha256
      AND json_extract(source.details_json, '$.source_observation_id')
          = NEW.external_source_observation_id
)
BEGIN
    SELECT RAISE(ABORT, 'phase1 signal evidence binding conflicts with raw lineage');
END;

CREATE TRIGGER phase1_expiry_deadlines_validate_lineage
BEFORE INSERT ON phase1_expiry_deadlines
WHEN NOT EXISTS (
    SELECT 1 FROM phase1_signals AS signal
    WHERE signal.signal_id = NEW.signal_id
      AND signal.calendar_digest = NEW.calendar_digest
      AND signal.publication_session < NEW.deadline_session
      AND signal.received_at <= NEW.observed_at
      AND NEW.completion_source_highwater = COALESCE((
          SELECT MAX(id) FROM phase1_session_completions
          WHERE completed_at <= NEW.observed_at
      ), 0)
      AND NEW.evidence_review_highwater = COALESCE((
          SELECT MAX(id) FROM phase1_signal_evidence_reviews
          WHERE review_at <= NEW.observed_at
      ), 0)
      AND NOT EXISTS (
          SELECT 1 FROM phase1_session_completions AS completion
          WHERE completion.signal_id = NEW.signal_id
            AND completion.session_date = signal.publication_session
            AND completion.completed_at <= NEW.observed_at
      )
      AND NOT EXISTS (
          SELECT 1 FROM phase1_signal_evidence_reviews AS evidence
          WHERE evidence.signal_id = NEW.signal_id
            AND evidence.review_at <= NEW.observed_at
            AND (
                json_extract(CAST(evidence.manifest_bytes AS TEXT), '$.event_exit_required') = 1
                OR json_extract(CAST(evidence.manifest_bytes AS TEXT), '$.thesis_invalidated') = 1
            )
      )
)
BEGIN
    SELECT RAISE(ABORT, 'phase1 expiry deadline lacks exact absence lineage');
END;

CREATE TRIGGER phase1_exit_review_manifests_validate_lineage
BEFORE INSERT ON phase1_exit_review_manifests
WHEN NOT EXISTS (
    SELECT 1
    FROM phase1_exit_reviews AS review
    JOIN phase1_signals AS signal ON signal.signal_id = review.signal_id
    WHERE review.review_id = NEW.review_id
      AND NEW.requested_symbols_json = json_array(signal.symbol)
      AND NEW.request_start < NEW.request_end
      AND NEW.received_through <= review.query_cutoff
)
BEGIN
    SELECT RAISE(ABORT, 'phase1 exit manifest conflicts with review lineage');
END;

CREATE TRIGGER phase1_exit_review_pages_validate_lineage
BEFORE INSERT ON phase1_exit_review_pages
WHEN NOT EXISTS (
    SELECT 1
    FROM phase1_exit_review_manifests AS manifest
    JOIN source_observations AS source ON source.id = NEW.source_observation_id
    JOIN phase1_source_payloads AS payload
      ON payload.source_observation_id = source.id
    WHERE manifest.review_id = NEW.review_id
      AND manifest.purpose = NEW.purpose
      AND source.source_type = NEW.source_type
      AND source.source_uri = NEW.request_url
      AND source.payload_sha256 = NEW.payload_sha256
      AND payload.payload_sha256 = NEW.payload_sha256
      AND source.provider = 'alpaca'
      AND lower(source.feed) = 'sip'
      AND source.health_result = 'OK'
      AND json_extract(source.details_json, '$.source_observation_id')
          = NEW.external_source_observation_id
)
BEGIN
    SELECT RAISE(ABORT, 'phase1 exit page conflicts with exact core source');
END;

CREATE TRIGGER phase1_exit_review_facts_validate_lineage
BEFORE INSERT ON phase1_exit_review_facts
WHEN NOT EXISTS (
    SELECT 1
    FROM phase1_exit_review_pages AS page
    JOIN source_observations AS source ON source.id = page.source_observation_id
    WHERE page.review_id = NEW.review_id
      AND page.purpose = NEW.purpose
      AND page.page_ordinal = NEW.page_ordinal
      AND page.source_observation_id = NEW.source_observation_id
      AND page.external_source_observation_id
          = NEW.external_source_observation_id
      AND page.payload_sha256 = NEW.payload_sha256
      AND source.retrieved_at = NEW.received_at
)
BEGIN
    SELECT RAISE(ABORT, 'phase1 exit fact conflicts with provider page');
END;

CREATE TRIGGER phase1_signal_events_validate_sequence
BEFORE INSERT ON phase1_signal_events
WHEN NEW.event_ordinal != COALESCE((
        SELECT MAX(event_ordinal) + 1 FROM phase1_signal_events
        WHERE signal_id = NEW.signal_id
    ), 0)
    OR (
        NEW.confirmation_execution_event_id IS NOT NULL
        AND NOT EXISTS (
            SELECT 1
            FROM execution_events AS confirmation
            JOIN phase1_signals AS signal ON signal.signal_id = NEW.signal_id
            WHERE confirmation.id = NEW.confirmation_execution_event_id
              AND confirmation.symbol = signal.symbol
              AND ((NEW.event_kind = 'LIVE_CONFIRM' AND confirmation.parsed_action = 'BOUGHT')
                OR (NEW.event_kind = 'LIVE_SKIP' AND confirmation.parsed_action = 'SKIPPED'))
              AND substr(confirmation.event_time, 1, 10) = signal.publication_session
              AND confirmation.event_time >= (
                    SELECT source_time FROM phase1_observations
                    WHERE observation_id = COALESCE(
                        NEW.trigger_observation_id,
                        (
                            SELECT trigger_observation_id
                            FROM phase1_signal_events
                            WHERE signal_id = NEW.signal_id
                              AND event_ordinal = NEW.event_ordinal - 1
                        )
                    )
                  )
              AND confirmation.event_time <= NEW.event_time
              AND confirmation.message_time <= NEW.message_time
        )
    )
    OR (
        NEW.event_ordinal > 0
        AND NOT EXISTS (
            SELECT 1 FROM phase1_signal_events
            WHERE signal_id = NEW.signal_id
              AND event_ordinal = NEW.event_ordinal - 1
              AND to_status = NEW.from_status
              AND received_at <= NEW.received_at
        )
    )
    OR (
        NEW.trigger_observation_id IS NOT NULL
        AND NOT EXISTS (
            SELECT 1 FROM phase1_observations
            WHERE observation_id = NEW.trigger_observation_id
              AND signal_id = NEW.signal_id
              AND observation_kind = 'TRADE'
              AND received_at <= NEW.received_at
        )
    )
    OR (
        NEW.quote_observation_id IS NOT NULL
        AND NOT EXISTS (
            SELECT 1 FROM phase1_observations
            WHERE observation_id = NEW.quote_observation_id
              AND signal_id = NEW.signal_id
              AND observation_kind = 'QUOTE'
              AND received_at <= NEW.received_at
              AND source_time >= (
                  SELECT source_time FROM phase1_observations
                  WHERE observation_id = NEW.trigger_observation_id
              )
        )
    )
    OR (
        NEW.session_completion_id IS NOT NULL
        AND NOT EXISTS (
            SELECT 1 FROM phase1_session_completions AS completion
            JOIN phase1_signals AS signal
              ON signal.signal_id = completion.signal_id
            WHERE completion.completion_id = NEW.session_completion_id
              AND completion.signal_id = NEW.signal_id
              AND completion.session_date = signal.publication_session
              AND (NEW.event_kind NOT IN ('FINALIZE_NOT_TRIGGERED', 'FINALIZE_NOT_FILLED', 'FINALIZE_UNRESOLVED', 'EXPIRE') OR completion.completed_at <= NEW.event_time)
              AND completion.completed_at <= NEW.received_at
        )
    )
    OR (
        NEW.exit_observation_id IS NOT NULL
        AND NOT EXISTS (
            SELECT 1
            FROM phase1_exit_review_facts AS fact
            JOIN phase1_exit_reviews AS review
              ON review.review_id = fact.review_id
            WHERE fact.fact_id = NEW.exit_observation_id
              AND review.signal_id = NEW.signal_id
              AND fact.fact_kind = 'BAR'
              AND fact.source_time = NEW.event_time
              AND fact.received_at <= NEW.received_at
        )
    )
    OR (
        NEW.signal_evidence_id IS NOT NULL
        AND NOT EXISTS (
            SELECT 1 FROM phase1_signal_evidence_reviews AS evidence
            WHERE evidence.evidence_id = NEW.signal_evidence_id
              AND evidence.signal_id = NEW.signal_id
              AND evidence.review_at <= NEW.received_at
              AND (
                  json_extract(CAST(evidence.manifest_bytes AS TEXT), '$.event_exit_required') = 1
                  OR json_extract(CAST(evidence.manifest_bytes AS TEXT), '$.thesis_invalidated') = 1
              )
        )
    )
    OR (
        NEW.expiry_source_id IS NOT NULL
        AND NOT EXISTS (
            SELECT 1 FROM phase1_expiry_deadlines AS expiry
            WHERE expiry.expiry_source_id = NEW.expiry_source_id
              AND expiry.signal_id = NEW.signal_id
              AND expiry.observed_at = NEW.event_time
              AND expiry.observed_at <= NEW.received_at
        )
    )
    OR (
        NEW.from_status = 'TRIGGERED_AWAITING_LIMIT'
        AND NEW.event_kind IN ('PAPER_FILL', 'LIVE_CONFIRM', 'LIVE_SKIP')
        AND EXISTS (
            SELECT 1 FROM phase1_signals AS signal
            WHERE signal.signal_id = NEW.signal_id
              AND signal.role != 'PRIMARY'
        )
    )
    OR (
        NEW.event_kind = 'SHADOW_FILL'
        AND NOT EXISTS (
            SELECT 1 FROM phase1_signals AS signal
            WHERE signal.signal_id = NEW.signal_id
              AND signal.role = 'WATCHLIST_SHADOW'
        )
    )
    OR NOT (
        (NEW.event_ordinal = 0 AND NEW.event_kind = 'PUBLISHED'
            AND NEW.from_status IS NULL AND NEW.to_status = 'PUBLISHED')
        OR (NEW.from_status = 'PUBLISHED'
            AND NEW.event_kind = 'TRIGGER_OBSERVED'
            AND NEW.to_status = 'TRIGGERED_AWAITING_LIMIT')
        OR (NEW.from_status = 'PUBLISHED'
            AND NEW.event_kind = 'FINALIZE_NOT_TRIGGERED'
            AND NEW.to_status = 'NOT_TRIGGERED')
        OR (NEW.from_status IN ('PUBLISHED', 'TRIGGERED_AWAITING_LIMIT')
            AND NEW.event_kind = 'FINALIZE_UNRESOLVED'
            AND NEW.to_status = 'UNRESOLVED')
        OR (NEW.from_status IN ('PUBLISHED', 'TRIGGERED_AWAITING_LIMIT')
            AND NEW.event_kind = 'EXPIRE' AND NEW.to_status = 'EXPIRED')
        OR (NEW.from_status IN ('PUBLISHED', 'TRIGGERED_AWAITING_LIMIT')
            AND NEW.event_kind = 'INVALIDATE' AND NEW.to_status = 'INVALIDATED')
        OR (NEW.from_status = 'TRIGGERED_AWAITING_LIMIT'
            AND NEW.event_kind = 'PAPER_FILL'
            AND NEW.to_status = 'TRIGGERED_PAPER')
        OR (NEW.from_status = 'TRIGGERED_AWAITING_LIMIT'
            AND NEW.event_kind = 'SHADOW_FILL'
            AND NEW.to_status = 'SHADOW_FILLED_INFORMATIONAL')
        OR (NEW.from_status = 'TRIGGERED_AWAITING_LIMIT'
            AND NEW.event_kind = 'LIVE_CONFIRM'
            AND NEW.to_status = 'LIVE_CONFIRMED')
        OR (NEW.from_status = 'TRIGGERED_AWAITING_LIMIT'
            AND NEW.event_kind = 'LIVE_SKIP'
            AND NEW.to_status = 'SKIPPED_LIVE_TRACKED_PAPER')
        OR (NEW.from_status = 'TRIGGERED_PAPER'
            AND NEW.event_kind = 'LIVE_CONFIRM'
            AND NEW.to_status = 'LIVE_CONFIRMED')
        OR (NEW.from_status = 'TRIGGERED_PAPER'
            AND NEW.event_kind = 'LIVE_SKIP'
            AND NEW.to_status = 'SKIPPED_LIVE_TRACKED_PAPER')
        OR (NEW.from_status = 'TRIGGERED_AWAITING_LIMIT'
            AND NEW.event_kind = 'FINALIZE_NOT_FILLED'
            AND NEW.to_status = 'NOT_FILLED_LIMIT')
        OR (NEW.from_status IN ('TRIGGERED_PAPER', 'LIVE_CONFIRMED', 'SKIPPED_LIVE_TRACKED_PAPER')
            AND NEW.event_kind = 'CLOSE' AND NEW.to_status = 'CLOSED')
        OR (NEW.from_status IN ('TRIGGERED_PAPER', 'LIVE_CONFIRMED', 'SKIPPED_LIVE_TRACKED_PAPER')
            AND NEW.event_kind = 'PARTIAL_EXIT' AND NEW.to_status = NEW.from_status)
    )
BEGIN
    SELECT RAISE(ABORT, 'phase1 lifecycle event is out of sequence');
END;

CREATE TRIGGER phase1_canonical_postings_validate_lineage
BEFORE INSERT ON phase1_canonical_postings
WHEN NOT EXISTS (
    SELECT 1 FROM phase1_signal_events
    WHERE lifecycle_event_id = NEW.lifecycle_event_id
      AND signal_id = NEW.signal_id
      AND received_at <= NEW.received_at
      AND (
          (NEW.entry_kind = 'BUY' AND (
              event_kind = 'PAPER_FILL'
              OR (event_kind IN ('LIVE_CONFIRM', 'LIVE_SKIP')
                  AND from_status = 'TRIGGERED_AWAITING_LIMIT')
          ))
          OR (NEW.entry_kind IN ('SALE', 'FEE')
              AND event_kind IN ('PARTIAL_EXIT', 'CLOSE'))
      )
)
OR (
    NEW.entry_kind = 'BUY'
    AND NOT EXISTS (
        SELECT 1 FROM phase1_signals AS signal
        WHERE signal.signal_id = NEW.signal_id
          AND signal.role = 'PRIMARY'
    )
)
OR (
    NEW.entry_kind = 'SALE'
    AND NOT EXISTS (
        SELECT 1 FROM phase1_signal_events AS event
        WHERE event.lifecycle_event_id = NEW.lifecycle_event_id
          AND event.shares = -NEW.shares_delta
          AND event.price_micros = NEW.unit_price_micros
          AND NEW.amount_micros = -NEW.shares_delta * NEW.unit_price_micros
          AND NEW.occurred_at = event.event_time
    )
)
OR (
    NEW.entry_kind = 'FEE'
    AND (
        NEW.amount_micros != -1000000
        OR NEW.occurred_at != (
            SELECT event_time FROM phase1_signal_events
            WHERE lifecycle_event_id = NEW.lifecycle_event_id
        )
        OR NEW.fee_schedule_version != 'PHASE1_US_EQUITY_EXIT_V1'
        OR NEW.fee_schedule_digest != '7a430c6cb0a4057c8bed785bf3033b771333fb228668b70513a664d402485a0f'
    )
)
BEGIN
    SELECT RAISE(ABORT, 'phase1 canonical posting requires lifecycle lineage');
END;

CREATE TRIGGER phase1_closed_trades_validate_lineage
BEFORE INSERT ON phase1_closed_trades
WHEN NOT EXISTS (
    SELECT 1
    FROM phase1_signal_events AS event
    WHERE event.lifecycle_event_id = NEW.lifecycle_event_id
      AND event.signal_id = NEW.signal_id
      AND event.event_kind = 'CLOSE'
      AND event.received_at <= NEW.received_at
      AND 1 = (
          SELECT COUNT(*) FROM phase1_canonical_postings
          WHERE lifecycle_event_id = event.lifecycle_event_id
            AND entry_kind = 'SALE'
      )
      AND 1 = (
          SELECT COUNT(*) FROM phase1_canonical_postings
          WHERE lifecycle_event_id = event.lifecycle_event_id
            AND entry_kind = 'FEE'
      )
)
BEGIN
    SELECT RAISE(ABORT, 'phase1 closed trade requires close lineage');
END;

CREATE TRIGGER phase1_validation_windows_no_conflicting_insert
BEFORE INSERT ON phase1_validation_windows
WHEN EXISTS (SELECT 1 FROM phase1_validation_windows WHERE id = NEW.id OR window_id = NEW.window_id COLLATE BINARY)
BEGIN SELECT RAISE(ABORT, 'phase1_validation_windows rejects conflicting inserts'); END;
CREATE TRIGGER phase1_source_payloads_no_conflicting_insert
BEFORE INSERT ON phase1_source_payloads
WHEN EXISTS (SELECT 1 FROM phase1_source_payloads WHERE id = NEW.id OR source_observation_id = NEW.source_observation_id)
BEGIN SELECT RAISE(ABORT, 'phase1_source_payloads rejects conflicting inserts'); END;
CREATE TRIGGER phase1_publication_manifests_no_conflicting_insert
BEFORE INSERT ON phase1_publication_manifests
WHEN EXISTS (SELECT 1 FROM phase1_publication_manifests WHERE id = NEW.id OR publication_report_id = NEW.publication_report_id)
BEGIN SELECT RAISE(ABORT, 'phase1_publication_manifests rejects conflicting inserts'); END;
CREATE TRIGGER phase1_publication_fetch_pages_no_conflicting_insert
BEFORE INSERT ON phase1_publication_fetch_pages
WHEN EXISTS (SELECT 1 FROM phase1_publication_fetch_pages WHERE id = NEW.id OR (publication_report_id = NEW.publication_report_id AND (((fetch_manifest_ordinal = NEW.fetch_manifest_ordinal OR fetch_manifest_digest = NEW.fetch_manifest_digest COLLATE BINARY) AND page_ordinal = NEW.page_ordinal) OR external_source_observation_id = NEW.external_source_observation_id COLLATE BINARY)))
BEGIN SELECT RAISE(ABORT, 'phase1_publication_fetch_pages rejects conflicting inserts'); END;
CREATE TRIGGER phase1_publication_facts_no_conflicting_insert
BEFORE INSERT ON phase1_publication_facts
WHEN EXISTS (SELECT 1 FROM phase1_publication_facts WHERE id = NEW.id OR (publication_report_id = NEW.publication_report_id AND (fact_ordinal = NEW.fact_ordinal OR (candidate_symbol = NEW.candidate_symbol COLLATE BINARY AND external_source_observation_id = NEW.external_source_observation_id COLLATE BINARY AND source_item_path = NEW.source_item_path COLLATE BINARY))))
BEGIN SELECT RAISE(ABORT, 'phase1_publication_facts rejects conflicting inserts'); END;
CREATE TRIGGER phase1_signals_no_conflicting_insert
BEFORE INSERT ON phase1_signals
WHEN EXISTS (SELECT 1 FROM phase1_signals WHERE id = NEW.id OR signal_id = NEW.signal_id COLLATE BINARY)
BEGIN SELECT RAISE(ABORT, 'phase1_signals rejects conflicting inserts'); END;
CREATE TRIGGER phase1_observation_fetch_manifests_no_conflicting_insert
BEFORE INSERT ON phase1_observation_fetch_manifests
WHEN EXISTS (SELECT 1 FROM phase1_observation_fetch_manifests WHERE id = NEW.id OR cohort_id = NEW.cohort_id COLLATE BINARY OR (signal_id = NEW.signal_id COLLATE BINARY AND purpose = NEW.purpose AND (manifest_digest = NEW.manifest_digest COLLATE BINARY OR semantic_manifest_digest = NEW.semantic_manifest_digest COLLATE BINARY OR (session_date = NEW.session_date AND collection_name = NEW.collection_name))))
BEGIN SELECT RAISE(ABORT, 'phase1_observation_fetch_manifests rejects conflicting inserts'); END;
CREATE TRIGGER phase1_observation_fetch_pages_no_conflicting_insert
BEFORE INSERT ON phase1_observation_fetch_pages
WHEN EXISTS (SELECT 1 FROM phase1_observation_fetch_pages WHERE id = NEW.id OR (cohort_id = NEW.cohort_id COLLATE BINARY AND (page_ordinal = NEW.page_ordinal OR source_observation_id = NEW.source_observation_id OR external_source_observation_id = NEW.external_source_observation_id COLLATE BINARY)))
BEGIN SELECT RAISE(ABORT, 'phase1_observation_fetch_pages rejects conflicting inserts'); END;
CREATE TRIGGER phase1_observations_no_conflicting_insert
BEFORE INSERT ON phase1_observations
WHEN EXISTS (SELECT 1 FROM phase1_observations WHERE id = NEW.id OR observation_id = NEW.observation_id COLLATE BINARY OR (signal_id = NEW.signal_id AND session_date = NEW.session_date AND cohort_ordinal = NEW.cohort_ordinal) OR (stream_id = NEW.stream_id AND source_ordinal = NEW.source_ordinal) OR (source_observation_id = NEW.source_observation_id AND source_item_ordinal = NEW.source_item_ordinal) OR (source_observation_id = NEW.source_observation_id AND source_item_path = NEW.source_item_path COLLATE BINARY))
BEGIN SELECT RAISE(ABORT, 'phase1_observations rejects conflicting inserts'); END;
CREATE TRIGGER phase1_session_completions_no_conflicting_insert
BEFORE INSERT ON phase1_session_completions
WHEN EXISTS (SELECT 1 FROM phase1_session_completions WHERE id = NEW.id OR completion_id = NEW.completion_id COLLATE BINARY OR (signal_id = NEW.signal_id AND session_date = NEW.session_date))
BEGIN SELECT RAISE(ABORT, 'phase1_session_completions rejects conflicting inserts'); END;
CREATE TRIGGER phase1_session_late_evidence_no_conflicting_insert
BEFORE INSERT ON phase1_session_late_evidence
WHEN EXISTS (SELECT 1 FROM phase1_session_late_evidence WHERE id = NEW.id OR late_evidence_id = NEW.late_evidence_id COLLATE BINARY OR (completion_id = NEW.completion_id COLLATE BINARY AND semantic_manifest_digest = NEW.semantic_manifest_digest COLLATE BINARY))
BEGIN SELECT RAISE(ABORT, 'phase1_session_late_evidence rejects conflicting inserts'); END;
CREATE TRIGGER phase1_session_late_evidence_pages_no_conflicting_insert
BEFORE INSERT ON phase1_session_late_evidence_pages
WHEN EXISTS (SELECT 1 FROM phase1_session_late_evidence_pages WHERE id = NEW.id OR (late_evidence_id = NEW.late_evidence_id COLLATE BINARY AND (page_ordinal = NEW.page_ordinal OR source_observation_id = NEW.source_observation_id OR external_source_observation_id = NEW.external_source_observation_id COLLATE BINARY)))
BEGIN SELECT RAISE(ABORT, 'phase1_session_late_evidence_pages rejects conflicting inserts'); END;
CREATE TRIGGER phase1_signal_evidence_reviews_no_conflicting_insert
BEFORE INSERT ON phase1_signal_evidence_reviews
WHEN EXISTS (SELECT 1 FROM phase1_signal_evidence_reviews WHERE id = NEW.id OR evidence_id = NEW.evidence_id COLLATE BINARY OR (signal_id = NEW.signal_id COLLATE BINARY AND review_at = NEW.review_at))
BEGIN SELECT RAISE(ABORT, 'phase1_signal_evidence_reviews rejects conflicting inserts'); END;
CREATE TRIGGER phase1_signal_evidence_bindings_no_conflicting_insert
BEFORE INSERT ON phase1_signal_evidence_bindings
WHEN EXISTS (SELECT 1 FROM phase1_signal_evidence_bindings WHERE id = NEW.id OR (evidence_id = NEW.evidence_id COLLATE BINARY AND (binding_ordinal = NEW.binding_ordinal OR source_observation_row_id = NEW.source_observation_row_id OR external_source_observation_id = NEW.external_source_observation_id COLLATE BINARY)))
BEGIN SELECT RAISE(ABORT, 'phase1_signal_evidence_bindings rejects conflicting inserts'); END;
CREATE TRIGGER phase1_expiry_deadlines_no_conflicting_insert
BEFORE INSERT ON phase1_expiry_deadlines
WHEN EXISTS (SELECT 1 FROM phase1_expiry_deadlines WHERE id = NEW.id OR expiry_source_id = NEW.expiry_source_id COLLATE BINARY OR (signal_id = NEW.signal_id COLLATE BINARY AND observed_at = NEW.observed_at))
BEGIN SELECT RAISE(ABORT, 'phase1_expiry_deadlines rejects conflicting inserts'); END;
CREATE TRIGGER phase1_exit_reviews_no_conflicting_insert
BEFORE INSERT ON phase1_exit_reviews
WHEN EXISTS (SELECT 1 FROM phase1_exit_reviews WHERE id = NEW.id OR review_id = NEW.review_id COLLATE BINARY OR (signal_id = NEW.signal_id COLLATE BINARY AND review_session = NEW.review_session))
BEGIN SELECT RAISE(ABORT, 'phase1_exit_reviews rejects conflicting inserts'); END;
CREATE TRIGGER phase1_exit_review_manifests_no_conflicting_insert
BEFORE INSERT ON phase1_exit_review_manifests
WHEN EXISTS (SELECT 1 FROM phase1_exit_review_manifests WHERE id = NEW.id OR (review_id = NEW.review_id COLLATE BINARY AND (purpose = NEW.purpose OR manifest_digest = NEW.manifest_digest COLLATE BINARY)))
BEGIN SELECT RAISE(ABORT, 'phase1_exit_review_manifests rejects conflicting inserts'); END;
CREATE TRIGGER phase1_exit_review_pages_no_conflicting_insert
BEFORE INSERT ON phase1_exit_review_pages
WHEN EXISTS (SELECT 1 FROM phase1_exit_review_pages WHERE id = NEW.id OR (review_id = NEW.review_id COLLATE BINARY AND purpose = NEW.purpose AND (page_ordinal = NEW.page_ordinal OR source_observation_id = NEW.source_observation_id OR external_source_observation_id = NEW.external_source_observation_id COLLATE BINARY)))
BEGIN SELECT RAISE(ABORT, 'phase1_exit_review_pages rejects conflicting inserts'); END;
CREATE TRIGGER phase1_exit_review_facts_no_conflicting_insert
BEFORE INSERT ON phase1_exit_review_facts
WHEN EXISTS (SELECT 1 FROM phase1_exit_review_facts WHERE id = NEW.id OR fact_id = NEW.fact_id COLLATE BINARY OR (review_id = NEW.review_id COLLATE BINARY AND purpose = NEW.purpose AND (fact_ordinal = NEW.fact_ordinal OR (source_observation_id = NEW.source_observation_id AND (source_item_ordinal = NEW.source_item_ordinal OR source_item_path = NEW.source_item_path COLLATE BINARY)))))
BEGIN SELECT RAISE(ABORT, 'phase1_exit_review_facts rejects conflicting inserts'); END;
CREATE TRIGGER phase1_equity_mark_sets_no_conflicting_insert
BEFORE INSERT ON phase1_equity_mark_sets
WHEN EXISTS (SELECT 1 FROM phase1_equity_mark_sets WHERE id = NEW.id OR mark_set_id = NEW.mark_set_id COLLATE BINARY OR session_date = NEW.session_date)
BEGIN SELECT RAISE(ABORT, 'phase1_equity_mark_sets rejects conflicting inserts'); END;
CREATE TRIGGER phase1_equity_mark_manifests_no_conflicting_insert
BEFORE INSERT ON phase1_equity_mark_manifests
WHEN EXISTS (SELECT 1 FROM phase1_equity_mark_manifests WHERE id = NEW.id OR (mark_set_id = NEW.mark_set_id COLLATE BINARY AND (purpose = NEW.purpose OR manifest_digest = NEW.manifest_digest COLLATE BINARY)))
BEGIN SELECT RAISE(ABORT, 'phase1_equity_mark_manifests rejects conflicting inserts'); END;
CREATE TRIGGER phase1_equity_mark_pages_no_conflicting_insert
BEFORE INSERT ON phase1_equity_mark_pages
WHEN EXISTS (SELECT 1 FROM phase1_equity_mark_pages WHERE id = NEW.id OR (mark_set_id = NEW.mark_set_id COLLATE BINARY AND purpose = NEW.purpose AND (page_ordinal = NEW.page_ordinal OR source_observation_id = NEW.source_observation_id OR external_source_observation_id = NEW.external_source_observation_id COLLATE BINARY)))
BEGIN SELECT RAISE(ABORT, 'phase1_equity_mark_pages rejects conflicting inserts'); END;
CREATE TRIGGER phase1_equity_mark_facts_no_conflicting_insert
BEFORE INSERT ON phase1_equity_mark_facts
WHEN EXISTS (SELECT 1 FROM phase1_equity_mark_facts WHERE id = NEW.id OR fact_id = NEW.fact_id COLLATE BINARY OR (mark_set_id = NEW.mark_set_id COLLATE BINARY AND purpose = NEW.purpose AND (fact_ordinal = NEW.fact_ordinal OR (source_observation_id = NEW.source_observation_id AND (source_item_ordinal = NEW.source_item_ordinal OR source_item_path = NEW.source_item_path COLLATE BINARY)))))
BEGIN SELECT RAISE(ABORT, 'phase1_equity_mark_facts rejects conflicting inserts'); END;
CREATE TRIGGER phase1_equity_mark_invalidations_no_conflicting_insert
BEFORE INSERT ON phase1_equity_mark_invalidations
WHEN EXISTS (SELECT 1 FROM phase1_equity_mark_invalidations WHERE id = NEW.id OR invalidation_id = NEW.invalidation_id COLLATE BINARY)
BEGIN SELECT RAISE(ABORT, 'phase1_equity_mark_invalidations rejects conflicting inserts'); END;
CREATE TRIGGER phase1_equity_mark_invalidation_pages_no_conflicting_insert
BEFORE INSERT ON phase1_equity_mark_invalidation_pages
WHEN EXISTS (SELECT 1 FROM phase1_equity_mark_invalidation_pages WHERE id = NEW.id OR (invalidation_id = NEW.invalidation_id COLLATE BINARY AND purpose = NEW.purpose AND (page_ordinal = NEW.page_ordinal OR source_observation_id = NEW.source_observation_id)))
BEGIN SELECT RAISE(ABORT, 'phase1_equity_mark_invalidation_pages rejects conflicting inserts'); END;
CREATE TRIGGER phase1_signal_events_no_conflicting_insert
BEFORE INSERT ON phase1_signal_events
WHEN EXISTS (SELECT 1 FROM phase1_signal_events WHERE id = NEW.id OR lifecycle_event_id = NEW.lifecycle_event_id COLLATE BINARY OR (signal_id = NEW.signal_id AND event_ordinal = NEW.event_ordinal))
BEGIN SELECT RAISE(ABORT, 'phase1_signal_events rejects conflicting inserts'); END;
CREATE TRIGGER phase1_canonical_postings_no_conflicting_insert
BEFORE INSERT ON phase1_canonical_postings
WHEN EXISTS (SELECT 1 FROM phase1_canonical_postings WHERE id = NEW.id OR posting_key = NEW.posting_key COLLATE BINARY)
BEGIN SELECT RAISE(ABORT, 'phase1_canonical_postings rejects conflicting inserts'); END;
CREATE TRIGGER phase1_equity_points_no_conflicting_insert
BEFORE INSERT ON phase1_equity_points
WHEN EXISTS (SELECT 1 FROM phase1_equity_points WHERE id = NEW.id OR point_id = NEW.point_id COLLATE BINARY OR (validation_window_id = NEW.validation_window_id AND ledger_name = NEW.ledger_name AND session_date = NEW.session_date AND at = NEW.at))
BEGIN SELECT RAISE(ABORT, 'phase1_equity_points rejects conflicting inserts'); END;
CREATE TRIGGER phase1_equity_point_marks_no_conflicting_insert
BEFORE INSERT ON phase1_equity_point_marks
WHEN EXISTS (SELECT 1 FROM phase1_equity_point_marks WHERE id = NEW.id OR (equity_point_id = NEW.equity_point_id AND (mark_ordinal = NEW.mark_ordinal OR symbol = NEW.symbol COLLATE BINARY OR observation_id = NEW.observation_id COLLATE BINARY)))
BEGIN SELECT RAISE(ABORT, 'phase1_equity_point_marks rejects conflicting inserts'); END;
CREATE TRIGGER phase1_closed_trades_no_conflicting_insert
BEFORE INSERT ON phase1_closed_trades
WHEN EXISTS (SELECT 1 FROM phase1_closed_trades WHERE id = NEW.id OR trade_id = NEW.trade_id COLLATE BINARY)
BEGIN SELECT RAISE(ABORT, 'phase1_closed_trades rejects conflicting inserts'); END;
CREATE TRIGGER phase1_adherence_checks_no_conflicting_insert
BEFORE INSERT ON phase1_adherence_checks
WHEN EXISTS (SELECT 1 FROM phase1_adherence_checks WHERE id = NEW.id OR check_id = NEW.check_id COLLATE BINARY OR (validation_window_id = NEW.validation_window_id AND signal_id IS NEW.signal_id AND check_name = NEW.check_name COLLATE BINARY))
BEGIN SELECT RAISE(ABORT, 'phase1_adherence_checks rejects conflicting inserts'); END;

CREATE TRIGGER phase1_validation_windows_no_update BEFORE UPDATE ON phase1_validation_windows BEGIN SELECT RAISE(ABORT, 'phase1_validation_windows is append-only'); END;
CREATE TRIGGER phase1_validation_windows_no_delete BEFORE DELETE ON phase1_validation_windows BEGIN SELECT RAISE(ABORT, 'phase1_validation_windows is append-only'); END;
CREATE TRIGGER phase1_source_payloads_no_update BEFORE UPDATE ON phase1_source_payloads BEGIN SELECT RAISE(ABORT, 'phase1_source_payloads is append-only'); END;
CREATE TRIGGER phase1_source_payloads_no_delete BEFORE DELETE ON phase1_source_payloads BEGIN SELECT RAISE(ABORT, 'phase1_source_payloads is append-only'); END;
CREATE TRIGGER phase1_publication_manifests_no_update BEFORE UPDATE ON phase1_publication_manifests BEGIN SELECT RAISE(ABORT, 'phase1_publication_manifests is append-only'); END;
CREATE TRIGGER phase1_publication_manifests_no_delete BEFORE DELETE ON phase1_publication_manifests BEGIN SELECT RAISE(ABORT, 'phase1_publication_manifests is append-only'); END;
CREATE TRIGGER phase1_publication_fetch_pages_no_update BEFORE UPDATE ON phase1_publication_fetch_pages BEGIN SELECT RAISE(ABORT, 'phase1_publication_fetch_pages is append-only'); END;
CREATE TRIGGER phase1_publication_fetch_pages_no_delete BEFORE DELETE ON phase1_publication_fetch_pages BEGIN SELECT RAISE(ABORT, 'phase1_publication_fetch_pages is append-only'); END;
CREATE TRIGGER phase1_publication_facts_no_update BEFORE UPDATE ON phase1_publication_facts BEGIN SELECT RAISE(ABORT, 'phase1_publication_facts is append-only'); END;
CREATE TRIGGER phase1_publication_facts_no_delete BEFORE DELETE ON phase1_publication_facts BEGIN SELECT RAISE(ABORT, 'phase1_publication_facts is append-only'); END;
CREATE TRIGGER phase1_signals_no_update BEFORE UPDATE ON phase1_signals BEGIN SELECT RAISE(ABORT, 'phase1_signals is append-only'); END;
CREATE TRIGGER phase1_signals_no_delete BEFORE DELETE ON phase1_signals BEGIN SELECT RAISE(ABORT, 'phase1_signals is append-only'); END;
CREATE TRIGGER phase1_observation_fetch_manifests_no_update BEFORE UPDATE ON phase1_observation_fetch_manifests BEGIN SELECT RAISE(ABORT, 'phase1_observation_fetch_manifests is append-only'); END;
CREATE TRIGGER phase1_observation_fetch_manifests_no_delete BEFORE DELETE ON phase1_observation_fetch_manifests BEGIN SELECT RAISE(ABORT, 'phase1_observation_fetch_manifests is append-only'); END;
CREATE TRIGGER phase1_observation_fetch_pages_no_update BEFORE UPDATE ON phase1_observation_fetch_pages BEGIN SELECT RAISE(ABORT, 'phase1_observation_fetch_pages is append-only'); END;
CREATE TRIGGER phase1_observation_fetch_pages_no_delete BEFORE DELETE ON phase1_observation_fetch_pages BEGIN SELECT RAISE(ABORT, 'phase1_observation_fetch_pages is append-only'); END;
CREATE TRIGGER phase1_observations_no_update BEFORE UPDATE ON phase1_observations BEGIN SELECT RAISE(ABORT, 'phase1_observations is append-only'); END;
CREATE TRIGGER phase1_observations_no_delete BEFORE DELETE ON phase1_observations BEGIN SELECT RAISE(ABORT, 'phase1_observations is append-only'); END;
CREATE TRIGGER phase1_session_completions_no_update BEFORE UPDATE ON phase1_session_completions BEGIN SELECT RAISE(ABORT, 'phase1_session_completions is append-only'); END;
CREATE TRIGGER phase1_session_completions_no_delete BEFORE DELETE ON phase1_session_completions BEGIN SELECT RAISE(ABORT, 'phase1_session_completions is append-only'); END;
CREATE TRIGGER phase1_session_late_evidence_no_update BEFORE UPDATE ON phase1_session_late_evidence BEGIN SELECT RAISE(ABORT, 'phase1_session_late_evidence is append-only'); END;
CREATE TRIGGER phase1_session_late_evidence_no_delete BEFORE DELETE ON phase1_session_late_evidence BEGIN SELECT RAISE(ABORT, 'phase1_session_late_evidence is append-only'); END;
CREATE TRIGGER phase1_session_late_evidence_pages_no_update BEFORE UPDATE ON phase1_session_late_evidence_pages BEGIN SELECT RAISE(ABORT, 'phase1_session_late_evidence_pages is append-only'); END;
CREATE TRIGGER phase1_session_late_evidence_pages_no_delete BEFORE DELETE ON phase1_session_late_evidence_pages BEGIN SELECT RAISE(ABORT, 'phase1_session_late_evidence_pages is append-only'); END;
CREATE TRIGGER phase1_signal_evidence_reviews_no_update BEFORE UPDATE ON phase1_signal_evidence_reviews BEGIN SELECT RAISE(ABORT, 'phase1_signal_evidence_reviews is append-only'); END;
CREATE TRIGGER phase1_signal_evidence_reviews_no_delete BEFORE DELETE ON phase1_signal_evidence_reviews BEGIN SELECT RAISE(ABORT, 'phase1_signal_evidence_reviews is append-only'); END;
CREATE TRIGGER phase1_signal_evidence_bindings_no_update BEFORE UPDATE ON phase1_signal_evidence_bindings BEGIN SELECT RAISE(ABORT, 'phase1_signal_evidence_bindings is append-only'); END;
CREATE TRIGGER phase1_signal_evidence_bindings_no_delete BEFORE DELETE ON phase1_signal_evidence_bindings BEGIN SELECT RAISE(ABORT, 'phase1_signal_evidence_bindings is append-only'); END;
CREATE TRIGGER phase1_expiry_deadlines_no_update BEFORE UPDATE ON phase1_expiry_deadlines BEGIN SELECT RAISE(ABORT, 'phase1_expiry_deadlines is append-only'); END;
CREATE TRIGGER phase1_expiry_deadlines_no_delete BEFORE DELETE ON phase1_expiry_deadlines BEGIN SELECT RAISE(ABORT, 'phase1_expiry_deadlines is append-only'); END;
CREATE TRIGGER phase1_exit_reviews_no_update BEFORE UPDATE ON phase1_exit_reviews BEGIN SELECT RAISE(ABORT, 'phase1_exit_reviews is append-only'); END;
CREATE TRIGGER phase1_exit_reviews_no_delete BEFORE DELETE ON phase1_exit_reviews BEGIN SELECT RAISE(ABORT, 'phase1_exit_reviews is append-only'); END;
CREATE TRIGGER phase1_exit_review_manifests_no_update BEFORE UPDATE ON phase1_exit_review_manifests BEGIN SELECT RAISE(ABORT, 'phase1_exit_review_manifests is append-only'); END;
CREATE TRIGGER phase1_exit_review_manifests_no_delete BEFORE DELETE ON phase1_exit_review_manifests BEGIN SELECT RAISE(ABORT, 'phase1_exit_review_manifests is append-only'); END;
CREATE TRIGGER phase1_exit_review_pages_no_update BEFORE UPDATE ON phase1_exit_review_pages BEGIN SELECT RAISE(ABORT, 'phase1_exit_review_pages is append-only'); END;
CREATE TRIGGER phase1_exit_review_pages_no_delete BEFORE DELETE ON phase1_exit_review_pages BEGIN SELECT RAISE(ABORT, 'phase1_exit_review_pages is append-only'); END;
CREATE TRIGGER phase1_exit_review_facts_no_update BEFORE UPDATE ON phase1_exit_review_facts BEGIN SELECT RAISE(ABORT, 'phase1_exit_review_facts is append-only'); END;
CREATE TRIGGER phase1_exit_review_facts_no_delete BEFORE DELETE ON phase1_exit_review_facts BEGIN SELECT RAISE(ABORT, 'phase1_exit_review_facts is append-only'); END;
CREATE TRIGGER phase1_equity_mark_sets_no_update BEFORE UPDATE ON phase1_equity_mark_sets BEGIN SELECT RAISE(ABORT, 'phase1_equity_mark_sets is append-only'); END;
CREATE TRIGGER phase1_equity_mark_sets_no_delete BEFORE DELETE ON phase1_equity_mark_sets BEGIN SELECT RAISE(ABORT, 'phase1_equity_mark_sets is append-only'); END;
CREATE TRIGGER phase1_equity_mark_manifests_no_update BEFORE UPDATE ON phase1_equity_mark_manifests BEGIN SELECT RAISE(ABORT, 'phase1_equity_mark_manifests is append-only'); END;
CREATE TRIGGER phase1_equity_mark_manifests_no_delete BEFORE DELETE ON phase1_equity_mark_manifests BEGIN SELECT RAISE(ABORT, 'phase1_equity_mark_manifests is append-only'); END;
CREATE TRIGGER phase1_equity_mark_pages_no_update BEFORE UPDATE ON phase1_equity_mark_pages BEGIN SELECT RAISE(ABORT, 'phase1_equity_mark_pages is append-only'); END;
CREATE TRIGGER phase1_equity_mark_pages_no_delete BEFORE DELETE ON phase1_equity_mark_pages BEGIN SELECT RAISE(ABORT, 'phase1_equity_mark_pages is append-only'); END;
CREATE TRIGGER phase1_equity_mark_facts_no_update BEFORE UPDATE ON phase1_equity_mark_facts BEGIN SELECT RAISE(ABORT, 'phase1_equity_mark_facts is append-only'); END;
CREATE TRIGGER phase1_equity_mark_facts_no_delete BEFORE DELETE ON phase1_equity_mark_facts BEGIN SELECT RAISE(ABORT, 'phase1_equity_mark_facts is append-only'); END;
CREATE TRIGGER phase1_equity_mark_invalidations_no_update BEFORE UPDATE ON phase1_equity_mark_invalidations BEGIN SELECT RAISE(ABORT, 'phase1_equity_mark_invalidations is append-only'); END;
CREATE TRIGGER phase1_equity_mark_invalidations_no_delete BEFORE DELETE ON phase1_equity_mark_invalidations BEGIN SELECT RAISE(ABORT, 'phase1_equity_mark_invalidations is append-only'); END;
CREATE TRIGGER phase1_equity_mark_invalidation_pages_no_update BEFORE UPDATE ON phase1_equity_mark_invalidation_pages BEGIN SELECT RAISE(ABORT, 'phase1_equity_mark_invalidation_pages is append-only'); END;
CREATE TRIGGER phase1_equity_mark_invalidation_pages_no_delete BEFORE DELETE ON phase1_equity_mark_invalidation_pages BEGIN SELECT RAISE(ABORT, 'phase1_equity_mark_invalidation_pages is append-only'); END;
CREATE TRIGGER phase1_signal_events_no_update BEFORE UPDATE ON phase1_signal_events BEGIN SELECT RAISE(ABORT, 'phase1_signal_events is append-only'); END;
CREATE TRIGGER phase1_signal_events_no_delete BEFORE DELETE ON phase1_signal_events BEGIN SELECT RAISE(ABORT, 'phase1_signal_events is append-only'); END;
CREATE TRIGGER phase1_canonical_postings_no_update BEFORE UPDATE ON phase1_canonical_postings BEGIN SELECT RAISE(ABORT, 'phase1_canonical_postings is append-only'); END;
CREATE TRIGGER phase1_canonical_postings_no_delete BEFORE DELETE ON phase1_canonical_postings BEGIN SELECT RAISE(ABORT, 'phase1_canonical_postings is append-only'); END;
CREATE TRIGGER phase1_equity_points_no_update BEFORE UPDATE ON phase1_equity_points BEGIN SELECT RAISE(ABORT, 'phase1_equity_points is append-only'); END;
CREATE TRIGGER phase1_equity_points_no_delete BEFORE DELETE ON phase1_equity_points BEGIN SELECT RAISE(ABORT, 'phase1_equity_points is append-only'); END;
CREATE TRIGGER phase1_equity_point_marks_no_update BEFORE UPDATE ON phase1_equity_point_marks BEGIN SELECT RAISE(ABORT, 'phase1_equity_point_marks is append-only'); END;
CREATE TRIGGER phase1_equity_point_marks_no_delete BEFORE DELETE ON phase1_equity_point_marks BEGIN SELECT RAISE(ABORT, 'phase1_equity_point_marks is append-only'); END;
CREATE TRIGGER phase1_closed_trades_no_update BEFORE UPDATE ON phase1_closed_trades BEGIN SELECT RAISE(ABORT, 'phase1_closed_trades is append-only'); END;
CREATE TRIGGER phase1_closed_trades_no_delete BEFORE DELETE ON phase1_closed_trades BEGIN SELECT RAISE(ABORT, 'phase1_closed_trades is append-only'); END;
CREATE TRIGGER phase1_adherence_checks_no_update BEFORE UPDATE ON phase1_adherence_checks BEGIN SELECT RAISE(ABORT, 'phase1_adherence_checks is append-only'); END;
CREATE TRIGGER phase1_adherence_checks_no_delete BEFORE DELETE ON phase1_adherence_checks BEGIN SELECT RAISE(ABORT, 'phase1_adherence_checks is append-only'); END;
