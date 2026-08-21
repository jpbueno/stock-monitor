CREATE TABLE phase2_windows (
    id INTEGER PRIMARY KEY CHECK(typeof(id) = 'integer' AND id > 0),
    window_id TEXT NOT NULL COLLATE BINARY UNIQUE
        CHECK(length(window_id) = 64 AND window_id NOT GLOB '*[^0-9a-f]*'),
    phase1_validation_window_id TEXT NOT NULL COLLATE BINARY,
    promotion_source_digest TEXT NOT NULL COLLATE BINARY
        CHECK(length(promotion_source_digest) = 64 AND promotion_source_digest NOT GLOB '*[^0-9a-f]*'),
    promotion_decision_digest TEXT NOT NULL COLLATE BINARY
        CHECK(length(promotion_decision_digest) = 64 AND promotion_decision_digest NOT GLOB '*[^0-9a-f]*'),
    promotion_signal_ids_json TEXT NOT NULL CHECK(length(promotion_signal_ids_json) > 2),
    promotion_through_session TEXT NOT NULL
        CHECK(length(promotion_through_session) = 10 AND promotion_through_session = strftime('%Y-%m-%d', promotion_through_session)),
    promotion_query_cutoff TEXT NOT NULL
        CHECK(length(promotion_query_cutoff) = 27 AND substr(promotion_query_cutoff, 1, 19) = strftime('%Y-%m-%dT%H:%M:%S', promotion_query_cutoff) AND substr(promotion_query_cutoff, 20, 1) = '.' AND substr(promotion_query_cutoff, 21, 6) NOT GLOB '*[^0-9]*' AND substr(promotion_query_cutoff, 27, 1) = 'Z'),
    start_execution_event_id INTEGER NOT NULL UNIQUE
        CHECK(typeof(start_execution_event_id) = 'integer' AND start_execution_event_id > 0),
    start_raw_message_id INTEGER NOT NULL
        CHECK(typeof(start_raw_message_id) = 'integer' AND start_raw_message_id > 0),
    started_session TEXT NOT NULL
        CHECK(length(started_session) = 10 AND started_session = strftime('%Y-%m-%d', started_session)),
    started_at TEXT NOT NULL
        CHECK(length(started_at) = 27 AND substr(started_at, 1, 19) = strftime('%Y-%m-%dT%H:%M:%S', started_at) AND substr(started_at, 20, 1) = '.' AND substr(started_at, 21, 6) NOT GLOB '*[^0-9]*' AND substr(started_at, 27, 1) = 'Z'),
    received_at TEXT NOT NULL
        CHECK(length(received_at) = 27 AND substr(received_at, 1, 19) = strftime('%Y-%m-%dT%H:%M:%S', received_at) AND substr(received_at, 20, 1) = '.' AND substr(received_at, 21, 6) NOT GLOB '*[^0-9]*' AND substr(received_at, 27, 1) = 'Z'),
    starting_capital_micros INTEGER NOT NULL
        CHECK(typeof(starting_capital_micros) = 'integer' AND starting_capital_micros = 5000000000),
    calendar_digest TEXT NOT NULL COLLATE BINARY
        CHECK(length(calendar_digest) = 64 AND calendar_digest NOT GLOB '*[^0-9a-f]*'),
    source_digest TEXT NOT NULL COLLATE BINARY
        CHECK(length(source_digest) = 64 AND source_digest NOT GLOB '*[^0-9a-f]*'),
    record_sha256 TEXT NOT NULL COLLATE BINARY
        CHECK(length(record_sha256) = 64 AND record_sha256 NOT GLOB '*[^0-9a-f]*'),
    CHECK(promotion_query_cutoff < started_at AND started_at <= received_at),
    FOREIGN KEY(phase1_validation_window_id) REFERENCES phase1_validation_windows(window_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
    FOREIGN KEY(start_execution_event_id) REFERENCES execution_events(id)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
    FOREIGN KEY(start_raw_message_id) REFERENCES raw_messages(id)
        ON UPDATE RESTRICT ON DELETE RESTRICT
) STRICT;

CREATE TRIGGER phase2_windows_validate_start_source
BEFORE INSERT ON phase2_windows
WHEN NOT EXISTS (
    SELECT 1
    FROM execution_events AS event
    JOIN raw_messages AS raw ON raw.id = event.raw_message_id
    WHERE event.id = NEW.start_execution_event_id
      AND event.raw_message_id = NEW.start_raw_message_id
      AND event.parsed_action = 'OPTION_PAPER_WINDOW_START'
      AND event.event_time = NEW.started_at
      AND raw.message_time <= NEW.received_at
)
BEGIN SELECT RAISE(ABORT, 'phase2 window requires exact start action source'); END;

CREATE TABLE phase2_authorizations (
    id INTEGER PRIMARY KEY CHECK(typeof(id) = 'integer' AND id > 0),
    authorization_id TEXT NOT NULL COLLATE BINARY UNIQUE
        CHECK(length(authorization_id) = 64 AND authorization_id NOT GLOB '*[^0-9a-f]*'),
    window_id TEXT NOT NULL COLLATE BINARY,
    signal_id TEXT NOT NULL COLLATE BINARY,
    signal_source_digest TEXT NOT NULL COLLATE BINARY
        CHECK(length(signal_source_digest) = 64 AND signal_source_digest NOT GLOB '*[^0-9a-f]*'),
    authorization_digest TEXT NOT NULL COLLATE BINARY
        CHECK(length(authorization_digest) = 64 AND authorization_digest NOT GLOB '*[^0-9a-f]*'),
    authorized_at TEXT NOT NULL
        CHECK(length(authorized_at) = 27 AND substr(authorized_at, 1, 19) = strftime('%Y-%m-%dT%H:%M:%S', authorized_at) AND substr(authorized_at, 20, 1) = '.' AND substr(authorized_at, 21, 6) NOT GLOB '*[^0-9]*' AND substr(authorized_at, 27, 1) = 'Z'),
    received_at TEXT NOT NULL
        CHECK(length(received_at) = 27 AND substr(received_at, 1, 19) = strftime('%Y-%m-%dT%H:%M:%S', received_at) AND substr(received_at, 20, 1) = '.' AND substr(received_at, 21, 6) NOT GLOB '*[^0-9]*' AND substr(received_at, 27, 1) = 'Z'),
    source_digest TEXT NOT NULL COLLATE BINARY
        CHECK(length(source_digest) = 64 AND source_digest NOT GLOB '*[^0-9a-f]*'),
    record_sha256 TEXT NOT NULL COLLATE BINARY
        CHECK(length(record_sha256) = 64 AND record_sha256 NOT GLOB '*[^0-9a-f]*'),
    CHECK(authorized_at <= received_at),
    UNIQUE(window_id, signal_id),
    FOREIGN KEY(window_id) REFERENCES phase2_windows(window_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
    FOREIGN KEY(signal_id) REFERENCES phase1_signals(signal_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT
) STRICT;

CREATE TRIGGER phase2_authorizations_validate_signal_lineage
BEFORE INSERT ON phase2_authorizations
WHEN NOT EXISTS (
    SELECT 1
    FROM phase1_signals AS signal
    JOIN phase2_windows AS window ON window.window_id = NEW.window_id
    WHERE signal.signal_id = NEW.signal_id
      AND signal.role = 'PRIMARY'
      AND signal.validation_window_id = window.phase1_validation_window_id
      AND signal.published_at > window.promotion_query_cutoff
      AND signal.published_at > window.started_at
      AND signal.received_at <= NEW.authorized_at
      AND instr(window.promotion_signal_ids_json, '"' || signal.signal_id || '"') = 0
)
BEGIN SELECT RAISE(ABORT, 'phase2 authorization requires a new post-cutoff primary signal'); END;

CREATE TABLE phase2_fee_schedules (
    id INTEGER PRIMARY KEY CHECK(typeof(id) = 'integer' AND id > 0),
    schedule_id TEXT NOT NULL COLLATE BINARY UNIQUE
        CHECK(length(schedule_id) > 0 AND schedule_id NOT GLOB '*[^A-Z0-9_:-]*'),
    effective_session TEXT NOT NULL
        CHECK(length(effective_session) = 10 AND effective_session = strftime('%Y-%m-%d', effective_session)),
    reviewed_at TEXT NOT NULL
        CHECK(length(reviewed_at) = 27 AND substr(reviewed_at, 1, 19) = strftime('%Y-%m-%dT%H:%M:%S', reviewed_at) AND substr(reviewed_at, 20, 1) = '.' AND substr(reviewed_at, 21, 6) NOT GLOB '*[^0-9]*' AND substr(reviewed_at, 27, 1) = 'Z'),
    currency TEXT NOT NULL CHECK(currency = 'USD'),
    contract_multiplier INTEGER NOT NULL
        CHECK(typeof(contract_multiplier) = 'integer' AND contract_multiplier = 100),
    entry_fee_per_contract_micros INTEGER NOT NULL
        CHECK(typeof(entry_fee_per_contract_micros) = 'integer' AND entry_fee_per_contract_micros >= 0),
    exit_fee_per_contract_micros INTEGER NOT NULL
        CHECK(typeof(exit_fee_per_contract_micros) = 'integer' AND exit_fee_per_contract_micros > 0),
    close_fee_reserve_per_contract_micros INTEGER NOT NULL
        CHECK(typeof(close_fee_reserve_per_contract_micros) = 'integer' AND close_fee_reserve_per_contract_micros >= exit_fee_per_contract_micros),
    source_sha256 TEXT NOT NULL COLLATE BINARY
        CHECK(length(source_sha256) = 64 AND source_sha256 NOT GLOB '*[^0-9a-f]*'),
    schedule_digest TEXT NOT NULL COLLATE BINARY
        CHECK(length(schedule_digest) = 64 AND schedule_digest NOT GLOB '*[^0-9a-f]*'),
    reviewed_bytes BLOB NOT NULL
        CHECK(typeof(reviewed_bytes) = 'blob' AND length(reviewed_bytes) > 0 AND json_valid(CAST(reviewed_bytes AS TEXT))),
    archived_at TEXT NOT NULL
        CHECK(length(archived_at) = 27 AND substr(archived_at, 1, 19) = strftime('%Y-%m-%dT%H:%M:%S', archived_at) AND substr(archived_at, 20, 1) = '.' AND substr(archived_at, 21, 6) NOT GLOB '*[^0-9]*' AND substr(archived_at, 27, 1) = 'Z'),
    source_digest TEXT NOT NULL COLLATE BINARY
        CHECK(length(source_digest) = 64 AND source_digest NOT GLOB '*[^0-9a-f]*'),
    record_sha256 TEXT NOT NULL COLLATE BINARY
        CHECK(length(record_sha256) = 64 AND record_sha256 NOT GLOB '*[^0-9a-f]*'),
    CHECK(reviewed_at <= archived_at),
    UNIQUE(schedule_id, schedule_digest)
) STRICT;

CREATE TABLE phase2_option_chain_sets (
    id INTEGER PRIMARY KEY CHECK(typeof(id) = 'integer' AND id > 0),
    chain_set_id TEXT NOT NULL COLLATE BINARY UNIQUE
        CHECK(length(chain_set_id) = 64 AND chain_set_id NOT GLOB '*[^0-9a-f]*'),
    authorization_id TEXT NOT NULL COLLATE BINARY,
    underlying TEXT NOT NULL COLLATE BINARY
        CHECK(length(underlying) BETWEEN 1 AND 6 AND underlying = upper(underlying)),
    collection_name TEXT NOT NULL CHECK(collection_name = 'snapshots'),
    requested_symbols_json TEXT NOT NULL
        CHECK(json_valid(requested_symbols_json)
          AND json_type(requested_symbols_json) = 'array'
          AND json_array_length(requested_symbols_json) = 1),
    request_digest TEXT NOT NULL COLLATE BINARY
        CHECK(length(request_digest) = 64 AND request_digest NOT GLOB '*[^0-9a-f]*'),
    manifest_digest TEXT NOT NULL COLLATE BINARY UNIQUE
        CHECK(length(manifest_digest) = 64 AND manifest_digest NOT GLOB '*[^0-9a-f]*'),
    expected_page_count INTEGER NOT NULL
        CHECK(typeof(expected_page_count) = 'integer' AND expected_page_count > 0),
    expected_fact_count INTEGER NOT NULL
        CHECK(typeof(expected_fact_count) = 'integer' AND expected_fact_count >= 0),
    review_candidate_fact_digests_json TEXT NOT NULL
        CHECK(json_valid(review_candidate_fact_digests_json)
          AND json_type(review_candidate_fact_digests_json) = 'array'),
    expected_manual_review_count INTEGER NOT NULL
        CHECK(typeof(expected_manual_review_count) = 'integer'
          AND expected_manual_review_count >= 0
          AND expected_manual_review_count = json_array_length(review_candidate_fact_digests_json)),
    terminal INTEGER NOT NULL CHECK(typeof(terminal) = 'integer' AND terminal = 1),
    query_cutoff TEXT NOT NULL
        CHECK(length(query_cutoff) = 27 AND substr(query_cutoff, 1, 19) = strftime('%Y-%m-%dT%H:%M:%S', query_cutoff) AND substr(query_cutoff, 20, 1) = '.' AND substr(query_cutoff, 21, 6) NOT GLOB '*[^0-9]*' AND substr(query_cutoff, 27, 1) = 'Z'),
    received_at TEXT NOT NULL
        CHECK(length(received_at) = 27 AND substr(received_at, 1, 19) = strftime('%Y-%m-%dT%H:%M:%S', received_at) AND substr(received_at, 20, 1) = '.' AND substr(received_at, 21, 6) NOT GLOB '*[^0-9]*' AND substr(received_at, 27, 1) = 'Z'),
    source_digest TEXT NOT NULL COLLATE BINARY
        CHECK(length(source_digest) = 64 AND source_digest NOT GLOB '*[^0-9a-f]*'),
    record_sha256 TEXT NOT NULL COLLATE BINARY
        CHECK(length(record_sha256) = 64 AND record_sha256 NOT GLOB '*[^0-9a-f]*'),
    CHECK(json_extract(requested_symbols_json, '$[0]') = underlying),
    CHECK(query_cutoff <= received_at),
    FOREIGN KEY(authorization_id) REFERENCES phase2_authorizations(authorization_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT
) STRICT;

CREATE TABLE phase2_option_chain_pages (
    id INTEGER PRIMARY KEY CHECK(typeof(id) = 'integer' AND id > 0),
    page_id TEXT NOT NULL COLLATE BINARY UNIQUE
        CHECK(length(page_id) = 64 AND page_id NOT GLOB '*[^0-9a-f]*'),
    chain_set_id TEXT NOT NULL COLLATE BINARY,
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
    source_time TEXT NOT NULL
        CHECK(length(source_time) = 27 AND substr(source_time, 1, 19) = strftime('%Y-%m-%dT%H:%M:%S', source_time) AND substr(source_time, 20, 1) = '.' AND substr(source_time, 21, 6) NOT GLOB '*[^0-9]*' AND substr(source_time, 27, 1) = 'Z'),
    retrieved_at TEXT NOT NULL
        CHECK(length(retrieved_at) = 27 AND substr(retrieved_at, 1, 19) = strftime('%Y-%m-%dT%H:%M:%S', retrieved_at) AND substr(retrieved_at, 20, 1) = '.' AND substr(retrieved_at, 21, 6) NOT GLOB '*[^0-9]*' AND substr(retrieved_at, 27, 1) = 'Z'),
    source_digest TEXT NOT NULL COLLATE BINARY
        CHECK(length(source_digest) = 64 AND source_digest NOT GLOB '*[^0-9a-f]*'),
    record_sha256 TEXT NOT NULL COLLATE BINARY
        CHECK(length(record_sha256) = 64 AND record_sha256 NOT GLOB '*[^0-9a-f]*'),
    CHECK(source_time <= retrieved_at),
    UNIQUE(chain_set_id, page_ordinal),
    UNIQUE(chain_set_id, source_observation_id),
    UNIQUE(chain_set_id, external_source_observation_id),
    FOREIGN KEY(chain_set_id) REFERENCES phase2_option_chain_sets(chain_set_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
    FOREIGN KEY(source_observation_id) REFERENCES source_observations(id)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
    FOREIGN KEY(source_observation_id, payload_sha256)
        REFERENCES phase1_source_payloads(source_observation_id, payload_sha256)
        ON UPDATE RESTRICT ON DELETE RESTRICT
) STRICT;

CREATE TRIGGER phase2_option_chain_pages_validate_lineage
BEFORE INSERT ON phase2_option_chain_pages
WHEN NOT EXISTS (
    SELECT 1
    FROM phase2_option_chain_sets AS chain
    JOIN source_observations AS source ON source.id = NEW.source_observation_id
    JOIN phase1_source_payloads AS payload
      ON payload.source_observation_id = source.id
     AND payload.payload_sha256 = NEW.payload_sha256
    WHERE chain.chain_set_id = NEW.chain_set_id
      AND source.payload_sha256 = NEW.payload_sha256
      AND source.source_type = NEW.source_type
      AND source.source_time = NEW.source_time
      AND source.retrieved_at = NEW.retrieved_at
      AND NEW.retrieved_at <= chain.query_cutoff
      AND NEW.retrieved_at <= chain.received_at
      AND (
          (NEW.page_ordinal = 1 AND NEW.request_page_token IS NULL)
          OR
          (NEW.page_ordinal > 1 AND EXISTS (
              SELECT 1 FROM phase2_option_chain_pages AS prior
              WHERE prior.chain_set_id = NEW.chain_set_id
                AND prior.page_ordinal = NEW.page_ordinal - 1
                AND prior.next_page_token = NEW.request_page_token
                AND prior.next_page_token IS NOT NULL
          ))
      )
)
BEGIN SELECT RAISE(ABORT, 'phase2 option chain page requires exact contiguous raw lineage'); END;

CREATE TABLE phase2_underlying_review_sets (
    id INTEGER PRIMARY KEY CHECK(typeof(id) = 'integer' AND id > 0),
    review_set_id TEXT NOT NULL COLLATE BINARY UNIQUE
        CHECK(length(review_set_id) = 64 AND review_set_id NOT GLOB '*[^0-9a-f]*'),
    window_id TEXT NOT NULL COLLATE BINARY,
    entry_id TEXT NOT NULL COLLATE BINARY,
    underlying TEXT NOT NULL COLLATE BINARY
        CHECK(length(underlying) BETWEEN 1 AND 6 AND underlying = upper(underlying)),
    review_session TEXT NOT NULL
        CHECK(length(review_session) = 10 AND review_session = strftime('%Y-%m-%d', review_session)),
    collection_name TEXT NOT NULL CHECK(collection_name = 'bars'),
    timeframe TEXT NOT NULL CHECK(timeframe = '1Min'),
    adjustment TEXT NOT NULL CHECK(adjustment = 'split'),
    feed TEXT NOT NULL CHECK(feed = 'sip'),
    requested_symbols_json TEXT NOT NULL
        CHECK(json_valid(requested_symbols_json)
          AND json_type(requested_symbols_json) = 'array'
          AND json_array_length(requested_symbols_json) = 1),
    request_digest TEXT NOT NULL COLLATE BINARY
        CHECK(length(request_digest) = 64 AND request_digest NOT GLOB '*[^0-9a-f]*'),
    manifest_digest TEXT NOT NULL COLLATE BINARY UNIQUE
        CHECK(length(manifest_digest) = 64 AND manifest_digest NOT GLOB '*[^0-9a-f]*'),
    expected_page_count INTEGER NOT NULL
        CHECK(typeof(expected_page_count) = 'integer' AND expected_page_count > 0),
    expected_fact_count INTEGER NOT NULL
        CHECK(typeof(expected_fact_count) = 'integer' AND expected_fact_count > 0),
    terminal INTEGER NOT NULL CHECK(typeof(terminal) = 'integer' AND terminal = 1),
    request_start TEXT NOT NULL
        CHECK(length(request_start) = 27 AND substr(request_start, 1, 19) = strftime('%Y-%m-%dT%H:%M:%S', request_start) AND substr(request_start, 20, 1) = '.' AND substr(request_start, 21, 6) NOT GLOB '*[^0-9]*' AND substr(request_start, 27, 1) = 'Z'),
    request_end TEXT NOT NULL
        CHECK(length(request_end) = 27 AND substr(request_end, 1, 19) = strftime('%Y-%m-%dT%H:%M:%S', request_end) AND substr(request_end, 20, 1) = '.' AND substr(request_end, 21, 6) NOT GLOB '*[^0-9]*' AND substr(request_end, 27, 1) = 'Z'),
    query_cutoff TEXT NOT NULL
        CHECK(length(query_cutoff) = 27 AND substr(query_cutoff, 1, 19) = strftime('%Y-%m-%dT%H:%M:%S', query_cutoff) AND substr(query_cutoff, 20, 1) = '.' AND substr(query_cutoff, 21, 6) NOT GLOB '*[^0-9]*' AND substr(query_cutoff, 27, 1) = 'Z'),
    received_at TEXT NOT NULL
        CHECK(length(received_at) = 27 AND substr(received_at, 1, 19) = strftime('%Y-%m-%dT%H:%M:%S', received_at) AND substr(received_at, 20, 1) = '.' AND substr(received_at, 21, 6) NOT GLOB '*[^0-9]*' AND substr(received_at, 27, 1) = 'Z'),
    source_digest TEXT NOT NULL COLLATE BINARY
        CHECK(length(source_digest) = 64 AND source_digest NOT GLOB '*[^0-9a-f]*'),
    record_sha256 TEXT NOT NULL COLLATE BINARY
        CHECK(length(record_sha256) = 64 AND record_sha256 NOT GLOB '*[^0-9a-f]*'),
    CHECK(json_extract(requested_symbols_json, '$[0]') = underlying),
    CHECK(request_start < request_end AND request_end <= query_cutoff AND query_cutoff <= received_at),
    UNIQUE(entry_id, review_session),
    FOREIGN KEY(window_id) REFERENCES phase2_windows(window_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
    FOREIGN KEY(entry_id) REFERENCES phase2_entries(entry_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT
) STRICT;

CREATE TABLE phase2_underlying_review_pages (
    id INTEGER PRIMARY KEY CHECK(typeof(id) = 'integer' AND id > 0),
    page_id TEXT NOT NULL COLLATE BINARY UNIQUE
        CHECK(length(page_id) = 64 AND page_id NOT GLOB '*[^0-9a-f]*'),
    review_set_id TEXT NOT NULL COLLATE BINARY,
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
    source_time TEXT NOT NULL
        CHECK(length(source_time) = 27 AND substr(source_time, 1, 19) = strftime('%Y-%m-%dT%H:%M:%S', source_time) AND substr(source_time, 20, 1) = '.' AND substr(source_time, 21, 6) NOT GLOB '*[^0-9]*' AND substr(source_time, 27, 1) = 'Z'),
    retrieved_at TEXT NOT NULL
        CHECK(length(retrieved_at) = 27 AND substr(retrieved_at, 1, 19) = strftime('%Y-%m-%dT%H:%M:%S', retrieved_at) AND substr(retrieved_at, 20, 1) = '.' AND substr(retrieved_at, 21, 6) NOT GLOB '*[^0-9]*' AND substr(retrieved_at, 27, 1) = 'Z'),
    source_digest TEXT NOT NULL COLLATE BINARY
        CHECK(length(source_digest) = 64 AND source_digest NOT GLOB '*[^0-9a-f]*'),
    record_sha256 TEXT NOT NULL COLLATE BINARY
        CHECK(length(record_sha256) = 64 AND record_sha256 NOT GLOB '*[^0-9a-f]*'),
    CHECK(source_time <= retrieved_at),
    UNIQUE(review_set_id, page_ordinal),
    UNIQUE(review_set_id, source_observation_id),
    UNIQUE(review_set_id, external_source_observation_id),
    UNIQUE(review_set_id, page_ordinal, source_observation_id, payload_sha256),
    FOREIGN KEY(review_set_id) REFERENCES phase2_underlying_review_sets(review_set_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
    FOREIGN KEY(source_observation_id) REFERENCES source_observations(id)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
    FOREIGN KEY(source_observation_id, payload_sha256)
        REFERENCES phase1_source_payloads(source_observation_id, payload_sha256)
        ON UPDATE RESTRICT ON DELETE RESTRICT
) STRICT;

CREATE TRIGGER phase2_underlying_review_pages_validate_lineage
BEFORE INSERT ON phase2_underlying_review_pages
WHEN NOT EXISTS (
    SELECT 1
    FROM phase2_underlying_review_sets AS review
    JOIN source_observations AS source ON source.id = NEW.source_observation_id
    JOIN phase1_source_payloads AS payload
      ON payload.source_observation_id = source.id
     AND payload.payload_sha256 = NEW.payload_sha256
    WHERE review.review_set_id = NEW.review_set_id
      AND source.payload_sha256 = NEW.payload_sha256
      AND source.source_type = NEW.source_type
      AND source.source_time = NEW.source_time
      AND source.retrieved_at = NEW.retrieved_at
      AND NEW.retrieved_at <= review.query_cutoff
      AND NEW.retrieved_at <= review.received_at
      AND (
          (NEW.page_ordinal = 1 AND NEW.request_page_token IS NULL)
          OR
          (NEW.page_ordinal > 1 AND EXISTS (
              SELECT 1 FROM phase2_underlying_review_pages AS prior
              WHERE prior.review_set_id = NEW.review_set_id
                AND prior.page_ordinal = NEW.page_ordinal - 1
                AND prior.next_page_token = NEW.request_page_token
                AND prior.next_page_token IS NOT NULL
          ))
      )
)
BEGIN SELECT RAISE(ABORT, 'phase2 underlying review page requires exact contiguous raw lineage'); END;

CREATE TABLE phase2_underlying_review_facts (
    id INTEGER PRIMARY KEY CHECK(typeof(id) = 'integer' AND id > 0),
    fact_id TEXT NOT NULL COLLATE BINARY UNIQUE
        CHECK(length(fact_id) = 64 AND fact_id NOT GLOB '*[^0-9a-f]*'),
    review_set_id TEXT NOT NULL COLLATE BINARY,
    source_observation_id INTEGER NOT NULL
        CHECK(typeof(source_observation_id) = 'integer' AND source_observation_id > 0),
    external_source_observation_id TEXT NOT NULL COLLATE BINARY
        CHECK(length(external_source_observation_id) > 0),
    fetch_page_ordinal INTEGER NOT NULL
        CHECK(typeof(fetch_page_ordinal) = 'integer' AND fetch_page_ordinal > 0),
    source_item_ordinal INTEGER NOT NULL
        CHECK(typeof(source_item_ordinal) = 'integer' AND source_item_ordinal > 0),
    source_item_path TEXT NOT NULL COLLATE BINARY
        CHECK(length(source_item_path) > 2 AND length(source_item_path) <= 512 AND source_item_path GLOB '$.*'),
    payload_sha256 TEXT NOT NULL COLLATE BINARY
        CHECK(length(payload_sha256) = 64 AND payload_sha256 NOT GLOB '*[^0-9a-f]*'),
    symbol TEXT NOT NULL COLLATE BINARY
        CHECK(length(symbol) BETWEEN 1 AND 6 AND symbol = upper(symbol)),
    bar_at TEXT NOT NULL
        CHECK(length(bar_at) = 27 AND substr(bar_at, 1, 19) = strftime('%Y-%m-%dT%H:%M:%S', bar_at) AND substr(bar_at, 20, 1) = '.' AND substr(bar_at, 21, 6) NOT GLOB '*[^0-9]*' AND substr(bar_at, 27, 1) = 'Z'),
    open_micros INTEGER NOT NULL CHECK(typeof(open_micros) = 'integer' AND open_micros > 0),
    high_micros INTEGER NOT NULL CHECK(typeof(high_micros) = 'integer' AND high_micros > 0),
    low_micros INTEGER NOT NULL CHECK(typeof(low_micros) = 'integer' AND low_micros > 0),
    close_micros INTEGER NOT NULL CHECK(typeof(close_micros) = 'integer' AND close_micros > 0),
    volume INTEGER NOT NULL CHECK(typeof(volume) = 'integer' AND volume >= 0),
    fact_digest TEXT NOT NULL COLLATE BINARY
        CHECK(length(fact_digest) = 64 AND fact_digest NOT GLOB '*[^0-9a-f]*'),
    source_digest TEXT NOT NULL COLLATE BINARY
        CHECK(length(source_digest) = 64 AND source_digest NOT GLOB '*[^0-9a-f]*'),
    record_sha256 TEXT NOT NULL COLLATE BINARY
        CHECK(length(record_sha256) = 64 AND record_sha256 NOT GLOB '*[^0-9a-f]*'),
    CHECK(low_micros <= open_micros
      AND low_micros <= close_micros
      AND high_micros >= open_micros
      AND high_micros >= close_micros),
    UNIQUE(review_set_id, fetch_page_ordinal, source_item_ordinal, source_item_path),
    FOREIGN KEY(review_set_id) REFERENCES phase2_underlying_review_sets(review_set_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
    FOREIGN KEY(review_set_id, fetch_page_ordinal, source_observation_id, payload_sha256)
        REFERENCES phase2_underlying_review_pages(review_set_id, page_ordinal, source_observation_id, payload_sha256)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
    FOREIGN KEY(source_observation_id) REFERENCES source_observations(id)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
    FOREIGN KEY(source_observation_id, payload_sha256)
        REFERENCES phase1_source_payloads(source_observation_id, payload_sha256)
        ON UPDATE RESTRICT ON DELETE RESTRICT
) STRICT;

CREATE TRIGGER phase2_underlying_review_facts_validate_lineage
BEFORE INSERT ON phase2_underlying_review_facts
WHEN NOT EXISTS (
    SELECT 1
    FROM phase2_underlying_review_sets AS review
    JOIN phase2_underlying_review_pages AS page
      ON page.review_set_id = review.review_set_id
     AND page.page_ordinal = NEW.fetch_page_ordinal
     AND page.source_observation_id = NEW.source_observation_id
     AND page.payload_sha256 = NEW.payload_sha256
    JOIN source_observations AS source ON source.id = page.source_observation_id
    WHERE review.review_set_id = NEW.review_set_id
      AND review.collection_name = 'bars'
      AND review.timeframe = '1Min'
      AND review.adjustment = 'split'
      AND review.feed = 'sip'
      AND review.underlying = NEW.symbol
      AND review.review_session = substr(NEW.bar_at, 1, 10)
      AND NEW.bar_at BETWEEN review.request_start AND review.request_end
      AND page.external_source_observation_id = NEW.external_source_observation_id
      AND source.payload_sha256 = NEW.payload_sha256
      AND page.retrieved_at <= review.query_cutoff
      AND page.retrieved_at <= review.received_at
)
BEGIN SELECT RAISE(ABORT, 'phase2 underlying fact requires exact persisted review page lineage'); END;

CREATE TABLE phase2_contract_snapshots (
    id INTEGER PRIMARY KEY CHECK(typeof(id) = 'integer' AND id > 0),
    snapshot_id TEXT NOT NULL COLLATE BINARY UNIQUE
        CHECK(length(snapshot_id) = 64 AND snapshot_id NOT GLOB '*[^0-9a-f]*'),
    authorization_id TEXT NOT NULL COLLATE BINARY,
    source_kind TEXT NOT NULL CHECK(source_kind IN ('PROVIDER_INDICATIVE', 'MANUAL_REVIEW')),
    chain_set_id TEXT COLLATE BINARY,
    reviewed_provider_snapshot_id TEXT COLLATE BINARY,
    occ_symbol TEXT NOT NULL COLLATE BINARY
        CHECK(length(occ_symbol) BETWEEN 15 AND 21 AND occ_symbol = upper(occ_symbol)),
    underlying TEXT NOT NULL COLLATE BINARY
        CHECK(length(underlying) BETWEEN 1 AND 6 AND underlying = upper(underlying)),
    expiration TEXT NOT NULL
        CHECK(length(expiration) = 10 AND expiration = strftime('%Y-%m-%d', expiration)),
    strike_micros INTEGER NOT NULL
        CHECK(typeof(strike_micros) = 'integer' AND strike_micros > 0),
    delta_micros INTEGER
        CHECK(delta_micros IS NULL OR (typeof(delta_micros) = 'integer' AND delta_micros BETWEEN 0 AND 1000000)),
    bid_micros INTEGER
        CHECK(bid_micros IS NULL OR (typeof(bid_micros) = 'integer' AND bid_micros >= 0)),
    ask_micros INTEGER
        CHECK(ask_micros IS NULL OR (typeof(ask_micros) = 'integer' AND ask_micros >= 0)),
    open_interest INTEGER
        CHECK(open_interest IS NULL OR (typeof(open_interest) = 'integer' AND open_interest >= 0)),
    daily_volume INTEGER
        CHECK(daily_volume IS NULL OR (typeof(daily_volume) = 'integer' AND daily_volume >= 0)),
    source_observation_id INTEGER
        CHECK(source_observation_id IS NULL OR (typeof(source_observation_id) = 'integer' AND source_observation_id > 0)),
    external_source_observation_id TEXT COLLATE BINARY
        CHECK(external_source_observation_id IS NULL OR length(external_source_observation_id) > 0),
    fetch_page_ordinal INTEGER
        CHECK(fetch_page_ordinal IS NULL OR (typeof(fetch_page_ordinal) = 'integer' AND fetch_page_ordinal > 0)),
    source_item_ordinal INTEGER
        CHECK(source_item_ordinal IS NULL OR (typeof(source_item_ordinal) = 'integer' AND source_item_ordinal > 0)),
    source_item_path TEXT COLLATE BINARY
        CHECK(source_item_path IS NULL OR (length(source_item_path) > 2 AND source_item_path GLOB '$.*')),
    payload_sha256 TEXT COLLATE BINARY
        CHECK(payload_sha256 IS NULL OR (length(payload_sha256) = 64 AND payload_sha256 NOT GLOB '*[^0-9a-f]*')),
    provider_fact_digest TEXT COLLATE BINARY
        CHECK(provider_fact_digest IS NULL OR (length(provider_fact_digest) = 64 AND provider_fact_digest NOT GLOB '*[^0-9a-f]*')),
    execution_event_id INTEGER
        CHECK(execution_event_id IS NULL OR (typeof(execution_event_id) = 'integer' AND execution_event_id > 0)),
    raw_message_id INTEGER
        CHECK(raw_message_id IS NULL OR (typeof(raw_message_id) = 'integer' AND raw_message_id > 0)),
    action_ordinal INTEGER
        CHECK(action_ordinal IS NULL OR (typeof(action_ordinal) = 'integer' AND action_ordinal >= 0)),
    action_source_digest TEXT COLLATE BINARY
        CHECK(action_source_digest IS NULL OR (length(action_source_digest) = 64 AND action_source_digest NOT GLOB '*[^0-9a-f]*')),
    observed_at TEXT
        CHECK(observed_at IS NULL OR (length(observed_at) = 27 AND substr(observed_at, 1, 19) = strftime('%Y-%m-%dT%H:%M:%S', observed_at) AND substr(observed_at, 20, 1) = '.' AND substr(observed_at, 21, 6) NOT GLOB '*[^0-9]*' AND substr(observed_at, 27, 1) = 'Z')),
    received_at TEXT NOT NULL
        CHECK(length(received_at) = 27 AND substr(received_at, 1, 19) = strftime('%Y-%m-%dT%H:%M:%S', received_at) AND substr(received_at, 20, 1) = '.' AND substr(received_at, 21, 6) NOT GLOB '*[^0-9]*' AND substr(received_at, 27, 1) = 'Z'),
    source_digest TEXT NOT NULL COLLATE BINARY
        CHECK(length(source_digest) = 64 AND source_digest NOT GLOB '*[^0-9a-f]*'),
    record_sha256 TEXT NOT NULL COLLATE BINARY
        CHECK(length(record_sha256) = 64 AND record_sha256 NOT GLOB '*[^0-9a-f]*'),
    CHECK(observed_at IS NULL OR observed_at <= received_at),
    CHECK((bid_micros IS NULL AND ask_micros IS NULL)
       OR (bid_micros IS NOT NULL AND ask_micros IS NOT NULL AND ask_micros >= bid_micros)),
    UNIQUE(chain_set_id, fetch_page_ordinal, source_item_ordinal, source_item_path),
    UNIQUE(reviewed_provider_snapshot_id),
    CHECK(
        (source_kind = 'PROVIDER_INDICATIVE'
         AND chain_set_id IS NOT NULL
         AND reviewed_provider_snapshot_id IS NULL
         AND source_observation_id IS NOT NULL
         AND external_source_observation_id IS NOT NULL
         AND fetch_page_ordinal IS NOT NULL
         AND source_item_ordinal IS NOT NULL
         AND source_item_path IS NOT NULL
         AND payload_sha256 IS NOT NULL
         AND provider_fact_digest IS NOT NULL
         AND open_interest IS NULL
         AND execution_event_id IS NULL
         AND raw_message_id IS NULL
         AND action_ordinal IS NULL
         AND action_source_digest IS NULL)
        OR
        (source_kind = 'MANUAL_REVIEW'
         AND chain_set_id IS NOT NULL
         AND reviewed_provider_snapshot_id IS NOT NULL
         AND source_observation_id IS NULL
         AND external_source_observation_id IS NULL
         AND fetch_page_ordinal IS NULL
         AND source_item_ordinal IS NULL
         AND source_item_path IS NULL
         AND payload_sha256 IS NULL
         AND provider_fact_digest IS NULL
         AND delta_micros IS NOT NULL
         AND bid_micros IS NOT NULL
         AND ask_micros IS NOT NULL
         AND open_interest IS NOT NULL
         AND daily_volume IS NOT NULL
         AND observed_at IS NOT NULL
         AND execution_event_id IS NOT NULL
         AND raw_message_id IS NOT NULL
         AND action_ordinal IS NOT NULL
         AND action_source_digest IS NOT NULL)
    ),
    FOREIGN KEY(authorization_id) REFERENCES phase2_authorizations(authorization_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
    FOREIGN KEY(chain_set_id) REFERENCES phase2_option_chain_sets(chain_set_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
    FOREIGN KEY(reviewed_provider_snapshot_id) REFERENCES phase2_contract_snapshots(snapshot_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
    FOREIGN KEY(source_observation_id) REFERENCES source_observations(id)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
    FOREIGN KEY(source_observation_id, payload_sha256)
        REFERENCES phase1_source_payloads(source_observation_id, payload_sha256)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
    FOREIGN KEY(execution_event_id) REFERENCES execution_events(id)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
    FOREIGN KEY(raw_message_id) REFERENCES raw_messages(id)
        ON UPDATE RESTRICT ON DELETE RESTRICT
) STRICT;

CREATE TRIGGER phase2_contract_snapshots_validate_provider_source
BEFORE INSERT ON phase2_contract_snapshots
WHEN NEW.source_kind = 'PROVIDER_INDICATIVE' AND NOT EXISTS (
    SELECT 1 FROM source_observations AS source
    JOIN phase1_source_payloads AS payload
      ON payload.source_observation_id = source.id
     AND payload.payload_sha256 = source.payload_sha256
    JOIN phase2_option_chain_pages AS page
      ON page.chain_set_id = NEW.chain_set_id
     AND page.source_observation_id = source.id
    JOIN phase2_option_chain_sets AS chain
      ON chain.chain_set_id = page.chain_set_id
    WHERE source.id = NEW.source_observation_id
      AND source.payload_sha256 = NEW.payload_sha256
      AND page.page_ordinal = NEW.fetch_page_ordinal
      AND chain.authorization_id = NEW.authorization_id
      AND chain.underlying = NEW.underlying
      AND source.source_time <= NEW.observed_at
      AND source.retrieved_at <= NEW.received_at
)
BEGIN SELECT RAISE(ABORT, 'phase2 provider snapshot requires persisted raw payload'); END;

CREATE TRIGGER phase2_contract_snapshots_validate_manual_source
BEFORE INSERT ON phase2_contract_snapshots
WHEN NEW.source_kind = 'MANUAL_REVIEW' AND NOT EXISTS (
    SELECT 1
    FROM execution_events AS event
    JOIN phase2_contract_snapshots AS provider
      ON provider.snapshot_id = NEW.reviewed_provider_snapshot_id
    JOIN phase2_option_chain_sets AS chain
      ON chain.chain_set_id = NEW.chain_set_id
    WHERE event.id = NEW.execution_event_id
      AND event.raw_message_id = NEW.raw_message_id
      AND event.action_ordinal = NEW.action_ordinal
      AND event.parsed_action = 'OPTION_PAPER_REVIEW'
      AND provider.source_kind = 'PROVIDER_INDICATIVE'
      AND provider.chain_set_id = NEW.chain_set_id
      AND provider.authorization_id = NEW.authorization_id
      AND provider.occ_symbol = NEW.occ_symbol
      AND provider.underlying = NEW.underlying
      AND provider.expiration = NEW.expiration
      AND provider.strike_micros = NEW.strike_micros
      AND provider.delta_micros = NEW.delta_micros
      AND provider.bid_micros = NEW.bid_micros
      AND provider.ask_micros = NEW.ask_micros
      AND provider.daily_volume = NEW.daily_volume
      AND EXISTS (
          SELECT 1 FROM json_each(chain.review_candidate_fact_digests_json) AS candidate
          WHERE candidate.value = provider.provider_fact_digest
      )
      AND json_extract(event.details_json, '$.normalized.occ_symbol') = NEW.occ_symbol
      AND json_extract(event.details_json, '$.normalized.delta') =
          '0.' || printf('%06d', NEW.delta_micros)
      AND json_extract(event.details_json, '$.normalized.open_interest') = NEW.open_interest
      AND json_extract(event.details_json, '$.normalized.volume') = NEW.daily_volume
      AND event.bid_micros = NEW.bid_micros
      AND event.ask_micros = NEW.ask_micros
      AND event.event_time = NEW.observed_at
      AND event.message_time <= NEW.received_at
)
BEGIN SELECT RAISE(ABORT, 'phase2 manual snapshot requires exact review action source'); END;

CREATE TABLE phase2_contract_selections (
    id INTEGER PRIMARY KEY CHECK(typeof(id) = 'integer' AND id > 0),
    selection_id TEXT NOT NULL COLLATE BINARY UNIQUE
        CHECK(length(selection_id) = 64 AND selection_id NOT GLOB '*[^0-9a-f]*'),
    authorization_id TEXT NOT NULL COLLATE BINARY,
    provider_snapshot_id TEXT NOT NULL COLLATE BINARY,
    manual_snapshot_id TEXT NOT NULL COLLATE BINARY,
    manual_review_terminal_cursor INTEGER NOT NULL
        CHECK(typeof(manual_review_terminal_cursor) = 'integer' AND manual_review_terminal_cursor > 0),
    expected_manual_review_count INTEGER NOT NULL
        CHECK(typeof(expected_manual_review_count) = 'integer' AND expected_manual_review_count > 0),
    selection_session TEXT NOT NULL
        CHECK(length(selection_session) = 10 AND selection_session = strftime('%Y-%m-%d', selection_session)),
    quantity INTEGER NOT NULL CHECK(typeof(quantity) = 'integer' AND quantity = 1),
    fee_schedule_id TEXT NOT NULL COLLATE BINARY,
    fee_schedule_digest TEXT NOT NULL COLLATE BINARY
        CHECK(length(fee_schedule_digest) = 64 AND fee_schedule_digest NOT GLOB '*[^0-9a-f]*'),
    event_exclusion_source_digest TEXT NOT NULL COLLATE BINARY
        CHECK(length(event_exclusion_source_digest) = 64 AND event_exclusion_source_digest NOT GLOB '*[^0-9a-f]*'),
    event_exclusion_authority_digest TEXT NOT NULL COLLATE BINARY
        CHECK(length(event_exclusion_authority_digest) = 64 AND event_exclusion_authority_digest NOT GLOB '*[^0-9a-f]*'),
    event_exclusion_row_references_json TEXT NOT NULL
        CHECK(length(event_exclusion_row_references_json) > 2 AND json_valid(event_exclusion_row_references_json) AND json_type(event_exclusion_row_references_json) = 'array'),
    event_exclusion_highwaters_json TEXT NOT NULL
        CHECK(length(event_exclusion_highwaters_json) > 2 AND json_valid(event_exclusion_highwaters_json) AND json_type(event_exclusion_highwaters_json) = 'object'),
    selection_portfolio_source_digest TEXT NOT NULL COLLATE BINARY
        CHECK(length(selection_portfolio_source_digest) = 64 AND selection_portfolio_source_digest NOT GLOB '*[^0-9a-f]*'),
    selection_portfolio_query_cutoff TEXT NOT NULL
        CHECK(length(selection_portfolio_query_cutoff) = 27 AND substr(selection_portfolio_query_cutoff, 1, 19) = strftime('%Y-%m-%dT%H:%M:%S', selection_portfolio_query_cutoff) AND substr(selection_portfolio_query_cutoff, 20, 1) = '.' AND substr(selection_portfolio_query_cutoff, 21, 6) NOT GLOB '*[^0-9]*' AND substr(selection_portfolio_query_cutoff, 27, 1) = 'Z'),
    selection_portfolio_row_references_json TEXT NOT NULL
        CHECK(length(selection_portfolio_row_references_json) > 2 AND json_valid(selection_portfolio_row_references_json) AND json_type(selection_portfolio_row_references_json) = 'array'),
    selection_portfolio_highwaters_json TEXT NOT NULL
        CHECK(length(selection_portfolio_highwaters_json) > 2 AND json_valid(selection_portfolio_highwaters_json) AND json_type(selection_portfolio_highwaters_json) = 'object'),
    selected_at TEXT NOT NULL
        CHECK(length(selected_at) = 27 AND substr(selected_at, 1, 19) = strftime('%Y-%m-%dT%H:%M:%S', selected_at) AND substr(selected_at, 20, 1) = '.' AND substr(selected_at, 21, 6) NOT GLOB '*[^0-9]*' AND substr(selected_at, 27, 1) = 'Z'),
    received_at TEXT NOT NULL
        CHECK(length(received_at) = 27 AND substr(received_at, 1, 19) = strftime('%Y-%m-%dT%H:%M:%S', received_at) AND substr(received_at, 20, 1) = '.' AND substr(received_at, 21, 6) NOT GLOB '*[^0-9]*' AND substr(received_at, 27, 1) = 'Z'),
    ranking_digest TEXT NOT NULL COLLATE BINARY
        CHECK(length(ranking_digest) = 64 AND ranking_digest NOT GLOB '*[^0-9a-f]*'),
    source_digest TEXT NOT NULL COLLATE BINARY
        CHECK(length(source_digest) = 64 AND source_digest NOT GLOB '*[^0-9a-f]*'),
    record_sha256 TEXT NOT NULL COLLATE BINARY
        CHECK(length(record_sha256) = 64 AND record_sha256 NOT GLOB '*[^0-9a-f]*'),
    CHECK(provider_snapshot_id <> manual_snapshot_id),
    CHECK(selection_portfolio_query_cutoff <= selected_at AND selected_at <= received_at),
    FOREIGN KEY(authorization_id) REFERENCES phase2_authorizations(authorization_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
    FOREIGN KEY(provider_snapshot_id) REFERENCES phase2_contract_snapshots(snapshot_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
    FOREIGN KEY(manual_snapshot_id) REFERENCES phase2_contract_snapshots(snapshot_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
    FOREIGN KEY(fee_schedule_id, fee_schedule_digest)
        REFERENCES phase2_fee_schedules(schedule_id, schedule_digest)
        ON UPDATE RESTRICT ON DELETE RESTRICT
) STRICT;

CREATE TRIGGER phase2_contract_selections_validate_snapshots
BEFORE INSERT ON phase2_contract_selections
WHEN NOT EXISTS (
    SELECT 1
    FROM phase2_contract_snapshots AS provider
    JOIN phase2_contract_snapshots AS manual
      ON manual.snapshot_id = NEW.manual_snapshot_id
    WHERE provider.snapshot_id = NEW.provider_snapshot_id
      AND provider.authorization_id = NEW.authorization_id
      AND manual.authorization_id = NEW.authorization_id
      AND provider.source_kind = 'PROVIDER_INDICATIVE'
      AND manual.source_kind = 'MANUAL_REVIEW'
      AND manual.reviewed_provider_snapshot_id = provider.snapshot_id
      AND manual.chain_set_id = provider.chain_set_id
      AND provider.occ_symbol = manual.occ_symbol
      AND provider.underlying = manual.underlying
      AND provider.expiration = manual.expiration
      AND provider.strike_micros = manual.strike_micros
      AND provider.delta_micros = manual.delta_micros
      AND provider.bid_micros = manual.bid_micros
      AND provider.ask_micros = manual.ask_micros
      AND provider.daily_volume = manual.daily_volume
      AND provider.observed_at <= manual.observed_at
      AND manual.observed_at < NEW.selected_at
)
BEGIN SELECT RAISE(ABORT, 'phase2 selection requires matching provider and manual snapshots'); END;

CREATE TRIGGER phase2_contract_selections_validate_chain_completeness
BEFORE INSERT ON phase2_contract_selections
WHEN NOT EXISTS (
    SELECT 1
    FROM phase2_contract_snapshots AS provider
    JOIN phase2_option_chain_sets AS chain
      ON chain.chain_set_id = provider.chain_set_id
    WHERE provider.snapshot_id = NEW.provider_snapshot_id
      AND provider.authorization_id = NEW.authorization_id
      AND chain.authorization_id = NEW.authorization_id
      AND chain.terminal = 1
      AND chain.expected_fact_count > 0
      AND NEW.expected_manual_review_count = chain.expected_manual_review_count
      AND (SELECT count(*)
           FROM phase2_option_chain_pages AS page
           WHERE page.chain_set_id = chain.chain_set_id)
          = chain.expected_page_count
      AND (SELECT max(page.page_ordinal)
           FROM phase2_option_chain_pages AS page
           WHERE page.chain_set_id = chain.chain_set_id)
          = chain.expected_page_count
      AND EXISTS (
          SELECT 1 FROM phase2_option_chain_pages AS terminal_page
          WHERE terminal_page.chain_set_id = chain.chain_set_id
            AND terminal_page.page_ordinal = chain.expected_page_count
            AND terminal_page.next_page_token IS NULL
      )
      AND NOT EXISTS (
          SELECT 1 FROM phase2_option_chain_pages AS early_page
          WHERE early_page.chain_set_id = chain.chain_set_id
            AND early_page.page_ordinal < chain.expected_page_count
            AND early_page.next_page_token IS NULL
      )
      AND (SELECT count(*)
           FROM phase2_contract_snapshots AS candidate
           WHERE candidate.chain_set_id = chain.chain_set_id
             AND candidate.source_kind = 'PROVIDER_INDICATIVE')
          = chain.expected_fact_count
      AND (SELECT count(*)
           FROM phase2_contract_snapshots AS review
           WHERE review.chain_set_id = chain.chain_set_id
             AND review.source_kind = 'MANUAL_REVIEW')
          = chain.expected_manual_review_count
      AND (SELECT max(review.execution_event_id)
           FROM phase2_contract_snapshots AS review
           WHERE review.chain_set_id = chain.chain_set_id
             AND review.source_kind = 'MANUAL_REVIEW')
          = NEW.manual_review_terminal_cursor
      AND NOT EXISTS (
          SELECT 1 FROM phase2_contract_snapshots AS late_review
          WHERE late_review.chain_set_id = chain.chain_set_id
            AND late_review.source_kind = 'MANUAL_REVIEW'
            AND late_review.observed_at >= NEW.selected_at
      )
)
BEGIN SELECT RAISE(ABORT, 'phase2 selection requires a complete terminal option chain'); END;

CREATE TRIGGER phase2_contract_selections_validate_fee_schedule
BEFORE INSERT ON phase2_contract_selections
WHEN NOT EXISTS (
    SELECT 1 FROM phase2_fee_schedules AS schedule
    WHERE schedule.schedule_id = NEW.fee_schedule_id
      AND schedule.schedule_digest = NEW.fee_schedule_digest
      AND schedule.currency = 'USD'
      AND schedule.contract_multiplier = 100
      AND schedule.effective_session <= NEW.selection_session
      AND schedule.reviewed_at <= NEW.selected_at
)
BEGIN SELECT RAISE(ABORT, 'phase2 selection requires archived reviewed fee schedule'); END;

CREATE TABLE phase2_entries (
    id INTEGER PRIMARY KEY CHECK(typeof(id) = 'integer' AND id > 0),
    entry_id TEXT NOT NULL COLLATE BINARY UNIQUE
        CHECK(length(entry_id) = 64 AND entry_id NOT GLOB '*[^0-9a-f]*'),
    window_id TEXT NOT NULL COLLATE BINARY,
    selection_id TEXT NOT NULL COLLATE BINARY UNIQUE,
    execution_event_id INTEGER NOT NULL UNIQUE
        CHECK(typeof(execution_event_id) = 'integer' AND execution_event_id > 0),
    raw_message_id INTEGER NOT NULL
        CHECK(typeof(raw_message_id) = 'integer' AND raw_message_id > 0),
    action_ordinal INTEGER NOT NULL
        CHECK(typeof(action_ordinal) = 'integer' AND action_ordinal >= 0),
    action_source_digest TEXT NOT NULL COLLATE BINARY
        CHECK(length(action_source_digest) = 64 AND action_source_digest NOT GLOB '*[^0-9a-f]*'),
    quantity INTEGER NOT NULL CHECK(typeof(quantity) = 'integer' AND quantity = 1),
    entry_ask_micros INTEGER NOT NULL
        CHECK(typeof(entry_ask_micros) = 'integer' AND entry_ask_micros BETWEEN 1 AND 500000),
    entry_fee_micros INTEGER NOT NULL
        CHECK(typeof(entry_fee_micros) = 'integer' AND entry_fee_micros >= 0),
    reserve_fee_micros INTEGER NOT NULL
        CHECK(typeof(reserve_fee_micros) = 'integer' AND reserve_fee_micros >= 0),
    all_in_initial_risk_micros INTEGER NOT NULL
        CHECK(typeof(all_in_initial_risk_micros) = 'integer'
          AND all_in_initial_risk_micros = entry_ask_micros * 100 + entry_fee_micros + reserve_fee_micros
          AND all_in_initial_risk_micros <= 50000000),
    fee_schedule_id TEXT NOT NULL COLLATE BINARY
        CHECK(length(fee_schedule_id) > 0 AND fee_schedule_id NOT GLOB '*[^A-Z0-9_:-]*'),
    fee_schedule_digest TEXT NOT NULL COLLATE BINARY
        CHECK(length(fee_schedule_digest) = 64 AND fee_schedule_digest NOT GLOB '*[^0-9a-f]*'),
    entered_at TEXT NOT NULL
        CHECK(length(entered_at) = 27 AND substr(entered_at, 1, 19) = strftime('%Y-%m-%dT%H:%M:%S', entered_at) AND substr(entered_at, 20, 1) = '.' AND substr(entered_at, 21, 6) NOT GLOB '*[^0-9]*' AND substr(entered_at, 27, 1) = 'Z'),
    received_at TEXT NOT NULL
        CHECK(length(received_at) = 27 AND substr(received_at, 1, 19) = strftime('%Y-%m-%dT%H:%M:%S', received_at) AND substr(received_at, 20, 1) = '.' AND substr(received_at, 21, 6) NOT GLOB '*[^0-9]*' AND substr(received_at, 27, 1) = 'Z'),
    source_digest TEXT NOT NULL COLLATE BINARY
        CHECK(length(source_digest) = 64 AND source_digest NOT GLOB '*[^0-9a-f]*'),
    record_sha256 TEXT NOT NULL COLLATE BINARY
        CHECK(length(record_sha256) = 64 AND record_sha256 NOT GLOB '*[^0-9a-f]*'),
    CHECK(entered_at <= received_at),
    FOREIGN KEY(window_id) REFERENCES phase2_windows(window_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
    FOREIGN KEY(selection_id) REFERENCES phase2_contract_selections(selection_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
    FOREIGN KEY(execution_event_id) REFERENCES execution_events(id)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
    FOREIGN KEY(raw_message_id) REFERENCES raw_messages(id)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
    FOREIGN KEY(fee_schedule_id, fee_schedule_digest)
        REFERENCES phase2_fee_schedules(schedule_id, schedule_digest)
        ON UPDATE RESTRICT ON DELETE RESTRICT
) STRICT;

CREATE TRIGGER phase2_entries_validate_manual_source
BEFORE INSERT ON phase2_entries
WHEN NOT EXISTS (
    SELECT 1
    FROM execution_events AS event
    JOIN phase2_contract_selections AS selection
      ON selection.selection_id = NEW.selection_id
    JOIN phase2_authorizations AS authorization
      ON authorization.authorization_id = selection.authorization_id
    JOIN phase2_contract_snapshots AS manual
      ON manual.snapshot_id = selection.manual_snapshot_id
    JOIN phase2_fee_schedules AS schedule
      ON schedule.schedule_id = selection.fee_schedule_id
     AND schedule.schedule_digest = selection.fee_schedule_digest
    WHERE authorization.window_id = NEW.window_id
      AND event.id = NEW.execution_event_id
      AND event.raw_message_id = NEW.raw_message_id
      AND event.action_ordinal = NEW.action_ordinal
      AND event.parsed_action = 'OPTION_PAPER_OPEN'
      AND event.ask_micros = NEW.entry_ask_micros
      AND json_extract(event.details_json, '$.normalized.occ_symbol') = manual.occ_symbol
      AND event.event_time = NEW.entered_at
      AND event.message_time <= NEW.received_at
      AND selection.selected_at <= NEW.entered_at
      AND selection.quantity = NEW.quantity
      AND selection.fee_schedule_id = NEW.fee_schedule_id
      AND selection.fee_schedule_digest = NEW.fee_schedule_digest
      AND schedule.entry_fee_per_contract_micros = NEW.entry_fee_micros
      AND schedule.close_fee_reserve_per_contract_micros = NEW.reserve_fee_micros
)
BEGIN SELECT RAISE(ABORT, 'phase2 entry requires exact open action source'); END;

CREATE TRIGGER phase2_entries_require_no_open_position
BEFORE INSERT ON phase2_entries
WHEN EXISTS (
    SELECT 1
    FROM phase2_entries AS existing
    WHERE NOT EXISTS (
          SELECT 1 FROM phase2_exits AS closed
          WHERE closed.entry_id = existing.entry_id
      )
)
BEGIN SELECT RAISE(ABORT, 'phase2 account already has an open position'); END;

CREATE TRIGGER phase2_entries_require_prior_exit_settlement
BEFORE INSERT ON phase2_entries
WHEN EXISTS (
    SELECT 1
    FROM phase2_entries AS prior_entry
    JOIN phase2_exits AS prior_exit ON prior_exit.entry_id = prior_entry.entry_id
    WHERE prior_exit.settlement_available_session > substr(NEW.entered_at, 1, 10)
)
BEGIN SELECT RAISE(ABORT, 'phase2 prior option sale proceeds are not settled'); END;

CREATE TABLE phase2_marks (
    id INTEGER PRIMARY KEY CHECK(typeof(id) = 'integer' AND id > 0),
    mark_id TEXT NOT NULL COLLATE BINARY UNIQUE
        CHECK(length(mark_id) = 64 AND mark_id NOT GLOB '*[^0-9a-f]*'),
    window_id TEXT NOT NULL COLLATE BINARY,
    entry_id TEXT NOT NULL COLLATE BINARY,
    session_date TEXT NOT NULL
        CHECK(length(session_date) = 10 AND session_date = strftime('%Y-%m-%d', session_date)),
    source_kind TEXT NOT NULL
        CHECK(source_kind IN ('MANUAL_MARK', 'INVALID_RAW', 'MISSING_DEADLINE')),
    execution_event_id INTEGER UNIQUE
        CHECK(execution_event_id IS NULL OR (typeof(execution_event_id) = 'integer' AND execution_event_id > 0)),
    raw_message_id INTEGER
        CHECK(raw_message_id IS NULL OR (typeof(raw_message_id) = 'integer' AND raw_message_id > 0)),
    action_ordinal INTEGER
        CHECK(action_ordinal IS NULL OR (typeof(action_ordinal) = 'integer' AND action_ordinal >= 0)),
    action_source_digest TEXT COLLATE BINARY
        CHECK(action_source_digest IS NULL OR (length(action_source_digest) = 64 AND action_source_digest NOT GLOB '*[^0-9a-f]*')),
    bid_micros INTEGER
        CHECK(bid_micros IS NULL OR (typeof(bid_micros) = 'integer' AND bid_micros > 0)),
    ask_micros INTEGER
        CHECK(ask_micros IS NULL OR (typeof(ask_micros) = 'integer' AND ask_micros >= bid_micros)),
    liquidation_value_micros INTEGER NOT NULL
        CHECK(typeof(liquidation_value_micros) = 'integer' AND liquidation_value_micros >= 0),
    valid INTEGER NOT NULL CHECK(typeof(valid) = 'integer' AND valid IN (0, 1)),
    failure_reason TEXT CHECK(failure_reason IS NULL OR length(failure_reason) > 0),
    calendar_digest TEXT COLLATE BINARY
        CHECK(calendar_digest IS NULL OR (length(calendar_digest) = 64 AND calendar_digest NOT GLOB '*[^0-9a-f]*')),
    deadline_start_at TEXT
        CHECK(deadline_start_at IS NULL OR (length(deadline_start_at) = 27 AND substr(deadline_start_at, 1, 19) = strftime('%Y-%m-%dT%H:%M:%S', deadline_start_at) AND substr(deadline_start_at, 20, 1) = '.' AND substr(deadline_start_at, 21, 6) NOT GLOB '*[^0-9]*' AND substr(deadline_start_at, 27, 1) = 'Z')),
    deadline_at TEXT
        CHECK(deadline_at IS NULL OR (length(deadline_at) = 27 AND substr(deadline_at, 1, 19) = strftime('%Y-%m-%dT%H:%M:%S', deadline_at) AND substr(deadline_at, 20, 1) = '.' AND substr(deadline_at, 21, 6) NOT GLOB '*[^0-9]*' AND substr(deadline_at, 27, 1) = 'Z')),
    marked_at TEXT NOT NULL
        CHECK(length(marked_at) = 27 AND substr(marked_at, 1, 19) = strftime('%Y-%m-%dT%H:%M:%S', marked_at) AND substr(marked_at, 20, 1) = '.' AND substr(marked_at, 21, 6) NOT GLOB '*[^0-9]*' AND substr(marked_at, 27, 1) = 'Z'),
    received_at TEXT NOT NULL
        CHECK(length(received_at) = 27 AND substr(received_at, 1, 19) = strftime('%Y-%m-%dT%H:%M:%S', received_at) AND substr(received_at, 20, 1) = '.' AND substr(received_at, 21, 6) NOT GLOB '*[^0-9]*' AND substr(received_at, 27, 1) = 'Z'),
    source_digest TEXT NOT NULL COLLATE BINARY
        CHECK(length(source_digest) = 64 AND source_digest NOT GLOB '*[^0-9a-f]*'),
    record_sha256 TEXT NOT NULL COLLATE BINARY
        CHECK(length(record_sha256) = 64 AND record_sha256 NOT GLOB '*[^0-9a-f]*'),
    CHECK((valid = 1 AND source_kind = 'MANUAL_MARK' AND failure_reason IS NULL)
       OR (valid = 0 AND liquidation_value_micros = 0 AND failure_reason IS NOT NULL)),
    CHECK(
        (source_kind = 'MANUAL_MARK'
         AND execution_event_id IS NOT NULL
         AND raw_message_id IS NOT NULL
         AND action_ordinal IS NOT NULL
         AND action_source_digest IS NOT NULL
         AND bid_micros IS NOT NULL
         AND ask_micros IS NOT NULL
         AND calendar_digest IS NULL
         AND deadline_start_at IS NULL
         AND deadline_at IS NULL)
        OR
        (source_kind = 'INVALID_RAW'
         AND valid = 0
         AND execution_event_id IS NOT NULL
         AND raw_message_id IS NOT NULL
         AND action_ordinal IS NOT NULL
         AND action_source_digest IS NOT NULL
         AND bid_micros IS NULL
         AND ask_micros IS NULL
         AND calendar_digest IS NULL
         AND deadline_start_at IS NULL
         AND deadline_at IS NULL)
        OR
        (source_kind = 'MISSING_DEADLINE'
         AND valid = 0
         AND execution_event_id IS NULL
         AND raw_message_id IS NULL
         AND action_ordinal IS NULL
         AND action_source_digest IS NULL
         AND bid_micros IS NULL
         AND ask_micros IS NULL
         AND calendar_digest IS NOT NULL
         AND deadline_start_at IS NOT NULL
         AND deadline_at IS NOT NULL
         AND deadline_start_at < deadline_at
         AND marked_at = deadline_at)
    ),
    CHECK(marked_at <= received_at),
    UNIQUE(entry_id, session_date),
    FOREIGN KEY(window_id) REFERENCES phase2_windows(window_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
    FOREIGN KEY(entry_id) REFERENCES phase2_entries(entry_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
    FOREIGN KEY(execution_event_id) REFERENCES execution_events(id)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
    FOREIGN KEY(raw_message_id) REFERENCES raw_messages(id)
        ON UPDATE RESTRICT ON DELETE RESTRICT
) STRICT;

CREATE TRIGGER phase2_marks_validate_manual_source
BEFORE INSERT ON phase2_marks
WHEN NEW.source_kind = 'MANUAL_MARK' AND NOT EXISTS (
    SELECT 1 FROM execution_events AS event
    JOIN phase2_entries AS entry ON entry.entry_id = NEW.entry_id
    JOIN phase2_contract_selections AS selection
      ON selection.selection_id = entry.selection_id
    JOIN phase2_contract_snapshots AS manual
      ON manual.snapshot_id = selection.manual_snapshot_id
    WHERE entry.window_id = NEW.window_id
      AND event.id = NEW.execution_event_id
      AND event.raw_message_id = NEW.raw_message_id
      AND event.action_ordinal = NEW.action_ordinal
      AND event.parsed_action = 'OPTION_PAPER_MARK'
      AND json_extract(event.details_json, '$.normalized.occ_symbol') = manual.occ_symbol
      AND event.bid_micros = NEW.bid_micros
      AND event.ask_micros = NEW.ask_micros
      AND event.event_time = NEW.marked_at
      AND event.message_time <= NEW.received_at
)
BEGIN SELECT RAISE(ABORT, 'phase2 mark requires exact mark action source'); END;

CREATE TRIGGER phase2_marks_validate_invalid_raw_source
BEFORE INSERT ON phase2_marks
WHEN NEW.source_kind = 'INVALID_RAW' AND NOT EXISTS (
    SELECT 1
    FROM execution_events AS event
    JOIN raw_messages AS raw ON raw.id = event.raw_message_id
    JOIN phase2_entries AS entry ON entry.entry_id = NEW.entry_id
    JOIN phase2_contract_selections AS selection
      ON selection.selection_id = entry.selection_id
    JOIN phase2_contract_snapshots AS manual
      ON manual.snapshot_id = selection.manual_snapshot_id
    WHERE entry.window_id = NEW.window_id
      AND event.id = NEW.execution_event_id
      AND event.raw_message_id = NEW.raw_message_id
      AND event.action_ordinal = NEW.action_ordinal
      AND event.parsed_action = 'PENDING_CLARIFICATION'
      AND event.bid_micros IS NULL
      AND event.ask_micros IS NULL
      AND raw.raw_text GLOB 'OPTION PAPER MARK ' || manual.occ_symbol || ' *'
      AND event.event_time = NEW.marked_at
      AND event.message_time <= NEW.received_at
)
BEGIN SELECT RAISE(ABORT, 'phase2 invalid mark requires exact pending raw source'); END;

CREATE TRIGGER phase2_marks_validate_missing_deadline
BEFORE INSERT ON phase2_marks
WHEN NEW.source_kind = 'MISSING_DEADLINE' AND NOT EXISTS (
    SELECT 1
    FROM phase2_entries AS entry
    JOIN phase2_windows AS window ON window.window_id = entry.window_id
    JOIN phase2_contract_selections AS selection
      ON selection.selection_id = entry.selection_id
    JOIN phase2_contract_snapshots AS manual
      ON manual.snapshot_id = selection.manual_snapshot_id
    WHERE entry.entry_id = NEW.entry_id
      AND entry.window_id = NEW.window_id
      AND NEW.calendar_digest = window.calendar_digest
      AND NEW.deadline_at <= NEW.received_at
      AND NOT EXISTS (
          SELECT 1
          FROM execution_events AS event
          JOIN raw_messages AS raw ON raw.id = event.raw_message_id
          WHERE event.event_time >= NEW.deadline_start_at
            AND event.event_time <= NEW.deadline_at
            AND event.message_time <= NEW.deadline_at
            AND (
                (event.parsed_action = 'OPTION_PAPER_MARK'
                 AND json_extract(event.details_json, '$.normalized.occ_symbol') = manual.occ_symbol)
                OR
                (event.parsed_action = 'PENDING_CLARIFICATION'
                 AND raw.raw_text GLOB 'OPTION PAPER MARK ' || manual.occ_symbol || ' *')
            )
      )
)
BEGIN SELECT RAISE(ABORT, 'phase2 missing mark requires calendar deadline without mark evidence'); END;

CREATE TABLE phase2_exit_reviews (
    id INTEGER PRIMARY KEY CHECK(typeof(id) = 'integer' AND id > 0),
    exit_review_id TEXT NOT NULL COLLATE BINARY UNIQUE
        CHECK(length(exit_review_id) = 64 AND exit_review_id NOT GLOB '*[^0-9a-f]*'),
    window_id TEXT NOT NULL COLLATE BINARY,
    entry_id TEXT NOT NULL COLLATE BINARY,
    underlying_review_set_id TEXT NOT NULL COLLATE BINARY UNIQUE,
    decision_fact_id TEXT COLLATE BINARY,
    review_session TEXT NOT NULL
        CHECK(length(review_session) = 10 AND review_session = strftime('%Y-%m-%d', review_session)),
    decision_kind TEXT NOT NULL
        CHECK(decision_kind IN ('HOLD', 'STOP', 'TARGET', 'MAX_HOLD_10_SESSIONS', 'DTE_21')),
    decision_digest TEXT NOT NULL COLLATE BINARY
        CHECK(length(decision_digest) = 64 AND decision_digest NOT GLOB '*[^0-9a-f]*'),
    holding_sessions INTEGER NOT NULL
        CHECK(typeof(holding_sessions) = 'integer' AND holding_sessions > 0),
    dte INTEGER NOT NULL CHECK(typeof(dte) = 'integer' AND dte >= 0),
    query_cutoff TEXT NOT NULL
        CHECK(length(query_cutoff) = 27 AND substr(query_cutoff, 1, 19) = strftime('%Y-%m-%dT%H:%M:%S', query_cutoff) AND substr(query_cutoff, 20, 1) = '.' AND substr(query_cutoff, 21, 6) NOT GLOB '*[^0-9]*' AND substr(query_cutoff, 27, 1) = 'Z'),
    evaluated_at TEXT NOT NULL
        CHECK(length(evaluated_at) = 27 AND substr(evaluated_at, 1, 19) = strftime('%Y-%m-%dT%H:%M:%S', evaluated_at) AND substr(evaluated_at, 20, 1) = '.' AND substr(evaluated_at, 21, 6) NOT GLOB '*[^0-9]*' AND substr(evaluated_at, 27, 1) = 'Z'),
    received_at TEXT NOT NULL
        CHECK(length(received_at) = 27 AND substr(received_at, 1, 19) = strftime('%Y-%m-%dT%H:%M:%S', received_at) AND substr(received_at, 20, 1) = '.' AND substr(received_at, 21, 6) NOT GLOB '*[^0-9]*' AND substr(received_at, 27, 1) = 'Z'),
    source_digest TEXT NOT NULL COLLATE BINARY
        CHECK(length(source_digest) = 64 AND source_digest NOT GLOB '*[^0-9a-f]*'),
    record_sha256 TEXT NOT NULL COLLATE BINARY
        CHECK(length(record_sha256) = 64 AND record_sha256 NOT GLOB '*[^0-9a-f]*'),
    CHECK(query_cutoff <= evaluated_at AND evaluated_at <= received_at),
    CHECK((decision_kind IN ('STOP', 'TARGET') AND decision_fact_id IS NOT NULL)
       OR (decision_kind IN ('HOLD', 'MAX_HOLD_10_SESSIONS', 'DTE_21')
           AND decision_fact_id IS NULL)),
    UNIQUE(entry_id, review_session),
    FOREIGN KEY(window_id) REFERENCES phase2_windows(window_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
    FOREIGN KEY(entry_id) REFERENCES phase2_entries(entry_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
    FOREIGN KEY(underlying_review_set_id) REFERENCES phase2_underlying_review_sets(review_set_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
    FOREIGN KEY(decision_fact_id) REFERENCES phase2_underlying_review_facts(fact_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT
) STRICT;

CREATE TRIGGER phase2_exit_reviews_validate_source
BEFORE INSERT ON phase2_exit_reviews
WHEN NOT EXISTS (
    SELECT 1
    FROM phase2_entries AS entry
    JOIN phase2_underlying_review_sets AS review
      ON review.review_set_id = NEW.underlying_review_set_id
     AND review.entry_id = entry.entry_id
     AND review.window_id = entry.window_id
    WHERE entry.entry_id = NEW.entry_id
      AND entry.window_id = NEW.window_id
      AND review.review_session = NEW.review_session
      AND review.query_cutoff = NEW.query_cutoff
      AND review.request_end <= NEW.evaluated_at
      AND review.received_at <= NEW.received_at
      AND review.terminal = 1
      AND (SELECT count(*)
           FROM phase2_underlying_review_pages AS page
           WHERE page.review_set_id = review.review_set_id)
          = review.expected_page_count
      AND (SELECT max(page.page_ordinal)
           FROM phase2_underlying_review_pages AS page
           WHERE page.review_set_id = review.review_set_id)
          = review.expected_page_count
      AND (SELECT count(*)
           FROM phase2_underlying_review_facts AS fact
           WHERE fact.review_set_id = review.review_set_id)
          = review.expected_fact_count
      AND EXISTS (
          SELECT 1 FROM phase2_underlying_review_pages AS terminal_page
          WHERE terminal_page.review_set_id = review.review_set_id
            AND terminal_page.page_ordinal = review.expected_page_count
            AND terminal_page.next_page_token IS NULL
      )
      AND NOT EXISTS (
          SELECT 1 FROM phase2_underlying_review_pages AS early_page
          WHERE early_page.review_set_id = review.review_set_id
            AND early_page.page_ordinal < review.expected_page_count
            AND early_page.next_page_token IS NULL
      )
      AND (NEW.decision_fact_id IS NULL OR EXISTS (
          SELECT 1 FROM phase2_underlying_review_facts AS decision_fact
          WHERE decision_fact.fact_id = NEW.decision_fact_id
            AND decision_fact.review_set_id = review.review_set_id
      ))
)
BEGIN SELECT RAISE(ABORT, 'phase2 exit review requires a complete exact underlying cohort'); END;

CREATE TABLE phase2_exits (
    id INTEGER PRIMARY KEY CHECK(typeof(id) = 'integer' AND id > 0),
    exit_id TEXT NOT NULL COLLATE BINARY UNIQUE
        CHECK(length(exit_id) = 64 AND exit_id NOT GLOB '*[^0-9a-f]*'),
    exit_review_id TEXT NOT NULL COLLATE BINARY UNIQUE,
    window_id TEXT NOT NULL COLLATE BINARY,
    entry_id TEXT NOT NULL COLLATE BINARY UNIQUE,
    execution_event_id INTEGER NOT NULL UNIQUE
        CHECK(typeof(execution_event_id) = 'integer' AND execution_event_id > 0),
    raw_message_id INTEGER NOT NULL
        CHECK(typeof(raw_message_id) = 'integer' AND raw_message_id > 0),
    action_ordinal INTEGER NOT NULL
        CHECK(typeof(action_ordinal) = 'integer' AND action_ordinal >= 0),
    action_source_digest TEXT NOT NULL COLLATE BINARY
        CHECK(length(action_source_digest) = 64 AND action_source_digest NOT GLOB '*[^0-9a-f]*'),
    exit_reason TEXT NOT NULL
        CHECK(exit_reason IN ('STOP', 'TARGET', 'MAX_HOLD_10_SESSIONS', 'DTE_21')),
    bid_micros INTEGER NOT NULL CHECK(typeof(bid_micros) = 'integer' AND bid_micros > 0),
    ask_micros INTEGER NOT NULL CHECK(typeof(ask_micros) = 'integer' AND ask_micros >= bid_micros),
    gross_proceeds_micros INTEGER NOT NULL
        CHECK(typeof(gross_proceeds_micros) = 'integer' AND gross_proceeds_micros >= 0),
    net_pnl_micros INTEGER NOT NULL CHECK(typeof(net_pnl_micros) = 'integer'),
    net_r_numerator_micros INTEGER NOT NULL CHECK(typeof(net_r_numerator_micros) = 'integer'),
    initial_risk_micros INTEGER NOT NULL
        CHECK(typeof(initial_risk_micros) = 'integer' AND initial_risk_micros > 0),
    underlying_review_set_id TEXT NOT NULL COLLATE BINARY,
    underlying_review_fact_id TEXT COLLATE BINARY,
    underlying_source_observation_id INTEGER
        CHECK(underlying_source_observation_id IS NULL OR (typeof(underlying_source_observation_id) = 'integer' AND underlying_source_observation_id > 0)),
    underlying_external_source_observation_id TEXT COLLATE BINARY
        CHECK(underlying_external_source_observation_id IS NULL OR length(underlying_external_source_observation_id) > 0),
    underlying_fetch_page_ordinal INTEGER
        CHECK(underlying_fetch_page_ordinal IS NULL OR (typeof(underlying_fetch_page_ordinal) = 'integer' AND underlying_fetch_page_ordinal > 0)),
    underlying_source_item_ordinal INTEGER
        CHECK(underlying_source_item_ordinal IS NULL OR (typeof(underlying_source_item_ordinal) = 'integer' AND underlying_source_item_ordinal > 0)),
    underlying_source_item_path TEXT COLLATE BINARY
        CHECK(underlying_source_item_path IS NULL OR (length(underlying_source_item_path) > 0 AND length(underlying_source_item_path) <= 512)),
    underlying_payload_sha256 TEXT COLLATE BINARY
        CHECK(underlying_payload_sha256 IS NULL OR (length(underlying_payload_sha256) = 64 AND underlying_payload_sha256 NOT GLOB '*[^0-9a-f]*')),
    underlying_fact_digest TEXT COLLATE BINARY
        CHECK(underlying_fact_digest IS NULL OR (length(underlying_fact_digest) = 64 AND underlying_fact_digest NOT GLOB '*[^0-9a-f]*')),
    underlying_symbol TEXT COLLATE BINARY
        CHECK(underlying_symbol IS NULL OR (length(underlying_symbol) BETWEEN 1 AND 6 AND underlying_symbol = upper(underlying_symbol))),
    underlying_bar_at TEXT
        CHECK(underlying_bar_at IS NULL OR (length(underlying_bar_at) = 27 AND substr(underlying_bar_at, 1, 19) = strftime('%Y-%m-%dT%H:%M:%S', underlying_bar_at) AND substr(underlying_bar_at, 20, 1) = '.' AND substr(underlying_bar_at, 21, 6) NOT GLOB '*[^0-9]*' AND substr(underlying_bar_at, 27, 1) = 'Z')),
    underlying_open_micros INTEGER
        CHECK(underlying_open_micros IS NULL OR (typeof(underlying_open_micros) = 'integer' AND underlying_open_micros > 0)),
    underlying_high_micros INTEGER
        CHECK(underlying_high_micros IS NULL OR (typeof(underlying_high_micros) = 'integer' AND underlying_high_micros > 0)),
    underlying_low_micros INTEGER
        CHECK(underlying_low_micros IS NULL OR (typeof(underlying_low_micros) = 'integer' AND underlying_low_micros > 0)),
    underlying_close_micros INTEGER
        CHECK(underlying_close_micros IS NULL OR (typeof(underlying_close_micros) = 'integer' AND underlying_close_micros > 0)),
    underlying_volume INTEGER
        CHECK(underlying_volume IS NULL OR (typeof(underlying_volume) = 'integer' AND underlying_volume >= 0)),
    underlying_feed TEXT CHECK(underlying_feed IS NULL OR underlying_feed = 'sip'),
    underlying_adjustment TEXT CHECK(underlying_adjustment IS NULL OR underlying_adjustment = 'split'),
    exit_decision_digest TEXT NOT NULL COLLATE BINARY
        CHECK(length(exit_decision_digest) = 64 AND exit_decision_digest NOT GLOB '*[^0-9a-f]*'),
    settlement_available_session TEXT NOT NULL
        CHECK(length(settlement_available_session) = 10 AND settlement_available_session = strftime('%Y-%m-%d', settlement_available_session)),
    settlement_calendar_digest TEXT NOT NULL COLLATE BINARY
        CHECK(length(settlement_calendar_digest) = 64 AND settlement_calendar_digest NOT GLOB '*[^0-9a-f]*'),
    exited_at TEXT NOT NULL
        CHECK(length(exited_at) = 27 AND substr(exited_at, 1, 19) = strftime('%Y-%m-%dT%H:%M:%S', exited_at) AND substr(exited_at, 20, 1) = '.' AND substr(exited_at, 21, 6) NOT GLOB '*[^0-9]*' AND substr(exited_at, 27, 1) = 'Z'),
    received_at TEXT NOT NULL
        CHECK(length(received_at) = 27 AND substr(received_at, 1, 19) = strftime('%Y-%m-%dT%H:%M:%S', received_at) AND substr(received_at, 20, 1) = '.' AND substr(received_at, 21, 6) NOT GLOB '*[^0-9]*' AND substr(received_at, 27, 1) = 'Z'),
    source_digest TEXT NOT NULL COLLATE BINARY
        CHECK(length(source_digest) = 64 AND source_digest NOT GLOB '*[^0-9a-f]*'),
    record_sha256 TEXT NOT NULL COLLATE BINARY
        CHECK(length(record_sha256) = 64 AND record_sha256 NOT GLOB '*[^0-9a-f]*'),
    CHECK(underlying_low_micros <= underlying_open_micros
      AND underlying_low_micros <= underlying_close_micros
      AND underlying_high_micros >= underlying_open_micros
      AND underlying_high_micros >= underlying_close_micros),
    CHECK(underlying_bar_at IS NULL OR (underlying_bar_at <= exited_at AND exited_at <= received_at)),
    CHECK(
        (exit_reason IN ('STOP', 'TARGET')
         AND underlying_review_fact_id IS NOT NULL
         AND underlying_source_observation_id IS NOT NULL
         AND underlying_external_source_observation_id IS NOT NULL
         AND underlying_fetch_page_ordinal IS NOT NULL
         AND underlying_source_item_ordinal IS NOT NULL
         AND underlying_source_item_path IS NOT NULL
         AND underlying_payload_sha256 IS NOT NULL
         AND underlying_fact_digest IS NOT NULL
         AND underlying_symbol IS NOT NULL
         AND underlying_bar_at IS NOT NULL
         AND underlying_open_micros IS NOT NULL
         AND underlying_high_micros IS NOT NULL
         AND underlying_low_micros IS NOT NULL
         AND underlying_close_micros IS NOT NULL
         AND underlying_volume IS NOT NULL
         AND underlying_feed IS NOT NULL
         AND underlying_adjustment IS NOT NULL)
        OR
        (exit_reason IN ('MAX_HOLD_10_SESSIONS', 'DTE_21')
         AND underlying_review_fact_id IS NULL
         AND underlying_source_observation_id IS NULL
         AND underlying_external_source_observation_id IS NULL
         AND underlying_fetch_page_ordinal IS NULL
         AND underlying_source_item_ordinal IS NULL
         AND underlying_source_item_path IS NULL
         AND underlying_payload_sha256 IS NULL
         AND underlying_fact_digest IS NULL
         AND underlying_symbol IS NULL
         AND underlying_bar_at IS NULL
         AND underlying_open_micros IS NULL
         AND underlying_high_micros IS NULL
         AND underlying_low_micros IS NULL
         AND underlying_close_micros IS NULL
         AND underlying_volume IS NULL
         AND underlying_feed IS NULL
         AND underlying_adjustment IS NULL)
    ),
    CHECK(settlement_available_session > substr(exited_at, 1, 10)),
    FOREIGN KEY(window_id) REFERENCES phase2_windows(window_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
    FOREIGN KEY(entry_id) REFERENCES phase2_entries(entry_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
    FOREIGN KEY(exit_review_id) REFERENCES phase2_exit_reviews(exit_review_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
    FOREIGN KEY(execution_event_id) REFERENCES execution_events(id)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
    FOREIGN KEY(raw_message_id) REFERENCES raw_messages(id)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
    FOREIGN KEY(underlying_review_set_id) REFERENCES phase2_underlying_review_sets(review_set_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
    FOREIGN KEY(underlying_review_fact_id) REFERENCES phase2_underlying_review_facts(fact_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
    FOREIGN KEY(underlying_source_observation_id) REFERENCES source_observations(id)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
    FOREIGN KEY(underlying_source_observation_id, underlying_payload_sha256)
        REFERENCES phase1_source_payloads(source_observation_id, payload_sha256)
        ON UPDATE RESTRICT ON DELETE RESTRICT
) STRICT;

CREATE TRIGGER phase2_exit_reviews_reject_ignored_required_close
BEFORE INSERT ON phase2_exit_reviews
WHEN EXISTS (
    SELECT 1
    FROM phase2_exit_reviews AS prior
    WHERE prior.entry_id = NEW.entry_id
      AND prior.review_session < NEW.review_session
      AND prior.decision_kind <> 'HOLD'
      AND NOT EXISTS (
          SELECT 1 FROM phase2_exits AS closed
          WHERE closed.exit_review_id = prior.exit_review_id
            AND closed.entry_id = prior.entry_id
      )
)
BEGIN SELECT RAISE(ABORT, 'phase2 required close cannot be ignored on a later review'); END;

CREATE TRIGGER phase2_exits_validate_review_decision
BEFORE INSERT ON phase2_exits
WHEN NOT EXISTS (
    SELECT 1
    FROM phase2_exit_reviews AS review
    WHERE review.exit_review_id = NEW.exit_review_id
      AND review.window_id = NEW.window_id
      AND review.entry_id = NEW.entry_id
      AND review.underlying_review_set_id = NEW.underlying_review_set_id
      AND review.decision_kind = NEW.exit_reason
      AND review.decision_kind <> 'HOLD'
      AND (
          (review.decision_kind IN ('STOP', 'TARGET')
           AND review.decision_fact_id = NEW.underlying_review_fact_id)
          OR
          (review.decision_kind IN ('MAX_HOLD_10_SESSIONS', 'DTE_21')
           AND review.decision_fact_id IS NULL
           AND NEW.underlying_review_fact_id IS NULL)
      )
      AND review.review_session = substr(NEW.exited_at, 1, 10)
      AND review.decision_digest = NEW.exit_decision_digest
      AND review.evaluated_at <= NEW.exited_at
      AND review.received_at <= NEW.received_at
)
BEGIN SELECT RAISE(ABORT, 'phase2 exit requires its exact non-hold review decision'); END;

CREATE TRIGGER phase2_exits_validate_manual_source
BEFORE INSERT ON phase2_exits
WHEN NOT EXISTS (
    SELECT 1 FROM execution_events AS event
    JOIN phase2_entries AS entry ON entry.entry_id = NEW.entry_id
    WHERE entry.window_id = NEW.window_id
      AND event.id = NEW.execution_event_id
      AND event.raw_message_id = NEW.raw_message_id
      AND event.action_ordinal = NEW.action_ordinal
      AND event.parsed_action = 'OPTION_PAPER_CLOSE'
      AND event.bid_micros = NEW.bid_micros
      AND event.ask_micros = NEW.ask_micros
      AND json_extract(event.details_json, '$.normalized.occ_symbol') = (
          SELECT manual.occ_symbol
          FROM phase2_contract_selections AS selection
          JOIN phase2_contract_snapshots AS manual
            ON manual.snapshot_id = selection.manual_snapshot_id
          WHERE selection.selection_id = entry.selection_id
      )
      AND event.event_time = NEW.exited_at
      AND event.message_time <= NEW.received_at
)
BEGIN SELECT RAISE(ABORT, 'phase2 exit requires exact close action source'); END;

CREATE TRIGGER phase2_exits_validate_underlying_source
BEFORE INSERT ON phase2_exits
WHEN NEW.exit_reason IN ('STOP', 'TARGET') AND NOT EXISTS (
    SELECT 1
    FROM phase2_underlying_review_facts AS fact
    JOIN source_observations AS source
      ON source.id = fact.source_observation_id
    JOIN phase1_source_payloads AS payload
      ON payload.source_observation_id = source.id
     AND payload.payload_sha256 = NEW.underlying_payload_sha256
    JOIN phase2_entries AS entry ON entry.entry_id = NEW.entry_id
    JOIN phase2_contract_selections AS selection
      ON selection.selection_id = entry.selection_id
    JOIN phase2_contract_snapshots AS manual
      ON manual.snapshot_id = selection.manual_snapshot_id
    JOIN phase2_underlying_review_sets AS review
      ON review.review_set_id = NEW.underlying_review_set_id
     AND review.entry_id = entry.entry_id
     AND review.window_id = entry.window_id
    JOIN phase2_underlying_review_pages AS page
      ON page.review_set_id = review.review_set_id
     AND page.page_ordinal = fact.fetch_page_ordinal
     AND page.source_observation_id = fact.source_observation_id
     AND page.payload_sha256 = fact.payload_sha256
    WHERE fact.fact_id = NEW.underlying_review_fact_id
      AND fact.review_set_id = NEW.underlying_review_set_id
      AND source.id = NEW.underlying_source_observation_id
      AND source.payload_sha256 = NEW.underlying_payload_sha256
      AND page.page_ordinal = NEW.underlying_fetch_page_ordinal
      AND fact.external_source_observation_id = NEW.underlying_external_source_observation_id
      AND fact.source_item_ordinal = NEW.underlying_source_item_ordinal
      AND fact.source_item_path = NEW.underlying_source_item_path
      AND fact.payload_sha256 = NEW.underlying_payload_sha256
      AND fact.fact_digest = NEW.underlying_fact_digest
      AND fact.symbol = NEW.underlying_symbol
      AND fact.bar_at = NEW.underlying_bar_at
      AND fact.open_micros = NEW.underlying_open_micros
      AND fact.high_micros = NEW.underlying_high_micros
      AND fact.low_micros = NEW.underlying_low_micros
      AND fact.close_micros = NEW.underlying_close_micros
      AND fact.volume = NEW.underlying_volume
      AND source.source_time <= NEW.underlying_bar_at
      AND source.retrieved_at <= NEW.received_at
      AND manual.underlying = NEW.underlying_symbol
      AND review.underlying = NEW.underlying_symbol
      AND review.timeframe = '1Min'
      AND review.adjustment = NEW.underlying_adjustment
      AND review.feed = NEW.underlying_feed
      AND review.review_session = substr(NEW.underlying_bar_at, 1, 10)
      AND NEW.underlying_bar_at BETWEEN review.request_start AND review.request_end
)
BEGIN SELECT RAISE(ABORT, 'phase2 exit requires exact persisted underlying bar source'); END;

CREATE TRIGGER phase2_exits_validate_underlying_completeness
BEFORE INSERT ON phase2_exits
WHEN NOT EXISTS (
    SELECT 1
    FROM phase2_underlying_review_sets AS review
    WHERE review.review_set_id = NEW.underlying_review_set_id
      AND review.entry_id = NEW.entry_id
      AND review.window_id = NEW.window_id
      AND review.terminal = 1
      AND (SELECT count(*)
           FROM phase2_underlying_review_pages AS page
           WHERE page.review_set_id = review.review_set_id)
          = review.expected_page_count
      AND (SELECT max(page.page_ordinal)
           FROM phase2_underlying_review_pages AS page
           WHERE page.review_set_id = review.review_set_id)
          = review.expected_page_count
      AND (SELECT count(*)
           FROM phase2_underlying_review_facts AS fact
           WHERE fact.review_set_id = review.review_set_id)
          = review.expected_fact_count
      AND EXISTS (
          SELECT 1 FROM phase2_underlying_review_pages AS terminal_page
          WHERE terminal_page.review_set_id = review.review_set_id
            AND terminal_page.page_ordinal = review.expected_page_count
            AND terminal_page.next_page_token IS NULL
      )
      AND NOT EXISTS (
          SELECT 1 FROM phase2_underlying_review_pages AS early_page
          WHERE early_page.review_set_id = review.review_set_id
            AND early_page.page_ordinal < review.expected_page_count
            AND early_page.next_page_token IS NULL
      )
)
BEGIN SELECT RAISE(ABORT, 'phase2 exit requires a complete terminal underlying review'); END;

CREATE TRIGGER phase2_exits_validate_settlement_calendar
BEFORE INSERT ON phase2_exits
WHEN NOT EXISTS (
    SELECT 1 FROM phase2_windows AS window
    WHERE window.window_id = NEW.window_id
      AND NEW.settlement_calendar_digest = window.calendar_digest
)
BEGIN SELECT RAISE(ABORT, 'phase2 exit settlement requires the window calendar'); END;

CREATE TABLE phase2_fee_records (
    id INTEGER PRIMARY KEY CHECK(typeof(id) = 'integer' AND id > 0),
    fee_id TEXT NOT NULL COLLATE BINARY UNIQUE
        CHECK(length(fee_id) = 64 AND fee_id NOT GLOB '*[^0-9a-f]*'),
    window_id TEXT NOT NULL COLLATE BINARY,
    entry_id TEXT NOT NULL COLLATE BINARY,
    exit_id TEXT COLLATE BINARY,
    fee_kind TEXT NOT NULL CHECK(fee_kind IN ('ENTRY', 'RESERVE', 'EXIT_ACTUAL')),
    amount_micros INTEGER NOT NULL
        CHECK(typeof(amount_micros) = 'integer'
          AND amount_micros >= 0
          AND (fee_kind <> 'EXIT_ACTUAL' OR amount_micros > 0)),
    fee_schedule_id TEXT NOT NULL COLLATE BINARY
        CHECK(length(fee_schedule_id) > 0 AND fee_schedule_id NOT GLOB '*[^A-Z0-9_:-]*'),
    fee_schedule_digest TEXT NOT NULL COLLATE BINARY
        CHECK(length(fee_schedule_digest) = 64 AND fee_schedule_digest NOT GLOB '*[^0-9a-f]*'),
    execution_event_id INTEGER UNIQUE
        CHECK(execution_event_id IS NULL OR (typeof(execution_event_id) = 'integer' AND execution_event_id > 0)),
    raw_message_id INTEGER
        CHECK(raw_message_id IS NULL OR (typeof(raw_message_id) = 'integer' AND raw_message_id > 0)),
    action_ordinal INTEGER
        CHECK(action_ordinal IS NULL OR (typeof(action_ordinal) = 'integer' AND action_ordinal >= 0)),
    action_source_digest TEXT COLLATE BINARY
        CHECK(action_source_digest IS NULL OR (length(action_source_digest) = 64 AND action_source_digest NOT GLOB '*[^0-9a-f]*')),
    recorded_at TEXT NOT NULL
        CHECK(length(recorded_at) = 27 AND substr(recorded_at, 1, 19) = strftime('%Y-%m-%dT%H:%M:%S', recorded_at) AND substr(recorded_at, 20, 1) = '.' AND substr(recorded_at, 21, 6) NOT GLOB '*[^0-9]*' AND substr(recorded_at, 27, 1) = 'Z'),
    source_digest TEXT NOT NULL COLLATE BINARY
        CHECK(length(source_digest) = 64 AND source_digest NOT GLOB '*[^0-9a-f]*'),
    record_sha256 TEXT NOT NULL COLLATE BINARY
        CHECK(length(record_sha256) = 64 AND record_sha256 NOT GLOB '*[^0-9a-f]*'),
    CHECK((fee_kind IN ('ENTRY', 'RESERVE')
           AND exit_id IS NULL
           AND execution_event_id IS NULL
           AND raw_message_id IS NULL
           AND action_ordinal IS NULL
           AND action_source_digest IS NULL)
       OR (fee_kind = 'EXIT_ACTUAL'
           AND exit_id IS NOT NULL
           AND execution_event_id IS NOT NULL
           AND raw_message_id IS NOT NULL
           AND action_ordinal IS NOT NULL
           AND action_source_digest IS NOT NULL)),
    UNIQUE(entry_id, fee_kind),
    FOREIGN KEY(window_id) REFERENCES phase2_windows(window_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
    FOREIGN KEY(entry_id) REFERENCES phase2_entries(entry_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
    FOREIGN KEY(exit_id) REFERENCES phase2_exits(exit_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
    FOREIGN KEY(execution_event_id) REFERENCES execution_events(id)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
    FOREIGN KEY(raw_message_id) REFERENCES raw_messages(id)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
    FOREIGN KEY(fee_schedule_id, fee_schedule_digest)
        REFERENCES phase2_fee_schedules(schedule_id, schedule_digest)
        ON UPDATE RESTRICT ON DELETE RESTRICT
) STRICT;

CREATE TRIGGER phase2_fee_records_validate_lineage
BEFORE INSERT ON phase2_fee_records
WHEN NOT EXISTS (
    SELECT 1 FROM phase2_entries AS entry
    LEFT JOIN phase2_exits AS exit ON exit.exit_id = NEW.exit_id
    WHERE entry.entry_id = NEW.entry_id
      AND entry.window_id = NEW.window_id
      AND entry.fee_schedule_id = NEW.fee_schedule_id
      AND entry.fee_schedule_digest = NEW.fee_schedule_digest
      AND (NEW.exit_id IS NULL OR (exit.entry_id = entry.entry_id AND exit.window_id = entry.window_id))
      AND ((NEW.fee_kind = 'ENTRY' AND NEW.amount_micros = entry.entry_fee_micros)
        OR (NEW.fee_kind = 'RESERVE' AND NEW.amount_micros = entry.reserve_fee_micros)
        OR NEW.fee_kind = 'EXIT_ACTUAL')
)
BEGIN SELECT RAISE(ABORT, 'phase2 fee requires exact entry and exit lineage'); END;

CREATE TRIGGER phase2_fee_records_validate_actual_source
BEFORE INSERT ON phase2_fee_records
WHEN NEW.fee_kind = 'EXIT_ACTUAL' AND NOT EXISTS (
    SELECT 1
    FROM execution_events AS event
    JOIN phase2_entries AS entry ON entry.entry_id = NEW.entry_id
    JOIN phase2_exits AS exit
      ON exit.exit_id = NEW.exit_id
     AND exit.entry_id = entry.entry_id
     AND exit.window_id = entry.window_id
    JOIN phase2_contract_selections AS selection
      ON selection.selection_id = entry.selection_id
    JOIN phase2_contract_snapshots AS manual
      ON manual.snapshot_id = selection.manual_snapshot_id
    WHERE event.id = NEW.execution_event_id
      AND event.raw_message_id = NEW.raw_message_id
      AND event.action_ordinal = NEW.action_ordinal
      AND event.parsed_action = 'FEE'
      AND json_extract(event.details_json, '$.normalized.asset_id') = manual.occ_symbol
      AND instr(json_extract(event.details_json, '$.normalized.amount_decimal'), '.') > 1
      AND substr(
              json_extract(event.details_json, '$.normalized.amount_decimal'),
              1,
              instr(json_extract(event.details_json, '$.normalized.amount_decimal'), '.') - 1
          ) NOT GLOB '*[^0-9]*'
      AND length(substr(json_extract(event.details_json, '$.normalized.amount_decimal'), instr(json_extract(event.details_json, '$.normalized.amount_decimal'), '.') + 1)) = 6
      AND substr(
              json_extract(event.details_json, '$.normalized.amount_decimal'),
              instr(json_extract(event.details_json, '$.normalized.amount_decimal'), '.') + 1
          ) NOT GLOB '*[^0-9]*'
      AND CAST(substr(json_extract(event.details_json, '$.normalized.amount_decimal'), 1, instr(json_extract(event.details_json, '$.normalized.amount_decimal'), '.') - 1) AS INTEGER) * 1000000
          + CAST(substr(json_extract(event.details_json, '$.normalized.amount_decimal'), instr(json_extract(event.details_json, '$.normalized.amount_decimal'), '.') + 1, 6) AS INTEGER)
          = NEW.amount_micros
      AND event.event_time >= exit.exited_at
      AND event.event_time <= NEW.recorded_at
      AND event.message_time <= NEW.recorded_at
)
BEGIN SELECT RAISE(ABORT, 'phase2 actual fee requires exact fee action source'); END;

CREATE TABLE phase2_equity_points (
    id INTEGER PRIMARY KEY CHECK(typeof(id) = 'integer' AND id > 0),
    point_id TEXT NOT NULL COLLATE BINARY UNIQUE
        CHECK(length(point_id) = 64 AND point_id NOT GLOB '*[^0-9a-f]*'),
    window_id TEXT NOT NULL COLLATE BINARY,
    entry_id TEXT COLLATE BINARY,
    mark_id TEXT COLLATE BINARY,
    exit_id TEXT COLLATE BINARY,
    session_date TEXT NOT NULL
        CHECK(length(session_date) = 10 AND session_date = strftime('%Y-%m-%d', session_date)),
    point_kind TEXT NOT NULL CHECK(point_kind IN ('START', 'MARK', 'EXIT')),
    cash_micros INTEGER NOT NULL CHECK(typeof(cash_micros) = 'integer'),
    position_value_micros INTEGER NOT NULL CHECK(typeof(position_value_micros) = 'integer'),
    equity_micros INTEGER NOT NULL CHECK(typeof(equity_micros) = 'integer'),
    high_water_micros INTEGER NOT NULL CHECK(typeof(high_water_micros) = 'integer'),
    drawdown_micros INTEGER NOT NULL
        CHECK(typeof(drawdown_micros) = 'integer' AND drawdown_micros >= 0),
    at TEXT NOT NULL
        CHECK(length(at) = 27 AND substr(at, 1, 19) = strftime('%Y-%m-%dT%H:%M:%S', at) AND substr(at, 20, 1) = '.' AND substr(at, 21, 6) NOT GLOB '*[^0-9]*' AND substr(at, 27, 1) = 'Z'),
    received_at TEXT NOT NULL
        CHECK(length(received_at) = 27 AND substr(received_at, 1, 19) = strftime('%Y-%m-%dT%H:%M:%S', received_at) AND substr(received_at, 20, 1) = '.' AND substr(received_at, 21, 6) NOT GLOB '*[^0-9]*' AND substr(received_at, 27, 1) = 'Z'),
    source_digest TEXT NOT NULL COLLATE BINARY
        CHECK(length(source_digest) = 64 AND source_digest NOT GLOB '*[^0-9a-f]*'),
    record_sha256 TEXT NOT NULL COLLATE BINARY
        CHECK(length(record_sha256) = 64 AND record_sha256 NOT GLOB '*[^0-9a-f]*'),
    CHECK(equity_micros = cash_micros + position_value_micros),
    CHECK(high_water_micros >= equity_micros AND drawdown_micros = high_water_micros - equity_micros),
    CHECK(at <= received_at),
    CHECK((point_kind = 'START' AND entry_id IS NULL AND mark_id IS NULL AND exit_id IS NULL)
       OR (point_kind = 'MARK' AND entry_id IS NOT NULL AND mark_id IS NOT NULL AND exit_id IS NULL)
       OR (point_kind = 'EXIT' AND entry_id IS NOT NULL AND mark_id IS NULL AND exit_id IS NOT NULL)),
    FOREIGN KEY(window_id) REFERENCES phase2_windows(window_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
    FOREIGN KEY(entry_id) REFERENCES phase2_entries(entry_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
    FOREIGN KEY(mark_id) REFERENCES phase2_marks(mark_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
    FOREIGN KEY(exit_id) REFERENCES phase2_exits(exit_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT
) STRICT;

CREATE TRIGGER phase2_equity_points_validate_lineage
BEFORE INSERT ON phase2_equity_points
WHEN (NEW.entry_id IS NOT NULL AND NOT EXISTS (
        SELECT 1 FROM phase2_entries WHERE entry_id = NEW.entry_id AND window_id = NEW.window_id
    )) OR (NEW.mark_id IS NOT NULL AND NOT EXISTS (
        SELECT 1 FROM phase2_marks WHERE mark_id = NEW.mark_id AND entry_id = NEW.entry_id AND window_id = NEW.window_id
    )) OR (NEW.exit_id IS NOT NULL AND NOT EXISTS (
        SELECT 1 FROM phase2_exits WHERE exit_id = NEW.exit_id AND entry_id = NEW.entry_id AND window_id = NEW.window_id
    ))
BEGIN SELECT RAISE(ABORT, 'phase2 equity point lineage is inconsistent'); END;

CREATE TRIGGER phase2_equity_points_validate_start
BEFORE INSERT ON phase2_equity_points
WHEN NEW.point_kind = 'START' AND NOT EXISTS (
    SELECT 1 FROM phase2_windows AS window
    WHERE window.window_id = NEW.window_id
      AND NEW.cash_micros = 5000000000
      AND NEW.position_value_micros = 0
      AND NEW.equity_micros = 5000000000
      AND NEW.high_water_micros = 5000000000
      AND NEW.drawdown_micros = 0
      AND NEW.session_date = window.started_session
      AND NEW.at = window.started_at
      AND NEW.received_at = window.received_at
)
BEGIN SELECT RAISE(ABORT, 'phase2 start equity must derive from exact window genesis'); END;

CREATE TRIGGER phase2_equity_points_one_start_per_window
BEFORE INSERT ON phase2_equity_points
WHEN NEW.point_kind = 'START' AND EXISTS (
    SELECT 1 FROM phase2_equity_points
    WHERE window_id = NEW.window_id COLLATE BINARY
      AND point_kind = 'START'
)
BEGIN SELECT RAISE(ABORT, 'phase2 start equity already exists'); END;

CREATE TABLE phase2_window_failures (
    id INTEGER PRIMARY KEY CHECK(typeof(id) = 'integer' AND id > 0),
    failure_id TEXT NOT NULL COLLATE BINARY UNIQUE
        CHECK(length(failure_id) = 64 AND failure_id NOT GLOB '*[^0-9a-f]*'),
    window_id TEXT NOT NULL COLLATE BINARY,
    session_date TEXT NOT NULL
        CHECK(length(session_date) = 10 AND session_date = strftime('%Y-%m-%d', session_date)),
    reason_code TEXT NOT NULL CHECK(length(reason_code) > 0 AND reason_code = upper(reason_code)),
    mark_id TEXT COLLATE BINARY,
    detected_at TEXT NOT NULL
        CHECK(length(detected_at) = 27 AND substr(detected_at, 1, 19) = strftime('%Y-%m-%dT%H:%M:%S', detected_at) AND substr(detected_at, 20, 1) = '.' AND substr(detected_at, 21, 6) NOT GLOB '*[^0-9]*' AND substr(detected_at, 27, 1) = 'Z'),
    received_at TEXT NOT NULL
        CHECK(length(received_at) = 27 AND substr(received_at, 1, 19) = strftime('%Y-%m-%dT%H:%M:%S', received_at) AND substr(received_at, 20, 1) = '.' AND substr(received_at, 21, 6) NOT GLOB '*[^0-9]*' AND substr(received_at, 27, 1) = 'Z'),
    source_digest TEXT NOT NULL COLLATE BINARY
        CHECK(length(source_digest) = 64 AND source_digest NOT GLOB '*[^0-9a-f]*'),
    record_sha256 TEXT NOT NULL COLLATE BINARY
        CHECK(length(record_sha256) = 64 AND record_sha256 NOT GLOB '*[^0-9a-f]*'),
    CHECK(detected_at <= received_at),
    FOREIGN KEY(window_id) REFERENCES phase2_windows(window_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
    FOREIGN KEY(mark_id) REFERENCES phase2_marks(mark_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT
) STRICT;

CREATE TRIGGER phase2_window_failures_validate_mark
BEFORE INSERT ON phase2_window_failures
WHEN NEW.mark_id IS NOT NULL AND NOT EXISTS (
    SELECT 1 FROM phase2_marks AS mark
    WHERE mark.mark_id = NEW.mark_id AND mark.window_id = NEW.window_id
)
BEGIN SELECT RAISE(ABORT, 'phase2 failure mark belongs to another window'); END;

CREATE TABLE phase2_window_restarts (
    id INTEGER PRIMARY KEY CHECK(typeof(id) = 'integer' AND id > 0),
    restart_id TEXT NOT NULL COLLATE BINARY UNIQUE
        CHECK(length(restart_id) = 64 AND restart_id NOT GLOB '*[^0-9a-f]*'),
    failed_window_id TEXT NOT NULL COLLATE BINARY UNIQUE,
    next_window_id TEXT NOT NULL COLLATE BINARY UNIQUE,
    start_execution_event_id INTEGER NOT NULL UNIQUE
        CHECK(typeof(start_execution_event_id) = 'integer' AND start_execution_event_id > 0),
    start_raw_message_id INTEGER NOT NULL
        CHECK(typeof(start_raw_message_id) = 'integer' AND start_raw_message_id > 0),
    restarted_at TEXT NOT NULL
        CHECK(length(restarted_at) = 27 AND substr(restarted_at, 1, 19) = strftime('%Y-%m-%dT%H:%M:%S', restarted_at) AND substr(restarted_at, 20, 1) = '.' AND substr(restarted_at, 21, 6) NOT GLOB '*[^0-9]*' AND substr(restarted_at, 27, 1) = 'Z'),
    received_at TEXT NOT NULL
        CHECK(length(received_at) = 27 AND substr(received_at, 1, 19) = strftime('%Y-%m-%dT%H:%M:%S', received_at) AND substr(received_at, 20, 1) = '.' AND substr(received_at, 21, 6) NOT GLOB '*[^0-9]*' AND substr(received_at, 27, 1) = 'Z'),
    source_digest TEXT NOT NULL COLLATE BINARY
        CHECK(length(source_digest) = 64 AND source_digest NOT GLOB '*[^0-9a-f]*'),
    record_sha256 TEXT NOT NULL COLLATE BINARY
        CHECK(length(record_sha256) = 64 AND record_sha256 NOT GLOB '*[^0-9a-f]*'),
    CHECK(failed_window_id <> next_window_id AND restarted_at <= received_at),
    FOREIGN KEY(failed_window_id) REFERENCES phase2_windows(window_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
    FOREIGN KEY(next_window_id) REFERENCES phase2_windows(window_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
    FOREIGN KEY(start_execution_event_id) REFERENCES execution_events(id)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
    FOREIGN KEY(start_raw_message_id) REFERENCES raw_messages(id)
        ON UPDATE RESTRICT ON DELETE RESTRICT
) STRICT;

CREATE TRIGGER phase2_window_restarts_validate_start_source
BEFORE INSERT ON phase2_window_restarts
WHEN NOT EXISTS (
    SELECT 1
    FROM phase2_windows AS next_window
    JOIN execution_events AS event ON event.id = NEW.start_execution_event_id
    WHERE next_window.window_id = NEW.next_window_id
      AND next_window.start_execution_event_id = NEW.start_execution_event_id
      AND next_window.start_raw_message_id = NEW.start_raw_message_id
      AND event.raw_message_id = NEW.start_raw_message_id
      AND event.parsed_action = 'OPTION_PAPER_WINDOW_START'
      AND event.event_time = NEW.restarted_at
      AND EXISTS (
          SELECT 1 FROM phase2_window_failures AS failure
          WHERE failure.window_id = NEW.failed_window_id
            AND failure.detected_at < NEW.restarted_at
      )
)
BEGIN SELECT RAISE(ABORT, 'phase2 restart requires failure and exact start action source'); END;

CREATE TABLE phase2_adherence_checks (
    id INTEGER PRIMARY KEY CHECK(typeof(id) = 'integer' AND id > 0),
    check_id TEXT NOT NULL COLLATE BINARY UNIQUE
        CHECK(length(check_id) = 64 AND check_id NOT GLOB '*[^0-9a-f]*'),
    window_id TEXT NOT NULL COLLATE BINARY,
    entry_id TEXT NOT NULL COLLATE BINARY,
    check_name TEXT NOT NULL CHECK(check_name IN (
        'QUOTE_FRESHNESS',
        'CONTRACT_LIQUIDITY',
        'SETTLED_FUNDS_ELIGIBILITY',
        'EXPIRATION_WINDOW',
        'EVENT_EXCLUSION',
        'ONE_POSITION_LIMIT',
        'ALL_IN_INITIAL_RISK',
        'ENTRY_EXECUTION',
        'EXIT_EXECUTION',
        'RECORD_COMPLETENESS'
    )),
    applicable INTEGER NOT NULL
        CHECK(typeof(applicable) = 'integer' AND applicable IN (0, 1)),
    passed INTEGER NOT NULL
        CHECK(typeof(passed) = 'integer' AND passed IN (0, 1)),
    hard_breach INTEGER NOT NULL
        CHECK(typeof(hard_breach) = 'integer' AND hard_breach IN (0, 1)),
    evidence_row_references_json TEXT NOT NULL
        CHECK(json_valid(evidence_row_references_json)
          AND json_type(evidence_row_references_json) = 'array'
          AND length(evidence_row_references_json) > 2),
    evidence_highwaters_json TEXT NOT NULL
        CHECK(json_valid(evidence_highwaters_json)
          AND json_type(evidence_highwaters_json) = 'object'
          AND length(evidence_highwaters_json) > 2),
    evidence_digest TEXT NOT NULL COLLATE BINARY
        CHECK(length(evidence_digest) = 64 AND evidence_digest NOT GLOB '*[^0-9a-f]*'),
    authority_digest TEXT NOT NULL COLLATE BINARY
        CHECK(length(authority_digest) = 64 AND authority_digest NOT GLOB '*[^0-9a-f]*'),
    evaluated_at TEXT NOT NULL
        CHECK(length(evaluated_at) = 27 AND substr(evaluated_at, 1, 19) = strftime('%Y-%m-%dT%H:%M:%S', evaluated_at) AND substr(evaluated_at, 20, 1) = '.' AND substr(evaluated_at, 21, 6) NOT GLOB '*[^0-9]*' AND substr(evaluated_at, 27, 1) = 'Z'),
    received_at TEXT NOT NULL
        CHECK(length(received_at) = 27 AND substr(received_at, 1, 19) = strftime('%Y-%m-%dT%H:%M:%S', received_at) AND substr(received_at, 20, 1) = '.' AND substr(received_at, 21, 6) NOT GLOB '*[^0-9]*' AND substr(received_at, 27, 1) = 'Z'),
    source_digest TEXT NOT NULL COLLATE BINARY
        CHECK(length(source_digest) = 64 AND source_digest NOT GLOB '*[^0-9a-f]*'),
    record_sha256 TEXT NOT NULL COLLATE BINARY
        CHECK(length(record_sha256) = 64 AND record_sha256 NOT GLOB '*[^0-9a-f]*'),
    CHECK(passed <= applicable),
    CHECK(hard_breach = 0 OR (applicable = 1 AND passed = 0)),
    CHECK(evaluated_at <= received_at),
    UNIQUE(entry_id, check_name),
    FOREIGN KEY(window_id) REFERENCES phase2_windows(window_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
    FOREIGN KEY(entry_id) REFERENCES phase2_entries(entry_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT
) STRICT;

CREATE TRIGGER phase2_adherence_checks_require_derived_writer
BEFORE INSERT ON phase2_adherence_checks
WHEN journal_phase2_adherence_write_allowed() <> 1
BEGIN SELECT RAISE(ABORT, 'phase2 adherence requires the source-derived Journal writer'); END;

CREATE TRIGGER phase2_adherence_checks_validate_lineage
BEFORE INSERT ON phase2_adherence_checks
WHEN NOT EXISTS (
    SELECT 1 FROM phase2_entries AS entry
    WHERE entry.entry_id = NEW.entry_id
      AND entry.window_id = NEW.window_id
      AND entry.entered_at <= NEW.evaluated_at
      AND NEW.evaluated_at <= NEW.received_at
)
BEGIN SELECT RAISE(ABORT, 'phase2 adherence lineage is inconsistent'); END;

CREATE TABLE phase2_gate_decisions (
    id INTEGER PRIMARY KEY CHECK(typeof(id) = 'integer' AND id > 0),
    decision_id TEXT NOT NULL COLLATE BINARY UNIQUE
        CHECK(length(decision_id) = 64 AND decision_id NOT GLOB '*[^0-9a-f]*'),
    window_id TEXT NOT NULL COLLATE BINARY,
    query_cutoff TEXT NOT NULL
        CHECK(length(query_cutoff) = 27 AND substr(query_cutoff, 1, 19) = strftime('%Y-%m-%dT%H:%M:%S', query_cutoff) AND substr(query_cutoff, 20, 1) = '.' AND substr(query_cutoff, 21, 6) NOT GLOB '*[^0-9]*' AND substr(query_cutoff, 27, 1) = 'Z'),
    status TEXT NOT NULL
        CHECK(status IN ('PASSED', 'FAILED', 'IN_PROGRESS', 'RESTART_REQUIRED')),
    closed_trade_count INTEGER NOT NULL
        CHECK(typeof(closed_trade_count) = 'integer' AND closed_trade_count >= 0),
    elapsed_days INTEGER NOT NULL CHECK(typeof(elapsed_days) = 'integer' AND elapsed_days >= 0),
    mean_net_r_numerator_micros INTEGER NOT NULL CHECK(typeof(mean_net_r_numerator_micros) = 'integer'),
    mean_net_r_denominator_micros INTEGER NOT NULL
        CHECK(typeof(mean_net_r_denominator_micros) = 'integer' AND mean_net_r_denominator_micros > 0),
    adherence_passed_count INTEGER NOT NULL
        CHECK(typeof(adherence_passed_count) = 'integer' AND adherence_passed_count >= 0),
    adherence_applicable_count INTEGER NOT NULL
        CHECK(typeof(adherence_applicable_count) = 'integer' AND adherence_applicable_count >= adherence_passed_count),
    expected_adherence_count INTEGER NOT NULL
        CHECK(typeof(expected_adherence_count) = 'integer' AND expected_adherence_count >= 0),
    adherence_terminal_cursor INTEGER
        CHECK(adherence_terminal_cursor IS NULL OR (typeof(adherence_terminal_cursor) = 'integer' AND adherence_terminal_cursor > 0)),
    adherence_source_highwater INTEGER NOT NULL
        CHECK(typeof(adherence_source_highwater) = 'integer' AND adherence_source_highwater >= 0),
    max_drawdown_micros INTEGER NOT NULL
        CHECK(typeof(max_drawdown_micros) = 'integer' AND max_drawdown_micros >= 0),
    hard_breach INTEGER NOT NULL CHECK(typeof(hard_breach) = 'integer' AND hard_breach IN (0, 1)),
    failure_id TEXT COLLATE BINARY,
    expected_exit_review_count INTEGER NOT NULL
        CHECK(typeof(expected_exit_review_count) = 'integer' AND expected_exit_review_count >= 0),
    actual_exit_review_count INTEGER NOT NULL
        CHECK(typeof(actual_exit_review_count) = 'integer' AND actual_exit_review_count >= 0),
    exit_review_terminal_cursor INTEGER
        CHECK(exit_review_terminal_cursor IS NULL OR (typeof(exit_review_terminal_cursor) = 'integer' AND exit_review_terminal_cursor > 0)),
    window_row_references_json TEXT NOT NULL
        CHECK(json_valid(window_row_references_json) AND json_type(window_row_references_json) = 'array' AND length(window_row_references_json) > 2),
    window_highwaters_json TEXT NOT NULL
        CHECK(json_valid(window_highwaters_json) AND json_type(window_highwaters_json) = 'object' AND length(window_highwaters_json) > 2),
    window_source_digest TEXT NOT NULL COLLATE BINARY
        CHECK(length(window_source_digest) = 64 AND window_source_digest NOT GLOB '*[^0-9a-f]*'),
    evaluated_at TEXT NOT NULL
        CHECK(length(evaluated_at) = 27 AND substr(evaluated_at, 1, 19) = strftime('%Y-%m-%dT%H:%M:%S', evaluated_at) AND substr(evaluated_at, 20, 1) = '.' AND substr(evaluated_at, 21, 6) NOT GLOB '*[^0-9]*' AND substr(evaluated_at, 27, 1) = 'Z'),
    received_at TEXT NOT NULL
        CHECK(length(received_at) = 27 AND substr(received_at, 1, 19) = strftime('%Y-%m-%dT%H:%M:%S', received_at) AND substr(received_at, 20, 1) = '.' AND substr(received_at, 21, 6) NOT GLOB '*[^0-9]*' AND substr(received_at, 27, 1) = 'Z'),
    source_digest TEXT NOT NULL COLLATE BINARY
        CHECK(length(source_digest) = 64 AND source_digest NOT GLOB '*[^0-9a-f]*'),
    record_sha256 TEXT NOT NULL COLLATE BINARY
        CHECK(length(record_sha256) = 64 AND record_sha256 NOT GLOB '*[^0-9a-f]*'),
    CHECK(query_cutoff <= evaluated_at AND evaluated_at <= received_at),
    CHECK((status IN ('FAILED', 'RESTART_REQUIRED')
           AND (hard_breach = 1 OR failure_id IS NOT NULL))
       OR (status IN ('PASSED', 'IN_PROGRESS')
           AND hard_breach = 0 AND failure_id IS NULL)),
    CHECK(status <> 'PASSED'
       OR (closed_trade_count >= 20
           AND elapsed_days >= 28
           AND mean_net_r_numerator_micros > 0
           AND max_drawdown_micros <= 250000000
           AND adherence_applicable_count > 0
           AND adherence_passed_count * 10 >= adherence_applicable_count * 9)),
    FOREIGN KEY(window_id) REFERENCES phase2_windows(window_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
    FOREIGN KEY(failure_id) REFERENCES phase2_window_failures(failure_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT
) STRICT;

CREATE TRIGGER phase2_gate_decisions_validate_elapsed_days
BEFORE INSERT ON phase2_gate_decisions
WHEN NOT EXISTS (
    SELECT 1
    FROM phase2_windows AS window
    WHERE window.window_id = NEW.window_id
      AND NEW.elapsed_days = CAST(
          julianday(substr(NEW.query_cutoff, 1, 10))
          - julianday(window.started_session)
          AS INTEGER
      )
)
BEGIN SELECT RAISE(ABORT, 'phase2 gate elapsed days must be derived from its window'); END;

CREATE TRIGGER phase2_gate_decisions_validate_failure
BEFORE INSERT ON phase2_gate_decisions
WHEN NEW.failure_id IS NOT NULL AND NOT EXISTS (
    SELECT 1 FROM phase2_window_failures AS failure
    WHERE failure.failure_id = NEW.failure_id AND failure.window_id = NEW.window_id
)
BEGIN SELECT RAISE(ABORT, 'phase2 gate failure belongs to another window'); END;

CREATE TRIGGER phase2_gate_decisions_validate_adherence
BEFORE INSERT ON phase2_gate_decisions
WHEN NOT EXISTS (
    SELECT 1
    WHERE NEW.expected_adherence_count = (
          SELECT count(*) FROM phase2_adherence_checks AS check_row
          WHERE check_row.window_id = NEW.window_id
            AND check_row.received_at <= NEW.query_cutoff
      )
      AND NEW.adherence_passed_count = (
          SELECT count(*) FROM phase2_adherence_checks AS check_row
          WHERE check_row.window_id = NEW.window_id
            AND check_row.received_at <= NEW.query_cutoff
            AND check_row.applicable = 1
            AND check_row.passed = 1
      )
      AND NEW.adherence_applicable_count = (
          SELECT count(*) FROM phase2_adherence_checks AS check_row
          WHERE check_row.window_id = NEW.window_id
            AND check_row.received_at <= NEW.query_cutoff
            AND check_row.applicable = 1
      )
      AND NEW.hard_breach = CASE WHEN EXISTS (
          SELECT 1 FROM phase2_adherence_checks AS check_row
          WHERE check_row.window_id = NEW.window_id
            AND check_row.received_at <= NEW.query_cutoff
            AND check_row.hard_breach = 1
      ) OR EXISTS (
          SELECT 1 FROM phase2_window_failures AS failure
          WHERE failure.window_id = NEW.window_id
            AND failure.received_at <= NEW.query_cutoff
      ) THEN 1 ELSE 0 END
      AND COALESCE(NEW.adherence_terminal_cursor, 0) = COALESCE((
          SELECT max(check_row.id) FROM phase2_adherence_checks AS check_row
          WHERE check_row.window_id = NEW.window_id
            AND check_row.received_at <= NEW.query_cutoff
      ), 0)
      AND NEW.adherence_source_highwater = COALESCE((
          SELECT max(check_row.id) FROM phase2_adherence_checks AS check_row
          WHERE check_row.received_at <= NEW.query_cutoff
      ), 0)
      AND (NEW.status <> 'PASSED' OR NOT EXISTS (
          SELECT 1 FROM phase2_exits AS closed
          WHERE closed.window_id = NEW.window_id
            AND closed.exited_at <= NEW.query_cutoff
            AND 10 <> (
                SELECT count(*) FROM phase2_adherence_checks AS check_row
                WHERE check_row.window_id = NEW.window_id
                  AND check_row.entry_id = closed.entry_id
                  AND check_row.received_at <= NEW.query_cutoff
            )
      ))
)
BEGIN SELECT RAISE(ABORT, 'phase2 gate requires exact source-derived adherence rows'); END;

CREATE TRIGGER phase2_gate_decisions_validate_exit_reviews
BEFORE INSERT ON phase2_gate_decisions
WHEN NOT EXISTS (
    SELECT 1
    WHERE NEW.actual_exit_review_count = NEW.expected_exit_review_count
      AND NEW.actual_exit_review_count = (
          SELECT count(*) FROM phase2_exit_reviews AS review
          WHERE review.window_id = NEW.window_id
            AND review.evaluated_at <= NEW.query_cutoff
      )
      AND COALESCE(NEW.exit_review_terminal_cursor, 0) = COALESCE((
          SELECT max(review.id) FROM phase2_exit_reviews AS review
          WHERE review.window_id = NEW.window_id
            AND review.evaluated_at <= NEW.query_cutoff
      ), 0)
      AND NEW.closed_trade_count = (
          SELECT count(*) FROM phase2_exits AS exit
          WHERE exit.window_id = NEW.window_id
            AND exit.exited_at <= NEW.query_cutoff
      )
      AND NEW.mean_net_r_numerator_micros = COALESCE((
          SELECT sum(exit.net_r_numerator_micros) FROM phase2_exits AS exit
          WHERE exit.window_id = NEW.window_id
            AND exit.exited_at <= NEW.query_cutoff
      ), 0)
      AND NEW.mean_net_r_denominator_micros = COALESCE((
          SELECT sum(exit.initial_risk_micros) FROM phase2_exits AS exit
          WHERE exit.window_id = NEW.window_id
            AND exit.exited_at <= NEW.query_cutoff
      ), 1)
      AND NEW.max_drawdown_micros = COALESCE((
          SELECT max(point.drawdown_micros) FROM phase2_equity_points AS point
          WHERE point.window_id = NEW.window_id
            AND point.at <= NEW.query_cutoff
      ), 0)
      AND NOT EXISTS (
          SELECT 1 FROM phase2_exit_reviews AS review
          WHERE review.window_id = NEW.window_id
            AND review.evaluated_at <= NEW.query_cutoff
            AND review.decision_kind <> 'HOLD'
            AND NOT EXISTS (
                SELECT 1 FROM phase2_exits AS closed
                WHERE closed.exit_review_id = review.exit_review_id
            )
      )
      AND (NEW.status NOT IN ('PASSED', 'IN_PROGRESS') OR NOT EXISTS (
          SELECT 1 FROM phase2_window_failures AS failure
          WHERE failure.window_id = NEW.window_id
            AND failure.detected_at <= NEW.query_cutoff
      ))
)
BEGIN SELECT RAISE(ABORT, 'phase2 gate requires exact complete window source rows'); END;

CREATE TABLE historical_replay_runs (
    id INTEGER PRIMARY KEY CHECK(typeof(id) = 'integer' AND id > 0),
    replay_run_id TEXT NOT NULL COLLATE BINARY UNIQUE
        CHECK(length(replay_run_id) = 64 AND replay_run_id NOT GLOB '*[^0-9a-f]*'),
    tier TEXT NOT NULL CHECK(tier = 'STRICT_POINT_IN_TIME'),
    started_session TEXT NOT NULL
        CHECK(length(started_session) = 10 AND started_session = strftime('%Y-%m-%d', started_session)),
    ended_session TEXT NOT NULL
        CHECK(length(ended_session) = 10 AND ended_session = strftime('%Y-%m-%d', ended_session)),
    query_cutoff TEXT NOT NULL
        CHECK(length(query_cutoff) = 27 AND substr(query_cutoff, 1, 19) = strftime('%Y-%m-%dT%H:%M:%S', query_cutoff) AND substr(query_cutoff, 20, 1) = '.' AND substr(query_cutoff, 21, 6) NOT GLOB '*[^0-9]*' AND substr(query_cutoff, 27, 1) = 'Z'),
    calendar_digest TEXT NOT NULL COLLATE BINARY
        CHECK(length(calendar_digest) = 64 AND calendar_digest NOT GLOB '*[^0-9a-f]*'),
    policy_digest TEXT NOT NULL COLLATE BINARY
        CHECK(length(policy_digest) = 64 AND policy_digest NOT GLOB '*[^0-9a-f]*'),
    expected_date_count INTEGER NOT NULL
        CHECK(typeof(expected_date_count) = 'integer' AND expected_date_count > 0),
    cursors_json TEXT NOT NULL CHECK(length(cursors_json) >= 2),
    highwaters_json TEXT NOT NULL CHECK(length(highwaters_json) >= 2),
    row_references_json TEXT NOT NULL CHECK(length(row_references_json) >= 2),
    recorded_at TEXT NOT NULL
        CHECK(length(recorded_at) = 27 AND substr(recorded_at, 1, 19) = strftime('%Y-%m-%dT%H:%M:%S', recorded_at) AND substr(recorded_at, 20, 1) = '.' AND substr(recorded_at, 21, 6) NOT GLOB '*[^0-9]*' AND substr(recorded_at, 27, 1) = 'Z'),
    source_digest TEXT NOT NULL COLLATE BINARY
        CHECK(length(source_digest) = 64 AND source_digest NOT GLOB '*[^0-9a-f]*'),
    record_sha256 TEXT NOT NULL COLLATE BINARY
        CHECK(length(record_sha256) = 64 AND record_sha256 NOT GLOB '*[^0-9a-f]*'),
    CHECK(started_session <= ended_session AND query_cutoff <= recorded_at)
) STRICT;

CREATE TABLE historical_replay_dates (
    id INTEGER PRIMARY KEY CHECK(typeof(id) = 'integer' AND id > 0),
    replay_date_id TEXT NOT NULL COLLATE BINARY UNIQUE
        CHECK(length(replay_date_id) = 64 AND replay_date_id NOT GLOB '*[^0-9a-f]*'),
    replay_run_id TEXT NOT NULL COLLATE BINARY,
    session_date TEXT NOT NULL
        CHECK(length(session_date) = 10 AND session_date = strftime('%Y-%m-%d', session_date)),
    report_cutoff TEXT NOT NULL
        CHECK(length(report_cutoff) = 27 AND substr(report_cutoff, 1, 19) = strftime('%Y-%m-%dT%H:%M:%S', report_cutoff) AND substr(report_cutoff, 20, 1) = '.' AND substr(report_cutoff, 21, 6) NOT GLOB '*[^0-9]*' AND substr(report_cutoff, 27, 1) = 'Z'),
    expected_role_count INTEGER NOT NULL
        CHECK(typeof(expected_role_count) = 'integer' AND expected_role_count = 3),
    expected_evidence_count INTEGER NOT NULL
        CHECK(typeof(expected_evidence_count) = 'integer' AND expected_evidence_count = 3),
    case_digest TEXT NOT NULL COLLATE BINARY
        CHECK(length(case_digest) = 64 AND case_digest NOT GLOB '*[^0-9a-f]*'),
    domain_input_digest TEXT NOT NULL COLLATE BINARY
        CHECK(length(domain_input_digest) = 64 AND domain_input_digest NOT GLOB '*[^0-9a-f]*'),
    mechanics_digest TEXT NOT NULL COLLATE BINARY
        CHECK(length(mechanics_digest) = 64 AND mechanics_digest NOT GLOB '*[^0-9a-f]*'),
    completion_digest TEXT NOT NULL COLLATE BINARY
        CHECK(length(completion_digest) = 64 AND completion_digest NOT GLOB '*[^0-9a-f]*'),
    row_references_json TEXT NOT NULL CHECK(length(row_references_json) >= 2),
    completed_at TEXT NOT NULL
        CHECK(length(completed_at) = 27 AND substr(completed_at, 1, 19) = strftime('%Y-%m-%dT%H:%M:%S', completed_at) AND substr(completed_at, 20, 1) = '.' AND substr(completed_at, 21, 6) NOT GLOB '*[^0-9]*' AND substr(completed_at, 27, 1) = 'Z'),
    recorded_at TEXT NOT NULL
        CHECK(length(recorded_at) = 27 AND substr(recorded_at, 1, 19) = strftime('%Y-%m-%dT%H:%M:%S', recorded_at) AND substr(recorded_at, 20, 1) = '.' AND substr(recorded_at, 21, 6) NOT GLOB '*[^0-9]*' AND substr(recorded_at, 27, 1) = 'Z'),
    source_digest TEXT NOT NULL COLLATE BINARY
        CHECK(length(source_digest) = 64 AND source_digest NOT GLOB '*[^0-9a-f]*'),
    record_sha256 TEXT NOT NULL COLLATE BINARY
        CHECK(length(record_sha256) = 64 AND record_sha256 NOT GLOB '*[^0-9a-f]*'),
    CHECK(report_cutoff <= completed_at AND completed_at <= recorded_at),
    UNIQUE(replay_run_id, session_date),
    FOREIGN KEY(replay_run_id) REFERENCES historical_replay_runs(replay_run_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT
) STRICT;

CREATE TRIGGER historical_replay_dates_validate_run
BEFORE INSERT ON historical_replay_dates
WHEN NOT EXISTS (
    SELECT 1 FROM historical_replay_runs AS run
    WHERE run.replay_run_id = NEW.replay_run_id
      AND NEW.session_date BETWEEN run.started_session AND run.ended_session
      AND NEW.report_cutoff <= run.query_cutoff
      AND NEW.completed_at <= run.recorded_at
      AND NEW.recorded_at <= run.recorded_at
)
BEGIN SELECT RAISE(ABORT, 'historical replay date exceeds run boundary'); END;

CREATE TABLE historical_replay_evidence (
    id INTEGER PRIMARY KEY CHECK(typeof(id) = 'integer' AND id > 0),
    replay_evidence_id TEXT NOT NULL COLLATE BINARY UNIQUE
        CHECK(length(replay_evidence_id) = 64 AND replay_evidence_id NOT GLOB '*[^0-9a-f]*'),
    replay_date_id TEXT NOT NULL COLLATE BINARY,
    role TEXT NOT NULL CHECK(role IN ('UNIVERSE_MEMBERSHIP', 'EVENT_STATE', 'SOURCE_EVIDENCE')),
    evidence_ordinal INTEGER NOT NULL
        CHECK(typeof(evidence_ordinal) = 'integer' AND evidence_ordinal BETWEEN 1 AND 3),
    subject TEXT NOT NULL COLLATE BINARY CHECK(length(subject) > 0),
    source_kind TEXT NOT NULL
        CHECK(source_kind IN ('RELEASE_DOCUMENT', 'PROVIDER_FACT')),
    source_observation_id INTEGER NOT NULL
        CHECK(typeof(source_observation_id) = 'integer' AND source_observation_id > 0),
    external_source_observation_id TEXT NOT NULL COLLATE BINARY
        CHECK(length(external_source_observation_id) > 0),
    source_item_ordinal INTEGER NOT NULL
        CHECK(typeof(source_item_ordinal) = 'integer' AND source_item_ordinal > 0),
    source_item_path TEXT NOT NULL COLLATE BINARY
        CHECK(length(source_item_path) > 0 AND length(source_item_path) <= 512),
    payload_sha256 TEXT NOT NULL COLLATE BINARY
        CHECK(length(payload_sha256) = 64 AND payload_sha256 NOT GLOB '*[^0-9a-f]*'),
    content_sha256 TEXT NOT NULL COLLATE BINARY
        CHECK(length(content_sha256) = 64 AND content_sha256 NOT GLOB '*[^0-9a-f]*'),
    authority_digest TEXT NOT NULL COLLATE BINARY
        CHECK(length(authority_digest) = 64 AND authority_digest NOT GLOB '*[^0-9a-f]*'),
    effective_at TEXT NOT NULL
        CHECK(length(effective_at) = 27 AND substr(effective_at, 1, 19) = strftime('%Y-%m-%dT%H:%M:%S', effective_at) AND substr(effective_at, 20, 1) = '.' AND substr(effective_at, 21, 6) NOT GLOB '*[^0-9]*' AND substr(effective_at, 27, 1) = 'Z'),
    published_at TEXT NOT NULL
        CHECK(length(published_at) = 27 AND substr(published_at, 1, 19) = strftime('%Y-%m-%dT%H:%M:%S', published_at) AND substr(published_at, 20, 1) = '.' AND substr(published_at, 21, 6) NOT GLOB '*[^0-9]*' AND substr(published_at, 27, 1) = 'Z'),
    retrieved_at TEXT NOT NULL
        CHECK(length(retrieved_at) = 27 AND substr(retrieved_at, 1, 19) = strftime('%Y-%m-%dT%H:%M:%S', retrieved_at) AND substr(retrieved_at, 20, 1) = '.' AND substr(retrieved_at, 21, 6) NOT GLOB '*[^0-9]*' AND substr(retrieved_at, 27, 1) = 'Z'),
    recorded_at TEXT NOT NULL
        CHECK(length(recorded_at) = 27 AND substr(recorded_at, 1, 19) = strftime('%Y-%m-%dT%H:%M:%S', recorded_at) AND substr(recorded_at, 20, 1) = '.' AND substr(recorded_at, 21, 6) NOT GLOB '*[^0-9]*' AND substr(recorded_at, 27, 1) = 'Z'),
    source_digest TEXT NOT NULL COLLATE BINARY
        CHECK(length(source_digest) = 64 AND source_digest NOT GLOB '*[^0-9a-f]*'),
    record_sha256 TEXT NOT NULL COLLATE BINARY
        CHECK(length(record_sha256) = 64 AND record_sha256 NOT GLOB '*[^0-9a-f]*'),
    CHECK(effective_at <= retrieved_at AND published_at <= retrieved_at AND retrieved_at <= recorded_at),
    UNIQUE(replay_date_id, role),
    UNIQUE(replay_date_id, evidence_ordinal),
    FOREIGN KEY(replay_date_id) REFERENCES historical_replay_dates(replay_date_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
    FOREIGN KEY(source_observation_id) REFERENCES source_observations(id)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
    FOREIGN KEY(source_observation_id, payload_sha256)
        REFERENCES phase1_source_payloads(source_observation_id, payload_sha256)
        ON UPDATE RESTRICT ON DELETE RESTRICT
) STRICT;

CREATE TRIGGER historical_replay_evidence_validate_cutoff
BEFORE INSERT ON historical_replay_evidence
WHEN NOT EXISTS (
    SELECT 1
    FROM historical_replay_dates AS date_source
    JOIN historical_replay_runs AS run ON run.replay_run_id = date_source.replay_run_id
    JOIN source_observations AS source ON source.id = NEW.source_observation_id
    JOIN phase1_source_payloads AS payload
      ON payload.source_observation_id = source.id
     AND payload.payload_sha256 = NEW.payload_sha256
    WHERE date_source.replay_date_id = NEW.replay_date_id
      AND NEW.retrieved_at <= date_source.report_cutoff
      AND NEW.retrieved_at <= run.query_cutoff
      AND source.payload_sha256 = NEW.payload_sha256
      AND source.source_time = NEW.published_at
      AND source.retrieved_at = NEW.retrieved_at
      AND source.source_time <= NEW.retrieved_at
)
BEGIN SELECT RAISE(ABORT, 'historical replay evidence exceeds exact report cutoff'); END;

CREATE TABLE historical_replay_run_seals (
    id INTEGER PRIMARY KEY CHECK(typeof(id) = 'integer' AND id > 0),
    replay_run_id TEXT NOT NULL COLLATE BINARY UNIQUE,
    expected_date_count INTEGER NOT NULL
        CHECK(typeof(expected_date_count) = 'integer' AND expected_date_count > 0),
    actual_date_count INTEGER NOT NULL
        CHECK(typeof(actual_date_count) = 'integer' AND actual_date_count > 0),
    actual_evidence_count INTEGER NOT NULL
        CHECK(typeof(actual_evidence_count) = 'integer' AND actual_evidence_count >= 0),
    date_terminal_cursor INTEGER NOT NULL
        CHECK(typeof(date_terminal_cursor) = 'integer' AND date_terminal_cursor > 0),
    evidence_terminal_cursor INTEGER
        CHECK(evidence_terminal_cursor IS NULL OR (typeof(evidence_terminal_cursor) = 'integer' AND evidence_terminal_cursor > 0)),
    source_observation_highwater INTEGER NOT NULL
        CHECK(typeof(source_observation_highwater) = 'integer' AND source_observation_highwater >= 0),
    sealed_at TEXT NOT NULL
        CHECK(length(sealed_at) = 27 AND substr(sealed_at, 1, 19) = strftime('%Y-%m-%dT%H:%M:%S', sealed_at) AND substr(sealed_at, 20, 1) = '.' AND substr(sealed_at, 21, 6) NOT GLOB '*[^0-9]*' AND substr(sealed_at, 27, 1) = 'Z'),
    source_digest TEXT NOT NULL COLLATE BINARY
        CHECK(length(source_digest) = 64 AND source_digest NOT GLOB '*[^0-9a-f]*'),
    record_sha256 TEXT NOT NULL COLLATE BINARY
        CHECK(length(record_sha256) = 64 AND record_sha256 NOT GLOB '*[^0-9a-f]*'),
    CHECK((actual_evidence_count = 0 AND evidence_terminal_cursor IS NULL)
       OR (actual_evidence_count > 0 AND evidence_terminal_cursor IS NOT NULL)),
    FOREIGN KEY(replay_run_id) REFERENCES historical_replay_runs(replay_run_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT
) STRICT;

CREATE TRIGGER historical_replay_run_seals_validate_counts
BEFORE INSERT ON historical_replay_run_seals
WHEN NOT EXISTS (
    SELECT 1
    FROM historical_replay_runs AS run
    WHERE run.replay_run_id = NEW.replay_run_id
      AND run.expected_date_count = NEW.expected_date_count
      AND NEW.actual_date_count = NEW.expected_date_count
      AND NEW.actual_date_count = (
          SELECT count(*) FROM historical_replay_dates AS replay_date
          WHERE replay_date.replay_run_id = run.replay_run_id
      )
      AND NEW.date_terminal_cursor = (
          SELECT max(replay_date.id) FROM historical_replay_dates AS replay_date
          WHERE replay_date.replay_run_id = run.replay_run_id
      )
      AND NEW.actual_evidence_count = (
          SELECT count(*)
          FROM historical_replay_evidence AS evidence
          JOIN historical_replay_dates AS replay_date
            ON replay_date.replay_date_id = evidence.replay_date_id
          WHERE replay_date.replay_run_id = run.replay_run_id
      )
      AND COALESCE(NEW.evidence_terminal_cursor, 0) = COALESCE((
          SELECT max(evidence.id)
          FROM historical_replay_evidence AS evidence
          JOIN historical_replay_dates AS replay_date
            ON replay_date.replay_date_id = evidence.replay_date_id
          WHERE replay_date.replay_run_id = run.replay_run_id
      ), 0)
      AND NEW.source_observation_highwater = COALESCE((
          SELECT max(evidence.source_observation_id)
          FROM historical_replay_evidence AS evidence
          JOIN historical_replay_dates AS replay_date
            ON replay_date.replay_date_id = evidence.replay_date_id
          WHERE replay_date.replay_run_id = run.replay_run_id
      ), 0)
      AND run.recorded_at <= NEW.sealed_at
      AND COALESCE((
          SELECT max(replay_date.recorded_at)
          FROM historical_replay_dates AS replay_date
          WHERE replay_date.replay_run_id = run.replay_run_id
      ), run.recorded_at) <= NEW.sealed_at
      AND COALESCE((
          SELECT max(evidence.recorded_at)
          FROM historical_replay_evidence AS evidence
          JOIN historical_replay_dates AS replay_date
            ON replay_date.replay_date_id = evidence.replay_date_id
          WHERE replay_date.replay_run_id = run.replay_run_id
      ), run.recorded_at) <= NEW.sealed_at
)
BEGIN SELECT RAISE(ABORT, 'historical replay seal does not match exact stored children'); END;

CREATE TRIGGER historical_replay_dates_reject_after_seal
BEFORE INSERT ON historical_replay_dates
WHEN EXISTS (
    SELECT 1 FROM historical_replay_run_seals AS seal
    WHERE seal.replay_run_id = NEW.replay_run_id
)
BEGIN SELECT RAISE(ABORT, 'historical replay run is already sealed'); END;

CREATE TRIGGER historical_replay_evidence_reject_after_seal
BEFORE INSERT ON historical_replay_evidence
WHEN EXISTS (
    SELECT 1
    FROM historical_replay_dates AS replay_date
    JOIN historical_replay_run_seals AS seal
      ON seal.replay_run_id = replay_date.replay_run_id
    WHERE replay_date.replay_date_id = NEW.replay_date_id
)
BEGIN SELECT RAISE(ABORT, 'historical replay run is already sealed'); END;

CREATE TRIGGER phase2_windows_require_journal_phase2_writer BEFORE INSERT ON phase2_windows WHEN journal_phase2_write_allowed('phase2_windows', NEW.record_sha256, NEW.window_id, NEW.phase1_validation_window_id, NEW.promotion_source_digest, NEW.promotion_decision_digest, NEW.promotion_signal_ids_json, NEW.promotion_through_session, NEW.promotion_query_cutoff, NEW.start_execution_event_id, NEW.start_raw_message_id, NEW.started_session, NEW.started_at, NEW.received_at, NEW.starting_capital_micros, NEW.calendar_digest, NEW.source_digest) IS NOT 1 BEGIN SELECT RAISE(ABORT, 'phase2_windows requires the source-specific Journal writer'); END;
CREATE TRIGGER phase2_authorizations_require_journal_phase2_writer BEFORE INSERT ON phase2_authorizations WHEN journal_phase2_write_allowed('phase2_authorizations', NEW.record_sha256, NEW.authorization_id, NEW.window_id, NEW.signal_id, NEW.signal_source_digest, NEW.authorization_digest, NEW.authorized_at, NEW.received_at, NEW.source_digest) IS NOT 1 BEGIN SELECT RAISE(ABORT, 'phase2_authorizations requires the source-specific Journal writer'); END;
CREATE TRIGGER phase2_fee_schedules_require_journal_phase2_writer BEFORE INSERT ON phase2_fee_schedules WHEN journal_phase2_write_allowed('phase2_fee_schedules', NEW.record_sha256, NEW.schedule_id, NEW.effective_session, NEW.reviewed_at, NEW.currency, NEW.contract_multiplier, NEW.entry_fee_per_contract_micros, NEW.exit_fee_per_contract_micros, NEW.close_fee_reserve_per_contract_micros, NEW.source_sha256, NEW.schedule_digest, NEW.reviewed_bytes, NEW.archived_at, NEW.source_digest) IS NOT 1 BEGIN SELECT RAISE(ABORT, 'phase2_fee_schedules requires the source-specific Journal writer'); END;
CREATE TRIGGER phase2_option_chain_sets_require_journal_phase2_writer BEFORE INSERT ON phase2_option_chain_sets WHEN journal_phase2_write_allowed('phase2_option_chain_sets', NEW.record_sha256, NEW.chain_set_id, NEW.authorization_id, NEW.underlying, NEW.collection_name, NEW.requested_symbols_json, NEW.request_digest, NEW.manifest_digest, NEW.expected_page_count, NEW.expected_fact_count, NEW.review_candidate_fact_digests_json, NEW.expected_manual_review_count, NEW.terminal, NEW.query_cutoff, NEW.received_at, NEW.source_digest) IS NOT 1 BEGIN SELECT RAISE(ABORT, 'phase2_option_chain_sets requires the source-specific Journal writer'); END;
CREATE TRIGGER phase2_option_chain_pages_require_journal_phase2_writer BEFORE INSERT ON phase2_option_chain_pages WHEN journal_phase2_write_allowed('phase2_option_chain_pages', NEW.record_sha256, NEW.page_id, NEW.chain_set_id, NEW.page_ordinal, NEW.source_observation_id, NEW.external_source_observation_id, NEW.source_type, NEW.request_url, NEW.request_page_token, NEW.next_page_token, NEW.payload_sha256, NEW.source_time, NEW.retrieved_at, NEW.source_digest) IS NOT 1 BEGIN SELECT RAISE(ABORT, 'phase2_option_chain_pages requires the source-specific Journal writer'); END;
CREATE TRIGGER phase2_underlying_review_sets_require_journal_phase2_writer BEFORE INSERT ON phase2_underlying_review_sets WHEN journal_phase2_write_allowed('phase2_underlying_review_sets', NEW.record_sha256, NEW.review_set_id, NEW.window_id, NEW.entry_id, NEW.underlying, NEW.review_session, NEW.collection_name, NEW.timeframe, NEW.adjustment, NEW.feed, NEW.requested_symbols_json, NEW.request_digest, NEW.manifest_digest, NEW.expected_page_count, NEW.expected_fact_count, NEW.terminal, NEW.request_start, NEW.request_end, NEW.query_cutoff, NEW.received_at, NEW.source_digest) IS NOT 1 BEGIN SELECT RAISE(ABORT, 'phase2_underlying_review_sets requires the source-specific Journal writer'); END;
CREATE TRIGGER phase2_underlying_review_pages_require_journal_phase2_writer BEFORE INSERT ON phase2_underlying_review_pages WHEN journal_phase2_write_allowed('phase2_underlying_review_pages', NEW.record_sha256, NEW.page_id, NEW.review_set_id, NEW.page_ordinal, NEW.source_observation_id, NEW.external_source_observation_id, NEW.source_type, NEW.request_url, NEW.request_page_token, NEW.next_page_token, NEW.payload_sha256, NEW.source_time, NEW.retrieved_at, NEW.source_digest) IS NOT 1 BEGIN SELECT RAISE(ABORT, 'phase2_underlying_review_pages requires the source-specific Journal writer'); END;
CREATE TRIGGER phase2_underlying_review_facts_require_journal_phase2_writer BEFORE INSERT ON phase2_underlying_review_facts WHEN journal_phase2_write_allowed('phase2_underlying_review_facts', NEW.record_sha256, NEW.fact_id, NEW.review_set_id, NEW.source_observation_id, NEW.external_source_observation_id, NEW.fetch_page_ordinal, NEW.source_item_ordinal, NEW.source_item_path, NEW.payload_sha256, NEW.symbol, NEW.bar_at, NEW.open_micros, NEW.high_micros, NEW.low_micros, NEW.close_micros, NEW.volume, NEW.fact_digest, NEW.source_digest) IS NOT 1 BEGIN SELECT RAISE(ABORT, 'phase2_underlying_review_facts requires the source-specific Journal writer'); END;
CREATE TRIGGER phase2_contract_snapshots_require_journal_phase2_writer BEFORE INSERT ON phase2_contract_snapshots WHEN journal_phase2_write_allowed('phase2_contract_snapshots', NEW.record_sha256, NEW.snapshot_id, NEW.authorization_id, NEW.source_kind, NEW.chain_set_id, NEW.reviewed_provider_snapshot_id, NEW.occ_symbol, NEW.underlying, NEW.expiration, NEW.strike_micros, NEW.delta_micros, NEW.bid_micros, NEW.ask_micros, NEW.open_interest, NEW.daily_volume, NEW.source_observation_id, NEW.external_source_observation_id, NEW.fetch_page_ordinal, NEW.source_item_ordinal, NEW.source_item_path, NEW.payload_sha256, NEW.provider_fact_digest, NEW.execution_event_id, NEW.raw_message_id, NEW.action_ordinal, NEW.action_source_digest, NEW.observed_at, NEW.received_at, NEW.source_digest) IS NOT 1 BEGIN SELECT RAISE(ABORT, 'phase2_contract_snapshots requires the source-specific Journal writer'); END;
CREATE TRIGGER phase2_contract_selections_require_journal_phase2_writer BEFORE INSERT ON phase2_contract_selections WHEN journal_phase2_write_allowed('phase2_contract_selections', NEW.record_sha256, NEW.selection_id, NEW.authorization_id, NEW.provider_snapshot_id, NEW.manual_snapshot_id, NEW.manual_review_terminal_cursor, NEW.expected_manual_review_count, NEW.selection_session, NEW.quantity, NEW.fee_schedule_id, NEW.fee_schedule_digest, NEW.event_exclusion_source_digest, NEW.event_exclusion_authority_digest, NEW.event_exclusion_row_references_json, NEW.event_exclusion_highwaters_json, NEW.selection_portfolio_source_digest, NEW.selection_portfolio_query_cutoff, NEW.selection_portfolio_row_references_json, NEW.selection_portfolio_highwaters_json, NEW.selected_at, NEW.received_at, NEW.ranking_digest, NEW.source_digest) IS NOT 1 BEGIN SELECT RAISE(ABORT, 'phase2_contract_selections requires the source-specific Journal writer'); END;
CREATE TRIGGER phase2_entries_require_journal_phase2_writer BEFORE INSERT ON phase2_entries WHEN journal_phase2_write_allowed('phase2_entries', NEW.record_sha256, NEW.entry_id, NEW.window_id, NEW.selection_id, NEW.execution_event_id, NEW.raw_message_id, NEW.action_ordinal, NEW.action_source_digest, NEW.quantity, NEW.entry_ask_micros, NEW.entry_fee_micros, NEW.reserve_fee_micros, NEW.all_in_initial_risk_micros, NEW.fee_schedule_id, NEW.fee_schedule_digest, NEW.entered_at, NEW.received_at, NEW.source_digest) IS NOT 1 BEGIN SELECT RAISE(ABORT, 'phase2_entries requires the source-specific Journal writer'); END;
CREATE TRIGGER phase2_marks_require_journal_phase2_writer BEFORE INSERT ON phase2_marks WHEN journal_phase2_write_allowed('phase2_marks', NEW.record_sha256, NEW.mark_id, NEW.window_id, NEW.entry_id, NEW.session_date, NEW.source_kind, NEW.execution_event_id, NEW.raw_message_id, NEW.action_ordinal, NEW.action_source_digest, NEW.bid_micros, NEW.ask_micros, NEW.liquidation_value_micros, NEW.valid, NEW.failure_reason, NEW.calendar_digest, NEW.deadline_start_at, NEW.deadline_at, NEW.marked_at, NEW.received_at, NEW.source_digest) IS NOT 1 BEGIN SELECT RAISE(ABORT, 'phase2_marks requires the source-specific Journal writer'); END;
CREATE TRIGGER phase2_exit_reviews_require_journal_phase2_writer BEFORE INSERT ON phase2_exit_reviews WHEN journal_phase2_write_allowed('phase2_exit_reviews', NEW.record_sha256, NEW.exit_review_id, NEW.window_id, NEW.entry_id, NEW.underlying_review_set_id, NEW.decision_fact_id, NEW.review_session, NEW.decision_kind, NEW.decision_digest, NEW.holding_sessions, NEW.dte, NEW.query_cutoff, NEW.evaluated_at, NEW.received_at, NEW.source_digest) IS NOT 1 BEGIN SELECT RAISE(ABORT, 'phase2_exit_reviews requires the source-specific Journal writer'); END;
CREATE TRIGGER phase2_exits_require_journal_phase2_writer BEFORE INSERT ON phase2_exits WHEN journal_phase2_write_allowed('phase2_exits', NEW.record_sha256, NEW.exit_id, NEW.exit_review_id, NEW.window_id, NEW.entry_id, NEW.execution_event_id, NEW.raw_message_id, NEW.action_ordinal, NEW.action_source_digest, NEW.exit_reason, NEW.bid_micros, NEW.ask_micros, NEW.gross_proceeds_micros, NEW.net_pnl_micros, NEW.net_r_numerator_micros, NEW.initial_risk_micros, NEW.underlying_review_set_id, NEW.underlying_review_fact_id, NEW.underlying_source_observation_id, NEW.underlying_external_source_observation_id, NEW.underlying_fetch_page_ordinal, NEW.underlying_source_item_ordinal, NEW.underlying_source_item_path, NEW.underlying_payload_sha256, NEW.underlying_fact_digest, NEW.underlying_symbol, NEW.underlying_bar_at, NEW.underlying_open_micros, NEW.underlying_high_micros, NEW.underlying_low_micros, NEW.underlying_close_micros, NEW.underlying_volume, NEW.underlying_feed, NEW.underlying_adjustment, NEW.exit_decision_digest, NEW.settlement_available_session, NEW.settlement_calendar_digest, NEW.exited_at, NEW.received_at, NEW.source_digest) IS NOT 1 BEGIN SELECT RAISE(ABORT, 'phase2_exits requires the source-specific Journal writer'); END;
CREATE TRIGGER phase2_fee_records_require_journal_phase2_writer BEFORE INSERT ON phase2_fee_records WHEN journal_phase2_write_allowed('phase2_fee_records', NEW.record_sha256, NEW.fee_id, NEW.window_id, NEW.entry_id, NEW.exit_id, NEW.fee_kind, NEW.amount_micros, NEW.fee_schedule_id, NEW.fee_schedule_digest, NEW.execution_event_id, NEW.raw_message_id, NEW.action_ordinal, NEW.action_source_digest, NEW.recorded_at, NEW.source_digest) IS NOT 1 BEGIN SELECT RAISE(ABORT, 'phase2_fee_records requires the source-specific Journal writer'); END;
CREATE TRIGGER phase2_equity_points_require_journal_phase2_writer BEFORE INSERT ON phase2_equity_points WHEN journal_phase2_write_allowed('phase2_equity_points', NEW.record_sha256, NEW.point_id, NEW.window_id, NEW.entry_id, NEW.mark_id, NEW.exit_id, NEW.session_date, NEW.point_kind, NEW.cash_micros, NEW.position_value_micros, NEW.equity_micros, NEW.high_water_micros, NEW.drawdown_micros, NEW.at, NEW.received_at, NEW.source_digest) IS NOT 1 BEGIN SELECT RAISE(ABORT, 'phase2_equity_points requires the source-specific Journal writer'); END;
CREATE TRIGGER phase2_window_failures_require_journal_phase2_writer BEFORE INSERT ON phase2_window_failures WHEN journal_phase2_write_allowed('phase2_window_failures', NEW.record_sha256, NEW.failure_id, NEW.window_id, NEW.session_date, NEW.reason_code, NEW.mark_id, NEW.detected_at, NEW.received_at, NEW.source_digest) IS NOT 1 BEGIN SELECT RAISE(ABORT, 'phase2_window_failures requires the source-specific Journal writer'); END;
CREATE TRIGGER phase2_window_restarts_require_journal_phase2_writer BEFORE INSERT ON phase2_window_restarts WHEN journal_phase2_write_allowed('phase2_window_restarts', NEW.record_sha256, NEW.restart_id, NEW.failed_window_id, NEW.next_window_id, NEW.start_execution_event_id, NEW.start_raw_message_id, NEW.restarted_at, NEW.received_at, NEW.source_digest) IS NOT 1 BEGIN SELECT RAISE(ABORT, 'phase2_window_restarts requires the source-specific Journal writer'); END;
CREATE TRIGGER phase2_adherence_checks_require_journal_phase2_writer BEFORE INSERT ON phase2_adherence_checks WHEN journal_phase2_write_allowed('phase2_adherence_checks', NEW.record_sha256, NEW.check_id, NEW.window_id, NEW.entry_id, NEW.check_name, NEW.applicable, NEW.passed, NEW.hard_breach, NEW.evidence_row_references_json, NEW.evidence_highwaters_json, NEW.evidence_digest, NEW.authority_digest, NEW.evaluated_at, NEW.received_at, NEW.source_digest) IS NOT 1 BEGIN SELECT RAISE(ABORT, 'phase2_adherence_checks requires the source-specific Journal writer'); END;
CREATE TRIGGER phase2_gate_decisions_require_journal_phase2_writer BEFORE INSERT ON phase2_gate_decisions WHEN journal_phase2_write_allowed('phase2_gate_decisions', NEW.record_sha256, NEW.decision_id, NEW.window_id, NEW.query_cutoff, NEW.status, NEW.closed_trade_count, NEW.elapsed_days, NEW.mean_net_r_numerator_micros, NEW.mean_net_r_denominator_micros, NEW.adherence_passed_count, NEW.adherence_applicable_count, NEW.expected_adherence_count, NEW.adherence_terminal_cursor, NEW.adherence_source_highwater, NEW.max_drawdown_micros, NEW.hard_breach, NEW.failure_id, NEW.expected_exit_review_count, NEW.actual_exit_review_count, NEW.exit_review_terminal_cursor, NEW.window_row_references_json, NEW.window_highwaters_json, NEW.window_source_digest, NEW.evaluated_at, NEW.received_at, NEW.source_digest) IS NOT 1 BEGIN SELECT RAISE(ABORT, 'phase2_gate_decisions requires the source-specific Journal writer'); END;
CREATE TRIGGER historical_replay_runs_require_journal_phase2_writer BEFORE INSERT ON historical_replay_runs WHEN journal_phase2_write_allowed('historical_replay_runs', NEW.record_sha256, NEW.replay_run_id, NEW.tier, NEW.started_session, NEW.ended_session, NEW.query_cutoff, NEW.calendar_digest, NEW.policy_digest, NEW.expected_date_count, NEW.cursors_json, NEW.highwaters_json, NEW.row_references_json, NEW.recorded_at, NEW.source_digest) IS NOT 1 BEGIN SELECT RAISE(ABORT, 'historical_replay_runs requires the source-specific Journal writer'); END;
CREATE TRIGGER historical_replay_dates_require_journal_phase2_writer BEFORE INSERT ON historical_replay_dates WHEN journal_phase2_write_allowed('historical_replay_dates', NEW.record_sha256, NEW.replay_date_id, NEW.replay_run_id, NEW.session_date, NEW.report_cutoff, NEW.expected_role_count, NEW.expected_evidence_count, NEW.case_digest, NEW.domain_input_digest, NEW.mechanics_digest, NEW.completion_digest, NEW.row_references_json, NEW.completed_at, NEW.recorded_at, NEW.source_digest) IS NOT 1 BEGIN SELECT RAISE(ABORT, 'historical_replay_dates requires the source-specific Journal writer'); END;
CREATE TRIGGER historical_replay_evidence_require_journal_phase2_writer BEFORE INSERT ON historical_replay_evidence WHEN journal_phase2_write_allowed('historical_replay_evidence', NEW.record_sha256, NEW.replay_evidence_id, NEW.replay_date_id, NEW.role, NEW.evidence_ordinal, NEW.subject, NEW.source_kind, NEW.source_observation_id, NEW.external_source_observation_id, NEW.source_item_ordinal, NEW.source_item_path, NEW.payload_sha256, NEW.content_sha256, NEW.authority_digest, NEW.effective_at, NEW.published_at, NEW.retrieved_at, NEW.recorded_at, NEW.source_digest) IS NOT 1 BEGIN SELECT RAISE(ABORT, 'historical_replay_evidence requires the source-specific Journal writer'); END;
CREATE TRIGGER historical_replay_run_seals_require_journal_phase2_writer BEFORE INSERT ON historical_replay_run_seals WHEN journal_phase2_write_allowed('historical_replay_run_seals', NEW.record_sha256, NEW.replay_run_id, NEW.expected_date_count, NEW.actual_date_count, NEW.actual_evidence_count, NEW.date_terminal_cursor, NEW.evidence_terminal_cursor, NEW.source_observation_highwater, NEW.sealed_at, NEW.source_digest) IS NOT 1 BEGIN SELECT RAISE(ABORT, 'historical_replay_run_seals requires the source-specific Journal writer'); END;

CREATE INDEX phase2_authorizations_window_signal ON phase2_authorizations(window_id, signal_id);
CREATE INDEX phase2_option_chain_pages_set_ordinal ON phase2_option_chain_pages(chain_set_id, page_ordinal);
CREATE INDEX phase2_underlying_review_pages_set_ordinal ON phase2_underlying_review_pages(review_set_id, page_ordinal);
CREATE INDEX phase2_underlying_review_facts_set_time ON phase2_underlying_review_facts(review_set_id, bar_at, source_item_ordinal);
CREATE INDEX phase2_contract_snapshots_authorization ON phase2_contract_snapshots(authorization_id, source_kind, occ_symbol);
CREATE INDEX phase2_entries_window ON phase2_entries(window_id, entered_at);
CREATE INDEX phase2_marks_window_session ON phase2_marks(window_id, session_date);
CREATE INDEX phase2_exit_reviews_window_session ON phase2_exit_reviews(window_id, review_session);
CREATE INDEX phase2_equity_points_window_time ON phase2_equity_points(window_id, at);
CREATE UNIQUE INDEX phase2_equity_points_unique_start_per_window
ON phase2_equity_points(window_id) WHERE point_kind = 'START';
CREATE INDEX phase2_adherence_checks_window_entry ON phase2_adherence_checks(window_id, entry_id, check_name);
CREATE INDEX historical_replay_dates_run_session ON historical_replay_dates(replay_run_id, session_date);
CREATE INDEX historical_replay_evidence_date_role ON historical_replay_evidence(replay_date_id, role);

CREATE TRIGGER phase2_windows_no_update BEFORE UPDATE ON phase2_windows BEGIN SELECT RAISE(ABORT, 'phase2_windows is append-only'); END;
CREATE TRIGGER phase2_windows_no_delete BEFORE DELETE ON phase2_windows BEGIN SELECT RAISE(ABORT, 'phase2_windows is append-only'); END;
CREATE TRIGGER phase2_windows_no_conflicting_insert BEFORE INSERT ON phase2_windows WHEN EXISTS (SELECT 1 FROM phase2_windows WHERE id = NEW.id OR window_id = NEW.window_id COLLATE BINARY OR start_execution_event_id = NEW.start_execution_event_id) BEGIN SELECT RAISE(ABORT, 'phase2_windows rejects conflicting inserts'); END;
CREATE TRIGGER phase2_authorizations_no_update BEFORE UPDATE ON phase2_authorizations BEGIN SELECT RAISE(ABORT, 'phase2_authorizations is append-only'); END;
CREATE TRIGGER phase2_authorizations_no_delete BEFORE DELETE ON phase2_authorizations BEGIN SELECT RAISE(ABORT, 'phase2_authorizations is append-only'); END;
CREATE TRIGGER phase2_authorizations_no_conflicting_insert BEFORE INSERT ON phase2_authorizations WHEN EXISTS (SELECT 1 FROM phase2_authorizations WHERE id = NEW.id OR authorization_id = NEW.authorization_id COLLATE BINARY OR (window_id = NEW.window_id COLLATE BINARY AND signal_id = NEW.signal_id COLLATE BINARY)) BEGIN SELECT RAISE(ABORT, 'phase2_authorizations rejects conflicting inserts'); END;
CREATE TRIGGER phase2_fee_schedules_no_update BEFORE UPDATE ON phase2_fee_schedules BEGIN SELECT RAISE(ABORT, 'phase2_fee_schedules is append-only'); END;
CREATE TRIGGER phase2_fee_schedules_no_delete BEFORE DELETE ON phase2_fee_schedules BEGIN SELECT RAISE(ABORT, 'phase2_fee_schedules is append-only'); END;
CREATE TRIGGER phase2_fee_schedules_no_conflicting_insert BEFORE INSERT ON phase2_fee_schedules WHEN EXISTS (SELECT 1 FROM phase2_fee_schedules WHERE id = NEW.id OR schedule_id = NEW.schedule_id COLLATE BINARY OR schedule_digest = NEW.schedule_digest COLLATE BINARY) BEGIN SELECT RAISE(ABORT, 'phase2_fee_schedules rejects conflicting inserts'); END;
CREATE TRIGGER phase2_option_chain_sets_no_update BEFORE UPDATE ON phase2_option_chain_sets BEGIN SELECT RAISE(ABORT, 'phase2_option_chain_sets is append-only'); END;
CREATE TRIGGER phase2_option_chain_sets_no_delete BEFORE DELETE ON phase2_option_chain_sets BEGIN SELECT RAISE(ABORT, 'phase2_option_chain_sets is append-only'); END;
CREATE TRIGGER phase2_option_chain_sets_no_conflicting_insert BEFORE INSERT ON phase2_option_chain_sets WHEN EXISTS (SELECT 1 FROM phase2_option_chain_sets WHERE id = NEW.id OR chain_set_id = NEW.chain_set_id COLLATE BINARY OR manifest_digest = NEW.manifest_digest COLLATE BINARY) BEGIN SELECT RAISE(ABORT, 'phase2_option_chain_sets rejects conflicting inserts'); END;
CREATE TRIGGER phase2_option_chain_pages_no_update BEFORE UPDATE ON phase2_option_chain_pages BEGIN SELECT RAISE(ABORT, 'phase2_option_chain_pages is append-only'); END;
CREATE TRIGGER phase2_option_chain_pages_no_delete BEFORE DELETE ON phase2_option_chain_pages BEGIN SELECT RAISE(ABORT, 'phase2_option_chain_pages is append-only'); END;
CREATE TRIGGER phase2_option_chain_pages_no_conflicting_insert BEFORE INSERT ON phase2_option_chain_pages WHEN EXISTS (SELECT 1 FROM phase2_option_chain_pages WHERE id = NEW.id OR page_id = NEW.page_id COLLATE BINARY OR (chain_set_id = NEW.chain_set_id COLLATE BINARY AND (page_ordinal = NEW.page_ordinal OR source_observation_id = NEW.source_observation_id OR external_source_observation_id = NEW.external_source_observation_id COLLATE BINARY))) BEGIN SELECT RAISE(ABORT, 'phase2_option_chain_pages rejects conflicting inserts'); END;
CREATE TRIGGER phase2_underlying_review_sets_no_update BEFORE UPDATE ON phase2_underlying_review_sets BEGIN SELECT RAISE(ABORT, 'phase2_underlying_review_sets is append-only'); END;
CREATE TRIGGER phase2_underlying_review_sets_no_delete BEFORE DELETE ON phase2_underlying_review_sets BEGIN SELECT RAISE(ABORT, 'phase2_underlying_review_sets is append-only'); END;
CREATE TRIGGER phase2_underlying_review_sets_no_conflicting_insert BEFORE INSERT ON phase2_underlying_review_sets WHEN EXISTS (SELECT 1 FROM phase2_underlying_review_sets WHERE id = NEW.id OR review_set_id = NEW.review_set_id COLLATE BINARY OR manifest_digest = NEW.manifest_digest COLLATE BINARY) BEGIN SELECT RAISE(ABORT, 'phase2_underlying_review_sets rejects conflicting inserts'); END;
CREATE TRIGGER phase2_underlying_review_pages_no_update BEFORE UPDATE ON phase2_underlying_review_pages BEGIN SELECT RAISE(ABORT, 'phase2_underlying_review_pages is append-only'); END;
CREATE TRIGGER phase2_underlying_review_pages_no_delete BEFORE DELETE ON phase2_underlying_review_pages BEGIN SELECT RAISE(ABORT, 'phase2_underlying_review_pages is append-only'); END;
CREATE TRIGGER phase2_underlying_review_pages_no_conflicting_insert BEFORE INSERT ON phase2_underlying_review_pages WHEN EXISTS (SELECT 1 FROM phase2_underlying_review_pages WHERE id = NEW.id OR page_id = NEW.page_id COLLATE BINARY OR (review_set_id = NEW.review_set_id COLLATE BINARY AND (page_ordinal = NEW.page_ordinal OR source_observation_id = NEW.source_observation_id OR external_source_observation_id = NEW.external_source_observation_id COLLATE BINARY))) BEGIN SELECT RAISE(ABORT, 'phase2_underlying_review_pages rejects conflicting inserts'); END;
CREATE TRIGGER phase2_underlying_review_facts_no_update BEFORE UPDATE ON phase2_underlying_review_facts BEGIN SELECT RAISE(ABORT, 'phase2_underlying_review_facts is append-only'); END;
CREATE TRIGGER phase2_underlying_review_facts_no_delete BEFORE DELETE ON phase2_underlying_review_facts BEGIN SELECT RAISE(ABORT, 'phase2_underlying_review_facts is append-only'); END;
CREATE TRIGGER phase2_underlying_review_facts_no_conflicting_insert BEFORE INSERT ON phase2_underlying_review_facts WHEN EXISTS (SELECT 1 FROM phase2_underlying_review_facts WHERE id = NEW.id OR fact_id = NEW.fact_id COLLATE BINARY OR (review_set_id = NEW.review_set_id COLLATE BINARY AND fetch_page_ordinal = NEW.fetch_page_ordinal AND (source_item_ordinal = NEW.source_item_ordinal OR source_item_path = NEW.source_item_path COLLATE BINARY))) BEGIN SELECT RAISE(ABORT, 'phase2_underlying_review_facts rejects conflicting inserts'); END;
CREATE TRIGGER phase2_contract_snapshots_no_update BEFORE UPDATE ON phase2_contract_snapshots BEGIN SELECT RAISE(ABORT, 'phase2_contract_snapshots is append-only'); END;
CREATE TRIGGER phase2_contract_snapshots_no_delete BEFORE DELETE ON phase2_contract_snapshots BEGIN SELECT RAISE(ABORT, 'phase2_contract_snapshots is append-only'); END;
CREATE TRIGGER phase2_contract_snapshots_no_conflicting_insert BEFORE INSERT ON phase2_contract_snapshots WHEN EXISTS (SELECT 1 FROM phase2_contract_snapshots WHERE id = NEW.id OR snapshot_id = NEW.snapshot_id COLLATE BINARY) BEGIN SELECT RAISE(ABORT, 'phase2_contract_snapshots rejects conflicting inserts'); END;
CREATE TRIGGER phase2_contract_selections_no_update BEFORE UPDATE ON phase2_contract_selections BEGIN SELECT RAISE(ABORT, 'phase2_contract_selections is append-only'); END;
CREATE TRIGGER phase2_contract_selections_no_delete BEFORE DELETE ON phase2_contract_selections BEGIN SELECT RAISE(ABORT, 'phase2_contract_selections is append-only'); END;
CREATE TRIGGER phase2_contract_selections_no_conflicting_insert BEFORE INSERT ON phase2_contract_selections WHEN EXISTS (SELECT 1 FROM phase2_contract_selections WHERE id = NEW.id OR selection_id = NEW.selection_id COLLATE BINARY) BEGIN SELECT RAISE(ABORT, 'phase2_contract_selections rejects conflicting inserts'); END;
CREATE TRIGGER phase2_entries_no_update BEFORE UPDATE ON phase2_entries BEGIN SELECT RAISE(ABORT, 'phase2_entries is append-only'); END;
CREATE TRIGGER phase2_entries_no_delete BEFORE DELETE ON phase2_entries BEGIN SELECT RAISE(ABORT, 'phase2_entries is append-only'); END;
CREATE TRIGGER phase2_entries_no_conflicting_insert BEFORE INSERT ON phase2_entries WHEN EXISTS (SELECT 1 FROM phase2_entries WHERE id = NEW.id OR entry_id = NEW.entry_id COLLATE BINARY OR selection_id = NEW.selection_id COLLATE BINARY OR execution_event_id = NEW.execution_event_id) BEGIN SELECT RAISE(ABORT, 'phase2_entries rejects conflicting inserts'); END;
CREATE TRIGGER phase2_marks_no_update BEFORE UPDATE ON phase2_marks BEGIN SELECT RAISE(ABORT, 'phase2_marks is append-only'); END;
CREATE TRIGGER phase2_marks_no_delete BEFORE DELETE ON phase2_marks BEGIN SELECT RAISE(ABORT, 'phase2_marks is append-only'); END;
CREATE TRIGGER phase2_marks_no_conflicting_insert BEFORE INSERT ON phase2_marks WHEN EXISTS (SELECT 1 FROM phase2_marks WHERE id = NEW.id OR mark_id = NEW.mark_id COLLATE BINARY OR execution_event_id = NEW.execution_event_id OR (entry_id = NEW.entry_id COLLATE BINARY AND session_date = NEW.session_date)) BEGIN SELECT RAISE(ABORT, 'phase2_marks rejects conflicting inserts'); END;
CREATE TRIGGER phase2_exit_reviews_no_update BEFORE UPDATE ON phase2_exit_reviews BEGIN SELECT RAISE(ABORT, 'phase2_exit_reviews is append-only'); END;
CREATE TRIGGER phase2_exit_reviews_no_delete BEFORE DELETE ON phase2_exit_reviews BEGIN SELECT RAISE(ABORT, 'phase2_exit_reviews is append-only'); END;
CREATE TRIGGER phase2_exit_reviews_no_conflicting_insert BEFORE INSERT ON phase2_exit_reviews WHEN EXISTS (SELECT 1 FROM phase2_exit_reviews WHERE id = NEW.id OR exit_review_id = NEW.exit_review_id COLLATE BINARY OR underlying_review_set_id = NEW.underlying_review_set_id COLLATE BINARY OR (entry_id = NEW.entry_id COLLATE BINARY AND review_session = NEW.review_session)) BEGIN SELECT RAISE(ABORT, 'phase2_exit_reviews rejects conflicting inserts'); END;
CREATE TRIGGER phase2_exits_no_update BEFORE UPDATE ON phase2_exits BEGIN SELECT RAISE(ABORT, 'phase2_exits is append-only'); END;
CREATE TRIGGER phase2_exits_no_delete BEFORE DELETE ON phase2_exits BEGIN SELECT RAISE(ABORT, 'phase2_exits is append-only'); END;
CREATE TRIGGER phase2_exits_no_conflicting_insert BEFORE INSERT ON phase2_exits WHEN EXISTS (SELECT 1 FROM phase2_exits WHERE id = NEW.id OR exit_id = NEW.exit_id COLLATE BINARY OR entry_id = NEW.entry_id COLLATE BINARY OR execution_event_id = NEW.execution_event_id) BEGIN SELECT RAISE(ABORT, 'phase2_exits rejects conflicting inserts'); END;
CREATE TRIGGER phase2_fee_records_no_update BEFORE UPDATE ON phase2_fee_records BEGIN SELECT RAISE(ABORT, 'phase2_fee_records is append-only'); END;
CREATE TRIGGER phase2_fee_records_no_delete BEFORE DELETE ON phase2_fee_records BEGIN SELECT RAISE(ABORT, 'phase2_fee_records is append-only'); END;
CREATE TRIGGER phase2_fee_records_no_conflicting_insert BEFORE INSERT ON phase2_fee_records WHEN EXISTS (SELECT 1 FROM phase2_fee_records WHERE id = NEW.id OR fee_id = NEW.fee_id COLLATE BINARY OR (entry_id = NEW.entry_id COLLATE BINARY AND fee_kind = NEW.fee_kind)) BEGIN SELECT RAISE(ABORT, 'phase2_fee_records rejects conflicting inserts'); END;
CREATE TRIGGER phase2_equity_points_no_update BEFORE UPDATE ON phase2_equity_points BEGIN SELECT RAISE(ABORT, 'phase2_equity_points is append-only'); END;
CREATE TRIGGER phase2_equity_points_no_delete BEFORE DELETE ON phase2_equity_points BEGIN SELECT RAISE(ABORT, 'phase2_equity_points is append-only'); END;
CREATE TRIGGER phase2_equity_points_no_conflicting_insert BEFORE INSERT ON phase2_equity_points WHEN EXISTS (SELECT 1 FROM phase2_equity_points WHERE id = NEW.id OR point_id = NEW.point_id COLLATE BINARY) BEGIN SELECT RAISE(ABORT, 'phase2_equity_points rejects conflicting inserts'); END;
CREATE TRIGGER phase2_window_failures_no_update BEFORE UPDATE ON phase2_window_failures BEGIN SELECT RAISE(ABORT, 'phase2_window_failures is append-only'); END;
CREATE TRIGGER phase2_window_failures_no_delete BEFORE DELETE ON phase2_window_failures BEGIN SELECT RAISE(ABORT, 'phase2_window_failures is append-only'); END;
CREATE TRIGGER phase2_window_failures_no_conflicting_insert BEFORE INSERT ON phase2_window_failures WHEN EXISTS (SELECT 1 FROM phase2_window_failures WHERE id = NEW.id OR failure_id = NEW.failure_id COLLATE BINARY) BEGIN SELECT RAISE(ABORT, 'phase2_window_failures rejects conflicting inserts'); END;
CREATE TRIGGER phase2_window_restarts_no_update BEFORE UPDATE ON phase2_window_restarts BEGIN SELECT RAISE(ABORT, 'phase2_window_restarts is append-only'); END;
CREATE TRIGGER phase2_window_restarts_no_delete BEFORE DELETE ON phase2_window_restarts BEGIN SELECT RAISE(ABORT, 'phase2_window_restarts is append-only'); END;
CREATE TRIGGER phase2_window_restarts_no_conflicting_insert BEFORE INSERT ON phase2_window_restarts WHEN EXISTS (SELECT 1 FROM phase2_window_restarts WHERE id = NEW.id OR restart_id = NEW.restart_id COLLATE BINARY OR failed_window_id = NEW.failed_window_id COLLATE BINARY OR next_window_id = NEW.next_window_id COLLATE BINARY) BEGIN SELECT RAISE(ABORT, 'phase2_window_restarts rejects conflicting inserts'); END;
CREATE TRIGGER phase2_adherence_checks_no_update BEFORE UPDATE ON phase2_adherence_checks BEGIN SELECT RAISE(ABORT, 'phase2_adherence_checks is append-only'); END;
CREATE TRIGGER phase2_adherence_checks_no_delete BEFORE DELETE ON phase2_adherence_checks BEGIN SELECT RAISE(ABORT, 'phase2_adherence_checks is append-only'); END;
CREATE TRIGGER phase2_adherence_checks_no_conflicting_insert BEFORE INSERT ON phase2_adherence_checks WHEN EXISTS (SELECT 1 FROM phase2_adherence_checks WHERE id = NEW.id OR check_id = NEW.check_id COLLATE BINARY OR (entry_id = NEW.entry_id COLLATE BINARY AND check_name = NEW.check_name)) BEGIN SELECT RAISE(ABORT, 'phase2_adherence_checks rejects conflicting inserts'); END;
CREATE TRIGGER phase2_gate_decisions_no_update BEFORE UPDATE ON phase2_gate_decisions BEGIN SELECT RAISE(ABORT, 'phase2_gate_decisions is append-only'); END;
CREATE TRIGGER phase2_gate_decisions_no_delete BEFORE DELETE ON phase2_gate_decisions BEGIN SELECT RAISE(ABORT, 'phase2_gate_decisions is append-only'); END;
CREATE TRIGGER phase2_gate_decisions_no_conflicting_insert BEFORE INSERT ON phase2_gate_decisions WHEN EXISTS (SELECT 1 FROM phase2_gate_decisions WHERE id = NEW.id OR decision_id = NEW.decision_id COLLATE BINARY) BEGIN SELECT RAISE(ABORT, 'phase2_gate_decisions rejects conflicting inserts'); END;
CREATE TRIGGER historical_replay_runs_no_update BEFORE UPDATE ON historical_replay_runs BEGIN SELECT RAISE(ABORT, 'historical_replay_runs is append-only'); END;
CREATE TRIGGER historical_replay_runs_no_delete BEFORE DELETE ON historical_replay_runs BEGIN SELECT RAISE(ABORT, 'historical_replay_runs is append-only'); END;
CREATE TRIGGER historical_replay_runs_no_conflicting_insert BEFORE INSERT ON historical_replay_runs WHEN EXISTS (SELECT 1 FROM historical_replay_runs WHERE id = NEW.id OR replay_run_id = NEW.replay_run_id COLLATE BINARY) BEGIN SELECT RAISE(ABORT, 'historical_replay_runs rejects conflicting inserts'); END;
CREATE TRIGGER historical_replay_dates_no_update BEFORE UPDATE ON historical_replay_dates BEGIN SELECT RAISE(ABORT, 'historical_replay_dates is append-only'); END;
CREATE TRIGGER historical_replay_dates_no_delete BEFORE DELETE ON historical_replay_dates BEGIN SELECT RAISE(ABORT, 'historical_replay_dates is append-only'); END;
CREATE TRIGGER historical_replay_dates_no_conflicting_insert BEFORE INSERT ON historical_replay_dates WHEN EXISTS (SELECT 1 FROM historical_replay_dates WHERE id = NEW.id OR replay_date_id = NEW.replay_date_id COLLATE BINARY OR (replay_run_id = NEW.replay_run_id COLLATE BINARY AND session_date = NEW.session_date)) BEGIN SELECT RAISE(ABORT, 'historical_replay_dates rejects conflicting inserts'); END;
CREATE TRIGGER historical_replay_evidence_no_update BEFORE UPDATE ON historical_replay_evidence BEGIN SELECT RAISE(ABORT, 'historical_replay_evidence is append-only'); END;
CREATE TRIGGER historical_replay_evidence_no_delete BEFORE DELETE ON historical_replay_evidence BEGIN SELECT RAISE(ABORT, 'historical_replay_evidence is append-only'); END;
CREATE TRIGGER historical_replay_evidence_no_conflicting_insert BEFORE INSERT ON historical_replay_evidence WHEN EXISTS (SELECT 1 FROM historical_replay_evidence WHERE id = NEW.id OR replay_evidence_id = NEW.replay_evidence_id COLLATE BINARY OR (replay_date_id = NEW.replay_date_id COLLATE BINARY AND (role = NEW.role OR evidence_ordinal = NEW.evidence_ordinal))) BEGIN SELECT RAISE(ABORT, 'historical_replay_evidence rejects conflicting inserts'); END;
CREATE TRIGGER historical_replay_run_seals_no_update BEFORE UPDATE ON historical_replay_run_seals BEGIN SELECT RAISE(ABORT, 'historical_replay_run_seals is append-only'); END;
CREATE TRIGGER historical_replay_run_seals_no_delete BEFORE DELETE ON historical_replay_run_seals BEGIN SELECT RAISE(ABORT, 'historical_replay_run_seals is append-only'); END;
CREATE TRIGGER historical_replay_run_seals_no_conflicting_insert BEFORE INSERT ON historical_replay_run_seals WHEN EXISTS (SELECT 1 FROM historical_replay_run_seals WHERE id = NEW.id OR replay_run_id = NEW.replay_run_id COLLATE BINARY) BEGIN SELECT RAISE(ABORT, 'historical_replay_run_seals rejects conflicting inserts'); END;
