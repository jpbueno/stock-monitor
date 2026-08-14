CREATE TABLE schema_migrations (
    version INTEGER PRIMARY KEY
        CHECK(typeof(version) = 'integer' AND version > 0),
    name TEXT NOT NULL UNIQUE,
    sha256 TEXT NOT NULL
        CHECK(length(sha256) = 64 AND sha256 NOT GLOB '*[^0-9a-f]*'),
    schema_sha256 TEXT NOT NULL
        CHECK(length(schema_sha256) = 64 AND schema_sha256 NOT GLOB '*[^0-9a-f]*'),
    applied_at TEXT NOT NULL
        CHECK(
            length(applied_at) = 27
            AND substr(applied_at, 5, 1) = '-'
            AND substr(applied_at, 8, 1) = '-'
            AND substr(applied_at, 11, 1) = 'T'
            AND substr(applied_at, 14, 1) = ':'
            AND substr(applied_at, 17, 1) = ':'
            AND substr(applied_at, 20, 1) = '.'
            AND substr(applied_at, 27, 1) = 'Z'
            AND substr(applied_at, 1, 4) NOT GLOB '*[^0-9]*'
            AND substr(applied_at, 6, 2) NOT GLOB '*[^0-9]*'
            AND substr(applied_at, 9, 2) NOT GLOB '*[^0-9]*'
            AND substr(applied_at, 12, 2) NOT GLOB '*[^0-9]*'
            AND substr(applied_at, 15, 2) NOT GLOB '*[^0-9]*'
            AND substr(applied_at, 18, 2) NOT GLOB '*[^0-9]*'
            AND substr(applied_at, 21, 6) NOT GLOB '*[^0-9]*'
            AND CAST(substr(applied_at, 1, 4) AS INTEGER) BETWEEN 1 AND 9999
            AND CAST(substr(applied_at, 12, 2) AS INTEGER) BETWEEN 0 AND 23 AND strftime('%Y-%m-%dT%H:%M:%f', applied_at) IS NOT NULL
            AND date(substr(applied_at, 1, 10)) IS NOT NULL
            AND time(substr(applied_at, 12, 8)) IS NOT NULL
            AND date(substr(applied_at, 1, 10)) = substr(applied_at, 1, 10)
            AND time(substr(applied_at, 12, 8)) = substr(applied_at, 12, 8)
        )
) STRICT;

CREATE TRIGGER schema_migrations_no_update
BEFORE UPDATE ON schema_migrations
BEGIN
    SELECT RAISE(ABORT, 'schema_migrations is append-only');
END;

CREATE TRIGGER schema_migrations_no_delete
BEFORE DELETE ON schema_migrations
BEGIN
    SELECT RAISE(ABORT, 'schema_migrations is append-only');
END;

CREATE TABLE raw_messages (
    id INTEGER PRIMARY KEY
        CHECK(typeof(id) = 'integer' AND id > 0),
    message_id TEXT NOT NULL COLLATE BINARY UNIQUE,
    message_time TEXT NOT NULL
        CHECK(
            length(message_time) = 27
            AND substr(message_time, 5, 1) = '-'
            AND substr(message_time, 8, 1) = '-'
            AND substr(message_time, 11, 1) = 'T'
            AND substr(message_time, 14, 1) = ':'
            AND substr(message_time, 17, 1) = ':'
            AND substr(message_time, 20, 1) = '.'
            AND substr(message_time, 27, 1) = 'Z'
            AND substr(message_time, 1, 4) NOT GLOB '*[^0-9]*'
            AND substr(message_time, 6, 2) NOT GLOB '*[^0-9]*'
            AND substr(message_time, 9, 2) NOT GLOB '*[^0-9]*'
            AND substr(message_time, 12, 2) NOT GLOB '*[^0-9]*'
            AND substr(message_time, 15, 2) NOT GLOB '*[^0-9]*'
            AND substr(message_time, 18, 2) NOT GLOB '*[^0-9]*'
            AND substr(message_time, 21, 6) NOT GLOB '*[^0-9]*'
            AND CAST(substr(message_time, 1, 4) AS INTEGER) BETWEEN 1 AND 9999
            AND CAST(substr(message_time, 12, 2) AS INTEGER) BETWEEN 0 AND 23 AND strftime('%Y-%m-%dT%H:%M:%f', message_time) IS NOT NULL
            AND date(substr(message_time, 1, 10)) IS NOT NULL
            AND time(substr(message_time, 12, 8)) IS NOT NULL
            AND date(substr(message_time, 1, 10)) = substr(message_time, 1, 10)
            AND time(substr(message_time, 12, 8)) = substr(message_time, 12, 8)
        ),
    raw_text TEXT NOT NULL,
    raw_sha256 TEXT NOT NULL
        CHECK(length(raw_sha256) = 64 AND raw_sha256 NOT GLOB '*[^0-9a-f]*')
) STRICT;

CREATE TRIGGER raw_messages_no_update
BEFORE UPDATE ON raw_messages
BEGIN
    SELECT RAISE(ABORT, 'raw_messages is append-only');
END;

CREATE TRIGGER raw_messages_no_delete
BEFORE DELETE ON raw_messages
BEGIN
    SELECT RAISE(ABORT, 'raw_messages is append-only');
END;

CREATE TABLE execution_events (
    id INTEGER PRIMARY KEY
        CHECK(typeof(id) = 'integer' AND id > 0),
    event_id TEXT NOT NULL COLLATE BINARY UNIQUE,
    raw_message_id INTEGER NOT NULL
        CHECK(typeof(raw_message_id) = 'integer' AND raw_message_id > 0),
    action_ordinal INTEGER NOT NULL
        CHECK(typeof(action_ordinal) = 'integer' AND action_ordinal >= 0),
    idempotency_key TEXT NOT NULL COLLATE BINARY UNIQUE,
    signal_id TEXT,
    parsed_action TEXT NOT NULL,
    symbol TEXT,
    shares INTEGER
        CHECK(shares IS NULL OR (typeof(shares) = 'integer' AND shares > 0)),
    price_micros INTEGER
        CHECK(price_micros IS NULL OR (typeof(price_micros) = 'integer' AND price_micros > 0)),
    bid_micros INTEGER
        CHECK(bid_micros IS NULL OR (typeof(bid_micros) = 'integer' AND bid_micros > 0)),
    ask_micros INTEGER
        CHECK(ask_micros IS NULL OR (typeof(ask_micros) = 'integer' AND ask_micros > 0)),
    recommended_stop_micros INTEGER
        CHECK(recommended_stop_micros IS NULL OR (typeof(recommended_stop_micros) = 'integer' AND recommended_stop_micros > 0)),
    user_confirmed_stop_micros INTEGER
        CHECK(user_confirmed_stop_micros IS NULL OR (typeof(user_confirmed_stop_micros) = 'integer' AND user_confirmed_stop_micros > 0)),
    event_time TEXT NOT NULL
        CHECK(length(event_time) = 27 AND substr(event_time, 1, 23) = strftime('%Y-%m-%dT%H:%M:%f', event_time) AND substr(event_time, 24, 3) NOT GLOB '*[^0-9]*' AND substr(event_time, 27, 1) = 'Z' AND CAST(substr(event_time, 1, 4) AS INTEGER) BETWEEN 1 AND 9999 AND CAST(substr(event_time, 12, 2) AS INTEGER) BETWEEN 0 AND 23 AND strftime('%Y-%m-%dT%H:%M:%f', event_time) IS NOT NULL),
    message_time TEXT NOT NULL
        CHECK(length(message_time) = 27 AND substr(message_time, 1, 23) = strftime('%Y-%m-%dT%H:%M:%f', message_time) AND substr(message_time, 24, 3) NOT GLOB '*[^0-9]*' AND substr(message_time, 27, 1) = 'Z' AND CAST(substr(message_time, 1, 4) AS INTEGER) BETWEEN 1 AND 9999 AND CAST(substr(message_time, 12, 2) AS INTEGER) BETWEEN 0 AND 23 AND strftime('%Y-%m-%dT%H:%M:%f', message_time) IS NOT NULL),
    compliance_result TEXT NOT NULL,
    reconciliation_state TEXT NOT NULL,
    details_json TEXT NOT NULL,
    FOREIGN KEY(raw_message_id) REFERENCES raw_messages(id)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
    UNIQUE(raw_message_id, action_ordinal)
) STRICT;

CREATE TRIGGER execution_events_no_update
BEFORE UPDATE ON execution_events
BEGIN
    SELECT RAISE(ABORT, 'execution_events is append-only');
END;

CREATE TRIGGER execution_events_no_delete
BEFORE DELETE ON execution_events
BEGIN
    SELECT RAISE(ABORT, 'execution_events is append-only');
END;

CREATE TABLE source_observations (
    id INTEGER PRIMARY KEY CHECK(typeof(id) = 'integer' AND id > 0),
    observation_sha256 TEXT NOT NULL COLLATE BINARY UNIQUE
        CHECK(length(observation_sha256) = 64 AND observation_sha256 NOT GLOB '*[^0-9a-f]*'),
    payload_sha256 TEXT NOT NULL COLLATE BINARY
        CHECK(length(payload_sha256) = 64 AND payload_sha256 NOT GLOB '*[^0-9a-f]*'),
    source_uri TEXT NOT NULL,
    source_type TEXT NOT NULL,
    provider TEXT NOT NULL,
    feed TEXT,
    source_time TEXT NOT NULL
        CHECK(length(source_time) = 27 AND substr(source_time, 1, 23) = strftime('%Y-%m-%dT%H:%M:%f', source_time) AND substr(source_time, 24, 3) NOT GLOB '*[^0-9]*' AND substr(source_time, 27, 1) = 'Z' AND CAST(substr(source_time, 1, 4) AS INTEGER) BETWEEN 1 AND 9999 AND CAST(substr(source_time, 12, 2) AS INTEGER) BETWEEN 0 AND 23 AND strftime('%Y-%m-%dT%H:%M:%f', source_time) IS NOT NULL),
    retrieved_at TEXT NOT NULL
        CHECK(length(retrieved_at) = 27 AND substr(retrieved_at, 1, 23) = strftime('%Y-%m-%dT%H:%M:%f', retrieved_at) AND substr(retrieved_at, 24, 3) NOT GLOB '*[^0-9]*' AND substr(retrieved_at, 27, 1) = 'Z' AND CAST(substr(retrieved_at, 1, 4) AS INTEGER) BETWEEN 1 AND 9999 AND CAST(substr(retrieved_at, 12, 2) AS INTEGER) BETWEEN 0 AND 23 AND strftime('%Y-%m-%dT%H:%M:%f', retrieved_at) IS NOT NULL),
    provider_sequence INTEGER
        CHECK(provider_sequence IS NULL OR (typeof(provider_sequence) = 'integer' AND provider_sequence >= 0)),
    delay_seconds INTEGER
        CHECK(delay_seconds IS NULL OR (typeof(delay_seconds) = 'integer' AND delay_seconds >= 0)),
    health_result TEXT NOT NULL,
    details_json TEXT NOT NULL
) STRICT;

CREATE TABLE account_checks (
    id INTEGER PRIMARY KEY CHECK(typeof(id) = 'integer' AND id > 0),
    check_id TEXT NOT NULL COLLATE BINARY UNIQUE,
    raw_message_id INTEGER NOT NULL
        CHECK(typeof(raw_message_id) = 'integer' AND raw_message_id > 0),
    execution_event_id INTEGER NOT NULL UNIQUE
        CHECK(typeof(execution_event_id) = 'integer' AND execution_event_id > 0),
    settled_cash_micros INTEGER NOT NULL
        CHECK(typeof(settled_cash_micros) = 'integer' AND settled_cash_micros >= 0),
    pending_order_count INTEGER NOT NULL
        CHECK(typeof(pending_order_count) = 'integer' AND pending_order_count >= 0),
    unlogged_position_count INTEGER NOT NULL
        CHECK(typeof(unlogged_position_count) = 'integer' AND unlogged_position_count >= 0),
    confirmed_at TEXT NOT NULL
        CHECK(length(confirmed_at) = 27 AND substr(confirmed_at, 1, 23) = strftime('%Y-%m-%dT%H:%M:%f', confirmed_at) AND substr(confirmed_at, 24, 3) NOT GLOB '*[^0-9]*' AND substr(confirmed_at, 27, 1) = 'Z' AND CAST(substr(confirmed_at, 1, 4) AS INTEGER) BETWEEN 1 AND 9999 AND CAST(substr(confirmed_at, 12, 2) AS INTEGER) BETWEEN 0 AND 23 AND strftime('%Y-%m-%dT%H:%M:%f', confirmed_at) IS NOT NULL),
    reconciliation_result TEXT NOT NULL,
    details_json TEXT NOT NULL,
    FOREIGN KEY(raw_message_id) REFERENCES raw_messages(id)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
    FOREIGN KEY(execution_event_id) REFERENCES execution_events(id)
        ON UPDATE RESTRICT ON DELETE RESTRICT
) STRICT;

CREATE TABLE report_claims (
    id INTEGER PRIMARY KEY CHECK(typeof(id) = 'integer' AND id > 0),
    session_date TEXT NOT NULL
        CHECK(length(session_date) = 10 AND session_date = strftime('%Y-%m-%d', session_date) AND CAST(substr(session_date, 1, 4) AS INTEGER) BETWEEN 1 AND 9999 AND strftime('%Y-%m-%d', session_date) IS NOT NULL),
    report_kind TEXT NOT NULL
        CHECK(length(report_kind) > 0 AND report_kind = upper(report_kind) AND report_kind NOT GLOB '*[^A-Z0-9_]*' AND substr(report_kind, 1, 1) GLOB '[A-Z]'),
    claim_token TEXT NOT NULL COLLATE BINARY UNIQUE,
    status TEXT NOT NULL
        CHECK(status IN ('IN_PROGRESS', 'FINALIZED')),
    created_at TEXT NOT NULL
        CHECK(length(created_at) = 27 AND substr(created_at, 1, 23) = strftime('%Y-%m-%dT%H:%M:%f', created_at) AND substr(created_at, 24, 3) NOT GLOB '*[^0-9]*' AND substr(created_at, 27, 1) = 'Z' AND CAST(substr(created_at, 1, 4) AS INTEGER) BETWEEN 1 AND 9999 AND CAST(substr(created_at, 12, 2) AS INTEGER) BETWEEN 0 AND 23 AND strftime('%Y-%m-%dT%H:%M:%f', created_at) IS NOT NULL),
    lease_started_at TEXT NOT NULL
        CHECK(length(lease_started_at) = 27 AND substr(lease_started_at, 1, 23) = strftime('%Y-%m-%dT%H:%M:%f', lease_started_at) AND substr(lease_started_at, 24, 3) NOT GLOB '*[^0-9]*' AND substr(lease_started_at, 27, 1) = 'Z' AND CAST(substr(lease_started_at, 1, 4) AS INTEGER) BETWEEN 1 AND 9999 AND CAST(substr(lease_started_at, 12, 2) AS INTEGER) BETWEEN 0 AND 23 AND strftime('%Y-%m-%dT%H:%M:%f', lease_started_at) IS NOT NULL),
    lease_expires_at TEXT NOT NULL
        CHECK(length(lease_expires_at) = 27 AND substr(lease_expires_at, 1, 23) = strftime('%Y-%m-%dT%H:%M:%f', lease_expires_at) AND substr(lease_expires_at, 24, 3) NOT GLOB '*[^0-9]*' AND substr(lease_expires_at, 27, 1) = 'Z' AND CAST(substr(lease_expires_at, 1, 4) AS INTEGER) BETWEEN 1 AND 9999 AND CAST(substr(lease_expires_at, 12, 2) AS INTEGER) BETWEEN 0 AND 23 AND strftime('%Y-%m-%dT%H:%M:%f', lease_expires_at) IS NOT NULL),
    finalized_at TEXT
        CHECK(finalized_at IS NULL OR (length(finalized_at) = 27 AND substr(finalized_at, 1, 23) = strftime('%Y-%m-%dT%H:%M:%f', finalized_at) AND substr(finalized_at, 24, 3) NOT GLOB '*[^0-9]*' AND substr(finalized_at, 27, 1) = 'Z' AND CAST(substr(finalized_at, 1, 4) AS INTEGER) BETWEEN 1 AND 9999 AND CAST(substr(finalized_at, 12, 2) AS INTEGER) BETWEEN 0 AND 23 AND strftime('%Y-%m-%dT%H:%M:%f', finalized_at) IS NOT NULL)),
    report_id INTEGER
        CHECK(report_id IS NULL OR (typeof(report_id) = 'integer' AND report_id > 0)),
    CHECK(lease_expires_at > lease_started_at),
    CHECK(
        (status = 'IN_PROGRESS' AND finalized_at IS NULL AND report_id IS NULL)
        OR (status = 'FINALIZED' AND finalized_at IS NOT NULL AND report_id IS NOT NULL
            AND finalized_at >= lease_started_at
            AND finalized_at < lease_expires_at)
    ),
    UNIQUE(session_date, report_kind),
    FOREIGN KEY(report_id) REFERENCES reports(id)
        ON UPDATE RESTRICT ON DELETE RESTRICT
) STRICT;

CREATE TABLE reports (
    id INTEGER PRIMARY KEY CHECK(typeof(id) = 'integer' AND id > 0),
    report_id TEXT NOT NULL COLLATE BINARY UNIQUE,
    claim_id INTEGER NOT NULL UNIQUE
        CHECK(typeof(claim_id) = 'integer' AND claim_id > 0),
    session_date TEXT NOT NULL
        CHECK(length(session_date) = 10 AND session_date = strftime('%Y-%m-%d', session_date) AND CAST(substr(session_date, 1, 4) AS INTEGER) BETWEEN 1 AND 9999 AND strftime('%Y-%m-%d', session_date) IS NOT NULL),
    report_kind TEXT NOT NULL
        CHECK(length(report_kind) > 0 AND report_kind = upper(report_kind) AND report_kind NOT GLOB '*[^A-Z0-9_]*' AND substr(report_kind, 1, 1) GLOB '[A-Z]'),
    body_text TEXT NOT NULL,
    content_sha256 TEXT NOT NULL COLLATE BINARY
        CHECK(length(content_sha256) = 64 AND content_sha256 NOT GLOB '*[^0-9a-f]*'),
    state_sha256 TEXT NOT NULL COLLATE BINARY
        CHECK(length(state_sha256) = 64 AND state_sha256 NOT GLOB '*[^0-9a-f]*'),
    observation_set_sha256 TEXT NOT NULL COLLATE BINARY
        CHECK(length(observation_set_sha256) = 64 AND observation_set_sha256 NOT GLOB '*[^0-9a-f]*'),
    archive_relative_path TEXT NOT NULL COLLATE BINARY UNIQUE,
    created_at TEXT NOT NULL
        CHECK(length(created_at) = 27 AND substr(created_at, 1, 23) = strftime('%Y-%m-%dT%H:%M:%f', created_at) AND substr(created_at, 24, 3) NOT GLOB '*[^0-9]*' AND substr(created_at, 27, 1) = 'Z' AND CAST(substr(created_at, 1, 4) AS INTEGER) BETWEEN 1 AND 9999 AND CAST(substr(created_at, 12, 2) AS INTEGER) BETWEEN 0 AND 23 AND strftime('%Y-%m-%dT%H:%M:%f', created_at) IS NOT NULL),
    UNIQUE(session_date, report_kind),
    FOREIGN KEY(claim_id) REFERENCES report_claims(id)
        ON UPDATE RESTRICT ON DELETE RESTRICT
) STRICT;

CREATE TABLE report_observations (
    id INTEGER PRIMARY KEY CHECK(typeof(id) = 'integer' AND id > 0),
    report_id INTEGER NOT NULL
        CHECK(typeof(report_id) = 'integer' AND report_id > 0),
    source_observation_id INTEGER NOT NULL
        CHECK(typeof(source_observation_id) = 'integer' AND source_observation_id > 0),
    observation_ordinal INTEGER NOT NULL
        CHECK(typeof(observation_ordinal) = 'integer' AND observation_ordinal >= 0),
    UNIQUE(report_id, source_observation_id),
    UNIQUE(report_id, observation_ordinal),
    FOREIGN KEY(report_id) REFERENCES reports(id)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
    FOREIGN KEY(source_observation_id) REFERENCES source_observations(id)
        ON UPDATE RESTRICT ON DELETE RESTRICT
) STRICT;

CREATE TABLE outbox (
    id INTEGER PRIMARY KEY CHECK(typeof(id) = 'integer' AND id > 0),
    idempotency_key TEXT NOT NULL COLLATE BINARY UNIQUE,
    origin_report_id INTEGER
        CHECK(origin_report_id IS NULL OR (typeof(origin_report_id) = 'integer' AND origin_report_id > 0)),
    origin_execution_event_id INTEGER
        CHECK(origin_execution_event_id IS NULL OR (typeof(origin_execution_event_id) = 'integer' AND origin_execution_event_id > 0)),
    destination TEXT NOT NULL,
    payload_text TEXT NOT NULL,
    payload_sha256 TEXT NOT NULL COLLATE BINARY
        CHECK(length(payload_sha256) = 64 AND payload_sha256 NOT GLOB '*[^0-9a-f]*'),
    created_at TEXT NOT NULL
        CHECK(length(created_at) = 27 AND substr(created_at, 1, 23) = strftime('%Y-%m-%dT%H:%M:%f', created_at) AND substr(created_at, 24, 3) NOT GLOB '*[^0-9]*' AND substr(created_at, 27, 1) = 'Z' AND CAST(substr(created_at, 1, 4) AS INTEGER) BETWEEN 1 AND 9999 AND CAST(substr(created_at, 12, 2) AS INTEGER) BETWEEN 0 AND 23 AND strftime('%Y-%m-%dT%H:%M:%f', created_at) IS NOT NULL),
    CHECK((origin_report_id IS NOT NULL) != (origin_execution_event_id IS NOT NULL)),
    FOREIGN KEY(origin_report_id) REFERENCES reports(id)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
    FOREIGN KEY(origin_execution_event_id) REFERENCES execution_events(id)
        ON UPDATE RESTRICT ON DELETE RESTRICT
) STRICT;

CREATE TABLE outbox_delivery_attempts (
    id INTEGER PRIMARY KEY CHECK(typeof(id) = 'integer' AND id > 0),
    outbox_id INTEGER NOT NULL
        CHECK(typeof(outbox_id) = 'integer' AND outbox_id > 0),
    attempt_ordinal INTEGER NOT NULL
        CHECK(typeof(attempt_ordinal) = 'integer' AND attempt_ordinal > 0),
    attempted_at TEXT NOT NULL
        CHECK(length(attempted_at) = 27 AND substr(attempted_at, 1, 23) = strftime('%Y-%m-%dT%H:%M:%f', attempted_at) AND substr(attempted_at, 24, 3) NOT GLOB '*[^0-9]*' AND substr(attempted_at, 27, 1) = 'Z' AND CAST(substr(attempted_at, 1, 4) AS INTEGER) BETWEEN 1 AND 9999 AND CAST(substr(attempted_at, 12, 2) AS INTEGER) BETWEEN 0 AND 23 AND strftime('%Y-%m-%dT%H:%M:%f', attempted_at) IS NOT NULL),
    delivery_status TEXT NOT NULL
        CHECK(delivery_status IN ('FAILED', 'DELIVERED')),
    external_delivery_id TEXT,
    error_class TEXT,
    details_json TEXT NOT NULL,
    UNIQUE(outbox_id, attempt_ordinal),
    FOREIGN KEY(outbox_id) REFERENCES outbox(id)
        ON UPDATE RESTRICT ON DELETE RESTRICT
) STRICT;

CREATE UNIQUE INDEX outbox_one_delivered_attempt
ON outbox_delivery_attempts(outbox_id)
WHERE delivery_status = 'DELIVERED';

CREATE TABLE scheduled_runs (
    id INTEGER PRIMARY KEY CHECK(typeof(id) = 'integer' AND id > 0),
    run_key TEXT NOT NULL COLLATE BINARY UNIQUE,
    run_kind TEXT NOT NULL
        CHECK(length(run_kind) > 0 AND run_kind = upper(run_kind) AND run_kind NOT GLOB '*[^A-Z0-9_]*' AND substr(run_kind, 1, 1) GLOB '[A-Z]'),
    session_date TEXT NOT NULL
        CHECK(length(session_date) = 10 AND session_date = strftime('%Y-%m-%d', session_date) AND CAST(substr(session_date, 1, 4) AS INTEGER) BETWEEN 1 AND 9999 AND strftime('%Y-%m-%d', session_date) IS NOT NULL),
    intended_run_at TEXT NOT NULL
        CHECK(length(intended_run_at) = 27 AND substr(intended_run_at, 1, 23) = strftime('%Y-%m-%dT%H:%M:%f', intended_run_at) AND substr(intended_run_at, 24, 3) NOT GLOB '*[^0-9]*' AND substr(intended_run_at, 27, 1) = 'Z' AND CAST(substr(intended_run_at, 1, 4) AS INTEGER) BETWEEN 1 AND 9999 AND CAST(substr(intended_run_at, 12, 2) AS INTEGER) BETWEEN 0 AND 23 AND strftime('%Y-%m-%dT%H:%M:%f', intended_run_at) IS NOT NULL),
    started_at TEXT NOT NULL
        CHECK(length(started_at) = 27 AND substr(started_at, 1, 23) = strftime('%Y-%m-%dT%H:%M:%f', started_at) AND substr(started_at, 24, 3) NOT GLOB '*[^0-9]*' AND substr(started_at, 27, 1) = 'Z' AND CAST(substr(started_at, 1, 4) AS INTEGER) BETWEEN 1 AND 9999 AND CAST(substr(started_at, 12, 2) AS INTEGER) BETWEEN 0 AND 23 AND strftime('%Y-%m-%dT%H:%M:%f', started_at) IS NOT NULL),
    finished_at TEXT
        CHECK(finished_at IS NULL OR (length(finished_at) = 27 AND substr(finished_at, 1, 23) = strftime('%Y-%m-%dT%H:%M:%f', finished_at) AND substr(finished_at, 24, 3) NOT GLOB '*[^0-9]*' AND substr(finished_at, 27, 1) = 'Z' AND CAST(substr(finished_at, 1, 4) AS INTEGER) BETWEEN 1 AND 9999 AND CAST(substr(finished_at, 12, 2) AS INTEGER) BETWEEN 0 AND 23 AND strftime('%Y-%m-%dT%H:%M:%f', finished_at) IS NOT NULL)),
    market_session_decision TEXT,
    report_id INTEGER
        CHECK(report_id IS NULL OR (typeof(report_id) = 'integer' AND report_id > 0)),
    report_path TEXT,
    outcome TEXT,
    error_class TEXT,
    CHECK(finished_at IS NULL OR finished_at >= started_at),
    CHECK(
        (finished_at IS NULL AND market_session_decision IS NULL
            AND report_id IS NULL AND report_path IS NULL
            AND outcome IS NULL AND error_class IS NULL)
        OR (finished_at IS NOT NULL AND market_session_decision IS NOT NULL
            AND outcome IS NOT NULL
            AND ((report_id IS NULL AND report_path IS NULL)
                OR (report_id IS NOT NULL AND report_path IS NOT NULL)))
    ),
    FOREIGN KEY(report_id) REFERENCES reports(id)
        ON UPDATE RESTRICT ON DELETE RESTRICT
) STRICT;

CREATE TABLE ledger_postings (
    id INTEGER PRIMARY KEY CHECK(typeof(id) = 'integer' AND id > 0),
    posting_key TEXT NOT NULL COLLATE BINARY UNIQUE,
    ledger_name TEXT NOT NULL,
    account_name TEXT NOT NULL,
    entry_kind TEXT NOT NULL,
    execution_event_id INTEGER
        CHECK(execution_event_id IS NULL OR (typeof(execution_event_id) = 'integer' AND execution_event_id > 0)),
    account_check_id INTEGER
        CHECK(account_check_id IS NULL OR (typeof(account_check_id) = 'integer' AND account_check_id > 0)),
    symbol TEXT,
    amount_micros INTEGER NOT NULL
        CHECK(typeof(amount_micros) = 'integer'),
    shares_delta INTEGER
        CHECK(shares_delta IS NULL OR typeof(shares_delta) = 'integer'),
    unit_price_micros INTEGER
        CHECK(unit_price_micros IS NULL OR (typeof(unit_price_micros) = 'integer' AND unit_price_micros > 0)),
    occurred_at TEXT NOT NULL
        CHECK(length(occurred_at) = 27 AND substr(occurred_at, 1, 23) = strftime('%Y-%m-%dT%H:%M:%f', occurred_at) AND substr(occurred_at, 24, 3) NOT GLOB '*[^0-9]*' AND substr(occurred_at, 27, 1) = 'Z' AND CAST(substr(occurred_at, 1, 4) AS INTEGER) BETWEEN 1 AND 9999 AND CAST(substr(occurred_at, 12, 2) AS INTEGER) BETWEEN 0 AND 23 AND strftime('%Y-%m-%dT%H:%M:%f', occurred_at) IS NOT NULL),
    details_json TEXT NOT NULL,
    FOREIGN KEY(execution_event_id) REFERENCES execution_events(id)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
    FOREIGN KEY(account_check_id) REFERENCES account_checks(id)
        ON UPDATE RESTRICT ON DELETE RESTRICT
) STRICT;

CREATE TABLE actual_positions (
    id INTEGER PRIMARY KEY CHECK(typeof(id) = 'integer' AND id > 0),
    symbol TEXT NOT NULL COLLATE BINARY UNIQUE,
    shares INTEGER NOT NULL
        CHECK(typeof(shares) = 'integer' AND shares >= 0),
    cost_basis_micros INTEGER NOT NULL
        CHECK(typeof(cost_basis_micros) = 'integer' AND cost_basis_micros >= 0),
    recommended_stop_micros INTEGER
        CHECK(recommended_stop_micros IS NULL OR (typeof(recommended_stop_micros) = 'integer' AND recommended_stop_micros > 0)),
    user_confirmed_stop_micros INTEGER
        CHECK(user_confirmed_stop_micros IS NULL OR (typeof(user_confirmed_stop_micros) = 'integer' AND user_confirmed_stop_micros > 0)),
    target_micros INTEGER
        CHECK(target_micros IS NULL OR (typeof(target_micros) = 'integer' AND target_micros > 0)),
    last_execution_event_id INTEGER NOT NULL
        CHECK(typeof(last_execution_event_id) = 'integer' AND last_execution_event_id > 0),
    updated_at TEXT NOT NULL
        CHECK(length(updated_at) = 27 AND substr(updated_at, 1, 23) = strftime('%Y-%m-%dT%H:%M:%f', updated_at) AND substr(updated_at, 24, 3) NOT GLOB '*[^0-9]*' AND substr(updated_at, 27, 1) = 'Z' AND CAST(substr(updated_at, 1, 4) AS INTEGER) BETWEEN 1 AND 9999 AND CAST(substr(updated_at, 12, 2) AS INTEGER) BETWEEN 0 AND 23 AND strftime('%Y-%m-%dT%H:%M:%f', updated_at) IS NOT NULL),
    revision INTEGER NOT NULL
        CHECK(typeof(revision) = 'integer' AND revision > 0),
    CHECK(
        shares > 0
        OR (cost_basis_micros = 0
            AND recommended_stop_micros IS NULL
            AND user_confirmed_stop_micros IS NULL
            AND target_micros IS NULL)
    ),
    FOREIGN KEY(last_execution_event_id) REFERENCES execution_events(id)
        ON UPDATE RESTRICT ON DELETE RESTRICT
) STRICT;

CREATE TABLE actual_cash_projection (
    id INTEGER PRIMARY KEY CHECK(typeof(id) = 'integer' AND id = 1),
    estimated_settled_cash_micros INTEGER NOT NULL
        CHECK(typeof(estimated_settled_cash_micros) = 'integer' AND estimated_settled_cash_micros >= 0),
    user_confirmed_settled_cash_micros INTEGER
        CHECK(user_confirmed_settled_cash_micros IS NULL OR (typeof(user_confirmed_settled_cash_micros) = 'integer' AND user_confirmed_settled_cash_micros >= 0)),
    deployed_capital_micros INTEGER NOT NULL
        CHECK(typeof(deployed_capital_micros) = 'integer' AND deployed_capital_micros >= 0),
    open_planned_risk_micros INTEGER NOT NULL
        CHECK(typeof(open_planned_risk_micros) = 'integer' AND open_planned_risk_micros >= 0),
    consecutive_losses INTEGER NOT NULL
        CHECK(typeof(consecutive_losses) = 'integer' AND consecutive_losses >= 0),
    weekly_high_water_micros INTEGER NOT NULL
        CHECK(typeof(weekly_high_water_micros) = 'integer' AND weekly_high_water_micros >= 0),
    monthly_high_water_micros INTEGER NOT NULL
        CHECK(typeof(monthly_high_water_micros) = 'integer' AND monthly_high_water_micros >= 0),
    last_ledger_posting_id INTEGER NOT NULL
        CHECK(typeof(last_ledger_posting_id) = 'integer' AND last_ledger_posting_id > 0),
    updated_at TEXT NOT NULL
        CHECK(length(updated_at) = 27 AND substr(updated_at, 1, 23) = strftime('%Y-%m-%dT%H:%M:%f', updated_at) AND substr(updated_at, 24, 3) NOT GLOB '*[^0-9]*' AND substr(updated_at, 27, 1) = 'Z' AND CAST(substr(updated_at, 1, 4) AS INTEGER) BETWEEN 1 AND 9999 AND CAST(substr(updated_at, 12, 2) AS INTEGER) BETWEEN 0 AND 23 AND strftime('%Y-%m-%dT%H:%M:%f', updated_at) IS NOT NULL),
    revision INTEGER NOT NULL
        CHECK(typeof(revision) = 'integer' AND revision > 0),
    FOREIGN KEY(last_ledger_posting_id) REFERENCES ledger_postings(id)
        ON UPDATE RESTRICT ON DELETE RESTRICT
) STRICT;

CREATE TABLE reconciliation_projection (
    id INTEGER PRIMARY KEY CHECK(typeof(id) = 'integer' AND id = 1),
    reconciliation_required INTEGER NOT NULL
        CHECK(typeof(reconciliation_required) = 'integer' AND reconciliation_required IN (0, 1)),
    reason TEXT,
    last_execution_event_id INTEGER
        CHECK(last_execution_event_id IS NULL OR (typeof(last_execution_event_id) = 'integer' AND last_execution_event_id > 0)),
    updated_at TEXT NOT NULL
        CHECK(length(updated_at) = 27 AND substr(updated_at, 1, 23) = strftime('%Y-%m-%dT%H:%M:%f', updated_at) AND substr(updated_at, 24, 3) NOT GLOB '*[^0-9]*' AND substr(updated_at, 27, 1) = 'Z' AND CAST(substr(updated_at, 1, 4) AS INTEGER) BETWEEN 1 AND 9999 AND CAST(substr(updated_at, 12, 2) AS INTEGER) BETWEEN 0 AND 23 AND strftime('%Y-%m-%dT%H:%M:%f', updated_at) IS NOT NULL),
    revision INTEGER NOT NULL
        CHECK(typeof(revision) = 'integer' AND revision > 0),
    CHECK(
        (reconciliation_required = 1 AND reason IS NOT NULL)
        OR (reconciliation_required = 0 AND reason IS NULL)
    ),
    FOREIGN KEY(last_execution_event_id) REFERENCES execution_events(id)
        ON UPDATE RESTRICT ON DELETE RESTRICT
) STRICT;

-- SQLite's REPLACE algorithm can suppress implicit-delete triggers when a caller
-- disables recursive_triggers. Reject every conflicting immutable insert before
-- conflict resolution so INSERT OR REPLACE cannot rewrite history.
CREATE TRIGGER schema_migrations_no_conflicting_insert
BEFORE INSERT ON schema_migrations
WHEN EXISTS (
    SELECT 1 FROM schema_migrations
    WHERE version = NEW.version OR name = NEW.name
)
BEGIN
    SELECT RAISE(ABORT, 'schema_migrations rejects conflicting inserts');
END;

CREATE TRIGGER raw_messages_no_conflicting_insert
BEFORE INSERT ON raw_messages
WHEN EXISTS (
    SELECT 1 FROM raw_messages
    WHERE id = NEW.id OR message_id = NEW.message_id COLLATE BINARY
)
BEGIN
    SELECT RAISE(ABORT, 'raw_messages rejects conflicting inserts');
END;

CREATE TRIGGER source_observations_no_conflicting_insert
BEFORE INSERT ON source_observations
WHEN EXISTS (
    SELECT 1 FROM source_observations
    WHERE id = NEW.id
       OR observation_sha256 = NEW.observation_sha256 COLLATE BINARY
)
BEGIN
    SELECT RAISE(ABORT, 'source_observations rejects conflicting inserts');
END;

CREATE TRIGGER execution_events_no_conflicting_insert
BEFORE INSERT ON execution_events
WHEN EXISTS (
    SELECT 1 FROM execution_events
    WHERE id = NEW.id
       OR event_id = NEW.event_id COLLATE BINARY
       OR idempotency_key = NEW.idempotency_key COLLATE BINARY
       OR (raw_message_id = NEW.raw_message_id
           AND action_ordinal = NEW.action_ordinal)
)
BEGIN
    SELECT RAISE(ABORT, 'execution_events rejects conflicting inserts');
END;

CREATE TRIGGER account_checks_no_conflicting_insert
BEFORE INSERT ON account_checks
WHEN EXISTS (
    SELECT 1 FROM account_checks
    WHERE id = NEW.id
       OR check_id = NEW.check_id COLLATE BINARY
       OR execution_event_id = NEW.execution_event_id
)
BEGIN
    SELECT RAISE(ABORT, 'account_checks rejects conflicting inserts');
END;

CREATE TRIGGER account_checks_validate_execution_event
BEFORE INSERT ON account_checks
WHEN NOT EXISTS (
    SELECT 1 FROM execution_events
    WHERE id = NEW.execution_event_id
      AND raw_message_id = NEW.raw_message_id
      AND parsed_action = 'ACCOUNT_CHECK'
      AND event_time = NEW.confirmed_at
)
BEGIN
    SELECT RAISE(ABORT, 'account_checks requires its matching execution event');
END;

CREATE TRIGGER report_claims_no_conflicting_insert
BEFORE INSERT ON report_claims
WHEN EXISTS (
    SELECT 1 FROM report_claims
    WHERE id = NEW.id
       OR claim_token = NEW.claim_token COLLATE BINARY
       OR (session_date = NEW.session_date AND report_kind = NEW.report_kind)
)
BEGIN
    SELECT RAISE(ABORT, 'report_claims rejects conflicting inserts');
END;

CREATE TRIGGER reports_no_conflicting_insert
BEFORE INSERT ON reports
WHEN EXISTS (
    SELECT 1 FROM reports
    WHERE id = NEW.id
       OR report_id = NEW.report_id COLLATE BINARY
       OR claim_id = NEW.claim_id
       OR archive_relative_path = NEW.archive_relative_path COLLATE BINARY
       OR (session_date = NEW.session_date AND report_kind = NEW.report_kind)
)
BEGIN
    SELECT RAISE(ABORT, 'reports rejects conflicting inserts');
END;

CREATE TRIGGER reports_validate_claim
BEFORE INSERT ON reports
WHEN NOT EXISTS (
    SELECT 1 FROM report_claims
    WHERE id = NEW.claim_id
      AND session_date = NEW.session_date
      AND report_kind = NEW.report_kind
      AND status = 'IN_PROGRESS'
      AND NEW.created_at >= lease_started_at
      AND NEW.created_at < lease_expires_at
)
BEGIN
    SELECT RAISE(ABORT, 'reports requires its matching active claim');
END;

CREATE TRIGGER report_observations_no_conflicting_insert
BEFORE INSERT ON report_observations
WHEN EXISTS (
    SELECT 1 FROM report_observations
    WHERE id = NEW.id
       OR (report_id = NEW.report_id
           AND source_observation_id = NEW.source_observation_id)
       OR (report_id = NEW.report_id
           AND observation_ordinal = NEW.observation_ordinal)
)
BEGIN
    SELECT RAISE(ABORT, 'report_observations rejects conflicting inserts');
END;

CREATE TRIGGER report_observations_validate_time
BEFORE INSERT ON report_observations
WHEN NOT EXISTS (
    SELECT 1
    FROM reports AS report
    JOIN source_observations AS observation
      ON observation.id = NEW.source_observation_id
    WHERE report.id = NEW.report_id
      AND observation.retrieved_at <= report.created_at
)
BEGIN
    SELECT RAISE(ABORT, 'report observations cannot use future evidence');
END;

CREATE TRIGGER outbox_no_conflicting_insert
BEFORE INSERT ON outbox
WHEN EXISTS (
    SELECT 1 FROM outbox
    WHERE id = NEW.id
       OR idempotency_key = NEW.idempotency_key COLLATE BINARY
)
BEGIN
    SELECT RAISE(ABORT, 'outbox rejects conflicting inserts');
END;

CREATE TRIGGER outbox_delivery_attempts_no_conflicting_insert
BEFORE INSERT ON outbox_delivery_attempts
WHEN EXISTS (
    SELECT 1 FROM outbox_delivery_attempts
    WHERE id = NEW.id
       OR (outbox_id = NEW.outbox_id
           AND attempt_ordinal = NEW.attempt_ordinal)
       OR (NEW.delivery_status = 'DELIVERED'
           AND outbox_id = NEW.outbox_id
           AND delivery_status = 'DELIVERED')
)
BEGIN
    SELECT RAISE(ABORT, 'outbox_delivery_attempts rejects conflicting inserts');
END;

CREATE TRIGGER ledger_postings_no_conflicting_insert
BEFORE INSERT ON ledger_postings
WHEN EXISTS (
    SELECT 1 FROM ledger_postings
    WHERE id = NEW.id OR posting_key = NEW.posting_key COLLATE BINARY
)
BEGIN
    SELECT RAISE(ABORT, 'ledger_postings rejects conflicting inserts');
END;

CREATE TRIGGER scheduled_runs_no_conflicting_insert
BEFORE INSERT ON scheduled_runs
WHEN EXISTS (
    SELECT 1 FROM scheduled_runs
    WHERE id = NEW.id OR run_key = NEW.run_key COLLATE BINARY
)
BEGIN
    SELECT RAISE(ABORT, 'scheduled_runs rejects conflicting inserts');
END;

CREATE TRIGGER source_observations_no_update
BEFORE UPDATE ON source_observations
BEGIN
    SELECT RAISE(ABORT, 'source_observations is append-only');
END;

CREATE TRIGGER source_observations_no_delete
BEFORE DELETE ON source_observations
BEGIN
    SELECT RAISE(ABORT, 'source_observations is append-only');
END;

CREATE TRIGGER account_checks_no_update
BEFORE UPDATE ON account_checks
BEGIN
    SELECT RAISE(ABORT, 'account_checks is append-only');
END;

CREATE TRIGGER account_checks_no_delete
BEFORE DELETE ON account_checks
BEGIN
    SELECT RAISE(ABORT, 'account_checks is append-only');
END;

CREATE TRIGGER reports_no_update
BEFORE UPDATE ON reports
BEGIN
    SELECT RAISE(ABORT, 'reports is append-only');
END;

CREATE TRIGGER reports_no_delete
BEFORE DELETE ON reports
BEGIN
    SELECT RAISE(ABORT, 'reports is append-only');
END;

CREATE TRIGGER report_observations_no_update
BEFORE UPDATE ON report_observations
BEGIN
    SELECT RAISE(ABORT, 'report_observations is append-only');
END;

CREATE TRIGGER report_observations_no_delete
BEFORE DELETE ON report_observations
BEGIN
    SELECT RAISE(ABORT, 'report_observations is append-only');
END;

CREATE TRIGGER outbox_no_update
BEFORE UPDATE ON outbox
BEGIN
    SELECT RAISE(ABORT, 'outbox is append-only');
END;

CREATE TRIGGER outbox_no_delete
BEFORE DELETE ON outbox
BEGIN
    SELECT RAISE(ABORT, 'outbox is append-only');
END;

CREATE TRIGGER outbox_delivery_attempts_no_update
BEFORE UPDATE ON outbox_delivery_attempts
BEGIN
    SELECT RAISE(ABORT, 'outbox_delivery_attempts is append-only');
END;

CREATE TRIGGER outbox_delivery_attempts_no_delete
BEFORE DELETE ON outbox_delivery_attempts
BEGIN
    SELECT RAISE(ABORT, 'outbox_delivery_attempts is append-only');
END;

CREATE TRIGGER ledger_postings_no_update
BEFORE UPDATE ON ledger_postings
BEGIN
    SELECT RAISE(ABORT, 'ledger_postings is append-only');
END;

CREATE TRIGGER ledger_postings_no_delete
BEFORE DELETE ON ledger_postings
BEGIN
    SELECT RAISE(ABORT, 'ledger_postings is append-only');
END;

CREATE TRIGGER report_claims_no_delete
BEFORE DELETE ON report_claims
BEGIN
    SELECT RAISE(ABORT, 'report_claims cannot be deleted');
END;

CREATE TRIGGER report_claims_controlled_update
BEFORE UPDATE ON report_claims
WHEN NOT (
    OLD.status = 'IN_PROGRESS'
    AND NEW.id = OLD.id
    AND NEW.session_date = OLD.session_date
    AND NEW.report_kind = OLD.report_kind
    AND NEW.created_at = OLD.created_at
    AND (
        (
            NEW.status = 'IN_PROGRESS'
            AND NEW.finalized_at IS NULL
            AND NEW.report_id IS NULL
            AND (
                (
                    NEW.claim_token = OLD.claim_token
                    AND NEW.lease_started_at = OLD.lease_started_at
                    AND NEW.lease_expires_at = OLD.lease_expires_at
                )
                OR (
                    NEW.claim_token != OLD.claim_token
                    AND NEW.lease_started_at >= OLD.lease_expires_at
                    AND NEW.lease_expires_at > NEW.lease_started_at
                )
            )
        )
        OR (
            NEW.status = 'FINALIZED'
            AND NEW.claim_token = OLD.claim_token
            AND NEW.lease_started_at = OLD.lease_started_at
            AND NEW.lease_expires_at = OLD.lease_expires_at
            AND NEW.finalized_at IS NOT NULL
            AND NEW.finalized_at >= NEW.lease_started_at
            AND NEW.finalized_at < NEW.lease_expires_at
            AND NEW.report_id IS NOT NULL
            AND EXISTS (
                SELECT 1 FROM reports
                WHERE id = NEW.report_id
                  AND claim_id = OLD.id
                  AND session_date = OLD.session_date
                  AND report_kind = OLD.report_kind
                  AND created_at = NEW.finalized_at
            )
            AND EXISTS (
                SELECT 1 FROM outbox
                WHERE origin_report_id = NEW.report_id
                  AND origin_execution_event_id IS NULL
                  AND created_at = NEW.finalized_at
            )
        )
    )
)
BEGIN
    SELECT RAISE(ABORT, 'report_claims permits only lease recovery or finalization');
END;

CREATE TRIGGER scheduled_runs_no_delete
BEFORE DELETE ON scheduled_runs
BEGIN
    SELECT RAISE(ABORT, 'scheduled_runs cannot be deleted');
END;

CREATE TRIGGER scheduled_runs_controlled_completion
BEFORE UPDATE ON scheduled_runs
WHEN NOT (
    OLD.finished_at IS NULL
    AND NEW.id = OLD.id
    AND NEW.run_key = OLD.run_key
    AND NEW.run_kind = OLD.run_kind
    AND NEW.session_date = OLD.session_date
    AND NEW.intended_run_at = OLD.intended_run_at
    AND NEW.started_at = OLD.started_at
    AND NEW.finished_at IS NOT NULL
    AND NEW.finished_at >= OLD.started_at
    AND NEW.market_session_decision IS NOT NULL
    AND NEW.outcome IS NOT NULL
    AND (
        (NEW.report_id IS NULL AND NEW.report_path IS NULL)
        OR (
            NEW.report_id IS NOT NULL
            AND NEW.report_path IS NOT NULL
            AND EXISTS (
                SELECT 1
                FROM reports AS report
                JOIN report_claims AS claim
                  ON claim.id = report.claim_id
                WHERE report.id = NEW.report_id
                  AND report.session_date = OLD.session_date
                  AND report.report_kind = OLD.run_kind
                  AND report.archive_relative_path = NEW.report_path
                  AND claim.status = 'FINALIZED'
                  AND claim.report_id = report.id
            )
        )
    )
)
BEGIN
    SELECT RAISE(ABORT, 'scheduled_runs permits only one completion');
END;

CREATE TRIGGER actual_positions_guard_insert
BEFORE INSERT ON actual_positions
WHEN journal_projection_write_allowed() != 1
BEGIN
    SELECT RAISE(ABORT, 'actual_positions requires journal transaction API');
END;

CREATE TRIGGER actual_positions_guard_update
BEFORE UPDATE ON actual_positions
WHEN journal_projection_write_allowed() != 1
BEGIN
    SELECT RAISE(ABORT, 'actual_positions requires journal transaction API');
END;

CREATE TRIGGER actual_positions_guard_delete
BEFORE DELETE ON actual_positions
WHEN journal_projection_write_allowed() != 1
BEGIN
    SELECT RAISE(ABORT, 'actual_positions requires journal transaction API');
END;

CREATE TRIGGER actual_cash_projection_guard_insert
BEFORE INSERT ON actual_cash_projection
WHEN journal_projection_write_allowed() != 1
BEGIN
    SELECT RAISE(ABORT, 'actual_cash_projection requires journal transaction API');
END;

CREATE TRIGGER actual_cash_projection_guard_update
BEFORE UPDATE ON actual_cash_projection
WHEN journal_projection_write_allowed() != 1
BEGIN
    SELECT RAISE(ABORT, 'actual_cash_projection requires journal transaction API');
END;

CREATE TRIGGER actual_cash_projection_guard_delete
BEFORE DELETE ON actual_cash_projection
WHEN journal_projection_write_allowed() != 1
BEGIN
    SELECT RAISE(ABORT, 'actual_cash_projection requires journal transaction API');
END;

CREATE TRIGGER reconciliation_projection_guard_insert
BEFORE INSERT ON reconciliation_projection
WHEN journal_projection_write_allowed() != 1
BEGIN
    SELECT RAISE(ABORT, 'reconciliation_projection requires journal transaction API');
END;

CREATE TRIGGER reconciliation_projection_guard_update
BEFORE UPDATE ON reconciliation_projection
WHEN journal_projection_write_allowed() != 1
BEGIN
    SELECT RAISE(ABORT, 'reconciliation_projection requires journal transaction API');
END;

CREATE TRIGGER reconciliation_projection_guard_delete
BEFORE DELETE ON reconciliation_projection
WHEN journal_projection_write_allowed() != 1
BEGIN
    SELECT RAISE(ABORT, 'reconciliation_projection requires journal transaction API');
END;
