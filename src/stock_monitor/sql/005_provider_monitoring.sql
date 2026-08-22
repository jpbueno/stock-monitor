CREATE TABLE canonical_report_contexts (
    id INTEGER PRIMARY KEY
        CHECK(typeof(id) = 'integer' AND id > 0),
    report_id INTEGER NOT NULL UNIQUE
        CHECK(typeof(report_id) = 'integer' AND report_id > 0),
    workflow_kind TEXT NOT NULL COLLATE BINARY
        CHECK(workflow_kind IN ('PREMARKET', 'CLOSE')),
    economic_at TEXT NOT NULL COLLATE BINARY
        CHECK(
            length(economic_at) = 27
            AND substr(economic_at, 5, 1) = '-'
            AND substr(economic_at, 8, 1) = '-'
            AND substr(economic_at, 11, 1) = 'T'
            AND substr(economic_at, 14, 1) = ':'
            AND substr(economic_at, 17, 1) = ':'
            AND substr(economic_at, 20, 1) = '.'
            AND substr(economic_at, 27, 1) = 'Z'
            AND substr(economic_at, 1, 4) NOT GLOB '*[^0-9]*'
            AND substr(economic_at, 6, 2) NOT GLOB '*[^0-9]*'
            AND substr(economic_at, 9, 2) NOT GLOB '*[^0-9]*'
            AND substr(economic_at, 12, 2) NOT GLOB '*[^0-9]*'
            AND substr(economic_at, 15, 2) NOT GLOB '*[^0-9]*'
            AND substr(economic_at, 18, 2) NOT GLOB '*[^0-9]*'
            AND substr(economic_at, 21, 6) NOT GLOB '*[^0-9]*'
            AND CAST(substr(economic_at, 1, 4) AS INTEGER) BETWEEN 1 AND 9999
            AND CAST(substr(economic_at, 12, 2) AS INTEGER) BETWEEN 0 AND 23
            AND strftime('%Y-%m-%dT%H:%M:%f', economic_at) IS NOT NULL
            AND date(substr(economic_at, 1, 10)) = substr(economic_at, 1, 10)
            AND time(substr(economic_at, 12, 8)) = substr(economic_at, 12, 8)
        ),
    retrieved_at TEXT NOT NULL COLLATE BINARY
        CHECK(
            length(retrieved_at) = 27
            AND substr(retrieved_at, 5, 1) = '-'
            AND substr(retrieved_at, 8, 1) = '-'
            AND substr(retrieved_at, 11, 1) = 'T'
            AND substr(retrieved_at, 14, 1) = ':'
            AND substr(retrieved_at, 17, 1) = ':'
            AND substr(retrieved_at, 20, 1) = '.'
            AND substr(retrieved_at, 27, 1) = 'Z'
            AND substr(retrieved_at, 1, 4) NOT GLOB '*[^0-9]*'
            AND substr(retrieved_at, 6, 2) NOT GLOB '*[^0-9]*'
            AND substr(retrieved_at, 9, 2) NOT GLOB '*[^0-9]*'
            AND substr(retrieved_at, 12, 2) NOT GLOB '*[^0-9]*'
            AND substr(retrieved_at, 15, 2) NOT GLOB '*[^0-9]*'
            AND substr(retrieved_at, 18, 2) NOT GLOB '*[^0-9]*'
            AND substr(retrieved_at, 21, 6) NOT GLOB '*[^0-9]*'
            AND CAST(substr(retrieved_at, 1, 4) AS INTEGER) BETWEEN 1 AND 9999
            AND CAST(substr(retrieved_at, 12, 2) AS INTEGER) BETWEEN 0 AND 23
            AND strftime('%Y-%m-%dT%H:%M:%f', retrieved_at) IS NOT NULL
            AND date(substr(retrieved_at, 1, 10)) = substr(retrieved_at, 1, 10)
            AND time(substr(retrieved_at, 12, 8)) = substr(retrieved_at, 12, 8)
        ),
    material_digest TEXT NOT NULL COLLATE BINARY
        CHECK(length(material_digest) = 64 AND material_digest NOT GLOB '*[^0-9a-f]*'),
    source_digest TEXT NOT NULL COLLATE BINARY
        CHECK(length(source_digest) = 64 AND source_digest NOT GLOB '*[^0-9a-f]*'),
    record_sha256 TEXT NOT NULL COLLATE BINARY
        CHECK(length(record_sha256) = 64 AND record_sha256 NOT GLOB '*[^0-9a-f]*'),
    CHECK(economic_at <= retrieved_at),
    FOREIGN KEY(report_id) REFERENCES reports(id)
        ON UPDATE RESTRICT ON DELETE RESTRICT
) STRICT;

CREATE TRIGGER canonical_report_contexts_validate_report
BEFORE INSERT ON canonical_report_contexts
WHEN NOT EXISTS (
    SELECT 1
    FROM reports
    WHERE id = NEW.report_id
      AND substr(NEW.economic_at, 1, 10) = session_date
      AND substr(NEW.retrieved_at, 1, 10) = session_date
      AND NEW.retrieved_at <= created_at
      AND (
          (NEW.workflow_kind = 'PREMARKET' AND report_kind = 'MORNING')
          OR (NEW.workflow_kind = 'CLOSE' AND report_kind = 'CLOSE')
      )
)
BEGIN
    SELECT RAISE(ABORT, 'canonical report context conflicts with report');
END;

CREATE TABLE actual_close_reviews (
    id INTEGER PRIMARY KEY
        CHECK(typeof(id) = 'integer' AND id > 0),
    review_id TEXT NOT NULL COLLATE BINARY UNIQUE
        CHECK(length(review_id) = 64 AND review_id NOT GLOB '*[^0-9a-f]*'),
    session_date TEXT NOT NULL COLLATE BINARY
        CHECK(
            length(session_date) = 10
            AND session_date = strftime('%Y-%m-%d', session_date)
            AND CAST(substr(session_date, 1, 4) AS INTEGER) BETWEEN 1 AND 9999
        ),
    review_at TEXT NOT NULL COLLATE BINARY
        CHECK(
            length(review_at) = 27
            AND substr(review_at, 1, 19) = strftime('%Y-%m-%dT%H:%M:%S', review_at)
            AND substr(review_at, 20, 1) = '.'
            AND substr(review_at, 21, 6) NOT GLOB '*[^0-9]*'
            AND substr(review_at, 27, 1) = 'Z'
            AND CAST(substr(review_at, 1, 4) AS INTEGER) BETWEEN 1 AND 9999
            AND CAST(substr(review_at, 12, 2) AS INTEGER) BETWEEN 0 AND 23
            AND strftime('%Y-%m-%dT%H:%M:%S', review_at) IS NOT NULL
        ),
    mark_cutoff TEXT NOT NULL COLLATE BINARY
        CHECK(
            length(mark_cutoff) = 27
            AND substr(mark_cutoff, 1, 19) = strftime('%Y-%m-%dT%H:%M:%S', mark_cutoff)
            AND substr(mark_cutoff, 20, 1) = '.'
            AND substr(mark_cutoff, 21, 6) NOT GLOB '*[^0-9]*'
            AND substr(mark_cutoff, 27, 1) = 'Z'
            AND CAST(substr(mark_cutoff, 1, 4) AS INTEGER) BETWEEN 1 AND 9999
            AND CAST(substr(mark_cutoff, 12, 2) AS INTEGER) BETWEEN 0 AND 23
            AND strftime('%Y-%m-%dT%H:%M:%S', mark_cutoff) IS NOT NULL
        ),
    query_cutoff TEXT NOT NULL COLLATE BINARY
        CHECK(
            length(query_cutoff) = 27
            AND substr(query_cutoff, 1, 19) = strftime('%Y-%m-%dT%H:%M:%S', query_cutoff)
            AND substr(query_cutoff, 20, 1) = '.'
            AND substr(query_cutoff, 21, 6) NOT GLOB '*[^0-9]*'
            AND substr(query_cutoff, 27, 1) = 'Z'
            AND CAST(substr(query_cutoff, 1, 4) AS INTEGER) BETWEEN 1 AND 9999
            AND CAST(substr(query_cutoff, 12, 2) AS INTEGER) BETWEEN 0 AND 23
            AND strftime('%Y-%m-%dT%H:%M:%S', query_cutoff) IS NOT NULL
        ),
    retrieved_at TEXT NOT NULL COLLATE BINARY
        CHECK(
            length(retrieved_at) = 27
            AND substr(retrieved_at, 1, 19) = strftime('%Y-%m-%dT%H:%M:%S', retrieved_at)
            AND substr(retrieved_at, 20, 1) = '.'
            AND substr(retrieved_at, 21, 6) NOT GLOB '*[^0-9]*'
            AND substr(retrieved_at, 27, 1) = 'Z'
            AND CAST(substr(retrieved_at, 1, 4) AS INTEGER) BETWEEN 1 AND 9999
            AND CAST(substr(retrieved_at, 12, 2) AS INTEGER) BETWEEN 0 AND 23
            AND strftime('%Y-%m-%dT%H:%M:%S', retrieved_at) IS NOT NULL
        ),
    expected_binding_count INTEGER NOT NULL
        CHECK(typeof(expected_binding_count) = 'integer' AND expected_binding_count >= 0),
    source_digest TEXT NOT NULL COLLATE BINARY
        CHECK(length(source_digest) = 64 AND source_digest NOT GLOB '*[^0-9a-f]*'),
    record_sha256 TEXT NOT NULL COLLATE BINARY
        CHECK(length(record_sha256) = 64 AND record_sha256 NOT GLOB '*[^0-9a-f]*'),
    CHECK(mark_cutoff < review_at),
    CHECK(review_at <= query_cutoff),
    CHECK(query_cutoff <= retrieved_at),
    CHECK(substr(mark_cutoff, 1, 10) = session_date),
    CHECK(substr(review_at, 1, 10) = session_date),
    CHECK(substr(query_cutoff, 1, 10) = session_date),
    CHECK(substr(retrieved_at, 1, 10) = session_date),
    UNIQUE(session_date, query_cutoff)
) STRICT;

CREATE TABLE actual_close_source_bindings (
    id INTEGER PRIMARY KEY
        CHECK(typeof(id) = 'integer' AND id > 0),
    review_id TEXT NOT NULL COLLATE BINARY,
    binding_ordinal INTEGER NOT NULL
        CHECK(typeof(binding_ordinal) = 'integer' AND binding_ordinal > 0),
    symbol TEXT COLLATE BINARY
        CHECK(
            symbol IS NULL
            OR (
                length(symbol) BETWEEN 1 AND 16
                AND symbol = upper(symbol)
                AND symbol NOT GLOB '*[^A-Z0-9.-]*'
                AND substr(symbol, 1, 1) GLOB '[A-Z]'
            )
        ),
    source_role TEXT NOT NULL COLLATE BINARY
        CHECK(source_role IN (
            'SIP_QUOTE',
            'SIP_MINUTE_BAR',
            'SIP_DAILY_BAR',
            'IEX_FRESHNESS',
            'EVENT_EVIDENCE',
            'PRIMARY_HALT_FEED',
            'TRADER_ALERT_HALT',
            'OPERATIONAL_STATUS',
            'CROSS_CHECK_CALENDAR'
        )),
    source_observation_id INTEGER
        CHECK(
            source_observation_id IS NULL
            OR (typeof(source_observation_id) = 'integer' AND source_observation_id > 0)
        ),
    failure_code TEXT COLLATE BINARY
        CHECK(
            failure_code IS NULL
            OR (
                length(failure_code) BETWEEN 1 AND 128
                AND failure_code = upper(failure_code)
                AND failure_code NOT GLOB '*[^A-Z0-9_]*'
                AND substr(failure_code, 1, 1) GLOB '[A-Z]'
            )
        ),
    received_at TEXT NOT NULL COLLATE BINARY
        CHECK(
            length(received_at) = 27
            AND substr(received_at, 1, 19) = strftime('%Y-%m-%dT%H:%M:%S', received_at)
            AND substr(received_at, 20, 1) = '.'
            AND substr(received_at, 21, 6) NOT GLOB '*[^0-9]*'
            AND substr(received_at, 27, 1) = 'Z'
            AND CAST(substr(received_at, 1, 4) AS INTEGER) BETWEEN 1 AND 9999
            AND CAST(substr(received_at, 12, 2) AS INTEGER) BETWEEN 0 AND 23
            AND strftime('%Y-%m-%dT%H:%M:%S', received_at) IS NOT NULL
        ),
    record_sha256 TEXT NOT NULL COLLATE BINARY
        CHECK(length(record_sha256) = 64 AND record_sha256 NOT GLOB '*[^0-9a-f]*'),
    CHECK(
        (source_observation_id IS NOT NULL AND failure_code IS NULL)
        OR (source_observation_id IS NULL AND failure_code IS NOT NULL)
    ),
    UNIQUE(review_id, binding_ordinal),
    FOREIGN KEY(review_id) REFERENCES actual_close_reviews(review_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
    FOREIGN KEY(source_observation_id) REFERENCES source_observations(id)
        ON UPDATE RESTRICT ON DELETE RESTRICT
) STRICT;

CREATE UNIQUE INDEX actual_close_source_bindings_unique_observation
ON actual_close_source_bindings(
    review_id,
    source_observation_id
)
WHERE source_observation_id IS NOT NULL;

CREATE UNIQUE INDEX actual_close_source_bindings_unique_failure_role
ON actual_close_source_bindings(
    review_id,
    COALESCE(symbol, ''),
    source_role
)
WHERE failure_code IS NOT NULL;

CREATE TRIGGER actual_close_source_bindings_validate_receipt
BEFORE INSERT ON actual_close_source_bindings
WHEN NOT EXISTS (
    SELECT 1
    FROM actual_close_reviews AS review
    WHERE review.review_id = NEW.review_id COLLATE BINARY
      AND substr(NEW.received_at, 1, 10) = review.session_date
      AND NEW.received_at >= review.retrieved_at
      AND (
          NEW.source_observation_id IS NULL
          OR EXISTS (
              SELECT 1
              FROM source_observations AS observation
              WHERE observation.id = NEW.source_observation_id
                AND observation.retrieved_at = NEW.received_at
          )
      )
)
BEGIN
    SELECT RAISE(ABORT, 'actual close source binding conflicts with receipt');
END;

CREATE TABLE close_recommendations (
    id INTEGER PRIMARY KEY
        CHECK(typeof(id) = 'integer' AND id > 0),
    recommendation_id TEXT NOT NULL COLLATE BINARY UNIQUE
        CHECK(
            length(recommendation_id) = 64
            AND recommendation_id NOT GLOB '*[^0-9a-f]*'
        ),
    review_id TEXT NOT NULL COLLATE BINARY,
    session_date TEXT NOT NULL COLLATE BINARY
        CHECK(
            length(session_date) = 10
            AND session_date = strftime('%Y-%m-%d', session_date)
            AND CAST(substr(session_date, 1, 4) AS INTEGER) BETWEEN 1 AND 9999
        ),
    symbol TEXT NOT NULL COLLATE BINARY
        CHECK(
            length(symbol) BETWEEN 1 AND 16
            AND symbol = upper(symbol)
            AND symbol NOT GLOB '*[^A-Z0-9.-]*'
            AND substr(symbol, 1, 1) GLOB '[A-Z]'
        ),
    recommended_stop_micros INTEGER NOT NULL
        CHECK(typeof(recommended_stop_micros) = 'integer' AND recommended_stop_micros > 0),
    action TEXT NOT NULL COLLATE BINARY
        CHECK(action IN ('HOLD', 'EXIT', 'TIGHTEN_STOP')),
    reasons_json TEXT NOT NULL COLLATE BINARY
        CHECK(
            json_valid(reasons_json)
            AND json_type(reasons_json) = 'array'
            AND json_array_length(reasons_json) > 0
        ),
    source_digest TEXT NOT NULL COLLATE BINARY
        CHECK(length(source_digest) = 64 AND source_digest NOT GLOB '*[^0-9a-f]*'),
    received_at TEXT NOT NULL COLLATE BINARY
        CHECK(
            length(received_at) = 27
            AND substr(received_at, 1, 19) = strftime('%Y-%m-%dT%H:%M:%S', received_at)
            AND substr(received_at, 20, 1) = '.'
            AND substr(received_at, 21, 6) NOT GLOB '*[^0-9]*'
            AND substr(received_at, 27, 1) = 'Z'
            AND CAST(substr(received_at, 1, 4) AS INTEGER) BETWEEN 1 AND 9999
            AND CAST(substr(received_at, 12, 2) AS INTEGER) BETWEEN 0 AND 23
            AND strftime('%Y-%m-%dT%H:%M:%S', received_at) IS NOT NULL
        ),
    record_sha256 TEXT NOT NULL COLLATE BINARY
        CHECK(length(record_sha256) = 64 AND record_sha256 NOT GLOB '*[^0-9a-f]*'),
    UNIQUE(session_date, symbol),
    FOREIGN KEY(review_id) REFERENCES actual_close_reviews(review_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT
) STRICT;

CREATE TRIGGER close_recommendations_validate_review
BEFORE INSERT ON close_recommendations
WHEN NOT EXISTS (
    SELECT 1
    FROM actual_close_reviews
    WHERE review_id = NEW.review_id COLLATE BINARY
      AND session_date = NEW.session_date
      AND substr(NEW.received_at, 1, 10) = session_date
      AND NEW.received_at >= retrieved_at
)
BEGIN
    SELECT RAISE(ABORT, 'close recommendation conflicts with review');
END;

CREATE TRIGGER close_recommendations_require_chronology
BEFORE INSERT ON close_recommendations
WHEN EXISTS (
    SELECT 1
    FROM close_recommendations
    WHERE symbol = NEW.symbol COLLATE BINARY
      AND (session_date >= NEW.session_date OR received_at >= NEW.received_at)
)
BEGIN
    SELECT RAISE(ABORT, 'close recommendation must advance chronology');
END;

CREATE TRIGGER canonical_report_contexts_no_conflicting_insert
BEFORE INSERT ON canonical_report_contexts
WHEN EXISTS (
    SELECT 1
    FROM canonical_report_contexts
    WHERE id = NEW.id OR report_id = NEW.report_id
)
BEGIN
    SELECT RAISE(ABORT, 'canonical_report_contexts rejects conflicting inserts');
END;

CREATE TRIGGER actual_close_reviews_no_conflicting_insert
BEFORE INSERT ON actual_close_reviews
WHEN EXISTS (
    SELECT 1
    FROM actual_close_reviews
    WHERE id = NEW.id
       OR review_id = NEW.review_id COLLATE BINARY
       OR (session_date = NEW.session_date AND query_cutoff = NEW.query_cutoff)
)
BEGIN
    SELECT RAISE(ABORT, 'actual_close_reviews rejects conflicting inserts');
END;

CREATE TRIGGER actual_close_source_bindings_no_conflicting_insert
BEFORE INSERT ON actual_close_source_bindings
WHEN EXISTS (
    SELECT 1
    FROM actual_close_source_bindings
    WHERE id = NEW.id
       OR (
           review_id = NEW.review_id COLLATE BINARY
           AND binding_ordinal = NEW.binding_ordinal
       )
       OR (
           review_id = NEW.review_id COLLATE BINARY
           AND NEW.source_observation_id IS NOT NULL
           AND source_observation_id = NEW.source_observation_id
       )
       OR (
           review_id = NEW.review_id COLLATE BINARY
           AND NEW.failure_code IS NOT NULL
           AND failure_code IS NOT NULL
           AND COALESCE(symbol, '') = COALESCE(NEW.symbol, '')
           AND source_role = NEW.source_role
       )
)
BEGIN
    SELECT RAISE(ABORT, 'actual_close_source_bindings rejects conflicting inserts');
END;

CREATE TRIGGER close_recommendations_no_conflicting_insert
BEFORE INSERT ON close_recommendations
WHEN EXISTS (
    SELECT 1
    FROM close_recommendations
    WHERE id = NEW.id
       OR recommendation_id = NEW.recommendation_id COLLATE BINARY
       OR (session_date = NEW.session_date AND symbol = NEW.symbol COLLATE BINARY)
)
BEGIN
    SELECT RAISE(ABORT, 'close_recommendations rejects conflicting inserts');
END;

CREATE TRIGGER canonical_report_contexts_no_update
BEFORE UPDATE ON canonical_report_contexts
BEGIN
    SELECT RAISE(ABORT, 'canonical_report_contexts is append-only');
END;

CREATE TRIGGER canonical_report_contexts_no_delete
BEFORE DELETE ON canonical_report_contexts
BEGIN
    SELECT RAISE(ABORT, 'canonical_report_contexts is append-only');
END;

CREATE TRIGGER actual_close_reviews_no_update
BEFORE UPDATE ON actual_close_reviews
BEGIN
    SELECT RAISE(ABORT, 'actual_close_reviews is append-only');
END;

CREATE TRIGGER actual_close_reviews_no_delete
BEFORE DELETE ON actual_close_reviews
BEGIN
    SELECT RAISE(ABORT, 'actual_close_reviews is append-only');
END;

CREATE TRIGGER actual_close_source_bindings_no_update
BEFORE UPDATE ON actual_close_source_bindings
BEGIN
    SELECT RAISE(ABORT, 'actual_close_source_bindings is append-only');
END;

CREATE TRIGGER actual_close_source_bindings_no_delete
BEFORE DELETE ON actual_close_source_bindings
BEGIN
    SELECT RAISE(ABORT, 'actual_close_source_bindings is append-only');
END;

CREATE TRIGGER close_recommendations_no_update
BEFORE UPDATE ON close_recommendations
BEGIN
    SELECT RAISE(ABORT, 'close_recommendations is append-only');
END;

CREATE TRIGGER close_recommendations_no_delete
BEFORE DELETE ON close_recommendations
BEGIN
    SELECT RAISE(ABORT, 'close_recommendations is append-only');
END;

DROP TRIGGER scheduled_runs_controlled_completion;

CREATE TRIGGER scheduled_runs_controlled_completion
BEFORE UPDATE ON scheduled_runs
WHEN NOT (
    OLD.finished_at IS NULL
    AND OLD.result_envelope_json IS NULL
    AND OLD.result_envelope_sha256 IS NULL
    AND NEW.result_envelope_json IS NOT NULL
    AND NEW.result_envelope_sha256 IS NOT NULL
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
                  AND (
                      report.report_kind = OLD.run_kind
                      OR (
                          OLD.run_kind = 'PREMARKET'
                          AND report.report_kind = 'MORNING'
                      )
                  )
                  AND report.archive_relative_path = NEW.report_path
                  AND claim.status = 'FINALIZED'
                  AND claim.report_id = report.id
                  AND claim.finalized_at >= OLD.started_at
                  AND claim.finalized_at <= NEW.finished_at
            )
        )
    )
)
BEGIN
    SELECT RAISE(ABORT, 'scheduled_runs permits only one completion');
END;
