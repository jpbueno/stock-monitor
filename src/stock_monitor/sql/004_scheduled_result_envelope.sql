ALTER TABLE scheduled_runs
ADD COLUMN result_envelope_json TEXT COLLATE BINARY
    CHECK(
        result_envelope_json IS NULL
        OR (
            typeof(result_envelope_json) = 'text'
            AND json_valid(result_envelope_json)
            AND json_type(result_envelope_json) = 'object'
        )
    );

ALTER TABLE scheduled_runs
ADD COLUMN result_envelope_sha256 TEXT COLLATE BINARY
    CHECK(
        result_envelope_sha256 IS NULL
        OR (
            typeof(result_envelope_sha256) = 'text'
            AND length(result_envelope_sha256) = 64
            AND result_envelope_sha256 NOT GLOB '*[^0-9a-f]*'
        )
    );

DROP TRIGGER scheduled_runs_controlled_completion;

CREATE TRIGGER scheduled_runs_require_started_insert
BEFORE INSERT ON scheduled_runs
WHEN NEW.finished_at IS NOT NULL
  OR NEW.result_envelope_json IS NOT NULL
  OR NEW.result_envelope_sha256 IS NOT NULL
BEGIN
    SELECT RAISE(ABORT, 'scheduled_runs inserts must start in progress');
END;

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
                  AND report.report_kind = OLD.run_kind
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
