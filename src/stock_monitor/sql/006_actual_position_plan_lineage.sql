DROP TRIGGER phase1_signal_events_validate_sequence;

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
              AND (
                  (
                      NEW.event_kind = 'LIVE_CONFIRM'
                      AND confirmation.parsed_action IN (
                          'BOUGHT',
                          'PARTIAL_FILL'
                      )
                  )
                  OR (
                      NEW.event_kind = 'LIVE_SKIP'
                      AND confirmation.parsed_action = 'SKIPPED'
                  )
              )
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
              AND (
                  NEW.event_kind NOT IN (
                      'FINALIZE_NOT_TRIGGERED',
                      'FINALIZE_NOT_FILLED',
                      'FINALIZE_UNRESOLVED',
                      'EXPIRE'
                  )
                  OR completion.completed_at <= NEW.event_time
              )
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
                  json_extract(
                      CAST(evidence.manifest_bytes AS TEXT),
                      '$.event_exit_required'
                  ) = 1
                  OR json_extract(
                      CAST(evidence.manifest_bytes AS TEXT),
                      '$.thesis_invalidated'
                  ) = 1
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
        (
            NEW.event_ordinal = 0
            AND NEW.event_kind = 'PUBLISHED'
            AND NEW.from_status IS NULL
            AND NEW.to_status = 'PUBLISHED'
        )
        OR (
            NEW.from_status = 'PUBLISHED'
            AND NEW.event_kind = 'TRIGGER_OBSERVED'
            AND NEW.to_status = 'TRIGGERED_AWAITING_LIMIT'
        )
        OR (
            NEW.from_status = 'PUBLISHED'
            AND NEW.event_kind = 'FINALIZE_NOT_TRIGGERED'
            AND NEW.to_status = 'NOT_TRIGGERED'
        )
        OR (
            NEW.from_status IN ('PUBLISHED', 'TRIGGERED_AWAITING_LIMIT')
            AND NEW.event_kind = 'FINALIZE_UNRESOLVED'
            AND NEW.to_status = 'UNRESOLVED'
        )
        OR (
            NEW.from_status IN ('PUBLISHED', 'TRIGGERED_AWAITING_LIMIT')
            AND NEW.event_kind = 'EXPIRE'
            AND NEW.to_status = 'EXPIRED'
        )
        OR (
            NEW.from_status IN ('PUBLISHED', 'TRIGGERED_AWAITING_LIMIT')
            AND NEW.event_kind = 'INVALIDATE'
            AND NEW.to_status = 'INVALIDATED'
        )
        OR (
            NEW.from_status = 'TRIGGERED_AWAITING_LIMIT'
            AND NEW.event_kind = 'PAPER_FILL'
            AND NEW.to_status = 'TRIGGERED_PAPER'
        )
        OR (
            NEW.from_status = 'TRIGGERED_AWAITING_LIMIT'
            AND NEW.event_kind = 'SHADOW_FILL'
            AND NEW.to_status = 'SHADOW_FILLED_INFORMATIONAL'
        )
        OR (
            NEW.from_status = 'TRIGGERED_AWAITING_LIMIT'
            AND NEW.event_kind = 'LIVE_CONFIRM'
            AND NEW.to_status = 'LIVE_CONFIRMED'
        )
        OR (
            NEW.from_status = 'TRIGGERED_AWAITING_LIMIT'
            AND NEW.event_kind = 'LIVE_SKIP'
            AND NEW.to_status = 'SKIPPED_LIVE_TRACKED_PAPER'
        )
        OR (
            NEW.from_status = 'TRIGGERED_PAPER'
            AND NEW.event_kind = 'LIVE_CONFIRM'
            AND NEW.to_status = 'LIVE_CONFIRMED'
        )
        OR (
            NEW.from_status = 'TRIGGERED_PAPER'
            AND NEW.event_kind = 'LIVE_SKIP'
            AND NEW.to_status = 'SKIPPED_LIVE_TRACKED_PAPER'
        )
        OR (
            NEW.from_status = 'TRIGGERED_AWAITING_LIMIT'
            AND NEW.event_kind = 'FINALIZE_NOT_FILLED'
            AND NEW.to_status = 'NOT_FILLED_LIMIT'
        )
        OR (
            NEW.from_status IN (
                'TRIGGERED_PAPER',
                'LIVE_CONFIRMED',
                'SKIPPED_LIVE_TRACKED_PAPER'
            )
            AND NEW.event_kind = 'CLOSE'
            AND NEW.to_status = 'CLOSED'
        )
        OR (
            NEW.from_status IN (
                'TRIGGERED_PAPER',
                'LIVE_CONFIRMED',
                'SKIPPED_LIVE_TRACKED_PAPER'
            )
            AND NEW.event_kind = 'PARTIAL_EXIT'
            AND NEW.to_status = NEW.from_status
        )
    )
BEGIN
    SELECT RAISE(ABORT, 'phase1 lifecycle event is out of sequence');
END;

CREATE TABLE actual_position_plan_bindings (
    id INTEGER PRIMARY KEY
        CHECK(typeof(id) = 'integer' AND id > 0),
    binding_id TEXT NOT NULL COLLATE BINARY UNIQUE
        CHECK(length(binding_id) = 64 AND binding_id NOT GLOB '*[^0-9a-f]*'),
    confirmation_execution_event_id INTEGER NOT NULL UNIQUE
        CHECK(
            typeof(confirmation_execution_event_id) = 'integer'
            AND confirmation_execution_event_id > 0
        ),
    confirmation_event_id TEXT NOT NULL COLLATE BINARY UNIQUE
        CHECK(
            length(confirmation_event_id) BETWEEN 1 AND 200
            AND instr(confirmation_event_id, char(0)) = 0
        ),
    signal_id TEXT NOT NULL COLLATE BINARY,
    position_lifecycle_id TEXT NOT NULL COLLATE BINARY
        CHECK(
            length(position_lifecycle_id) = 64
            AND position_lifecycle_id NOT GLOB '*[^0-9a-f]*'
        ),
    binding_ordinal INTEGER NOT NULL
        CHECK(typeof(binding_ordinal) = 'integer' AND binding_ordinal > 0),
    binding_kind TEXT NOT NULL COLLATE BINARY
        CHECK(binding_kind IN (
            'LIVE_CONFIRM_ANCHOR',
            'PARTIAL_FILL_CONTINUATION'
        )),
    symbol TEXT NOT NULL COLLATE BINARY
        CHECK(
            length(symbol) BETWEEN 1 AND 16
            AND symbol = upper(symbol)
            AND symbol NOT GLOB '*[^A-Z0-9.-]*'
            AND substr(symbol, 1, 1) GLOB '[A-Z]'
        ),
    parent_order_id TEXT COLLATE BINARY
        CHECK(
            parent_order_id IS NULL
            OR (
                length(parent_order_id) BETWEEN 1 AND 200
                AND parent_order_id = trim(parent_order_id)
                AND instr(parent_order_id, char(0)) = 0
            )
        ),
    fill_group_planned_shares INTEGER
        CHECK(
            fill_group_planned_shares IS NULL
            OR (
                typeof(fill_group_planned_shares) = 'integer'
                AND fill_group_planned_shares > 0
            )
        ),
    fill_shares INTEGER NOT NULL
        CHECK(typeof(fill_shares) = 'integer' AND fill_shares > 0),
    cumulative_group_shares INTEGER NOT NULL
        CHECK(
            typeof(cumulative_group_shares) = 'integer'
            AND cumulative_group_shares > 0
        ),
    event_time TEXT NOT NULL COLLATE BINARY
        CHECK(
            length(event_time) = 27
            AND substr(event_time, 1, 19) = strftime('%Y-%m-%dT%H:%M:%S', event_time)
            AND substr(event_time, 20, 1) = '.'
            AND substr(event_time, 21, 6) NOT GLOB '*[^0-9]*'
            AND substr(event_time, 27, 1) = 'Z'
        ),
    message_time TEXT NOT NULL COLLATE BINARY
        CHECK(
            length(message_time) = 27
            AND substr(message_time, 1, 19) = strftime('%Y-%m-%dT%H:%M:%S', message_time)
            AND substr(message_time, 20, 1) = '.'
            AND substr(message_time, 21, 6) NOT GLOB '*[^0-9]*'
            AND substr(message_time, 27, 1) = 'Z'
        ),
    action_received_at TEXT NOT NULL COLLATE BINARY
        CHECK(
            length(action_received_at) = 27
            AND substr(action_received_at, 1, 19) = strftime('%Y-%m-%dT%H:%M:%S', action_received_at)
            AND substr(action_received_at, 20, 1) = '.'
            AND substr(action_received_at, 21, 6) NOT GLOB '*[^0-9]*'
            AND substr(action_received_at, 27, 1) = 'Z'
        ),
    recorded_at TEXT NOT NULL COLLATE BINARY
        CHECK(
            length(recorded_at) = 27
            AND substr(recorded_at, 1, 19) = strftime('%Y-%m-%dT%H:%M:%S', recorded_at)
            AND substr(recorded_at, 20, 1) = '.'
            AND substr(recorded_at, 21, 6) NOT GLOB '*[^0-9]*'
            AND substr(recorded_at, 27, 1) = 'Z'
        ),
    action_source_digest TEXT NOT NULL COLLATE BINARY
        CHECK(
            length(action_source_digest) = 64
            AND action_source_digest NOT GLOB '*[^0-9a-f]*'
        ),
    primary_plan_digest TEXT NOT NULL COLLATE BINARY
        CHECK(
            length(primary_plan_digest) = 64
            AND primary_plan_digest NOT GLOB '*[^0-9a-f]*'
        ),
    source_digest TEXT NOT NULL COLLATE BINARY
        CHECK(length(source_digest) = 64 AND source_digest NOT GLOB '*[^0-9a-f]*'),
    record_sha256 TEXT NOT NULL COLLATE BINARY
        CHECK(length(record_sha256) = 64 AND record_sha256 NOT GLOB '*[^0-9a-f]*'),
    CHECK(event_time <= message_time),
    CHECK(message_time <= action_received_at),
    CHECK(action_received_at <= recorded_at),
    CHECK((parent_order_id IS NULL) = (fill_group_planned_shares IS NULL)),
    CHECK(
        (parent_order_id IS NULL AND cumulative_group_shares = fill_shares)
        OR (
            parent_order_id IS NOT NULL
            AND cumulative_group_shares <= fill_group_planned_shares
        )
    ),
    CHECK(
        (binding_kind = 'LIVE_CONFIRM_ANCHOR' AND binding_ordinal = 1)
        OR (
            binding_kind = 'PARTIAL_FILL_CONTINUATION'
            AND binding_ordinal > 1
        )
    ),
    UNIQUE(position_lifecycle_id, binding_ordinal),
    FOREIGN KEY(confirmation_execution_event_id) REFERENCES execution_events(id)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
    FOREIGN KEY(signal_id) REFERENCES phase1_signals(signal_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT
) STRICT;

CREATE TRIGGER actual_position_plan_bindings_validate_lineage
BEFORE INSERT ON actual_position_plan_bindings
WHEN NOT EXISTS (
    SELECT 1
    FROM execution_events AS action
    JOIN phase1_signals AS signal
      ON signal.signal_id = NEW.signal_id
    WHERE action.id = NEW.confirmation_execution_event_id
      AND action.event_id = NEW.confirmation_event_id COLLATE BINARY
      AND action.symbol = NEW.symbol COLLATE BINARY
      AND action.shares = NEW.fill_shares
      AND action.event_time = NEW.event_time
      AND action.message_time = NEW.message_time
      AND json_extract(action.details_json, '$.source.received_at')
          = NEW.action_received_at
      AND (
          NEW.binding_kind != 'LIVE_CONFIRM_ANCHOR'
          OR NOT EXISTS (
              SELECT 1
              FROM execution_events AS earlier_entry
              WHERE earlier_entry.signal_id = action.signal_id COLLATE BINARY
                AND earlier_entry.symbol = action.symbol COLLATE BINARY
                AND earlier_entry.parsed_action IN (
                    'BOUGHT',
                    'PARTIAL_FILL'
                )
                AND earlier_entry.id < action.id
          )
      )
      AND (
          NEW.binding_kind != 'PARTIAL_FILL_CONTINUATION'
          OR action.id = (
              SELECT MIN(next_entry.id)
              FROM execution_events AS next_entry
              WHERE next_entry.signal_id = action.signal_id COLLATE BINARY
                AND next_entry.symbol = action.symbol COLLATE BINARY
                AND next_entry.parsed_action IN (
                    'BOUGHT',
                    'PARTIAL_FILL'
                )
                AND next_entry.id > COALESCE((
                    SELECT MAX(binding.confirmation_execution_event_id)
                    FROM actual_position_plan_bindings AS binding
                    WHERE binding.signal_id = NEW.signal_id COLLATE BINARY
                      AND binding.position_lifecycle_id
                          = NEW.position_lifecycle_id COLLATE BINARY
                ), 0)
          )
      )
      AND signal.symbol = NEW.symbol COLLATE BINARY
      AND signal.role = 'PRIMARY'
      AND signal.primary_plan_digest = NEW.primary_plan_digest COLLATE BINARY
      AND NEW.binding_ordinal = COALESCE((
          SELECT MAX(binding_ordinal) + 1
          FROM actual_position_plan_bindings
          WHERE signal_id = NEW.signal_id COLLATE BINARY
            AND position_lifecycle_id
                = NEW.position_lifecycle_id COLLATE BINARY
      ), 1)
      AND NEW.cumulative_group_shares = COALESCE((
          SELECT SUM(fill_shares)
          FROM actual_position_plan_bindings
          WHERE signal_id = NEW.signal_id COLLATE BINARY
            AND position_lifecycle_id
                = NEW.position_lifecycle_id COLLATE BINARY
      ), 0) + NEW.fill_shares
      AND (
          (
              NEW.parent_order_id IS NULL
              AND action.parsed_action = 'BOUGHT'
              AND json_extract(
                  action.details_json,
                  '$.normalized.parent_order_id'
              ) IS NULL
              AND json_extract(
                  action.details_json,
                  '$.normalized.fill_group_planned_shares'
              ) IS NULL
          )
          OR (
              NEW.parent_order_id IS NOT NULL
              AND action.parsed_action = 'PARTIAL_FILL'
              AND json_extract(
                  action.details_json,
                  '$.normalized.parent_order_id'
              ) = NEW.parent_order_id
              AND json_extract(
                  action.details_json,
                  '$.normalized.fill_group_planned_shares'
              ) = NEW.fill_group_planned_shares
              AND NEW.fill_group_planned_shares = signal.planned_shares
          )
      )
      AND (
          (
              NEW.binding_kind = 'LIVE_CONFIRM_ANCHOR'
              AND (
                  EXISTS (
                      SELECT 1
                      FROM phase1_signal_events AS lifecycle
                      WHERE lifecycle.signal_id = NEW.signal_id COLLATE BINARY
                        AND lifecycle.event_kind = 'LIVE_CONFIRM'
                        AND lifecycle.to_status = 'LIVE_CONFIRMED'
                        AND lifecycle.confirmation_execution_event_id
                            = NEW.confirmation_execution_event_id
                        AND lifecycle.received_at <= NEW.recorded_at
                  )
                  OR (
                      (SELECT COUNT(*)
                       FROM phase1_signal_events AS lifecycle
                       WHERE lifecycle.signal_id = NEW.signal_id COLLATE BINARY
                         AND lifecycle.event_kind = 'LIVE_CONFIRM'
                         AND lifecycle.to_status = 'LIVE_CONFIRMED'
                         AND lifecycle.received_at <= NEW.recorded_at) = 1
                      AND EXISTS (
                      SELECT 1
                      FROM actual_position_plan_bindings AS plan_anchor
                          JOIN phase1_signal_events AS lifecycle
                            ON lifecycle.signal_id = plan_anchor.signal_id
                           AND lifecycle.confirmation_execution_event_id
                               = plan_anchor.confirmation_execution_event_id
                          WHERE plan_anchor.signal_id
                                = NEW.signal_id COLLATE BINARY
                            AND plan_anchor.binding_kind
                                = 'LIVE_CONFIRM_ANCHOR'
                            AND plan_anchor.binding_ordinal = 1
                            AND lifecycle.event_kind = 'LIVE_CONFIRM'
                            AND lifecycle.to_status = 'LIVE_CONFIRMED'
                            AND lifecycle.received_at <= NEW.recorded_at
                      )
                      AND EXISTS (
                          SELECT 1
                          FROM actual_positions AS current_position
                          WHERE current_position.signal_id
                                = action.signal_id COLLATE BINARY
                            AND current_position.symbol
                                = NEW.symbol COLLATE BINARY
                            AND current_position.shares > 0
                            AND current_position.last_execution_event_id
                                >= NEW.confirmation_execution_event_id
                      )
                      AND NOT EXISTS (
                          SELECT 1
                          FROM actual_positions AS other_open
                          WHERE other_open.symbol = NEW.symbol COLLATE BINARY
                            AND other_open.shares > 0
                            AND other_open.signal_id
                                != action.signal_id COLLATE BINARY
                      )
                      AND NOT EXISTS (
                          SELECT 1
                          FROM actual_position_plan_bindings AS prior_anchor
                          JOIN execution_events AS prior_action
                            ON prior_action.id
                               = prior_anchor.confirmation_execution_event_id
                          LEFT JOIN actual_positions AS prior_position
                            ON prior_position.signal_id = prior_action.signal_id
                          WHERE prior_anchor.signal_id
                                = NEW.signal_id COLLATE BINARY
                            AND prior_anchor.binding_kind
                                = 'LIVE_CONFIRM_ANCHOR'
                            AND prior_anchor.binding_ordinal = 1
                            AND (
                                prior_position.signal_id IS NULL
                                OR prior_position.shares != 0
                            )
                      )
                      AND NOT EXISTS (
                          SELECT 1
                          FROM actual_position_plan_bindings AS prior_anchor
                          JOIN execution_events AS prior_action
                            ON prior_action.id
                               = prior_anchor.confirmation_execution_event_id
                          WHERE prior_anchor.signal_id
                                = NEW.signal_id COLLATE BINARY
                            AND prior_anchor.binding_kind
                                = 'LIVE_CONFIRM_ANCHOR'
                            AND prior_anchor.binding_ordinal = 1
                            AND prior_action.signal_id
                                = action.signal_id COLLATE BINARY
                      )
                  )
              )
          )
          OR (
              NEW.binding_kind = 'PARTIAL_FILL_CONTINUATION'
              AND NEW.parent_order_id IS NOT NULL
              AND NOT EXISTS (
                  SELECT 1
                  FROM phase1_signal_events AS lifecycle
                  WHERE lifecycle.confirmation_execution_event_id
                      = NEW.confirmation_execution_event_id
              )
              AND EXISTS (
                  SELECT 1
                  FROM actual_position_plan_bindings AS anchor
                  JOIN execution_events AS anchor_action
                    ON anchor_action.id
                       = anchor.confirmation_execution_event_id
                  WHERE anchor.signal_id = NEW.signal_id COLLATE BINARY
                    AND anchor.position_lifecycle_id
                        = NEW.position_lifecycle_id COLLATE BINARY
                    AND anchor.binding_kind = 'LIVE_CONFIRM_ANCHOR'
                    AND anchor.parent_order_id = NEW.parent_order_id COLLATE BINARY
                    AND anchor.fill_group_planned_shares
                        = NEW.fill_group_planned_shares
                    AND anchor_action.signal_id
                        = action.signal_id COLLATE BINARY
              )
              AND NOT EXISTS (
                  SELECT 1
                  FROM actual_position_plan_bindings AS prior
                  WHERE prior.signal_id = NEW.signal_id COLLATE BINARY
                    AND prior.position_lifecycle_id
                        = NEW.position_lifecycle_id COLLATE BINARY
                    AND (
                        prior.parent_order_id IS NULL
                        OR prior.parent_order_id
                            != NEW.parent_order_id COLLATE BINARY
                        OR prior.fill_group_planned_shares
                            != NEW.fill_group_planned_shares
                    )
              )
          )
      )
)
BEGIN
    SELECT RAISE(ABORT, 'actual position plan binding conflicts with lineage');
END;

CREATE TRIGGER actual_position_plan_bindings_require_journal_provider_monitoring_writer
BEFORE INSERT ON actual_position_plan_bindings
WHEN journal_provider_monitoring_write_allowed() != 1
BEGIN
    SELECT RAISE(ABORT, 'actual position plan bindings require the journal provider monitoring writer');
END;

CREATE TRIGGER actual_position_plan_bindings_no_conflicting_insert
BEFORE INSERT ON actual_position_plan_bindings
WHEN EXISTS (
    SELECT 1
    FROM actual_position_plan_bindings
    WHERE id = NEW.id
       OR binding_id = NEW.binding_id COLLATE BINARY
       OR confirmation_execution_event_id
          = NEW.confirmation_execution_event_id
       OR confirmation_event_id = NEW.confirmation_event_id COLLATE BINARY
       OR (
           signal_id = NEW.signal_id COLLATE BINARY
           AND position_lifecycle_id
               = NEW.position_lifecycle_id COLLATE BINARY
           AND binding_ordinal = NEW.binding_ordinal
       )
)
BEGIN
    SELECT RAISE(ABORT, 'actual_position_plan_bindings rejects conflicting inserts');
END;

CREATE TRIGGER actual_position_plan_bindings_no_update
BEFORE UPDATE ON actual_position_plan_bindings
BEGIN
    SELECT RAISE(ABORT, 'actual_position_plan_bindings is append-only');
END;

CREATE TRIGGER actual_position_plan_bindings_no_delete
BEFORE DELETE ON actual_position_plan_bindings
BEGIN
    SELECT RAISE(ABORT, 'actual_position_plan_bindings is append-only');
END;
