DROP TRIGGER actual_close_source_bindings_validate_receipt;

CREATE TRIGGER actual_close_source_bindings_validate_receipt
BEFORE INSERT ON actual_close_source_bindings
WHEN NOT EXISTS (
    SELECT 1
    FROM actual_close_reviews AS review
    WHERE review.review_id = NEW.review_id COLLATE BINARY
      AND substr(NEW.received_at, 1, 10) = review.session_date
      AND NEW.received_at >= review.review_at
      AND NEW.received_at <= review.query_cutoff
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
