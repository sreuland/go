-- +migrate Up

ALTER TABLE history_operations ADD is_payment smallint DEFAULT 0;
CREATE INDEX "index_history_operations_on_is_payment" ON history_operations USING btree (is_payment);

-- +migrate Down

DROP INDEX "index_history_operations_on_is_payment";
ALTER TABLE history_operations DROP COLUMN is_payment;
