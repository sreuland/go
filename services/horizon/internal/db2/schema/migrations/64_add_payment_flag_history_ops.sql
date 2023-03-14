-- +migrate Up

ALTER TABLE history_operations ADD asset_balance_changed smallint DEFAULT 0;

-- +migrate Down

ALTER TABLE history_operations DROP COLUMN asset_balance_changed;
