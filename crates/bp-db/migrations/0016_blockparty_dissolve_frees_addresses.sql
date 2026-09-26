-- A dissolved Blockparty used to keep its member rows. With the pool-wide
-- UNIQUE (address) on blockparty_member and a global UNIQUE ("adminAddress")
-- on blockparty_group, every address that was ever in a party stayed locked
-- out of the next one for good. The dissolve path now deletes the member rows
-- and the join link in the same transaction as the status flip (like
-- Group-Solo); this migration clears what earlier dissolves left behind and
-- scopes the admin-address uniqueness to live parties. The group row and its
-- block history stay: the history carries its own split snapshot.

DELETE FROM blockparty_member m
USING blockparty_group g
WHERE m."groupId" = g.id AND g.status = 'dissolved';

DELETE FROM blockparty_join_link l
USING blockparty_group g
WHERE l."groupId" = g.id AND g.status = 'dissolved';

ALTER TABLE blockparty_group DROP CONSTRAINT IF EXISTS "UQ_blockparty_group_admin_address";
CREATE UNIQUE INDEX IF NOT EXISTS "UQ_blockparty_group_admin_address_live"
    ON blockparty_group ("adminAddress")
    WHERE status <> 'dissolved';
