-- Customer-set extranonce prefix per worker (the custom-extranonce API).
--
-- One paying customer wants to pick his own extranonce-1 per worker instead of
-- taking the pool-allocated one. Auth is a stored bearer TOKEN: the address
-- proves key control once by signing a challenge, the API issues a token
-- (random, only its hash stored — the `adminTokenHash` pattern), and every
-- headless "set the EN for this worker" call carries that token. The feature is
-- Solo-only and cannot move money (the coinbase still pays the address), so a
-- long-lived token is an acceptable, low-stakes credential.
--
--   pplns_extranonce_challenge — the short-lived message an address signs to be
--                                issued a token (PK address; nonced + expiring
--                                so the signature itself is one-time and never
--                                becomes a reusable credential).
--   pplns_extranonce_token     — the issued token's hash (PK address). Re-issue
--                                overwrites it, revoking the previous token.
--   pplns_custom_extranonce    — the applied override, read at channel-open.
--
-- `prefix` is the 4-byte extranonce prefix as an unsigned 32-bit value. Stored
-- as bigint because Postgres has no unsigned integer type; the CHECK pins the
-- u32 range so the Rust side can narrow bigint -> u32 without a fallible
-- conversion at every read.
--
-- UNIQUE (address, prefix) on the overrides — deliberately scoped to ONE
-- address, not global. Prefix uniqueness only matters between connections that
-- hash the SAME coinbase (same payouts + template; see bp_common::extranonce).
-- The pool is non-custodial, so:
--   * same address, two workers, same prefix -> Solo pays the same address ->
--     identical coinbase -> the prefix is the sole work-partitioner -> the two
--     workers would grind the same search space. Rejected here.
--   * different addresses, same prefix -> different payout outputs ->
--     different coinbase -> different header regardless of the prefix ->
--     harmless. Allowed; a global UNIQUE would reject it for no reason.
--
-- Idempotent (IF NOT EXISTS): a fresh DB bootstrapped from db/schema.sql
-- already has these tables, so this migration is a no-op there.
CREATE TABLE IF NOT EXISTS pplns_extranonce_challenge (
    address character varying(62) NOT NULL,
    message text NOT NULL,
    "createdAt" bigint NOT NULL,
    "expiresAt" bigint NOT NULL,
    CONSTRAINT pplns_extranonce_challenge_pkey PRIMARY KEY (address)
);

CREATE TABLE IF NOT EXISTS pplns_extranonce_token (
    address character varying(62) NOT NULL,
    "tokenHash" character varying(64) NOT NULL,
    "createdAt" bigint DEFAULT ((EXTRACT(epoch FROM now()) * (1000)::numeric))::bigint NOT NULL,
    CONSTRAINT pplns_extranonce_token_pkey PRIMARY KEY (address)
);

CREATE TABLE IF NOT EXISTS pplns_custom_extranonce (
    address character varying(62) NOT NULL,
    worker character varying NOT NULL,
    prefix bigint NOT NULL,
    "createdAt" bigint DEFAULT ((EXTRACT(epoch FROM now()) * (1000)::numeric))::bigint NOT NULL,
    "updatedAt" bigint DEFAULT ((EXTRACT(epoch FROM now()) * (1000)::numeric))::bigint NOT NULL,
    CONSTRAINT pplns_custom_extranonce_pkey PRIMARY KEY (address, worker),
    -- DEFERRABLE so a batch update can SWAP prefixes between two of the
    -- address's own workers inside one transaction. Postgres checks a plain
    -- UNIQUE per statement, so `rig1 := rig2's prefix` would collide with
    -- rig2's still-old row and abort the batch. Deferred, the check runs at
    -- COMMIT: transient in-transaction duplicates are fine, a genuine
    -- duplicate (two workers left on the same prefix) still fails. Stays
    -- INITIALLY IMMEDIATE so single-row writes behave exactly as before —
    -- only the batch path opts in via `SET CONSTRAINTS ... DEFERRED`.
    CONSTRAINT pplns_custom_extranonce_address_prefix_key
        UNIQUE (address, prefix) DEFERRABLE INITIALLY IMMEDIATE,
    CONSTRAINT pplns_custom_extranonce_prefix_u32 CHECK (prefix >= 0 AND prefix <= 4294967295)
);

CREATE INDEX IF NOT EXISTS "IDX_pplns_extranonce_challenge_expiresAt"
    ON pplns_extranonce_challenge USING btree ("expiresAt");

-- The reserved-prefix rule, at the data instead of only in the handler.
--
-- `0x00……` is the SV2 extranonce allocator's worker partition and `0x01……` is
-- SV1's (`bp_common::extranonce::{SV2_WORKER_ID, SV1_WORKER_ID}`). A
-- customer-set prefix inside one of them can later be handed to another
-- channel by the allocator. That only costs work when the two hash the SAME
-- coinbase — same address, both Solo, i.e. one customer running several rigs
-- of which one has an override — but then both search one space and one of
-- them mines for nothing. Workers 2..=255 are unowned and no allocator ever
-- emits into them, which is exactly what makes a hand-set prefix safe to hold
-- indefinitely.
--
-- `bp_api::controllers::custom_extranonce::parse_prefix` has always rejected
-- these, and it is the only writer in the code. But the table is hand-writable,
-- `bin/blitzpool/src/custom_extranonce.rs` loads every row without re-checking,
-- and rows HAVE been set by hand on the test server. That left the rule with
-- exactly one enforcement point, and not the one closest to the data.
--
-- 33554432 = 0x02000000, the first prefix above SV1's partition.
--
-- Deliberately NOT folded into the CREATE TABLE above: on a database where the
-- table already exists that statement no-ops, so the constraint would never
-- reach it. As its own guarded ALTER it applies to both a fresh database and an
-- existing one, and re-running the migration is a no-op either way.
--
-- ⚠️ This FAILS if a row already violates it, and that failure is the point:
-- such a row is the collision this constraint exists to prevent and wants
-- looking at, not migrating around. Find offenders with
--   SELECT address, worker, to_hex(prefix) FROM pplns_custom_extranonce
--    WHERE prefix < 33554432;
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'pplns_custom_extranonce_prefix_unreserved'
    ) THEN
        ALTER TABLE pplns_custom_extranonce
            ADD CONSTRAINT pplns_custom_extranonce_prefix_unreserved
            CHECK (prefix >= 33554432);
    END IF;
END
$$;
