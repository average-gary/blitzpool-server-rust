-- A fifth rejected-share column pair on `client_statistics_entity`, for
-- shares whose job existed but had been retired past the network-jitter
-- grace window.
--
-- Until now `Stale` folded into `rejectedJobNotFound*` in
-- `bp_share_stats_sink::hooks`, for the "there is no column for it" reason
-- migration 0010 removed for version rolling. The two mean opposite things
-- to an operator: `JobNotFound` is a job the pool no longer has at all
-- (GC'd, or never sent) and points at a miner or proxy holding work it was
-- never given, while `Stale` is the ordinary tail of a block transition and
-- needs no action at all. Folded together, a normal block change reads as a
-- fleet of broken miners.
--
-- The pool-wide `pool_rejected_statistics_entity` and the per-address
-- `client_rejected_statistics_entity` already keep `Stale` as their own row,
-- so the per-session counters were the last place the two were
-- indistinguishable. With this pair the five `RejectedReason` variants each
-- have exactly one home and the breakdown sums to `rejectedCount` again.
--
-- ⚠️ Rows written before this migration keep `Stale` inside
-- `rejectedJobNotFound*`. That cannot be backfilled — the distinction was
-- never stored — so a worker chart spanning the deploy shows the split
-- starting mid-range. Deliberate: inventing a ratio to smooth it would be
-- worse than a visible seam.
ALTER TABLE client_statistics_entity
  ADD COLUMN IF NOT EXISTS "rejectedStaleCount" INTEGER DEFAULT 0 NOT NULL,
  ADD COLUMN IF NOT EXISTS "rejectedStaleDiff1" REAL DEFAULT '0'::real NOT NULL;
