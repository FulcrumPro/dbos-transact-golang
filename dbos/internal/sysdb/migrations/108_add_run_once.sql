-- Migration 108: Add run_once to workflow_status. A run-once workflow body may
-- start at most once: the second dequeue of a marked row cancels it instead of
-- re-running the body, and Resume/Fork refuse it. ADD COLUMN with a constant
-- default is catalog-only.

ALTER TABLE %s."workflow_status" ADD COLUMN IF NOT EXISTS "run_once" BOOLEAN NOT NULL DEFAULT FALSE;
