-- Migration 002: Add category, subcategory, and price-at-add fields to sitreps
-- Safe to re-run (all statements use IF NOT EXISTS / IF EXISTS guards).

ALTER TABLE sitreps ADD COLUMN IF NOT EXISTS category        VARCHAR(255);
ALTER TABLE sitreps ADD COLUMN IF NOT EXISTS subcategory     VARCHAR(255);
ALTER TABLE sitreps ADD COLUMN IF NOT EXISTS price_at_add    NUMERIC(12,4);
ALTER TABLE sitreps ADD COLUMN IF NOT EXISTS price_at_add_date DATE;

CREATE INDEX IF NOT EXISTS idx_sitreps_category ON sitreps(category);
