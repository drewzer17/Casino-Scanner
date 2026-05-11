-- migrations/001_create_sitreps.sql
-- AI Buildout sitreps table — Phase A schema
-- Run via psycopg2 against DATABASE_URL.

CREATE TABLE IF NOT EXISTS sitreps (
    ticker VARCHAR(10) PRIMARY KEY,
    company_name VARCHAR(255) NOT NULL,
    last_updated DATE DEFAULT CURRENT_DATE,
    what_they_do TEXT,
    hidden_angles JSONB,
    bear_case TEXT,
    contrarian_case TEXT,
    what_kills_it TEXT,
    kill_probability_low INT,
    kill_probability_high INT,
    kill_horizon_months INT,
    kill_components JSONB,
    winners JSONB,
    source_pdf VARCHAR(255),
    raw_text TEXT NOT NULL,
    sections_parsed INT NOT NULL,
    parse_warnings JSONB,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_sitreps_sections_parsed ON sitreps(sections_parsed);
CREATE INDEX IF NOT EXISTS idx_sitreps_company ON sitreps(company_name);
