-- ===========================================
-- 📊 PROPENSITY ENGINE DATABASE SCHEMA
-- ===========================================
-- Run this in Supabase SQL Editor to create all tables
-- https://supabase.com → Your Project → SQL Editor
-- ===========================================

-- Enable UUID extension (usually already enabled in Supabase)
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- ===========================================
-- 🏢 COMPANY MASTER TABLE
-- ===========================================
-- Central table for all companies we track

CREATE TABLE IF NOT EXISTS company_master (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    
    -- Basic Info
    company_name VARCHAR(255) NOT NULL,
    normalized_name VARCHAR(255), -- "acme logistics llc" -> "acme logistics"
    
    -- Location
    address TEXT,
    city VARCHAR(100),
    state VARCHAR(2),
    zip_code VARCHAR(10),
    
    -- External IDs (for data enrichment)
    ticker VARCHAR(10),           -- Stock ticker if public
    cik VARCHAR(10),              -- SEC CIK number
    linkedin_url TEXT,
    glassdoor_url TEXT,
    website_url TEXT,
    
    -- Classification
    industry VARCHAR(100),
    company_size VARCHAR(50),     -- 'small', 'medium', 'large', 'enterprise'
    
    -- Metadata
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    data_source VARCHAR(50),      -- Where we first found this company
    
    -- Constraints
    UNIQUE(normalized_name, zip_code)
);

-- Index for fast lookups
CREATE INDEX IF NOT EXISTS idx_company_zip ON company_master(zip_code);
CREATE INDEX IF NOT EXISTS idx_company_city ON company_master(city);
CREATE INDEX IF NOT EXISTS idx_company_name ON company_master(normalized_name);


-- ===========================================
-- 📈 SIGNAL HISTORY TABLE
-- ===========================================
-- Stores all signals over time for trend analysis

CREATE TABLE IF NOT EXISTS signal_history (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    company_id UUID REFERENCES company_master(id) ON DELETE CASCADE,
    
    -- When this snapshot was taken
    record_date DATE NOT NULL DEFAULT CURRENT_DATE,
    
    -- Pipeline 1: Expansion (Permits)
    permit_value DECIMAL,
    permit_description TEXT,
    permit_date DATE,
    expansion_score DECIMAL,
    
    -- Pipeline 2: Distress (WARN)
    nearby_warn_count INTEGER DEFAULT 0,
    nearest_warn_distance_miles DECIMAL,
    nearest_warn_company VARCHAR(255),
    distress_score DECIMAL,
    
    -- Pipeline 3: Macro (Economic)
    freight_trend VARCHAR(20),    -- 'expanding', 'stable', 'contracting'
    macro_modifier DECIMAL DEFAULT 1.0,
    
    -- Pipeline 4: Glassdoor Sentiment
    glassdoor_rating DECIMAL,
    glassdoor_review_count INTEGER,
    sentiment_score DECIMAL,
    
    -- Pipeline 5: Job Velocity
    job_post_count_30d INTEGER,
    job_velocity_score DECIMAL,
    
    -- Pipeline 6: Inventory Turnover
    inventory_turnover_ratio DECIMAL,
    turnover_score DECIMAL,
    
    -- Pipeline 7: Labor Market
    local_unemployment_rate DECIMAL,
    market_tightness_score DECIMAL,
    
    -- Final Score
    propensity_score INTEGER,     -- 0-100
    score_tier VARCHAR(20),       -- 'hot', 'warm', 'cool', 'cold'
    
    -- Metadata
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    
    -- Constraints
    UNIQUE(company_id, record_date)
);

-- Indexes for analysis queries
CREATE INDEX IF NOT EXISTS idx_signal_date ON signal_history(record_date);
CREATE INDEX IF NOT EXISTS idx_signal_score ON signal_history(propensity_score DESC);
CREATE INDEX IF NOT EXISTS idx_signal_company ON signal_history(company_id);


-- ===========================================
-- 🏗️ RAW PERMITS TABLE
-- ===========================================
-- Raw permit data from Pipeline 1

CREATE TABLE IF NOT EXISTS raw_permits (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    
    -- Source info
    source_city VARCHAR(100) NOT NULL,
    source_dataset VARCHAR(100),
    permit_id VARCHAR(100),
    
    -- Permit details
    issue_date DATE,
    work_description TEXT,
    reported_cost DECIMAL,
    
    -- Location
    address TEXT,
    contractor_name VARCHAR(255),
    
    -- Processing status
    matched_company_id UUID REFERENCES company_master(id),
    is_industrial BOOLEAN DEFAULT FALSE,
    
    -- Metadata
    ingested_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    
    UNIQUE(source_city, permit_id)
);

CREATE INDEX IF NOT EXISTS idx_permits_date ON raw_permits(issue_date);
CREATE INDEX IF NOT EXISTS idx_permits_industrial ON raw_permits(is_industrial) WHERE is_industrial = TRUE;


-- ===========================================
-- ⚠️ RAW WARN NOTICES TABLE
-- ===========================================
-- Raw WARN data from Pipeline 2

CREATE TABLE IF NOT EXISTS raw_warn_notices (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    
    -- Source info
    source_state VARCHAR(2) NOT NULL,
    
    -- WARN details
    company_name VARCHAR(255) NOT NULL,
    notice_date DATE,
    effective_date DATE,
    affected_count INTEGER,
    layoff_type VARCHAR(50),      -- 'layoff', 'closure', 'relocation'
    
    -- Location
    city VARCHAR(100),
    zip_code VARCHAR(10),
    address TEXT,
    
    -- Classification
    is_industrial BOOLEAN DEFAULT FALSE,
    
    -- Metadata
    ingested_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    
    UNIQUE(source_state, company_name, notice_date)
);

CREATE INDEX IF NOT EXISTS idx_warn_date ON raw_warn_notices(notice_date);
CREATE INDEX IF NOT EXISTS idx_warn_zip ON raw_warn_notices(zip_code);


-- ===========================================
-- 💼 RAW JOB POSTINGS TABLE
-- ===========================================
-- Raw job data from Pipeline 5

CREATE TABLE IF NOT EXISTS raw_job_postings (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    
    -- Job details
    company_name VARCHAR(255),
    job_title VARCHAR(255),
    job_description TEXT,
    
    -- Location
    city VARCHAR(100),
    state VARCHAR(2),
    zip_code VARCHAR(10),
    
    -- Source
    source_board VARCHAR(50),     -- 'indeed', 'linkedin', etc.
    job_url TEXT,
    posted_date DATE,
    
    -- Classification
    is_industrial BOOLEAN DEFAULT FALSE,
    matched_company_id UUID REFERENCES company_master(id),
    
    -- Metadata
    ingested_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_jobs_company ON raw_job_postings(company_name);
CREATE INDEX IF NOT EXISTS idx_jobs_date ON raw_job_postings(posted_date);
CREATE INDEX IF NOT EXISTS idx_jobs_zip ON raw_job_postings(zip_code);


-- ===========================================
-- 📊 ECONOMIC INDICATORS TABLE
-- ===========================================
-- Macro data from Pipeline 3

CREATE TABLE IF NOT EXISTS economic_indicators (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    
    -- Indicator info
    series_id VARCHAR(50) NOT NULL,
    series_name VARCHAR(100),
    
    -- Data point
    record_date DATE NOT NULL,
    value DECIMAL NOT NULL,
    
    -- Trend calculation
    pct_change_mom DECIMAL,       -- Month over month
    pct_change_yoy DECIMAL,       -- Year over year
    trend_direction VARCHAR(20),  -- 'up', 'down', 'stable'
    
    -- Metadata
    ingested_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    
    UNIQUE(series_id, record_date)
);


-- ===========================================
-- 📍 ZIP CODE REFERENCE TABLE
-- ===========================================
-- For geography lookups

CREATE TABLE IF NOT EXISTS zip_reference (
    zip_code VARCHAR(10) PRIMARY KEY,
    city VARCHAR(100),
    county VARCHAR(100),
    state VARCHAR(2),
    fips_code VARCHAR(10),        -- For BLS lookups
    latitude DECIMAL,
    longitude DECIMAL
);


-- ===========================================
-- 🎯 HOT LEADS VIEW
-- ===========================================
-- Convenient view for high-propensity targets

CREATE OR REPLACE VIEW hot_leads AS
SELECT 
    cm.id,
    cm.company_name,
    cm.city,
    cm.state,
    cm.zip_code,
    cm.website_url,
    sh.propensity_score,
    sh.score_tier,
    sh.permit_value,
    sh.permit_description,
    sh.job_post_count_30d,
    sh.glassdoor_rating,
    sh.local_unemployment_rate,
    sh.record_date
FROM company_master cm
JOIN signal_history sh ON cm.id = sh.company_id
WHERE sh.propensity_score >= 75
  AND sh.record_date = (
      SELECT MAX(record_date) 
      FROM signal_history 
      WHERE company_id = cm.id
  )
ORDER BY sh.propensity_score DESC;


-- ===========================================
-- 🔄 UPDATE TRIGGER
-- ===========================================
-- Auto-update the updated_at timestamp

CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ language 'plpgsql';

CREATE TRIGGER update_company_master_updated_at
    BEFORE UPDATE ON company_master
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();


-- ===========================================
-- ✅ VERIFICATION
-- ===========================================

-- List all tables to verify creation
SELECT table_name 
FROM information_schema.tables 
WHERE table_schema = 'public' 
ORDER BY table_name;
