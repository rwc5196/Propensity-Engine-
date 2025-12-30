# 🎯 Propensity Scoring Engine: Complete Setup Guide

## 📖 What Is This?

This system helps you find companies that **need to hire workers** before they even post job listings. It's like having a crystal ball that predicts which businesses will need help!

Think of it like this:
- **Pipeline 1 (Permits)**: "This company is building a bigger warehouse" → They'll need more workers!
- **Pipeline 2 (WARN)**: "That company is closing down" → Their workers need new jobs!
- **Pipeline 3 (Freight)**: "Lots of packages are being shipped" → Warehouses will be busy!
- **Pipeline 4 (Reviews)**: "Employees are unhappy here" → People are quitting!
- **Pipeline 5 (Jobs)**: "This company posted 20 jobs this week" → They're desperate!
- **Pipeline 6 (Inventory)**: "Products flying off shelves" → They need warehouse workers!
- **Pipeline 7 (Unemployment)**: "Nobody in this area is looking for work" → Companies MUST use agencies!

When we combine all these signals, we get a **"Propensity Score"** from 0-100 that tells us how likely a company is to buy our staffing services.

---

## 🗂️ Project Structure (What's In Each Folder)

```
propensity_engine/
│
├── docs/                          # 📚 Instructions (you are here!)
│   └── STEP_BY_STEP_GUIDE.md
│
├── config/                        # ⚙️ Settings files
│   ├── .env.example               # Template for your secret keys
│   └── settings.py                # Main configuration
│
├── database/                      # 💾 Database stuff
│   ├── schema.sql                 # Creates all the tables
│   └── connection.py              # How to connect to the database
│
├── pipelines/                     # 🔧 The 7 data collectors
│   ├── __init__.py
│   ├── pipeline_1_permits.py      # Building permits (expansion signal)
│   ├── pipeline_2_warn.py         # Layoff notices (distress signal)
│   ├── pipeline_3_macro.py        # Economic trends (timing signal)
│   ├── pipeline_4_glassdoor.py    # Employee reviews (churn signal)
│   ├── pipeline_5_jobs.py         # Job postings (demand signal)
│   ├── pipeline_6_inventory.py    # SEC filings (throughput signal)
│   └── pipeline_7_labor.py        # Unemployment data (market signal)
│
├── orchestration/                 # 🤖 AI-powered automation
│   ├── scoring_engine.py          # Calculates propensity scores
│   └── sales_agent.py             # AI that writes emails
│
├── scripts/                       # 🚀 Helper scripts
│   ├── run_all_pipelines.py       # Runs everything
│   └── setup_database.py          # Sets up Supabase
│
└── requirements.txt               # 📦 List of packages needed
```

---

## 🚀 SETUP INSTRUCTIONS (Follow These Steps!)

### STEP 1: Get Your Free Accounts (10 minutes)

You need accounts on these **free** websites. Open each link and sign up:

| Service | What It Does | Link | Cost |
|---------|-------------|------|------|
| **Supabase** | Stores all your data | https://supabase.com | FREE |
| **Google AI Studio** | Powers the AI | https://aistudio.google.com | FREE |
| **FRED** | Economic data | https://fred.stlouisfed.org/docs/api/api_key.html | FREE |
| **Railway** | Runs your code | https://railway.app | $5/month |

**Write down these keys when you get them:**
- [ ] Supabase URL: `_______________________`
- [ ] Supabase API Key: `_______________________`
- [ ] Google Gemini API Key: `_______________________`
- [ ] FRED API Key: `_______________________`

---

### STEP 2: Install Python (5 minutes)

**If you don't have Python installed:**

**Windows:**
1. Go to https://www.python.org/downloads/
2. Download Python 3.13 (click the big yellow button)
3. Run the installer
4. ⚠️ **IMPORTANT**: Check the box that says "Add Python to PATH"
5. Click "Install Now"

**Mac:**
```bash
brew install python@3.13
```

**Check it worked:** Open a terminal/command prompt and type:
```bash
python --version
```
You should see `Python 3.13.x`

---

### STEP 3: Download This Project (2 minutes)

Open your terminal/command prompt and run these commands one at a time:

```bash
# Go to where you want to save the project
cd Documents

# Create a new folder
mkdir propensity_engine
cd propensity_engine
```

---

### STEP 4: Create Your Settings File (5 minutes)

Create a file called `.env` in your project folder. Copy this and fill in your keys:

```
# ===========================================
# 🔐 SECRET KEYS - KEEP THESE PRIVATE!
# ===========================================

# Supabase (your database)
SUPABASE_URL=https://xxxxx.supabase.co
SUPABASE_KEY=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.xxxxx

# Google Gemini (the AI)
GEMINI_API_KEY=AIzaSyxxxxxxxxxxxxxxxxx

# FRED (economic data)
FRED_API_KEY=xxxxxxxxxxxxxxxx

# SEC (for inventory data) - No key needed, just your email
SEC_USER_AGENT=PropensityEngine your@email.com

# BLS (unemployment data) - Optional, increases limits
BLS_API_KEY=xxxxxxxxxxxxxxxx

# ===========================================
# 🎯 TARGET SETTINGS
# ===========================================

# Which area to search (start with DFW)
TARGET_CITIES=Dallas,Fort Worth,Arlington,Irving,Plano
TARGET_STATE=TX
TARGET_ZIPS=75001,75006,75019,75038,75039,75050,75060,75061,75062,76010,76011,76012
```

---

### STEP 5: Install Required Packages (3 minutes)

In your terminal, run:

```bash
pip install -r requirements.txt
```

If you get an error, try:
```bash
pip install --break-system-packages -r requirements.txt
```

---

### STEP 6: Set Up Your Database (5 minutes)

1. Go to https://supabase.com and log in
2. Click "New Project"
3. Name it `propensity_engine`
4. Set a secure password (save it!)
5. Choose a region close to you
6. Wait 2 minutes for it to create

Then run:
```bash
python scripts/setup_database.py
```

---

### STEP 7: Test Each Pipeline (15 minutes)

Test each pipeline one by one to make sure they work:

```bash
# Test Pipeline 1: Building Permits
python -m pipelines.pipeline_1_permits

# Test Pipeline 2: WARN Notices
python -m pipelines.pipeline_2_warn

# Test Pipeline 3: Economic Data
python -m pipelines.pipeline_3_macro

# ... and so on for all 7
```

Each one should print some data. If you see errors, check your `.env` file!

---

### STEP 8: Run Everything Together (2 minutes)

```bash
python scripts/run_all_pipelines.py
```

This will:
1. ✅ Collect data from all 7 sources
2. ✅ Calculate propensity scores
3. ✅ Save everything to your database

---

### STEP 9: Set Up Automatic Daily Runs (10 minutes)

**Option A: Use Railway (Recommended)**

1. Go to https://railway.app
2. Connect your GitHub account
3. Click "New Project" → "Deploy from GitHub"
4. Select your propensity_engine repo
5. Add your environment variables (the `.env` stuff)
6. Railway will run your code automatically!

**Option B: Use GitHub Actions (Free)**

See the `.github/workflows/daily_run.yml` file for automatic scheduling.

---

## 📊 Understanding the Propensity Score

The final score (0-100) is calculated like this:

| Signal | Weight | What High Score Means |
|--------|--------|----------------------|
| Expansion (Permits) | 25% | Company is physically growing |
| Distress (WARN) | 20% | Competitor nearby is failing |
| Job Velocity | 20% | Posting many jobs (high churn) |
| Sentiment (Glassdoor) | 15% | Employees are unhappy |
| Market Tightness (BLS) | 10% | Hard to find workers locally |
| Macro Trends | 10% | Economy favors hiring |

**Score Interpretation:**
- **80-100**: 🔥 HOT LEAD - Contact immediately!
- **60-79**: 👍 WARM LEAD - Add to outreach list
- **40-59**: 🤔 COOL LEAD - Monitor for changes
- **0-39**: ❄️ COLD - Not ready yet

---

## 🆘 Troubleshooting (When Things Go Wrong)

### "ModuleNotFoundError"
```bash
pip install [missing_module_name]
```

### "Connection refused" (Supabase)
- Check your SUPABASE_URL and SUPABASE_KEY in `.env`
- Make sure there are no extra spaces

### "API rate limit exceeded"
- Wait 1 hour and try again
- These free APIs have daily limits

### "No data returned"
- The filters might be too strict
- Try expanding the date range or zip codes

---

## 🎓 Glossary (Big Words Explained)

| Term | Simple Explanation |
|------|-------------------|
| **Pipeline** | A program that collects one type of data |
| **API** | A way for programs to talk to websites |
| **Propensity Score** | A number showing how likely someone is to buy |
| **WARN Notice** | A legal paper companies file before layoffs |
| **Building Permit** | Permission to build or expand a building |
| **ETL** | Extract (get data), Transform (clean it), Load (save it) |
| **Microservice** | A small program that does one specific job |

---

## 🏁 What's Next?

After setup, you can:
1. **View your data** at https://supabase.com → Your Project → Table Editor
2. **Customize filters** in `config/settings.py`
3. **Add more cities** by updating TARGET_CITIES in `.env`
4. **Set up email automation** using the `orchestration/sales_agent.py`

---

## 📞 Need Help?

If you get stuck:
1. Read the error message carefully
2. Google the exact error text
3. Check that all your API keys are correct
4. Make sure Python 3.13 is installed

Good luck! 🚀
