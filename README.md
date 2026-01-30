# Airflow Healthcare Data Pipeline

An automated data pipeline using Apache Airflow to scrape, transform, and maintain healthcare data (clinic facilities and insurance plans) for Malaysian healthcare providers.

## Features

- **Clinic Data Sync**: Daily sync from Google Sheets to Supabase
- **Clinic Enrichment**: Hourly Google Maps scraping for location data, ratings, and contact info
- **Insurance Scraping**: Daily web scraping of 5 major Malaysian insurance providers
- **LLM Analysis**: Weekly deep-dive PDF analysis using GPT-4o for detailed insurance plan extraction

## Architecture

### Data Sources
- Google Sheets (clinic master list)
- Google Maps (clinic enrichment)
- Insurance provider websites (AIA, Prudential, Allianz, Great Eastern, Etiqa)
- PDF brochures (detailed plan information)

### Tech Stack
- **Apache Airflow**: Workflow orchestration
- **Playwright**: Browser automation for web scraping
- **Supabase**: PostgreSQL database
- **OpenAI GPT-4o**: Intelligent PDF data extraction
- **Google Cloud APIs**: Sheets & Drive integration

## Setup

1. **Clone the repository**
   ```bash
   git clone <your-repo-url>
   cd airflow-healthcare-data-pipeline
   ```

2. **Create virtual environment**
   ```bash
   python3 -m venv .venv
   source .venv/bin/activate  # On Windows: .venv\Scripts\activate
   pip install -r requirements.txt
   ```

3. **Configure environment variables**
   ```bash
   cp env.template .env
   # Edit .env with your credentials (see Configuration section)
   ```

4. **Install Playwright browsers**
   ```bash
   playwright install chromium
   ```

5. **Setup Google Cloud credentials**
   - Create a service account in Google Cloud Console
   - Enable Google Sheets API and Google Drive API
   - Download the JSON key and save to `airflow_home/credentials/service-account.json`

6. **Run Airflow**
   ```bash
   ./setup_env.sh  # Sets up Airflow home directory
   airflow standalone  # Or use Docker: docker-compose up
   ```

## Configuration

Required environment variables in `.env`:

```bash
# Supabase (your database)
SUPABASE_URL=https://your-project.supabase.co
SUPABASE_SERVICE_ROLE_KEY=your-service-role-key

# OpenAI (for LLM insurance analysis)
OPENAI_API_KEY=sk-...

# Scraping settings
ENRICHMENT_HEADLESS=true  # Run browser in headless mode
ENRICHMENT_BATCH_SIZE=50  # Clinics to enrich per run
```

## DAGs

### 1. `clinic_facilities_sync`
- **Schedule**: Daily at 2 AM UTC
- **Purpose**: Sync clinic data from Google Sheets to database
- **Change Detection**: Only runs if the sheet has been modified

### 2. `clinic_enrichment`
- **Schedule**: Hourly
- **Purpose**: Scrape Google Maps for missing clinic details (coordinates, phone, hours, ratings)

### 3. `insurance_sync`
- **Schedule**: Daily at 3 AM UTC
- **Purpose**: Fast scrape of insurance plan listings from provider websites

### 4. `insurance_scraper` (LLM)
- **Schedule**: Weekly (Sundays at 2 AM)
- **Purpose**: Deep PDF analysis for detailed plan information
- **Cost**: ~$0.50-$1.00 per run (OpenAI API usage)

## Database Schema

### Clinic Facilities Table
- Basic info: name, address, city, state, postcode
- Enriched data: latitude, longitude, phone, website, operating_hours
- Metadata: google_place_id, google_rating, is_24_hours

### Insurance Plans Table
- Provider info: provider_name, contact_phone, website
- Plan details: plan_name, plan_type, coverage_type
- Financial: annual_limit, lifetime_limit, monthly_premium_min/max, deductible
- Coverage: outpatient, maternity, dental, optical, mental_health
- Conditions: covered_conditions[], excluded_conditions[]

## Development

### Running Tests
```bash
python test_prudential.py  # Test Prudential scraper
```

### Adding a New Insurance Provider
1. Add provider config to `helpers/insurance_scraper.py` in `PROVIDERS` dict
2. Create `scrape_<provider>` method in `InsuranceScraper` class
3. Add task to `insurance_sync_dag.py`
4. (Optional) Create LLM scraper method for PDF analysis

## Deployment

### Docker
```bash
docker-compose up -d
```

### Production Considerations
- Use Airflow's `CeleryExecutor` or `KubernetesExecutor` for scalability
- Set up monitoring with Airflow's built-in metrics
- Configure email alerts for DAG failures
- Store `.env` securely (use secrets manager in production)

## Security Notes

⚠️ **NEVER commit these files to public repositories:**
- `.env` (contains API keys and database credentials)
- `airflow_home/credentials/*.json` (Google Cloud service account keys)
- `airflow_home/airflow.db` (contains sensitive metadata)
- `.venv/` or `venv/` (virtual environment)

## License

MIT

## Contributors

Built for healthcare data aggregation and analysis.
