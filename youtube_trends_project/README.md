# youtube_trends_project

End-to-end mini data-engineering project:

- Pulls **YouTube trending videos, categories, regions, and comments** from the YouTube Data API v3
- Uses **Kafka producers** (one per data object) to publish JSON messages
- Provides **PySpark jobs** to read those Kafka topics and validate/check them
- Designed with **OOP, SOLID principles, and simple dependency injection**

## Quick Start

### 1. Create & activate virtualenv (Windows PowerShell)

```powershell
cd path\to\youtube_trends_project
python -m venv .venv
.\.venv\Scripts\activate
```

### 2. Install dependencies

```powershell
pip install -e .
```

### 3. Configure environment

Copy `.env.example` to `.env` and fill in values:

```powershell
copy .env.example .env
```

Edit `.env` with your YouTube API key and Kafka bootstrap servers.

### 4. Run producers

```powershell
python -m youtube_trends.main --region IL
```

To fetch comments for a specific video:

```powershell
python -m youtube_trends.main --region IL --fetch-comments-for-video <VIDEO_ID>
```

### 5. Run Spark check job

Ensure your Kafka broker is running and accessible, then:

```powershell
python -m youtube_trends.jobs.spark_check_videos
```
