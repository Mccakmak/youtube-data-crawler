# 📦 YouTube Data Crawler

This project provides a modular Python framework to search, collect, and translate YouTube video data for use in research, media analysis, or social data mining. It supports keyword-based search, video/channel-level metadata collection, and translation of multilingual content using Google Translate.

---

## 🧠 What This Project Does

This framework enables you to:

- 🔍 Search for YouTube videos using specific **keywords** within a date range.
- 📺 Collect detailed **video**, **channel**, and optionally **comment metadata** using the **YouTube Data API v3**.
- 🌍 Translate non-English **titles, descriptions**, or **comments** into English using **Google Translate API**.
- 🚀 Handle large-scale data collection via **parallel processing** and **API key rotation**.
- 💾 Save all results as clean, structured `.csv` files for analysis or integration.

---
## 📄 Script Descriptions

### 🔹 `keyword_search.py`

This script searches YouTube for videos based on keywords and a specified time range.

- ✅ Inputs:
  - `input_data/keywords.csv`: A list of keywords.
  - API keys from `keys_related/valid_api_keys.txt`
- ⚙️ Functionality:
  - Performs parallel keyword searches using multiple API keys.
  - Avoids hitting API quota by rotating keys.
  - Filters duplicates.
- 📤 Output:
  - `input_data/New_SCS_YT_regular_video.csv` — contains matched `video_id`s and `keyword` tags.

---

### 🔹 `YouTube Data Collection.py`

This is the **core metadata collection script**. It accepts a CSV of `video_id`s or `channel_id`s and collects associated metadata.

- ✅ Inputs:
  - CSV file from `input_data/` with either `video_id` or `channel_id`.
- ⚙️ Functionality:
  - Fetches video metadata (title, description, views, likes, publish date, etc.).
  - Fetches channel metadata (channel title, country, subscriber count, etc.).
  - Optionally collects top-level comments for each video.
  - Handles API pagination, errors, and retries.
- 📤 Output files (saved in `output_data/`):
  - `{input_name}_video.csv`: Video-level metadata.
  - `{input_name}_channels.csv`: Channel-level metadata.
  - `{input_name}_comment.csv`: Raw comments (if enabled).
  - `{input_name}_comment_combined.csv`: Grouped comments per video (if enabled).

---

### 🔹 `translate.py`

This script translates non-English text (e.g., titles, descriptions, comments) to English.

- ✅ Inputs:
  - A CSV file with a text column you want to translate.
- ⚙️ Functionality:
  - Detects language using `langdetect`.
  - Skips English content.
  - Uses `googletrans` with retry/backoff support.
  - Processes text in parallel using multiprocessing.
- 📤 Output:
  - `{original_filename}_translated.csv`: Same data with translated text column.

---

## 🧪 Example Use Case

You want to:
1. Collect YouTube videos related to “climate change” published in 2024.
2. Extract the video titles, descriptions, and channel data.
3. Translate all non-English titles and descriptions into English.
