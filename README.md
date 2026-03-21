# email-to-gdoc

A Python script that aggregates monthly feature announcement emails from Gmail into a formatted Google Doc — complete with a table of contents, page breaks, inline images, hyperlinks, upcoming events, and recent blog posts.

## What it does

Each month, the script:

1. **Searches Gmail** for `[Feature Announcement][GA]` and `[Feature Announcement][Preview]` emails in a given month
2. **Extracts** the customer-shareable release note section (after the `— Release Note can be shared externally —` divider), stripping boilerplate, signatures, and metadata tags
3. **Creates a Google Doc** with:
   - Table of contents with hyperlinks to each section
   - "General Availability" section and "Preview" section, each feature on its own page
   - Inline images (downloaded from Gmail, uploaded to GCS, inserted via Docs API)
   - Hyperlinks restored from the original HTML emails
4. **Appends an Events section** scraped from the events page (upcoming only, with dates and links)
5. **Appends a Blog Posts section** from the blog's RSS feed — last 3 months, grouped by month, with hyperlinks

## Prerequisites

- Python 3.9+
- A Google Cloud project with the following APIs enabled:
  - Gmail API
  - Google Docs API
  - Google Drive API
  - Cloud Storage API
- A GCS bucket (publicly readable) for hosting inline images
- OAuth 2.0 credentials (Desktop app type) downloaded as `credentials.json`

## Setup

### 1. Install dependencies

```bash
pip install -r requirements.txt
```

### 2. Create OAuth credentials

1. Go to [Google Cloud Console](https://console.cloud.google.com/) → APIs & Services → Credentials
2. Create an **OAuth 2.0 Client ID** (Application type: Desktop app)
3. Download the JSON file and save it (default: `~/credentials.json`)

### 3. Configure environment variables

```bash
export GCS_PROJECT=your-gcp-project-id
export GCS_BUCKET=your-gcs-bucket-name

# Optional — override default paths
export CREDENTIALS_PATH=~/credentials.json
export TOKEN_PATH=~/token.json
```

Or edit the constants at the top of `email_to_doc.py` directly.

### 4. Create a GCS bucket

```bash
gcloud storage buckets create gs://your-gcs-bucket-name \
  --project=your-gcp-project-id \
  --location=US
```

Make sure the bucket allows public object reads, or the Docs API will not be able to display the images.

### 5. First run — OAuth consent

On the first run the script will open a browser window for OAuth consent. After you approve, a `token.json` is saved locally for subsequent runs.

## Usage

```bash
python3 email_to_doc.py
```

By default it targets the current hard-coded `year` and `month` in `main()` at the bottom of the file. Edit those to change the target month:

```python
if __name__ == '__main__':
    main(year=2026, month=2)
```

The script prints progress to stdout and ends with the Google Doc URL.

## How it works

### Multi-pass Google Doc construction

Because the Docs API requires absolute character offsets, the document is built in several passes:

| Pass | Action |
|------|--------|
| 1 | Insert all text + heading/paragraph styles in one `batchUpdate` |
| 2 | Apply ToC hyperlinks (using heading positions from Pass 1) |
| 3 | Insert section (page) breaks (must happen before marker deletion) |
| 3.5 | Apply `<a href>` hyperlinks and delete `LNKS`/`LNKE` markers (fresh doc fetch) |
| 4 | Replace `IMG_PLACEHOLDER_N` tokens with actual inline images (fresh doc fetch) |

### Image pipeline

```
Gmail attachment → download bytes → upload to GCS (publicRead) → insertInlineImage URI
```

### HTML link markers

`<a href="url">text</a>` in HTML emails becomes `LNKS{n}Ztext LNKE{n}Z` in the working text buffer so that link positions survive plain-text cleaning. Pass 3.5 finds these markers in the live document and applies `updateTextStyle` (with the URL) before deleting them.

### Global placeholder renaming

When processing multiple emails, image and link placeholder numbers are re-numbered globally using a single-pass `re.sub` with a lookup function — this avoids substring collision bugs (e.g. `IMG_PLACEHOLDER_1` matching inside `IMG_PLACEHOLDER_10`).

## File structure

```
email_to_doc.py   — main script
requirements.txt  — Python dependencies
.gitignore        — excludes credentials.json, token.json
```

## Notes

- `credentials.json` and `token.json` are excluded from version control (see `.gitignore`). Never commit them.
- The GCS bucket name must be globally unique across all of GCS.
- The Docs API has a rate limit of ~60 writes/minute. The script uses exponential backoff automatically.
- macOS users: the script disables SSL verification for `urllib.request` calls due to a known Python/macOS certificate issue. If you are on Linux you can remove the `ssl.CERT_NONE` context.
