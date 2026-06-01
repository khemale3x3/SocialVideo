# Instagram Downloader with Posts, Reels, Images, and Profile Metadata

A Selenium-based scraper that downloads Instagram posts and reels media while saving full metadata and profile information.

## What this script does

- Scrapes Instagram profile pages for timeline posts and reels
- Downloads video files and photo files separately
- Extracts full metadata for each media item
- Saves `profile_info.json` and the user's HD profile picture as `username.jpg`
- Writes a combined metadata summary JSON
- Organizes output by profile and media type

## Features

- Multi-threaded profile processing
- Supports posts, reels, and tagged endpoint scraping
- Saves image metadata inside the same metadata set
- Downloads HD profile pictures
- Saves `profile_info.json` plus combined `username_full_metadata.json`
- Progress bar reporting and retry-safe downloads

## Prerequisites

1. Python 3.8+
2. Chrome browser installed
3. ChromeDriver available for Selenium
4. Instagram session IDs

## Install dependencies

```bash
pip install selenium pandas tqdm requests
```

## Setup

### 1. Get Instagram Session IDs

1. Log into Instagram in Chrome.
2. Open Developer Tools (F12).
3. Navigate to Application/Storage > Cookies > https://www.instagram.com.
4. Copy the `sessionid` cookie value.

### 2. Configure input

Create `input.csv` with profile URLs:

```csv
url
https://www.instagram.com/username1
https://www.instagram.com/username2
```

The script will automatically create or append to `inputdone.csv`.

## Running the script

```bash
python semifinal.py
```

## Output layout

```
videos/
└── username1/
    ├── profile_info.json
    ├── username1.jpg
    ├── username1_full_metadata.json
    ├── username1_posts_metadata.csv
    ├── username1_reels_metadata.csv
    ├── posts/
    │   ├── images/
    │   │   └── 20240101_ABC123_photo.jpg
    │   └── videos/
    │       └── 20240101_ABC123_video.mp4
    └── reels/
        ├── images/
        └── videos/
```

## Metadata details

Saved metadata includes:

- username and profile details
- user friendship status fields and full profile info
- post/reel ID, shortcode, timestamp, caption
- hashtags and mentions
- likes, comments, views, shares, saved counts
- image URL list and HD image versions
- owner/profile fields and media type

## Notes

- Image posts are downloaded in the `images` subfolders.
- Video posts and reels are downloaded in the `videos` subfolders.
- The script saves HD profile pics in the profile root folder.
- `profile_info.json` contains the raw user object from Instagram GraphQL.

## Troubleshooting

- If no content is downloaded, confirm session IDs are valid.
- Reduce `Config.MAX_WORKERS` if Instagram rate limits requests.
- Use `Config.HEADLESS = False` for debugging browser behavior.

## Safety

- Use only on public accounts you have permission to scrape.
- Respect Instagram's Terms of Service and rate limits.

## License

Use at your own risk. This script is provided for educational purposes only.</content>
<parameter name="filePath">e:\Analyzenovember11-11-2025\videosdownloads\README.md