import os
import requests
from datetime import datetime, timedelta, timezone
from dateutil import parser
import isodate
from flask import Flask, request, jsonify

app = Flask(__name__)

# Config from environment variables
AIRTABLE_API_KEY = os.environ.get('AIRTABLE_API_KEY')
AIRTABLE_BASE_ID = os.environ.get('AIRTABLE_BASE_ID')
YOUTUBE_API_KEY = os.environ.get('YOUTUBE_API_KEY')

if not all([AIRTABLE_API_KEY, AIRTABLE_BASE_ID, YOUTUBE_API_KEY]):
    raise RuntimeError("Missing one or more environment variables: AIRTABLE_API_KEY, AIRTABLE_BASE_ID, YOUTUBE_API_KEY")

AIRTABLE_BASE_URL = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}"
HEADERS = {
    "Authorization": f"Bearer {AIRTABLE_API_KEY}",
    "Content-Type": "application/json"
}

YOUTUBE_CHANNELS_ENDPOINT = "https://www.googleapis.com/youtube/v3/channels"
YOUTUBE_PLAYLIST_ITEMS_ENDPOINT = "https://www.googleapis.com/youtube/v3/playlistItems"
YOUTUBE_VIDEOS_ENDPOINT = "https://www.googleapis.com/youtube/v3/videos"


def get_airtable_record(table_name, record_id):
    url = f"{AIRTABLE_BASE_URL}/{table_name}/{record_id}"
    resp = requests.get(url, headers=HEADERS)
    resp.raise_for_status()
    return resp.json().get('fields')


def get_uploads_playlist_id(channel_id):
    params = {
        "part": "contentDetails",
        "id": channel_id,
        "key": YOUTUBE_API_KEY
    }
    resp = requests.get(YOUTUBE_CHANNELS_ENDPOINT, params=params)
    resp.raise_for_status()
    items = resp.json().get('items', [])
    if not items:
        return None
    return items[0]['contentDetails']['relatedPlaylists']['uploads']


def get_recent_video_ids(playlist_id, cutoff_date):
    video_ids = []
    next_page = None
    while True:
        params = {
            "part": "contentDetails",
            "playlistId": playlist_id,
            "maxResults": 50,
            "key": YOUTUBE_API_KEY
        }
        if next_page:
            params['pageToken'] = next_page

        resp = requests.get(YOUTUBE_PLAYLIST_ITEMS_ENDPOINT, params=params)
        resp.raise_for_status()
        data = resp.json()

        found_recent = False
        for item in data.get('items', []):
            published_at = parser.parse(item['contentDetails']['videoPublishedAt'])
            if published_at >= cutoff_date:
                video_ids.append(item['contentDetails']['videoId'])
                found_recent = True

        if not found_recent:
            break

        next_page = data.get('nextPageToken')
        if not next_page:
            break

    return video_ids


def get_video_stats_batch(video_ids):
    stats = []
    for i in range(0, len(video_ids), 50):
        batch = video_ids[i:i + 50]
        params = {
            "part": "contentDetails,statistics",
            "id": ",".join(batch),
            "key": YOUTUBE_API_KEY
        }
        resp = requests.get(YOUTUBE_VIDEOS_ENDPOINT, params=params)
        resp.raise_for_status()
        stats.extend(resp.json().get('items', []))
    return stats


def is_longform(iso_duration):
    try:
        duration = isodate.parse_duration(iso_duration)
        return duration.total_seconds() >= 180
    except Exception:
        return False


def update_airtable_record(table_name, record_id, fields):
    url = f"{AIRTABLE_BASE_URL}/{table_name}/{record_id}"
    payload = {"fields": fields}
    resp = requests.patch(url, headers=HEADERS, json=payload)
    resp.raise_for_status()


@app.route('/update-engagement-for-campaign', methods=['POST'])
def update_engagement_for_campaign():
    data = request.get_json()
    if not data or 'campaignRecordId' not in data:
        return jsonify({"error": "Missing 'campaignRecordId' in request body"}), 400

    campaign_record_id = data['campaignRecordId']
    campaign_table = data.get('campaignTableName', 'Campaigns')

    try:
        campaign_fields = get_airtable_record(campaign_table, campaign_record_id)
    except Exception as e:
        return jsonify({"error": f"Failed to fetch campaign record: {str(e)}"}), 500

    linked_influencer_ids = campaign_fields.get('Creator', [])  # Adjust field name if needed

    if not linked_influencer_ids:
        return jsonify({"message": "No linked influencers found for this campaign."}), 200

    CUTOFF_DATE = datetime.utcnow().replace(tzinfo=timezone.utc) - timedelta(days=90)

    influencer_table = 'Influencers'  # adjust if your influencer table has a different name
    update_results = []

    for influencer_id in linked_influencer_ids:
        try:
            influencer_fields = get_airtable_record(influencer_table, influencer_id)
        except Exception as e:
            update_results.append({"influencerId": influencer_id, "status": "failed", "reason": f"Failed to fetch influencer record: {str(e)}"})
            continue

        channel_id = influencer_fields.get('YouTube Channel ID')
        if not channel_id:
            update_results.append({"influencerId": influencer_id, "status": "skipped", "reason": "No YouTube Channel ID"})
            continue

        playlist_id = get_uploads_playlist_id(channel_id)
        if not playlist_id:
            update_results.append({"influencerId": influencer_id, "status": "skipped", "reason": "No uploads playlist"})
            continue

        video_ids = get_recent_video_ids(playlist_id, CUTOFF_DATE)
        if not video_ids:
            update_results.append({"influencerId": influencer_id, "status": "skipped", "reason": "No recent videos"})
            continue

        video_stats = get_video_stats_batch(video_ids)

        longform_views, longform_likes, longform_comments = [], [], []

        for video in video_stats:
            try:
                if is_longform(video['contentDetails']['duration']):
                    stats = video.get('statistics', {})
                    longform_views.append(int(stats.get('viewCount', 0)))
                    longform_likes.append(int(stats.get('likeCount', 0)))
                    longform_comments.append(int(stats.get('commentCount', 0)))
            except Exception:
                continue

        if not longform_views:
            update_results.append({"influencerId": influencer_id, "status": "skipped", "reason": "No longform videos found"})
            continue

        avg_views = int(sum(longform_views) / len(longform_views))
        avg_likes = int(sum(longform_likes) / len(longform_likes))
        avg_comments = int(sum(longform_comments) / len(longform_comments))

        try:
            update_airtable_record(influencer_table, influencer_id, {
                "LGVPV90": avg_views,
                "LGLPV90": avg_likes,
                "LGCPV90": avg_comments
            })
            update_results.append({"influencerId": influencer_id, "status": "success"})
        except Exception as e:
            update_results.append({"influencerId": influencer_id, "status": "failed", "reason": f"Failed to update Airtable: {str(e)}"})

    return jsonify({
        "message": "Engagement update complete.",
        "campaignRecordId": campaign_record_id,
        "results": update_results
    })


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.getenv("PORT", 5000)))
