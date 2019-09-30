from pathlib import Path
import csv
from internet_scholar import AthenaDatabase, read_dict_from_s3_url, AthenaLogger, compress
import googleapiclient.discovery
import logging
import json
from datetime import datetime
import boto3
import argparse
import time
from googleapiclient.errors import HttpError


CREATE_VIDEO_RELATED_JSON = """
create external table if not exists youtube_related_video
(
    kind string,
    etag string,
    id   struct<
        videoId: string,
        kind:    string
    >,
    relatedToVideoId string,
    retrieved_at timestamp,
    rank int
)
PARTITIONED BY (creation_date String)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
    'serialization.format' = '1',
    'ignore.malformed.json' = 'true'
)
LOCATION 's3://{s3_bucket}/youtube_related_video/'
TBLPROPERTIES ('has_encrypted_data'='false')
"""


TRENDING_VIDEOS = """
select
  youtube.id as id,
  youtube.snippet.title as title,
  youtube.snippet.channelId as channel_id,
  youtube.snippet.channelTitle as channel_title,
  count(distinct twitter.user.id) as mentions
from
  twitter_stream as twitter,
  unnest(twitter.entities.urls) t(tweet_url),
  validated_url,
  youtube_video_snippet as youtube
where
  twitter.creation_date = cast(current_date - interval '1' day as varchar) and
  validated_url.url = tweet_url.expanded_url and
  url_extract_host(validated_url.validated_url) = 'www.youtube.com' and
  url_extract_parameter(validated_url.validated_url, 'v') = youtube.id
group by
  youtube.id,
  youtube.snippet.title,
  youtube.snippet.channelId,
  youtube.snippet.channelTitle
order by
  mentions desc
limit {};
"""


class YoutubeRelatedVideo:
    WAIT_WHEN_SERVICE_UNAVAILABLE = 30
    NUMBER_OF_VIDEOS = 50
    NUMBER_OF_RELATED_VIDEOS = 10

    def __init__(self, credentials, athena_data, s3_admin, s3_data):
        self.credentials = credentials
        self.athena_data = athena_data
        self.s3_admin = s3_admin
        self.s3_data = s3_data

    def collect_related_video(self, region_code):
        athena_db = AthenaDatabase(database=self.athena_data, s3_output=self.s3_admin)

        trending_filename = Path(Path(__file__).parent, 'tmp', 'trending.csv')
        Path(trending_filename).parent.mkdir(parents=True, exist_ok=True)
        trending_videos = athena_db.query_athena_and_download(
            query_string=TRENDING_VIDEOS.format(self.NUMBER_OF_VIDEOS),
            filename=trending_filename)

        with open(trending_videos, newline='', encoding="utf8") as csv_reader:
            output_json = Path(Path(__file__).parent, 'tmp', 'youtube_related_video.json')
            Path(output_json).parent.mkdir(parents=True, exist_ok=True)

            with open(output_json, 'w') as json_writer:
                reader = csv.DictReader(csv_reader)
                current_key = 0
                youtube = googleapiclient.discovery.build(serviceName="youtube",
                                                          version="v3",
                                                          developerKey=self.credentials[current_key]['developer_key'],
                                                          cache_discovery=False)
                num_videos = 0
                for trending_video in reader:
                    service_unavailable = 0
                    no_response = True
                    while no_response:
                        try:
                            response = youtube.search().list(part="id",
                                                             type='video',
                                                             regionCode=region_code,
                                                             relatedToVideoId=trending_video['id'],
                                                             maxResults=self.NUMBER_OF_RELATED_VIDEOS).execute()
                            no_response = False
                        except HttpError as e:
                            if "403" in str(e):
                                logging.info("Invalid {} developer key: {}".format(
                                    current_key,
                                    self.credentials[current_key]['developer_key']))
                                current_key = current_key + 1
                                if current_key >= len(self.credentials):
                                    raise
                                else:
                                    youtube = googleapiclient.discovery.build(serviceName="youtube",
                                                                              version="v3",
                                                                              developerKey=
                                                                              self.credentials[current_key][
                                                                                  'developer_key'],
                                                                              cache_discovery=False)
                            elif "503" in str(e):
                                logging.info("Service unavailable")
                                service_unavailable = service_unavailable + 1
                                if service_unavailable <= 10:
                                    time.sleep(self.WAIT_WHEN_SERVICE_UNAVAILABLE)
                                else:
                                    raise
                            else:
                                raise

                    rank = 1
                    for item in response.get('items', {}):
                        item['relatedToVideoId'] = trending_video['id']
                        item['retrieved_at'] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
                        item['rank'] = rank
                        rank = rank + 1
                        num_videos = num_videos + 1
                        json_writer.write("{}\n".format(json.dumps(item)))

        logging.info("Compress file %s", output_json)
        compressed_file = compress(filename=output_json, delete_original=True)

        s3 = boto3.resource('s3')
        s3_filename = "youtube_related_video/creation_date={today}/{num_videos}.json.bz2".format(
            today=datetime.utcnow().strftime("%Y-%m-%d"),
            num_videos=num_videos)
        logging.info("Upload file %s to bucket %s at %s", compressed_file, self.s3_data, s3_filename)
        s3.Bucket(self.s3_data).upload_file(str(compressed_file), s3_filename)

        logging.info("Recreate table for Youtube related video snippets")
        athena_db.query_athena_and_wait(query_string="DROP TABLE IF EXISTS youtube_related_video")
        athena_db.query_athena_and_wait(query_string=CREATE_VIDEO_RELATED_JSON.format(s3_bucket=self.s3_data))
        athena_db.query_athena_and_wait(query_string="MSCK REPAIR TABLE youtube_related_video")

        logging.info("Concluded collecting related video snippets")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--config', help='S3 Bucket with configuration', required=True)
    args = parser.parse_args()

    config = read_dict_from_s3_url(url=args.config)
    logger = AthenaLogger(app_name="youtube_related_video",
                          s3_bucket=config['aws']['s3-admin'],
                          athena_db=config['aws']['athena-admin'])
    try:
        youtube_related_video = YoutubeRelatedVideo(credentials=config['youtube'],
                                                    athena_data=config['aws']['athena-data'],
                                                    s3_admin=config['aws']['s3-admin'],
                                                    s3_data=config['aws']['s3-data'])
        youtube_related_video.collect_related_video(region_code=config['parameter']['region_code'])
    finally:
        logger.save_to_s3()
        logger.recreate_athena_table()


if __name__ == '__main__':
    main()
