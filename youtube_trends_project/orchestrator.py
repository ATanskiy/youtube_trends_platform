import json
import isodate
from datetime import datetime, timezone
import pandas as pd
from youtube_client import YouTubeClient
from youtube_client_pandas import YouTubeClientPandas
from kafka_producer import KafkaProducerService
# from spark_session import SparkSessionFactory
# from spark_consumer import SparkKafkaConsumer
# from spark_schema import SparkSchema
from settings import (
    YOUTUBE_API_KEY,
    KAFKA_BOOTSTRAP_SERVERS,
)


class Orchestrator:
    def __init__(self):
        self.youtube_client = YouTubeClient(YOUTUBE_API_KEY)
        self.youtube_client_pandas = YouTubeClientPandas(YOUTUBE_API_KEY)        
        self.kafka_producer = KafkaProducerService(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)        
        # spark_factory = SparkSessionFactory()
        # self.spark = spark_factory.create_session()
        # self.schema_provider = SparkSchema()
        # self.spark_consumer = SparkKafkaConsumer(self.spark, self.schema_provider)

    def get_regions_list(self):        
        df = self.youtube_client_pandas.get_regions_df() 
        return df['id'].to_list()        
    
    def produce_regions_df(self):        
        df = self.youtube_client_pandas.get_regions_df()         
        json_string = df.to_json()
        print(json_string)
        # for index, row in df.iterrows():
        #     print(row.to_json())

    def produce_categories_df(self, regions:str):                
        categories_df = pd.DataFrame()
        for region in regions:
            df = self.youtube_client_pandas.get_video_categories_df(region)        
            categories_df = pd.concat([categories_df, df], ignore_index=True)

        for index, row in categories_df.iterrows():
            print(row.to_json())
        # print(df.items)
        # self.kafka_producer.send("youtube_categories", category.get("id"), category)

    def produce_videos_df(self, regions):
        videos_df = pd.DataFrame()
        for region in regions:
            df = self.youtube_client_pandas.get_videos_df(region)
            videos_df = pd.concat([videos_df, df], ignore_index=True)
            # self.produce_comments_df(videos_df["id"].to_list())

        # videos_df.to_html("C:/Naya/Python/videos.html")
        for index, row in videos_df.iterrows():
            print(row.to_json())

    def produce_comments_df(self, video_ids):
        for vid in video_ids:
            df = self.youtube_client_pandas.get_comments_df()
            print(df.items)            


    def produce_regions(self):              
        regions = self.youtube_client.get_regions()        
        for region in self.youtube_client.get_regions():            
            region_dict = {}                        
            region_dict["id"] = region.get("id")
            region_dict["name"] = region.get("snippet").get("name")            
            region_dict["created_at"] = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M")
            self.kafka_producer.send("youtube_regions", region.get("id"), region_dict)        


    def produce_categories(self, regions):
        for region in regions:
            for category in self.youtube_client.get_video_categories(region):                
                category_dict = {}            
                category_dict["id"] = category.get("id")
                category_dict["name"] = category.get("snippet")["title"]
                category_dict["created_at"] = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M")
                self.kafka_producer.send("youtube_categories", category.get("id"), category_dict)       


    def produce_videos(self, regions, max_results=50):
        for region in regions:
            for video in self.youtube_client.get_videos(region, max_results=max_results):
                video_dict = {}                            
                video_dict["id"] = video.get("id")
                video_dict["title"] = video.get("snippet")["localized"]["title"]
                video_dict["description"] = video.get("snippet").get("localized").get("description")
                video_dict["region_id"] = region
                video_dict["published_at"] = video.get("snippet")["publishedAt"]
                video_dict["channel_id"] = video.get("snippet")["channelId"]
                video_dict["channel_title"] = video.get("snippet")["channelTitle"]
                video_dict["category_id"] = video.get("snippet")["categoryId"]
                video_dict["duration"] = str(isodate.parse_duration(video.get("contentDetails")["duration"]))
                video_dict["view_count"] =   int(video.get("statistics")["viewCount"])
                video_dict["like_count"] = int(video.get("statistics")["likeCount"])
                video_dict["favorite_count"] = int(video.get("statistics")["favoriteCount"])
                video_dict["comment_count"] = int(video.get("statistics")["commentCount"])
                video_dict["created_at"] = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M")
                # print(video_dict)
                self.kafka_producer.send("youtube_videos", video.get("id"), video_dict)


    def produce_comments(self, video_ids):        
        counter = 0
        for vid in video_ids:
            for comment in self.youtube_client.get_comments(vid):
                comment_dict = {}            
                comment_dict[comment.get("id")] = { \
                    "id": comment.get("id"), \
                    "videoId": comment.get("snippet").get("videoId"), \
                    "text": comment.get("snippet").get("topLevelComment").get("snippet").get("textOriginal")
                    # "channelId": comment.get("items").get("snippet").get("channelId"), \
                    # "totalReplayCount": comment.get("items").get("snippet").get("totalReplyCount") \
                    }            
                comment_json = json.dumps(comment_dict)  
                print(comment_json)
            if counter == 3:
                break
                # self.kafka_producer.send("youtube_comments", comment.get("id"), comment)    
