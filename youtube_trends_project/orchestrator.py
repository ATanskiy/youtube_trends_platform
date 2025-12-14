import json
import random
import isodate
from datetime import datetime, timezone
import pandas as pd
from youtube_client import YouTubeClient
from youtube_client_pandas import YouTubeClientPandas
from kafka_producer import KafkaProducerService
from settings import (
    YOUTUBE_API_KEY,
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC_REGIONS,
    KAFKA_TOPIC_LANGUAGES,
    KAFKA_TOPIC_CATEGORIES,
    KAFKA_TOPIC_VIDEOS
)

class Orchestrator:
    def __init__(self):
        self.youtube_client = YouTubeClient(YOUTUBE_API_KEY)
        self.youtube_client_pandas = YouTubeClientPandas(YOUTUBE_API_KEY)        
        self.kafka_producer = KafkaProducerService(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)        

    def start(self, name:str, regions):
        if name == "regions":
            self.produce_regions() 
        if name == "languages":
            self.produce_languages()    
        if name == "categories":
            self.produce_categories(["US"])    
        if name == "videos":
            self.produce_videos(regions)
    
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
        for region in regions:
            region_dict = {}                        
            region_dict["id"] = region.get("id")
            region_dict["name"] = region.get("snippet").get("name")            
            region_dict["created_at"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
            self.kafka_producer.send(KAFKA_TOPIC_REGIONS, region.get("id"), region_dict)

    def produce_languages(self):              
        languages = self.youtube_client.get_languages()        
        for language in languages:
            language_dict = {}                        
            language_dict["id"] = language.get("id")
            language_dict["name"] = language.get("snippet").get("name")            
            language_dict["created_at"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
            self.kafka_producer.send(KAFKA_TOPIC_LANGUAGES, language.get("id"), language_dict)        


    def produce_categories(self, regions):
        for region in regions:
            for category in self.youtube_client.get_video_categories(region):                
                category_dict = {}            
                category_dict["id"] = int(category.get("id"), 0)
                category_dict["name"] = category.get("snippet")["title"]
                category_dict["created_at"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
                self.kafka_producer.send(KAFKA_TOPIC_CATEGORIES, category.get("id"), category_dict)       


    def produce_videos(self, regions, max_results=50):        
        LANGUAGE_FIX_MAP = {
            "iw": "he",  # Hebrew
            "ji": "yi",  # Yiddish
            "in": "id"   # Indonesian
        }

        for region in regions:
            for video in self.youtube_client.get_videos(region, max_results=max_results):
                video_dict = {}                            
                video_dict["id"] = video.get("id")
                snippet = video.get("snippet", {})
                video_dict["title"] = snippet.get("localized")["title"]
                video_dict["description"] = snippet.get("localized")["description"]
                video_dict["region_id"] = region
                video_dict["language_id"] =  snippet.get("defaultAudioLanguage") or snippet.get("audioLanguage")  or "und"
                video_dict["language_id_src"] =  (snippet.get("defaultAudioLanguage") or snippet.get("audioLanguage")  or "und").split("-")[0]
                video_dict["published_at"] = snippet.get("publishedAt") 
                video_dict["channel_id"] = snippet.get("channelId")
                video_dict["channel_title"] = snippet.get("channelTitle")
                video_dict["category_id"] = int(snippet.get("categoryId"), 0)
                video_dict["duration"] = str(isodate.parse_duration(video.get("contentDetails")["duration"]))
                statistics = video.get("statistics", {})
                video_dict["view_count"] =   int(statistics.get("viewCount", 0))
                video_dict["like_count"] = int(statistics.get("likeCount", 0))
                video_dict["favorite_count"] = int(statistics.get("favoriteCount", 0))
                video_dict["comment_count"] = int(statistics.get("commentCount", 0))
                video_dict["created_at"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
                self.kafka_producer.send(KAFKA_TOPIC_VIDEOS, video.get("id"), video_dict)


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