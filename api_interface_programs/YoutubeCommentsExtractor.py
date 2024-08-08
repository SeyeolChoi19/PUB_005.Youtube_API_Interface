import os, json 

import datetime as dt 
import pandas   as pd 

from googleapiclient.discovery import build 

class YoutubeCommentsExtractor:
    def __init__(self, service_name: str = "youtube", api_ver = "v3"):
        self.service_name = service_name 
        self.api_version  = api_ver 
        self.current_date = str(dt.datetime.now().date())

    def comment_extraction_settings_method(self, output_path: str, videos_list: list[str]):
        self.output_path = output_path.format(self.current_date)
        self.videos_list = videos_list
        self.youtube_api = build(self.service_name, self.api_version, developerKey = os.getenv("YOUTUBE_API_KEY"))
        os.makedirs(output_path.format(self.current_date), exist_ok = True)
    
        self.youtube_data_dictionary = {
            "extraction_date"    : [], "channel_id"          : [],
            "channel_name"       : [], "video_id"            : [], 
            "video_title"        : [], "video_upload_date"   : [],
            "video_views_count"  : [], "video_likes_count"   : [],
            "comments_count"     : [], "comment_channel_id"  : [], 
            "commentor_name"     : [], "comment_id"          : [],
            "comment_date"       : [], "comment_text"        : [], 
            "comment_like_count" : [], "comment_reply_count" : [],
            "comment_type"       : []
        }
        
    def __extract_video_data(self, video_id: str):
        response       = self.youtube_api.videos().list(id = video_id, part = "statistics,snippet").execute()
        channel_id     = response["items"][0]["snippet"].get("channelId", "unknown")
        channel_name   = response["items"][0]["snippet"].get("channelTitle", "unknown")
        video_title    = response["items"][0]["snippet"].get("title", "unknown")
        upload_date    = response["items"][0]["snippet"].get("publishedAt", "unknown").replace("T", " ").replace("Z", "")
        video_views    = int(response["items"][0]["statistics"].get("viewCount", 0))
        video_likes    = int(response["items"][0]["statistics"].get("likesCount", 0))
        comments_count = int(response["items"][0]["statistics"].get("commentCount", 0))
        results_list   = [self.current_date, channel_id, channel_name, video_id, video_title, upload_date, video_views, video_likes, comments_count]       
        print(video_id)
        
        return results_list
        
    def __parse_comments_response(self, video_stats_list: list[str], response: dict):
        page_token = None 

        if (len(response["items"]) > 0):
            page_token = response.get("nextPageToken", None)

            for json_object in response["items"]:
                comment_id     = json_object["id"]
                comment_date   = json_object["snippet"].get("publishedAt", "9999-99-99T99:99:99Z").replace("T", " ").replace("Z", "")
                commentor_id   = json_object["snippet"]["authorChannelId"].get("value", "unknown")
                commentor_name = json_object["snippet"].get("authorDisplayName", "unknown")
                comment_text   = json_object["snippet"].get("textOriginal", "unknown")
                comment_likes  = int(json_object["snippet"].get("likeCount", 0))
                reply_count    = 0
                comments_data  = [commentor_id, commentor_name, comment_id, comment_date, comment_text, comment_likes, reply_count, "reply"]
                print(comment_id)

                for (key, value) in zip(self.youtube_data_dictionary.keys(), video_stats_list + comments_data):
                    self.youtube_data_dictionary[key].append(value)

        return page_token
    
    def __parse_comment_threads_response(self, video_stats_list: list[str], response: dict):
        page_token = response.get("nextPageToken", None)

        for json_object in response["items"]:
            comment_id     = json_object["id"]
            comment_date   = json_object["snippet"]["topLevelComment"]["snippet"].get("publishedAt", "9999-99-99 99:99:99").replace("T", " ").replace("Z", "")
            commentor_id   = json_object["snippet"]["topLevelComment"]["snippet"]["authorChannelId"].get("value", "unknown")
            commentor_name = json_object["snippet"]["topLevelComment"]["snippet"].get("authorDisplayName", "unknown")
            comment_text   = json_object["snippet"]["topLevelComment"]["snippet"].get("textOriginal", "unknown")
            comment_likes  = int(json_object["snippet"]["topLevelComment"]["snippet"].get("likeCount", 0))
            reply_count    = int(json_object["snippet"].get("totalReplyCount", 0))
            threads_data   = [commentor_id, commentor_name, comment_id, comment_date, comment_text, comment_likes, reply_count, "parent"]
            self.__extract_sub_comments_data(comment_id, video_stats_list)
            print(comment_id)

            for (key, value) in zip(self.youtube_data_dictionary.keys(), video_stats_list + threads_data):
                self.youtube_data_dictionary[key].append(value)
        
        return page_token

    def __extract_sub_comments_data(self, comment_id: str, video_stats_list: list[str]):
        page_token = None 

        while True: 
            response   = self.youtube_api.comments().list(parentId = comment_id, part = "id,snippet", pageToken = page_token).execute()
            page_token = self.__parse_comments_response(video_stats_list, response)

            if (page_token == None):
                break

    def get_video_stats(self):     
        for video_id in self.videos_list:
            video_stats_list, page_token = self.__extract_video_data(video_id), None
        
            while True:
                response   = self.youtube_api.commentThreads().list(videoId = video_id, part = "snippet,id,replies", pageToken = page_token).execute()
                page_token = self.__parse_comment_threads_response(video_stats_list, response)
                print("response_formed")
                
                if (page_token == None):
                    break

    def save_output_data(self):
        output_data = pd.DataFrame(self.youtube_data_dictionary)
        output_data.to_excel(os.path.join(self.output_path, f"{self.current_date} Youtube Comments Data.xlsx"), index = False)

if (__name__ == "__main__"):
    with open("./config/YoutubeAPIConfig.json", "r", encoding = "utf-8") as f:
        config_dict = json.load(f)

    comments_object = YoutubeCommentsExtractor(**config_dict["YoutubeCommentsExtractor"]["constructor"])
    comments_object.comment_extraction_settings_method(**config_dict["YoutubeCommentsExtractor"]["comment_extraction_settings_method"])
    comments_object.get_video_stats()
    comments_object.save_output_data()
