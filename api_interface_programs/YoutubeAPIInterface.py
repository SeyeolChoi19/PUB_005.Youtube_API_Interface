import os, json

import datetime as dt 
import pandas   as pd 

from googleapiclient.discovery import build 

class YoutubeAPIInterface:
    def __init__(self, service_name: str = "youtube", api_ver = "v3"):
        self.service_name = service_name
        self.api_version  = api_ver 
        self.current_date = str(dt.datetime.now().date())

    def youtube_api_settings_method(self, output_path: str, earliest_date: str, latest_date: str, max_results: int, brand_names: list[str], channel_names: list[str], channel_ids: list[str]):
        self.output_path    = output_path.format(self.current_date)
        self.earliest_date  = earliest_date 
        self.latest_date    = latest_date
        self.max_results    = max_results
        self.brand_names    = brand_names
        self.channel_names  = channel_names 
        self.channel_ids    = channel_ids 
        self.channel_uids   = [f"UU{channel_id[2:]}" for channel_id in channel_ids]
        self.youtube_api    = build(self.service_name, self.api_version, developerKey = "AIzaSyDx-5HlTjstptydqqyuyq5oeJLLurd7gh8")

        self.output_data_dictionary = {
            "brand_name"     : [], "channel_name"      : [],
            "channel_id"     : [], "upload_date"       : [], 
            "video_title"    : [], "video_ids"         : [], 
            "video_url"      : [], "video_likes"       : [], 
            "favorite_count" : [], "view_count"        : [], 
            "comment_no"     : [], "total_engagements" : []
        }

        self.subscriber_count = {
            "brand_name"       : [], 
            "channel_name"     : [], 
            "extraction_date"  : [], 
            "subscriber_count" : []
        }
    
    def get_video_ids(self):
       def save_results(brand_name: str, channel_name: str, channel_id: str, response: build):
            for index in range(len(response["items"])):
                upload_date = response["items"][index]["snippet"]["publishedAt"][0:10]
            
                if (self.latest_date >= upload_date >= self.earliest_date):
                    video_id     = response["items"][index]["snippet"]["resourceId"]["videoId"]
                    video_title  = response["items"][index]["snippet"]["title"]
                    video_url    = f"https://www.youtube.com/watch?v={video_id}"
                    results_list = [brand_name, channel_name, channel_id, upload_date, video_title, video_id, video_url]

                    for (key, value) in zip(["brand_name", "channel_name", "channel_id", "upload_date", "video_title", "video_ids", "video_url"], results_list):
                        self.output_data_dictionary[key].append(value)

            return upload_date

        for (brand_name, channel_name, channel_id, channel_uid) in zip(self.brand_names, self.channel_names, self.channel_ids, self.channel_uids):
            upload_date = str(self.current_date)
            page_token  = None 

            while (upload_date >= self.earliest_date):
                response    = self.youtube_api.playlistItems().list(playlistId = channel_uid, part = "snippet", maxResults = self.max_results, pageToken = page_token).execute()
                page_token  = response.get("nextPageToken", None)
                upload_date = save_results(brand_name, channel_name, channel_id, response)

                if ((upload_date < self.earliest_date) or (page_token == None)):
                    break

    def get_video_stats(self):
        for video_id in self.output_data_dictionary["video_ids"]:
            response    = self.youtube_api.videos().list(id = video_id, part = "statistics").execute()
            likes       = int(response["items"][0]["statistics"].get("likeCount", 0))
            comments    = int(response["items"][0]["statistics"].get("commentCount", 0))
            view_count  = int(response["items"][0]["statistics"].get("viewCount", 0))
            favorites   = int(response["items"][0]["statistics"].get("favoriteCount", 0))
            engagement  = likes + comments
            result_list = [likes, view_count, comments, favorites, engagement]
            
            for (key, value) in zip(["video_likes", "view_count", "comment_no", "favorite_count", "total_engagements"], result_list):
                self.output_data_dictionary[key].append(value)

    def get_subscriber_count(self):
        for (brand_name, channel_name, channel_id) in zip(self.brand_names, self.channel_names, self.channel_ids):
            response_object  = youtube_interface.youtube_api.channels().list(id = channel_id, part = "statistics").execute()
            subscriber_count = int(response_object["items"][0]["statistics"]["subscriberCount"])
            results_list     = [brand_name, channel_name, self.current_date, subscriber_count]

            for (key, value) in zip(self.subscriber_count.keys(), results_list):
                self.subscriber_count[key].append(value)      

    def save_youtube_data(self):
        os.makedirs(self.output_path, exist_ok = True)
        file_names = [f"{self.current_date} Youtube Data V3 API Data.xlsx", f"{self.current_date} Youtube Data V3 API Subscriber Count.xlsx"]

        for (file_name, data_dictionary) in zip(file_names, [self.output_data_dictionary, self.subscriber_count]):
            dataframe = pd.DataFrame(data_dictionary)
            dataframe.to_excel(os.path.join(self.output_path, file_name), index = False)

if (__name__ == "__main__"):
    with open("./config/YoutubeAPIConfig.json", "r", encoding = "utf-8") as f:
        config_dict = json.load(f)

    youtube_interface = YoutubeAPIInterface(**config_dict["YoutubeAPI"]["constructor"])
    youtube_interface.youtube_api_settings_method(**config_dict["YoutubeAPI"]["youtube_api_settings_method"])
    youtube_interface.get_video_ids()
    youtube_interface.get_video_stats()
    youtube_interface.get_subscriber_count()
    youtube_interface.save_youtube_data()
