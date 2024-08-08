import os, json 

import datetime as dt 
import pandas   as pd 

from config.youtube_authentication  import get_authenticated_service

class YoutubeAnalyticsInterface:
    def __init__(self, service_name: str, api_version: str, api_scope: list[str]):
        self.service_name = service_name 
        self.api_version  = api_version 
        self.api_scope    = api_scope 

    def analytics_settings_method(self, video_id_file: str, video_id_column: str, brand_column: str, output_file_name: str, start_date: str, end_date: str, metrics_string, dimension_string: str, filter_column: str, pickle_files_list: list[str], token_files_list: list[str], target_brands_list: list[str], channel_names_list: list[str]):
        self.video_id_file         = video_id_file 
        self.video_id_column       = video_id_column
        self.brand_column          = brand_column
        self.output_file_name      = output_file_name
        self.start_date            = start_date 
        self.end_date              = end_date 
        self.metrics_string        = metrics_string 
        self.dimension_string      = dimension_string
        self.filter_column         = filter_column
        self.pickle_files_list     = pickle_files_list 
        self.token_files_list      = token_files_list 
        self.target_brands_list    = target_brands_list
        self.channel_names_list    = channel_names_list
        self.output_data_list      = []

    def __free_form_reports(self, metric_string: str, filter_column: str, filter_value: str, authentication_object: any):
        youtube_report = authentication_object.reports().query(
            ids        = "channel==MINE",
            startDate  = self.start_date,
            endDate    = str(dt.datetime.now().date()),
            metrics    = metric_string,
            filters    = f"{filter_column}=={filter_value}",
            dimensions = self.dimension_string
        ).execute()

        return youtube_report

    def extract_data(self):
        def data_merging_function(video_id: str, brand_name: str, channel_name: str, metrics_string_array: list[str], output_dataframe: pd.DataFrame, authentication_object: any):
            for metric in metrics_string_array:
                try:
                    query_result_object              = self.__free_form_reports(metric, self.filter_column, video_id, authentication_object)
                    result_dataframe                 = pd.DataFrame(query_result_object["rows"], columns = [row["name"] for row in query_result_object["columnHeaders"]])
                    result_dataframe["video"]        = video_id
                    result_dataframe["brand_name"]   = brand_name
                    result_dataframe["channel_name"] = channel_name
                    shared_columns                   = [column for column in result_dataframe.columns if (column in output_dataframe.columns)]
                    output_dataframe                 = pd.merge(output_dataframe, result_dataframe, how = "left", on = shared_columns)
                except Exception as E:
                    pass
            
            return output_dataframe

        def get_youtube_report(brand_name: str, channel_name: str, metrics_string_array: list[str], video_id_list: list[str], authentication_object: any):
            for (index, video_id) in enumerate(video_id_list):
                output_dataframe = pd.DataFrame({"video" : [video_id], "brand_name" : [brand_name], "channel_name" : [channel_name]})
                merged_dataframe = data_merging_function(video_id, brand_name, channel_name, metrics_string_array, output_dataframe, authentication_object)
                print(f"{str(index + 1).zfill(len(str(len(video_id_list))))}/{len(video_id_list)}: {brand_name} {video_id}")

                if (type(merged_dataframe) == pd.DataFrame):
                    self.output_data_list.append(merged_dataframe)

        video_id_table   = pd.read_excel(self.video_id_file)

        for (pickle_file, token_file, brand, channel) in zip(self.pickle_files_list, self.token_files_list, self.target_brands_list, self.channel_names_list):
            video_id_list         = list(video_id_table[video_id_table[self.brand_column] == brand][self.video_id_column])
            authentication_object = get_authenticated_service(token_file, self.service_name, self.api_version, pickle_file, self.api_scope)
            metrics_string_array  = self.metrics_string.split(",")
            get_youtube_report(brand, channel, metrics_string_array, video_id_list, authentication_object)

if (__name__ == "__main__"):
    with open("./config/YoutubeAPIConfig.json", "r", encoding = "utf-8") as f:
        config_dict = json.load(f)
    
    analytics_object = YoutubeAnalyticsInterface(**config_dict["YoutubeAnalyticsInterface"]["constructor"])
    analytics_object.analytics_settings_method(**config_dict["YoutubeAnalyticsInterface"]["analytics_settings_method"])
    analytics_object.extract_data()
