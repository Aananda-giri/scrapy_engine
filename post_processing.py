from scrapy_engine.spiders.functions import compress_file, merge_crawled_json_files, save_nepali_paragraphs_to_csv

if __name__=="__main__":
    # Merge json files with same name
    merge_same_named_json_files(delete_merged=True)

    # Save paragraphs to a csv file
    save_nepali_paragraphs_to_csv(csv_file_name = "crawled_nepali_news_dataset.csv")

    # compress the file
    compress_file(input_file_path="crawled_nepali_news_dataset.csv", output_file_path="crawled_nepali_news_dataset.csv")