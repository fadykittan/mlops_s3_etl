import s3_etl

BUCKET_NAME = "fady-my-bucket"
LOCAL_FILE_PATH_OPEN_CLOSE = "dataset/dataset_open_close.csv"

FILE_PATH_OPEN_CLOSE = "upload/dataset_open_close.csv"

FILE_PATH_HIGH_LOW = "copy/dataset_high_low.csv"

SOURCE_FILE_PATH = "dataset_high_low.csv"
SOURCE_BUCKET_NAME = "naya-college-public"

RESULT_FILE_PATH = "result/dataset_joined.csv"

def main():
    s3_etl.create_bucket(BUCKET_NAME)
    s3_etl.upload_file(BUCKET_NAME, LOCAL_FILE_PATH_OPEN_CLOSE, FILE_PATH_OPEN_CLOSE)
    s3_etl.copy_file(SOURCE_BUCKET_NAME, SOURCE_FILE_PATH, BUCKET_NAME, FILE_PATH_HIGH_LOW)
    
    open_close_df = s3_etl.read_file_as_pandas(BUCKET_NAME, FILE_PATH_OPEN_CLOSE)
    high_low_df = s3_etl.read_file_as_pandas(BUCKET_NAME, FILE_PATH_HIGH_LOW)
    joined_df = s3_etl.join_dataframes_by_key(open_close_df, high_low_df, "Date")
    print(joined_df)

    joined_df.to_csv(RESULT_FILE_PATH, index=False)

    s3_etl.upload_file(BUCKET_NAME, RESULT_FILE_PATH, RESULT_FILE_PATH)

if __name__ == "__main__":
    main()