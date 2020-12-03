# pre-processing (data "cleaning")
The data "cleaning" portion of the process takes in the raw .csv files generated via twint. This is broken up into 3 scala files:
 - clean: removes unwanted metadata, tags tweets by keyword and joins tweets from different keyword pulls into one dataframe/file
 - users: takes the dataframe generated by the clean scala file and generates a userlist to perform a user-scrape via twint for location data
 - getLocation: takes the .csv file generated for the user data from twint and joins with clean dataset to tag a particular tweet with a state, dropping all tweets that cannot be linked to a state.
 
 ## To run:
 Ensure that hadoop distributed file system and spark are running either locally or on the cluster. Submit job with the following script:
 ```
 ./run_spark.sh [scala-object] [hdfs-input-path] [hdfs-output-path]
 ```

# pre-processing
In order to run the python file: **python3 preprocess_csv.py \[name of file to process] \[name of file to save]**

For example: **python3 preprocess_csv.py clean.csv clean_preprocessed.csv** 
