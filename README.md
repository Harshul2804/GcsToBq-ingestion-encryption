# GcsToBq-ingestion-encryption

This project focuses on ingestion activity(reading files from bigquery and writing it to google cloud platform).
Ingestion is the activity where sensetive data is taken care of and is encrypted so hashing algorithm is used on certain algorithms.
Data sharding is used to divide the data that is to be writen in gcs in multiple files. depending upon the size of the file, multiples shards are created.
