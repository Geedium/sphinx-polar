# Kaggle Big Data Dataset Tool
This tool filters and optimizes CSV files (**artists** and **tracks**) to generate claims more easily and saves records after filtering to a SQL database.

Important: Ensure you have the **~/.kaggle/kaggle.json** configuration file set up before running the tool. You can obtain this configuration from Kaggle's Public API. Additionally, Python must be installed on your system to run the CLI and successfully extract CSVs.

## Prerequisites
 - Python
 - Node.js
 - PostgreSQL

## Python
Ensure Python is installed on your system. You can download it from the [official Python website](https://www.python.org/downloads/).

## Node.js
Ensure Node.js is installed on your system. You can download it from the [official Node.js website](https://nodejs.org/).

## PostgreSQL
Ensure PostgreSQL is installed and running on your system. You can download it from the [official PostgreSQL website](https://www.postgresql.org/download/).

# Installing Node.js Dependencies
Install the necessary dependencies using your preferred package manager:
```bash
yarn install
# or
npm install
```
There are also alternatives like **pnpm**.

# Environment Variables
Create a **.env** file in the root directory of your project and insert the required environment variables as specified in the **.env.example** file. These variables include database connection details, AWS S3 bucket information, and any other necessary configurations.

# Running Phase
First, build the project:
```bash
yarn build
```
This command compiles the TypeScript source files to JavaScript. After running this command, there should be a **dist** folder containing the distribution JavaScript files.

## 1. Ingesting Data from Data Source
To download CSV files associated with the artist and track, run:
```bash
yarn bake
```
This command uses Python to download the required CSV files from Kaggle. It utilizes the kaggle CLI to fetch the datasets. Refer to the **installKaggle.mjs** script for details on how it works.

# 2. Data Transformation
Transform the data and upload it to S3:
```bash
yarn start -t
```
This command reads the downloaded CSV files, filters and transforms the data according to specified criteria, and then uploads the processed data to an AWS S3 bucket. The filtering criteria include:
 - Ignoring tracks with no name.
 - Ignoring tracks shorter than 1 minute.
 - Loading only artists that have tracks after filtering.
The data transformation involves exploding the track release date into separate columns (**year**, **month**, **day**) and transforming the track danceability into string values (**Low**, **Medium**, **High**) based on float.

# 3. Pulling Data Directly from S3 (Optional)
If you want to download the transformed data files from S3 bucket which you can configure inside source entry file, remember to rebuild afterwards, run:
```bash
yarn start -f
```
This command downloads the processed data files from your specified S3 bucket. It requires a network connection.

# 4. Connecting to PostgreSQL
To create new records in PostgreSQL from the local CSV files (after transformation), run:
```bash
yarn start -c
```
This command will:
 - Create the **artists** and **tracks** tables in your PostgreSQL database.
 - Insert the data into the respective tables.

# 5. Data Processing
To create the SQL views (**track_info**, **most_energizing_tracks**, **tracks_with_artist_followers**), run:
```bash
yarn start -v
```
This command sets up SQL views that perform the following tasks:
 - **track_info**: Selects track information including **id**, **name**, **popularity**, **energy**, **danceability**, and the number of artist followers.
 - **tracks_with_artist_followers**: Filters tracks to only include those where artists have followers.
 - **most_energizing_tracks**: Picks the most energizing track for each release year.

# Handling Cases
## Deleting Tracks and Artists
To drop records and tables, run:
```bash
yarn start -d
```
This command will delete all records from the **artists** and **tracks** tables and drop the tables from your PostgreSQL database. Use this command with caution as it will remove all data.
