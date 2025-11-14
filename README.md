This is the public repository for my research project, **Financial micro-incentives and internet discourse: Evidence from a Bitcoin based discussion platform**, joint with Keyan Kousha of Stacker News.

I am conducting this research in public, which is how I envision all research should be done. All code is public and verifiable. The data is not publicly available, but you can view the schema [here](https://github.com/stackernews/stacker.news/blob/master/prisma/schema.prisma) (you may have to find the commit on the date of the data extraction, which was October 5, 2025). 

Every once in a while I post updates on the progress of the project. You can find a list of such posts [here](https://stacker.news/SimpleStacker#research-in-public).


## Local setup

Create a `config.yaml.local` file in the root directory of this repo. The file contains:

```yaml
LOCAL_PATH: <path to this repo on your machine>
RAW_DATA_PATH: <path to the directory where you will dump raw tables from SQL database>
DATA_PATH: <path to the directory where you will dump processed data>
R_PATH: <path to Rscript binary>
OPENAI_API_KEY: <OpenAI api key>
DB_CONN_STR: <url of SN database connection>
```

## Run order

#### Dump tables for analysis

`src/notebooks/dump-tables.ipynb`

Dump the raw data tables used for analysis to parquet files.

#### Pay to post analysis

`src/notebooks/pay-to-post-analysis.ipynb`






