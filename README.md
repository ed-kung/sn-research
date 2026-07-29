This is the public repository for my research project, **Can Direct Micropayment Incentives Shape the Quality of Online Discourse?**, joint with Keyan Kousha of Stacker News.

I am conducting this research in public, which is how I think all research should be done. All of the code used to produce the analysis is public and verifiable. The data is not publicly available, but you can view the schema [here](https://github.com/stackernews/stacker.news/blob/master/prisma/schema.prisma) (you may have to find the commit on the date of the data extraction, which was October 5, 2025). 

Every once in a while I post updates on the progress of the project. You can find a list of such posts [here](https://stacker.news/SimpleStacker#research-in-public).

## Local setup

Create a `config.yaml.local` file in the root directory of this repo. The file contains:

```yaml
LOCAL_PATH: <path to this repo on your machine>
RAW_DATA_PATH: <path to the directory where you will dump raw tables from SQL database>
DATA_PATH: <path to the directory where you will dump processed data>
R_PATH: <path to Rscript binary>
```

## Main files to run

The analysis is carried out in a series of Jupyter notebooks. They are found in `src/notebooks/XX_notebook_name.ipynb`. They should already be listed in the correct order.








