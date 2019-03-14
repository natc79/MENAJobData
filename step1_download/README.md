# Scrape Data

This scrapes job advertisement data from various MENA websites.  
Data is stored in a sqlite database to reduce chances of
duplicate entries and to make it easier to process for
extraction and manipulation.


## Set-up Notes (local installation)

```
conda env update -f environment.yml -n downloader 
```

## To run:

```
activate downloader
python src/main_download.py
```

