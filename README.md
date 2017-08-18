# Topmerger

Utility script to update database entries from [PLPR-Scraper](https://github.com/datenschule/plpr-scraper) with their
respective agenda topics from [topscraper](https://github.com/datenschule/topscraper).


## Install
```
pip install -r requirements.txt
```

## Run
e.g: 
```
python main.py --db_url=postgres://postgres@192.168.99.101:32771/plpr_psp --agw_path data/agw.json --parliament_path data/bundestag.json
```