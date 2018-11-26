"""
Purpose:  This class consists of functions to scrape job advertisement data
from OLX websites.  It is developed to store information in a SQL database
that contains historical data on page views and other information.

TODO:  Exploit that we can send out requests to different websites at once.
Parallel process this....

Author:  Natalie Chun
Created: 22 November 2018
"""

from olxdownloader import OLXDownloader
from wuzzufdownloader import WuzzufDownloader


def main():
    """Function to run downloads of various data."""
    
    rundata = {
        'olx' : True,
        'wuzzuf' : False,
        'tanqeeb': True
    }
    
    for rundata, value in rundata.items():
        if value:
            if rundata == 'olx':
    
                countryparams = [
                    {"country":"jordan", "url":"https://olx.jo/en/", "timezone":"Asia/Amman"},
                    {"country":"egypt", "url":"https://olx.com.eg/en/", "timezone":"Africa/Cairo"}
                ]
                for params in countryparams:
                    od = OLXDownloader(params)
                    od.run_all()
            elif rundata == 'tanqeeb':
                countryparams = [
                    {"country":"algeria", "webname":"algerie", "timezone":"Africa/Algiers"},
                    {"country":"egypt", "webname":"egypt", "timezone":"Africa/Cairo"},
                    {"country":"jordan", "webname":"jordan", "timezone":"Asia/Amman"},
                    {"country":"morocco", "webname":"morocco", "timezone":"Africa/Casablanca"},
                    {"country":"tunisia", "webname":"tunisia", "timezone":"Africa/Tunis"}
                ]
                for params in countryparams:
                    td = TanQeebDownloader(params)
                    td.run_all()
            elif rundata == 'wuzzuf':
                wd = WuzzufDownloader()
                wd.run_all()
        
        
if __name__ == "__main__":
    main()

