# -*- coding: utf-8 -*-

import mechanicalsoup
from tqdm import tqdm
import requests
from bs4 import BeautifulSoup

browser = mechanicalsoup.StatefulBrowser()
browser.open("https://polybox.ethz.ch/index.php/s/IOWjGrU3mjyzDSV/authenticate")

# Fill-in the search form
#browser.select_form('password')
#browser.select_form("input[id='password']")
form = browser.select_form()

#ask octavian dot ganea at inf.ethz.ch
password=""
if len(password)<2:
        print("password is requied, please email octavian.")
form.print_summary()
#browser["input[id='password']"] = "pboh1"
form.set("password", password)

browser.submit_selected()
# Display the results
#for link in browser.get_current_page().select('download'):
#    print(link.text, '->', link.attrs['href'])

#print(browser.get_current_page())
urls=browser.get_current_page().findAll('a',{'id':'download'})
download_url= urls[0].get("href")
print(download_url)
response = browser.follow_link(download_url)

file_name='your_filename_here.pdf'
with open(file_name, 'wb') as f:
    f.write(response.content)
print("Download done")
