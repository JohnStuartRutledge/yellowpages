# -*- coding: utf-8 -*-
'''
YELLOW PAGES WEB CRAWLER
#description     :Spider that collects property info from yellowpages.com
#author          :John Rutledge
#date            :2011-08-27
#version         :0.1
#usage           :python yellowpages_scraper.py
#python_version  :2.7.0 
#==============================================================================
'''
import os
import re
import sys
import time
import sqlite3
import urllib
import urllib2
import urlparse
from lxml import etree
from pprint import pprint
from datetime import datetime
from pyquery import PyQuery as pq


now = datetime.now()
AGENT = 'Yellow Pages Monitor'

#-----------------------------------------------------------------------------
# 
#-----------------------------------------------------------------------------

class YellowSpider(object):

    def __init__(self, db_name, root):
        ''' YELLOW PAGES WEB-CRAWLER
        @db_name    - name of the database to store scrappings
        @root       - root url from which to begin the scrape
        '''
        self.root = root
        self.host = urlparse.urlparse(root)[1]

        # connect to DB
        db       = os.path.join(os.getcwd(), db_name)
        self.con = sqlite3.connect(db)
        self.cur = self.con.cursor()

    def nullcheck(self, item):
        if item:
            return item
        else:
            return 'NULL'

    def crawl(self, page=None):
        ''' Primary function that crawls individual webpages and extracts the
        property information into an sqlite database.
        '''
        pg = page if page else self.root
        apartments = [] # container for holding property info
        d = pq(url=pg)

        # locate unit containing all property information
        prop_unit = d('div').filter('.listing_content')
        for i, x in enumerate(prop_unit):

            apt  = ['NULL' for x in range(14)]
            prop = prop_unit.eq(i)
            p    = prop('div').filter('.info')

            # basic info
            apt[0] = self.nullcheck(p('h3 a').text())                           # prop_name
            apt[2] = self.nullcheck(p('span').filter('.street-address').text()) # prop_address
            apt[3] = self.nullcheck(p('span').filter('.locality').text())       # prop_city
            apt[4] = self.nullcheck(p('span').filter('.region').text())         # prop_state
            apt[5] = self.nullcheck(p('span').filter('.postal-code').text())    # prop_zip
            apt[9] = self.nullcheck(p('span').filter('.business-phone').text()) # prop_phone

            # keywords
            biz_cats = prop('ul').filter('.business-categories')('li a')
            apt[10] = '|'.join(biz_cats.eq(i).text() for i, x in enumerate(biz_cats))

            # geo information
            apt[6] = self.nullcheck(prop('span').filter('.latitude').text())  # latitude
            apt[7] = self.nullcheck(prop('span').filter('.longitude').text()) # longitude
            apt[8] = self.nullcheck(prop('span').filter('.distance').text())  # distance

            # stars
            stars = prop('span').filter('.average-rating')('span').text()
            review_count = prop('span').filter('.review-count')('span').filter('.count').text()
            if stars:
                stars = stars[0:3]
            else:
                stars = 'NULL'
            apt[11] = self.nullcheck(stars) # stars

            if not review_count:
                review_count = 0
            apt[12] = review_count

            # links
            apt[1]  = self.nullcheck(prop('li a').filter('.track-visit-website').attr('href')) # website
            profile = prop('li a').filter('.track-more-info').attr('href') 
            if profile:
                apt[13] = 'http://www.yellowpages.com' + profile
            else:
                apt[13] = 'NULL'

            # replace missing values with NULL
            for n, item in enumerate(apt):
                if item is None:
                    item = 'NULL'
                apt[n] = str(item)

            # append the info for this apartment to the DB
            # apartments.append(tuple(apt))
            self.cur.execute("""INSERT INTO properties (name, url, address,
            city, state, zip, latitude, longitude, distance, phone, keywords,
            stars, review_count, profile_link) VALUES (?, ?, ?, ?, ?, ?, ?,
            ?, ?, ?, ?, ?, ?, ?)""", tuple(apt))

            prop_id = self.cur.lastrowid
            self.con.commit()

            #-----------------------------------------------------------------

            # check for neighborhood information
            hoods = []
            hood  = prop('ul').filter('.business-neighborhoods')('li a')
            if hood:
                for j, y in enumerate(hood):
                    hoods.append(hood.eq(j).text())

            # insert hoods into neighborhoods table
            if hoods:
                for place in hoods:
                    # check neighborhood table to see if hood already
                    # exists. If it does then return the hood_id
                    hood_id = ''
                    rs = self.cur.execute("""SELECT id FROM neighborhoods
                    WHERE neighborhood=\"%s\"""" % place).fetchone()
                    if rs:
                        hood_id = rs[0]
                    # if hood_id not found, add it to the table and return last_insert_id
                    if not hood_id:
                        sql = 'INSERT INTO neighborhoods (neighborhood) VALUES (\"%s\")' % place
                        self.cur.execute(sql)
                        hood_id = self.cur.lastrowid 
                        self.con.commit()

                    # TODO check if this prop_id and this hood_id exist together in
                    # the hood lookup table
                    # if yes then pass, else insert new (prop_id, hood_id)
                    self.cur.execute("""INSERT INTO hood_lookup (property_id, hood_id)
                    VALUES (?, ?)""", (prop_id, hood_id))
                    self.con.commit()

        #---------------------------------------------------------------------

        # find the pagination links at bottom and grab the next page to scrape
        next_link = d('ol').filter('.track-pagination')('li').filter('.next')
        if next_link:
            next_pg = 'http://www.yellowpages.com' + next_link('a').attr('href')
            time.sleep(0.2) # be polite to their servers
            print('NOW SCRAPING: %s' % next_pg)
            self.crawl(next_pg)
        else:
            print('Job is complete!')
            return

#-----------------------------------------------------------------------------
# 
#-----------------------------------------------------------------------------

if __name__ == '__main__':
    # scrape prperties in Austin Texas
    root = 'http://www.yellowpages.com/austin-tx/'
    start_url = "http://www.yellowpages.com/austin-tx/apartments?g=Austin%2C+TX&order=name&refinements%5Bheadingtext%5D=Apartments"
    
    spider = YellowSpider('yellowPages.db3', root)
    spider.crawl(start_url)

