# -*- coding: utf-8 -*-
from __future__ import unicode_literals
import pika
import time
import re
import json
import glob
import csv
import datetime
from multiprocessing import Pool
from pymongo import MongoClient
import logging
import sys
import utilities
import argparse
import argparse
import requests
import xml.etree.ElementTree as ET
from bs4 import BeautifulSoup

ARTICLE_LIMIT = 50000

# Read config file
#ln_user, ln_password, db_collection, whitelist_file, pool_size, log_dir, log_level, auth_db, auth_user, auth_pass, db_host = utilities.parse_config()

# Set up logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

handler = logging.FileHandler("cloacina_run.log")
handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.info("Writing logs to {0}".format("cloacina_run.log"))

# Set up connection to Mongo
db_collection = "ln_arabic" #"test2"
connection = MongoClient()
db = connection.lexisnexis
collection = db[db_collection]

# Set up incoming arguments.
parser = argparse.ArgumentParser()
parser.add_argument("--user", help="LexisNexis username")
parser.add_argument("--password", help="Password for LexisNexis username")
# we get source from Rabbit now
#parser.add_argument("--source", help="sourcename;start_yyyy-mm-dd;end_yyyy-mm-dd")


# Authentication
def authenticate(username, password):
    request = u"""
    <SOAP-ENV:Envelope
       xmlns:SOAP-ENV="http://schemas.xmlsoap.org/soap/envelope/"
       SOAP-ENV:encodingStyle= "http://schemas.xmlsoap.org/soap/encoding/">
        <soap:Body xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
          <Authenticate xmlns="http://authenticate.authentication.services.v1.wsapi.lexisnexis.com">
            <authId>{0}</authId>
            <password>{1}</password>
          </Authenticate>
        </soap:Body>
    </SOAP-ENV:Envelope>
    """.format(username, password)

    headers = {"Host": "www.lexisnexis.com",
            "Content-Type": "text/xml; charset=UTF-8",
            "Content-Length": len(request),
            "SOAPAction": "Authenticate"}

    t = requests.post(url="https://www.lexisnexis.com/wsapi/v1/services/Authentication",
                     headers = headers,
                     data = request)

    t = t.text
    p = ET.fromstring(t)
    p = p[0][0]
    for i in p.findall('{http://authenticate.authentication.services.v1.wsapi.lexisnexis.com}binarySecurityToken'):
        return i.text

authToken = authenticate(ln_user, ln_password)
if not authToken:
    # logger.error("No auth token generated")
    print "No auth token generated"
    #quit()

print authToken

with open('source_name_id.json') as source_file:
    source_dict = json.load(source_file)

# Run query to get list of hits
def run_query(source_name, date, start_result, end_result, authToken):
    #searchterm = "a OR an OR the"
    searchterm = u"a AND NOT playoff! OR teammate! OR NFL OR halftime OR NBA OR quarterback OR goalie OR NHL OR postseason OR head coach OR N.F.L. OR N.B.A. OR field goal! OR playoff!"
    if re.search("Arabic", source_name):
        searchterm = u"أن OR من OR هذا OR أن OR يا"
        #print searchterm

    source = source_dict[source_name]

    req = u"""<SOAP-ENV:Envelope
       xmlns:SOAP-ENV="http://schemas.xmlsoap.org/soap/envelope/"
       SOAP-ENV:encodingStyle= "http://schemas.xmlsoap.org/soap/encoding/">
        <soap:Body xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
      <Search xmlns="http://search.search.services.v1.wsapi.lexisnexis.com">
       <binarySecurityToken>{authToken}</binarySecurityToken>
       <sourceInformation>
        <ns1:sourceIdList xmlns:ns1="http://common.search.services.v1.wsapi.lexisnexis.com">
         <ns2:sourceId xmlns:ns2="http://common.services.v1.wsapi.lexisnexis.com">{source}</ns2:sourceId>
        </ns1:sourceIdList>
       </sourceInformation>
       <query>{searchterm}</query>
       <projectId>8412</projectId>
       <searchOptions>
        <ns3:dateRestriction xmlns:ns3="http://common.search.services.v1.wsapi.lexisnexis.com">
         <ns3:startDate>{date}</ns3:startDate>
         <ns3:endDate>{date}</ns3:endDate>
        </ns3:dateRestriction>
       </searchOptions>
       <retrievalOptions>
        <ns4:documentView xmlns:ns4="http://result.common.services.v1.wsapi.lexisnexis.com">FullText</ns4:documentView>
        <ns5:documentMarkup xmlns:ns5="http://result.common.services.v1.wsapi.lexisnexis.com">Display</ns5:documentMarkup>
        <ns6:documentRange xmlns:ns6="http://result.common.services.v1.wsapi.lexisnexis.com">
         <ns6:begin>{start_result}</ns6:begin>
         <ns6:end>{end_result}</ns6:end>
        </ns6:documentRange>
       </retrievalOptions>
      </Search>
    </soap:Body>
    </SOAP-ENV:Envelope>""".format(authToken = authToken, date = date, source = source, searchterm = searchterm, start_result = start_result, end_result = end_result)

    req = req.encode("utf-8")

    headers = {"Host": "www.lexisnexis.com",
                "Content-Type": "text/xml; charset=UTF-8",
                "Content-Length": len(req),
                "Origin" : "http://www.lexisnexis.com",
                "User-Agent" : "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/43.0.2357.132 Safari/537.36",
                "SOAPAction": "Search"}
    req = req.encode('utf-8')
    try:
        t = requests.post(url = u"http://www.lexisnexis.com/wsapi/v1/services/Search",
                         headers = headers,
                         data = req)
        return t

    except Exception as e:
        print "Problem in `run_query` for {0} on {1}: {2}".format(source_name, date, e)



# Get the total sources for a source-date
def get_source_day_total(source_name, date, authToken):
    try:
        t = run_query(source_name, date, 1, 10, authToken)
        if t.status_code == 500:
            #print "There was an error. Check the log file"
            print "Error 500 from server on getting source-day total for {0} on {1}: {2}".format(source_name, date, t.text)
            return 0
        c = re.findall('">(\d+?)</ns3:documentsFound', t.text)

        if c != []:
            try:
                c_int = int(c[0])
            except TypeError as e:
                c_int = 0
                print "Error for {0}, {1}: {2}".format(source_name, date, e)
            return c_int
        else:
            print "In get_source_day_total, couldn't find total documents: {0}".format(t.text)
            return 0
    except Exception as e:
        print "Problem getting total for {0}, {1}: {2}".format(source_name, date, e)
        return 0

def construct_page_list(total_results):
    total_results = int(total_results)
    base_iter = total_results / 10
    remainder = total_results % 10
    iter_list = []
    if total_results == 0:
        return [(0, 0)]
    if total_results < 10:
        return [(1, total_results)]
    for i in range(base_iter):
        iter_list.append((i * 10 + 1, i * 10 + 10))
    if remainder:
        iter_list.append((range(base_iter)[-1] * 10 + 10 + 1, range(base_iter)[-1] * 10 + 10 + remainder))
    return iter_list

def extract_from_b64(encoded_doc):
    #doc = base64.urlsafe_b64decode(encoded_doc)
    doc = encoded_doc.decode("base64")
    doc = doc.decode('utf-8')
    doc = re.sub("<p>", " ", doc)
    doc = re.sub('<div class="BODY-2">', " ", doc)
    soup = BeautifulSoup(doc)
    news_source = soup.find("meta", {"name":"sourceName"})['content']
    article_title = soup.find("title").text.strip()
    try:
        publication_date = soup.find("div", {"class":"PUB-DATE"}).text.strip()
    except AttributeError:
        publication_date = soup.find("div", {"class":"DATE"}).text.strip()
    article_body = soup.find("div", {"class":"BODY"}).text.strip()
    doc_id = soup.find("meta", {"name":"documentToken"})['content']

    data = {"news_source" : news_source,
            "publication_date_raw" : publication_date,
            "article_title" : article_title,
            "article_body" : article_body,
            "doc_id" : doc_id}

    return data

# The guts of the download function
def download_day_source(source_name, date, source_day_total, authToken):
    iter_list = construct_page_list(source_day_total)
    print iter_list
    results_list = []
    # if source_day_total is 0, just pass?
    for p in iter_list:
        t = run_query(source_name, date, p[0], p[1], authToken)
        results_list.append(t.text)
    output_list = [] # keep outside the iter
    junk_list = []
    for t in results_list:
        soup = BeautifulSoup(t)
        for num, i in enumerate(soup.findAll("ns1:document")):
            try:
                t = i.text
                d = extract_from_b64(t)
                output_list.append(d)
            except:
                junk_list.append(t) # error handling ¯\_(ツ)_/¯
    if junk_list:
        print "There were {0} problems getting text from the base 64 in download_day_source.".format(len(junk_list))

    output = {"stories" : output_list,
              "junk" : junk_list}
    return output

def add_entry(collection, news_source, article_title, publication_date_raw, article_body, lang, doc_id):
    toInsert = {"news_source": news_source,
                "article_title": article_title,
                "publication_date_raw": publication_date_raw,
                "date_added": datetime.datetime.utcnow(),
                "article_body": article_body,
                "stanford": 0,
                "language": lang,
                "doc_id" : doc_id}
    object_id = collection.insert(toInsert)
    return object_id

# Download a source
def download_wrapper(source, source_day_total, authToken):
    #try:
    output = download_day_source(source['source_name'], source['date'], source_day_total, authToken)

    lang = 'english'

    mongo_error = []
    for result in output['stories']:
        try:
            entry_id = add_entry(collection, result['news_source'],
                result['article_title'], result['publication_date_raw'],
                result['article_body'], lang, result['doc_id'])
        except Exception as e:
            mongo_error.append(e)
    if mongo_error:
        logger.warning("There were error(s) in the Mongo loading {0}".format(mongo_error))
    #except Exception as e:
    #    logger.warning("Error downloading {0}: {1}".format(source, e))


# Handle the rate limit
global doc_count
doc_count = 0
print doc_count

def download(ch, method, properties, body):
    source = json.loads(body)
    print(" [x] Received %r" % source)
    total = get_source_day_total(source["source_name"], source["date"])
    total = int(total)
    print total
    download_wrapper(source, total, authToken)
    global doc_count
    doc_count += total
    print doc_count
    ch.basic_ack(delivery_tag = method.delivery_tag)
    if doc_count > ARTICLE_LIMIT:
        # use the incorrect nowait arg to kill it. Otherwise it marches on.
        ch.basic_cancel(consumer_tag = '', nowait = True)


if __name__ == "__main__":
    args = parser.parse_args()
    ln_user = args.user
    ln_password = args.password
    authToken = authenticate(ln_user, ln_password)
    if not authToken:
        # logger.error("No auth token generated")
        print "No auth token generated"
        #quit()
    print authToken
    # Set up connection to queue.
    # The queue contains source-days left to download.
    credentials = pika.PlainCredentials('guest', 'guest')
    parameters = pika.ConnectionParameters('localhost', 5672, '/', credentials)
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()
    channel.queue_declare(queue='cloacina_process2', durable=True)
    channel.confirm_delivery()

    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(download,
                                  queue='cloacina_process2')
    channel.start_consuming()
