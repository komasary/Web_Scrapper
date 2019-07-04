import requests
import json
import gzip
import io
import re
from bs4 import BeautifulSoup
import pandas as pd

#Static fields for urls and their name tags constants
class VARS:
    folder_base='temp_crawler/'      #data/cc/may2019/
#this is where the desired domains and brackets go.
    dict_doms = {'bicycling':'https://www.bicycling.com/'}        #Domain queries dictionary
    date_brackets = ["2019-13"]


#Constants for urls related data
class URLS:
    ending='&matchType=domain&output=json'

# Creates url with the different domains structures
def create_url(domain):
    result_list = []
    print("Accessing url {}".format(domain))
    for i,date in enumerate(VARS.date_brackets):
        print("Index {}".format(i+1))

        #build url

        build_url = 'http://index.commoncrawl.org/CC-MAIN-{}-index?url={}{}'.format(date,domain,URLS.ending)

        # Python standard request library
        res = requests.get(build_url)
        if res.status_code == 200:
            record_lines = res.content.splitlines()   #Make into lines
            print("Total of {} results".format(len(record_lines)))

            #Add each line to result
            for record in record_lines:
                #Skip null lines
                if record is None:
                    continue
                record=record.decode('utf-8')       #Make sure record is a string and not bytes
                json_string=json.loads(record)      #Put it as a json string
                result_list.append(json_string)     #Append it to result

    return result_list


#Input/Output write to file using pandas dataframe to_csv
def save_to_file(records_per_search, name):
    #Create dataframe using list of records
    df = pd.DataFrame(records_per_search)

    # Write file full path name
    path=VARS.folder_base + name + 'Content.csv'

    # Write to csv
    df.to_csv(path)



def retrieve_content(records_per_search, path, size=100):
    print('Processing {} results...'.format(size))

    root_node = {}                 #Root node for the json file

    for i,record in enumerate(records_per_search):

        offset=int(record['offset'])            #Use offset
        length = int(record['length'])          #Use length of message

        offset_end = offset + length - 1

        # Requests
        # Make request to specific common crawl archive based on current record
        if record is None:
            continue
        res = requests.get(record['url'])

        #Skip if null
        if res is None:
            continue

        # Check response status 200 (ready) 206 (partial)
        if res.status_code==200 or res.status_code==206:

            #Find href in links
            try:
                #Create a bs4 instant using the response results
                soup = BeautifulSoup(res.text, "html.parser")
                root_node[str(i)]=[]
                for link in soup.findAll('a'):

                    # if keyword found
                    if 'bicycling' in str(link):
                        # add line
                        root_node[str(i)]=str(link)
            except Exception as e:
                print(e)

        
        #If index reaches total allowed entries per document
        if i == size:
            break       #End loop

    df = pd.DataFrame(root_node, index=[0])
    df.to_csv(VARS.folder_base + path + '.csv')



# Per each name tag and url, run domain search and save results
folders,urls=zip(* VARS.dict_doms.items())
for folder,url in zip(folders,urls):
    all_records = create_url(url)               #Creates list
    save_to_file(all_records, folder)           #Save all results found to file (api search)
    retrieve_content(all_records, folder,100)       #Save all results found to file (site parse from api results)


