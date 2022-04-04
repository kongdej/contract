from PyPDF2 import PdfFileWriter, PdfFileReader
import pdfplumber
import pymongo
import meilisearch
import csv
import sys
import os
import glob
from sklearn.feature_extraction.text import ENGLISH_STOP_WORDS

import argparse
parser = argparse.ArgumentParser()
parser.add_argument("book",help="input pdf file, ex: NPORP/NPORP-2-2")
parser.add_argument("-x","--delete",help="delete project collection",action="store_true")
parser.add_argument("-d","--directory",help="output diretory")
parser.add_argument("-p","--production",help="production",action="store_true")
args = parser.parse_args()

path = '/Users/kongdej/Projects/siteline/public/contract/'

if args.directory != None:
    path = args.directory

if args.production:
    meiliSERVER="http://mysites.egat.co.th:7700"
    uri = "mongodb://kongdej:gearman1@10.40.58.56:27017/?authSource=admin&readPreference=primary&appname=MongoDB%20Compass%20Community&ssl=false";
else:    
    meiliSERVER="http://127.0.0.1:7700"
    uri = "mongodb://localhost:27017/"

# meilisearch ######################################
APIKEY="Tangmo"
client = meilisearch.Client(meiliSERVER,APIKEY)

# mongodb ##########################################
myclient = pymongo.MongoClient(uri)
mydb = myclient["contracts"]

# required file csv argument
if args.book:
    file = args.book    
    file_csv = file +'.csv'
    file_pdf = file +'.pdf'
    project = file.split('/')[0]
    volume = file_csv.split('-')[1]
    book = file_csv.split('-')[2].split('.')[0]
    id = 0
    part = ''
    section = ''
    section_i = ''

    # delet all project
    if args.delete:
        print('delete ' + project)
        client.index(project).delete_all_documents() // meilisearch

    # Create a new directory because it does not exist
    path += project+'/'+volume+'/'+book
    isExist = os.path.exists(path)
    if not isExist:   
        os.makedirs(path)
        print(path," is created!")
    else:
        print(path," already exists.")
    
    files = glob.glob(path+'/*.pdf')

    # delete all pdf files
    for f in files:
        try:
            print ('remove ', f)
            os.remove(f)
        except OSError as e:
            print("Error: %s : %s" % (f, e.strerror))

    # delete mongodb
    print ('create collection', project)
    mycol = mydb[project]
    collist = mydb.list_collection_names()
    if args.delete:
         mycol.drop()

    if project in collist:
        print("The collection exists.")
        # todo check!!
        print ("delete database",project,volume,book)
        x = mycol.delete_many({
            "project":project,
            "volume":volume,
            "book":book
        })
       
        print(x.deleted_count, " documents deleted.")
     
    # build toc from csv file
    toc = []
    b = 0
    with open(file_csv) as infile:
        reader = csv.reader(infile) 
        for row in reader:
            #print(row)
            if len(row[0]) > 0:    
                if len(row[1]) == 0:
                    if row[0].startswith('PART'):
                        part = row[0]
                    if row[0].startswith('SECTION'):
                        section = row[0]
                
                if len(row[2]) > 0:
                    ts = row[0].split(' ')
                    title = row[0]
                    section = ts[0]
                    page=ts[-1]
                    s_page = row[1]
                    n_page = row[2]
                    
                    #title = row[0] + ' ' + row[1] + ' ' + row[2]
                    #section = row[0]
                    #page = row[2]
                    #s_page = row[3]
                    #n_page = row[4]

                    filename = volume + '-' + book + ' ' + title.replace('/','-') + '.pdf'
                    s_tag = title.split(' ')
                    toc.append({
                        "id": volume+'-'+book + '-' + str(id),
                        "project": project,
                        "volume_book": "Volume "+volume+" Book " + book,
                        "volume": volume,
                        "book": book,
                        "part": part,
                        "section": section,
                        "title": title,
                        "tag": ' '.join(s_tag[:-1]),
                        "page": page,
                        "s_page": s_page,
                        "n_page": n_page,
                        "text": '',
                        "file": filename
                    })
                    id += 1

               
        # for testing
        #    if b > 5:
        #        break
        #    else:
        #        b += 1
        
    
    # extract text from pdf
    with pdfplumber.open(file_pdf) as pdf:
        inputpdf = PdfFileReader(open(file_pdf, "rb"))
        i = 0        
        for item in range(len(toc)):
            out_text = ''
            out_pdf = PdfFileWriter()
            for n in range(int(toc[item]['n_page'])):
                out_text += pdf.pages[int(toc[item]['s_page']) + n - 1].extract_text(x_tolerance=1)    
                out_pdf.addPage(inputpdf.getPage(int(toc[item]['s_page']) + n - 1))  

            filepdf = path+'/'+toc[item]['file']
            with open(filepdf, "wb") as outputStream:
                out_pdf.write(outputStream)

            #clean out_text
            print('-------------------------')
            found = False
            texts = ''
            for line in out_text.split('\n'):
                for t in line.split(' '):
                    if toc[item]['section'].strip() == t.strip():
                        found = True
                    if i < len(toc) - 1:
                        if toc[item + 1]['section'].strip() == t.strip():
                            found = False
                if found:
                    texts += line
                    print (line)
            print('-------------------------')

            out_text = texts
            #out_text = out_text.replace('Nam Phong Power Plant Replacement Project','')
            
            words = [word for word in out_text.split() if word.lower() not in ENGLISH_STOP_WORDS]
            out_text = " ".join(words)

            toc[item]['text'] = out_text.lower()

            client.index(project).add_documents([toc[item]])

            x = mycol.insert_one(toc[item])
            
            
            #print (i,toc[item],x)  
            i += 1
    