
from PyPDF2 import PdfFileWriter, PdfFileReader
import pdfplumber
import pymongo
import meilisearch
import csv
import os
import sys
import glob
from sklearn.feature_extraction.text import ENGLISH_STOP_WORDS

path = '/Users/kongdej/Projects/contract/documents/'
meiliSERVER="http://127.0.0.1:7700"
uri = "mongodb://localhost:27017/"


# meilisearch ######################################
APIKEY="Tangmo"
client = meilisearch.Client(meiliSERVER, APIKEY)


# mongodb ##########################################
myclient = pymongo.MongoClient(uri)
mydb = myclient["contracts"]

# required file csv argument
file = 'BPRP1/BPRP1-2-2'    
file_csv = file +'.csv'
file_pdf = file +'.pdf'
project = file.split('/')[0]
volume = file_csv.split('-')[1]
book = file_csv.split('-')[2].split('.')[0]
id = 0
part = ''
section = ''
section_i = ''

# meilisearch: delete all project
print('delete ' + project)
client.index(project).delete_all_documents() 

# Create a new directory because it does not exist
path += project+'/'+volume+'/'+book
isExist = os.path.exists(path)
if not isExist:   
    os.makedirs(path)
    print(path," is created!")
else:
    print(path," already exists.")

files = glob.glob(path + '/*.pdf')

# delete all pdf files
for f in files:
    try:
        print ('remove ', f)
        os.remove(f)
    except OSError as e:
        print("Error: %s : %s" % (f, e.strerror))

# mongodb: create collection
print ('create collection', project)
mycol = mydb[project]
#mycol.drop()

collist = mydb.list_collection_names()
if project in collist:
    print("The collection exists.")
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

            # get part or section 
            # example: PART 4 General
            if len(row[1]) == 0:
                if row[0].startswith('PART'):
                    part = row[0]
                if row[0].startswith('SECTION'):
                    section = row[0]
            
            # get item
            # example: ['3C.12.5  Incoming Feeders  3C-67', '67', '1 ']
            if len(row[2]) > 0:
                title = row[0]
                ts = row[0].split(' ')
                section = ts[0]
                page=ts[-1]
                s_page = row[1]
                n_page = row[2]

                filename = volume + '-' + book + ' ' + title.replace('/','-') + '.pdf'
                toc.append({
                    "id": volume+'-'+book + '-' + str(id),
                    "project": project,
                    "volume_book": "Volume "+volume+" Book " + book,
                    "volume": volume,
                    "book": book,
                    "part": part,
                    "section": section,
                    "title": title,
                    "tag": ' '.join(ts[:-1]),
                    "page": page,
                    "s_page": s_page,
                    "n_page": n_page,
                    "text": '',
                    "file": filename
                })

                id += 1

# extract text from pdf
with pdfplumber.open(file_pdf) as pdf:
    inputpdf = PdfFileReader(open(file_pdf, "rb"), strict = False)
    i = 0        
    for item in range(len(toc)):
        print (toc[item]['s_page'], toc[item]['n_page'] )
        #print ("======")
        
        out_text = ''
        out_pdf = PdfFileWriter()
        for n in range(int(toc[item]['n_page'])):
            ####################################################################################
            out_text += pdf.pages[int(toc[item]['s_page']) + n - 1].extract_text(x_tolerance=1)    
            out_pdf.addPage(inputpdf.getPage(int(toc[item]['s_page']) + n - 1))  
            ####################################################################################

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
        
        words = [word for word in out_text.split() if word.lower() not in ENGLISH_STOP_WORDS]
        out_text = " ".join(words)

        toc[item]['text'] = out_text.lower()
        client.index(project).add_documents([toc[item]]) # meilisearch

        x = mycol.insert_one(toc[item]) # mongodb
        
        
        #print (i,toc[item],x)  
        i += 1
