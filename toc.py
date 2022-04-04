import pdfplumber

filename = './BPRP1/BPRP1-2-2.pdf'

with pdfplumber.open(filename) as pdf:
    n = len(pdf.pages)
    print (pdf.metadata)
    print ('number of pages', n)
    
    for i in range(n):
        text = pdf.pages[i].extract_text(x_tolerance=1)
        x = text.find("TableofContent")
        if x != -1:
            print(text)

        x = text.find("Table of Content")
        if x != -1:
            print(text)

        x = text.find("Table of")
        if x != -1:
            print(text)
            exit()
        
        #print(i)
