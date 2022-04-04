import pdfplumber

file_pdf = 'BPRP1/BPRP1-2-2.pdf'
with pdfplumber.open(file_pdf) as pdf:
    text = pdf.pages[1].extract_text()
    print (text)