import os
from PyPDF2 import PdfFileReader, PdfFileWriter

file_pdf = 'BPRP1/BPRP1-2-2.pdf'
inputpdf = PdfFileReader(open(file_pdf, "rb"), strict = False)

out_pdf = PdfFileWriter()  
out_pdf.addPage(inputpdf.getPage(1))  
out_pdf.addPage(inputpdf.getPage(4))  

with open('./new.pdf', "wb") as outputStream:
    out_pdf.write(outputStream)
    outputStream.close()