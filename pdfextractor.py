import PyPDF2
from PIL import Image
import struct
import numpy as np
import azure.storage.blob
import argparse
from io import BytesIO
import os
import azure.storage.blob
import json
import re
from resizeimage import resizeimage

from azure.storage.common import CloudStorageAccount
from azure.storage.blob.models import BlobBlock

import http.client
import base64

"""
Links:
PDF format: http://www.adobe.com/content/dam/Adobe/en/devnet/acrobat/pdfs/pdf_reference_1-7.pdf
CCITT Group 4: https://www.itu.int/rec/dologin_pub.asp?lang=e&id=T-REC-T.6-198811-I!!PDF-E&type=items
Extract images from pdf: http://stackoverflow.com/questions/2693820/extract-images-from-pdf-without-resampling-in-python
Extract images coded with CCITTFaxDecode in .net: http://stackoverflow.com/questions/2641770/extracting-image-from-pdf-with-ccittfaxdecode-filter
TIFF format and tags: http://www.awaresystems.be/imaging/tiff/faq.html
"""

def get_all(data, key, values):
    if type(data) == str:
        data = json.loads(data)

    if type(data) is dict:
        for jsonkey in data:
            if type(data[jsonkey]) in (list, dict):
                get_all(data[jsonkey], key, values)
            elif jsonkey == key:
                value = re.sub(r'[^a-zA-Z0-9]','', data[jsonkey])

                if (len(value) >= 4):
                     values.append(value)

    elif type(data) is list:
        for item in data:
            if type(item) in (list, dict):
                get_all(item, key, values)

def createIndexEntry(args, key, filename, image_id, image, metadata, thumbnail, terms):

    dup_terms = set()
    uniq_terms = []

    for term in terms:
        if term not in dup_terms:
            uniq_terms.append(term)
            dup_terms.add(term)

    script = {   
        "value": [
            {
                "@search.action" : "upload",
                "key": key,
                "filename" : os.path.basename(filename),
                "imageid" : str(image_id),
                "container" : args.container,
                "folder" : args.folder,
                "volume" : args.volume,
                "terms" : uniq_terms,
                "image" : image,
                "metadata": metadata,
                "thumbnail": thumbnail,
                "url": args.storageUrl
            }
        ]

    }

    conn = http.client.HTTPSConnection(args.searchUrl)

    headers = {"Content-type": " application/json",
               "api-key": args.searchKey}

    conn.request("POST", "/indexes/council-books/docs/index?api-version=2017-11-11", json.dumps(script), headers)

    response = conn.getresponse()
    
    print('Index (Document) Creation Status', response.status, response.reason)

    print(response.read())  

def createIndex(args):

    script = {   
        "name": "council-books",  

        "fields": [
            {"name": "key", "type": "Edm.String", "key": "true", "searchable": "false", "sortable": "false", "facetable": "false"},
            {"name": "filename", "type": "Edm.String", "searchable": "false", "sortable": "false"},
            {"name": "imageid", "type": "Edm.String", "searchable": "false", "sortable": "false"},
            {"name": "image", "type": "Edm.String", "searchable": "false", "sortable": "false"},
            {"name": "metadata", "type": "Edm.String", "searchable": "false", "sortable": "false"},
            {"name": "thumbnail", "type": "Edm.String", "searchable": "false", "sortable": "false"},
            {"name": "container", "type": "Edm.String", "searchable": "false", "sortable": "false"},
            {"name": "folder", "type": "Edm.String", "searchable": "false", "sortable": "false"},
            {"name": "volume", "type": "Edm.String", "searchable": "false", "sortable": "false"},
            {"name": "url", "type": "Edm.String", "searchable": "false", "sortable": "false"},
            {"name": "terms", "type": "Collection(Edm.String)"}
        ]

    }

    conn = http.client.HTTPSConnection(args.searchUrl)

    headers = {"Content-type": " application/json",
               "api-key": args.searchKey}

    conn.request("POST", "/indexes?api-version=2017-11-11", json.dumps(script), headers)

    response = conn.getresponse()
    
    print('Index Creation Status', response.status, response.reason)

    print(response.read())


def ocrImage(service, args, imageUrl, jpeg_img_name):
    conn = http.client.HTTPSConnection(args.ocrUrl)
    headers = {"Content-type": " application/json",
               "Ocp-Apim-Subscription-Key": args.ocrKey}

    params = {'url': imageUrl}
    conn.request("POST", "/vision/v2.0/ocr", json.dumps(params), headers)

    response = conn.getresponse()
    print('OCR Status', response.status, response.reason)

    if (response.status == 200):
        ocrData = BytesIO(response.read())
        writeImage(service, args, jpeg_img_name + '.json', ocrData)
        print('OCR Saved')

        ocrData.seek(0)

        return ocrData
    else:
        return None
    
def writeImage(service, args, filename, stream):
    service.create_blob_from_stream(args.container, args.folder + '/' + args.volume + '/' + filename, stream)                                               

def tiff_header_for_CCITT(width, height, img_size, CCITT_group=4):
    tiff_header_struct = '<' + '2s' + 'h' + 'l' + 'h' + 'hhll' * 8 + 'h'
    return struct.pack(tiff_header_struct,
                       b'II',  # Byte order indication: Little indian
                       42,  # Version number (always 42)
                       8,  # Offset to first IFD
                       8,  # Number of tags in IFD
                       256, 4, 1, width,  # ImageWidth, LONG, 1, width
                       257, 4, 1, height,  # ImageLength, LONG, 1, lenght
                       258, 3, 1, 1,  # BitsPerSample, SHORT, 1, 1
                       259, 3, 1, CCITT_group,  # Compression, SHORT, 1, 4 = CCITT Group 4 fax encoding
                       262, 3, 1, 0,  # Threshholding, SHORT, 1, 0 = WhiteIsZero
                       273, 4, 1, struct.calcsize(tiff_header_struct),  # StripOffsets, LONG, 1, len of header
                       278, 4, 1, height,  # RowsPerStrip, LONG, 1, lenght
                       279, 4, 1, img_size,  # StripByteCounts, LONG, 1, size of image
                       0  # last IFD
                       )

if __name__ == '__main__':
    argParser = argparse.ArgumentParser(description='Process the Council Books.')

    argParser.add_argument('input', metavar='i', nargs='+',
                        help='the Council Book')
    argParser.add_argument('--account', help='The Azure Storage Account')
    argParser.add_argument('--key', help='The Azure Storage Key')
    argParser.add_argument('--container', help='The Azure Storage Container',
        default='council-books')
    argParser.add_argument('--ocrUrl', help='The OCR Url',
        default='https://australiaeast.api.cognitive.microsoft.com')
    argParser.add_argument('--ocrKey', help='The Key for the OCR service')
    
    argParser.add_argument('--searchUrl', help='The Url Search Service')
    argParser.add_argument('--searchKey', help='The Key Search Service')
    argParser.add_argument('--folder', help='The Folder Name',
        default='books')
    argParser.add_argument('--volume', help='The Volume')
    argParser.add_argument('--storageUrl', help='The Storage URL',
        default='https://tsoblob1.blob.core.windows.net/')

    args = argParser.parse_args()

    print("Input File: '%s'"% args.input[0]) 

    input = PyPDF2.PdfFileReader(open(args.input[0], "rb"))

    account = CloudStorageAccount(account_name=args.account, account_key=args.key)
        
    service = account.create_block_blob_service()

    service.create_container(args.container)
    service.set_container_acl(args.container, public_access=azure.storage.blob.models.PublicAccess.Container)

    createIndex(args)

    image_id = 0

    for index in range(input.getNumPages()):
        
        page = input.getPage(index)

        xObject = page['/Resources']['/XObject'].getObject()

        for obj in xObject:
     
            if xObject[obj]['/Subtype'] == '/Image':
                size = (xObject[obj]['/Width'], xObject[obj]['/Height'])
 
 
                if xObject[obj]['/ColorSpace'] == '/DeviceRGB':
                    mode = "RGB"
                else:
                    mode = "P"

                if xObject[obj]['/Filter'] == '/CCITTFaxDecode':
                    if xObject[obj]['/DecodeParms']['/K'] == -1:
                        CCITT_group = 4
                    else:
                        CCITT_group = 3
 
                    width = xObject[obj]['/Width']
                    height = xObject[obj]['/Height']
                    data = xObject[obj]._data 
                    img_size = len(data)

                    tiff_header = tiff_header_for_CCITT(width, height, img_size, CCITT_group)

                    imagefile = BytesIO() 
                    imagefile.write(tiff_header + data)

                    jpeg_img_name = obj[1:] + '_' + str(image_id)

                    print('Found Image: ' + jpeg_img_name)
                    
                    jpegfile = BytesIO()
                    
                    image = Image.open(imagefile)
                    image.save(jpegfile, "JPEG", quality=60)

                    jpegfile.seek(0)
                    
                    print('Saving Image: ' + jpeg_img_name)
                
                    writeImage(service, args, jpeg_img_name + '.jpeg', jpegfile)
                    
                    print('Saving Thumbnail: ' + jpeg_img_name)

                    cover = resizeimage.resize_thumbnail(image, [100, 200])

                    thumbnailfile = BytesIO()
                    cover.save(thumbnailfile, 'PNG')
                    thumbnailfile.seek(0)
 
                    writeImage(service, args, jpeg_img_name + '_thumbnail.png', thumbnailfile)
 
                    print('Image Saved: ' + jpeg_img_name)

                    ocrData = ocrImage(service, args, "https://tsoblob1.blob.core.windows.net/" + 
                                      args.container + "/" + args.folder + "/" + args.volume + "/" +jpeg_img_name + '.jpeg',
                                      jpeg_img_name)
                    terms = []
                    get_all(json.loads(ocrData.read()), 'text', terms)
                    print('Length', len(terms))         
                   
                    print('OCR Completed: ' + jpeg_img_name)
                    print('Creating Index Entry: ' + jpeg_img_name)
                    
                    createIndexEntry(args, 
                                    base64.b64encode((args.folder + '/' + args.volume + '/' + str(image_id) + '/' + jpeg_img_name).encode()).decode('utf8'),
                                    args.input[0],
                                    image_id,
                                    jpeg_img_name + '.jpeg', 
                                    jpeg_img_name + '.json', 
                                    jpeg_img_name + '_thumbnail.png', 
                                    terms)
                    print('Index Entry Created: ' + jpeg_img_name)
 
                    print('Image Processed: ' + jpeg_img_name)
                    print('')

                    image_id += 1

                elif xObject[obj]['/Filter'] == '/FlateDecode':
                    data = xObject[obj].getData()

                    img = Image.frombytes(mode, size, data)
                    img.save(obj[1:] + '_' + str(image_id) + ".png")
                    image_id += 1

                elif xObject[obj]['/Filter'] == '/DCTDecode':
                    data = xObject[obj].getData()

                    img = open(obj[1:] + '_' + str(image_id) + ".jpg", "wb")
                    img.write(data)
                    img.close()
                    image_id += 1

                elif xObject[obj]['/Filter'] == '/JPXDecode':
                    data = xObject[obj].getData()

                    img = open(obj[1:] + '_' + str(image_id) + ".jp2", "wb")
                    img.write(data)
                    img.close()
                    image_id += 1

    print('Completed Image Scan')                
 