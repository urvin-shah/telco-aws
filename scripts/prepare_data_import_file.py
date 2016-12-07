from __future__ import print_function # Python 2/3 compatibility
from datetime import datetime
from time import time
import sys
import traceback
import uuid
import csv
import json
import decimal 
import re
import urllib2
import os

class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            if o % 1 > 0:
                return float(o)
            else:
                return int(o)
        return super(DecimalEncoder, self).default(o)

def getDawaId(kvh):
  if len(kvh) != 12:
    raise ValueError("Invalid KVH : " + kvh + " Length : " + str(kvhLength))

  response = urllib2.urlopen("https://dawa.aws.dk/adgangsadresser?kvh="+kvh).read()
  if not response:
    raise ValueError("Get Dawa Info failed. KVH : " + kvh)

  dawaInfo = json.loads(response)[0]

  dawaId = dawaInfo['id']

  if not dawaId:
    raise ValueError("Invailid DawaId: "+ str(dawaId) +", KVH : " + str(kvh))

  return dawaId

def prepareData():

  datafileName = 'sample_data/bb_dk_address_mapping_dev11'

  os.path.isfile(datafileName) and os.remove(datafileName)
  
  with open("sample_data/DK_BB_KVH_Data_File_2m.csv") as csvfile:
      spamreader = csv.reader(csvfile, delimiter=';')
      spamreader.next()#skip the first row
      createdTime = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
      index = 1
      with open(datafileName,'a+') as dataFile:
        for row in spamreader:

            kvh = formatKVH(row[0].strip())

            if len(kvh) != 12:
              raise ValueError("Line : " +index+ ", Invalid KVH : " + kvh + " to " + kvh)

            #dawaId = getDawaId(kvh)
            dawaId = 'empty'
            vdslDownload = int(row[9].strip() or 0)
            vdslUpload = int(row[10].strip() or 0)
            adslDownloadA = int(row[11].strip() or 0)
            adslUploadA = int(row[12].strip() or 0)
            adslDownloadM = int(row[13].strip() or 0)
            adslUploadM = int(row[14].strip() or 0)

            maxDownload = decimal.Decimal(max([vdslDownload, adslDownloadA, adslDownloadM]))/decimal.Decimal(1000)
            maxUpload = decimal.Decimal(max([vdslUpload, adslUploadA, adslUploadM]))/decimal.Decimal(1000)

            item = {
                 'kvh': {'s':kvh},
                 'dawa_id': {'s':dawaId},
                 'max_download_speed': {'n':maxDownload},
                 'max_upload_speed': {'n':maxUpload},
                 'created_time': {'s':createdTime}
            }

            data =json.dumps(item, cls=DecimalEncoder) 

            dataFile.write("{}\n".format(data))
            index+=1
            if index%10000 == 0:
              print (index)

def formatKVH(kvh):
  kvh = kvh.strip()
  kvhLength = len(kvh)
  #print("KVH : " + kvh + ", Length : " + str(kvhLength))
  if kvhLength != 10 and kvhLength != 11:
    raise ValueError("Invalid KVH : " + kvh + " Length : " + str(kvhLength))
  municipality = "0" + kvh[:3]
  #print('municipality : '+ municipality)
  roadTickCode = kvh[3:7]
  #print('roadTickCode : '+ roadTickCode)
  houseNumber = kvh[7::]
  #print('houseNumber 1: '+ houseNumber)
  houseNumber = houseNumber.lstrip("0").rjust(4,"_")
  #print('houseNumber 2: '+ houseNumber)
  return municipality + roadTickCode + houseNumber

try:
  startTime = time()
  
  prepareData()
  
  print ("total time:" + str(round(time()-startTime, 3)) + "s")
except:
    traceback.print_exc()
