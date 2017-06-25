
# coding: utf-8

# In[29]:

import os
import requests
from pprint import pprint
import json
from datetime import datetime,time, timedelta
import glob
import time
import pandas as pd
import logging
import urllib.request
import logging
import logging.handlers
import boto3
import botocore
import botocore.session


# In[ ]:




# In[30]:

import logging
import logging.handlers

logger=logging.getLogger(__name__)
logger.setLevel(logging.INFO)

logfile1 = time.strftime("%Y-%m-%d_%H_%M_%S"+".log")
print (logfile1)
handler= logging.FileHandler(logfile1)
handler.setLevel(logging.INFO)

formatter= logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.info('hello ')



# In[ ]:




# In[24]:

logger.info('Reading Json')
with open('intial_config.json') as data_file:    
    data = json.load(data_file)

# print(data)


# In[25]:

links=[]
for values in (data['link1'],data['link2'],data['link3'],data['link4'],data['link5'],data['link6'],data['link7'],data['link8']):
               links.append(values)
links


# In[26]:

# path=os.getcwd()
# #os.mkdir('Data1')
# final_path= path+'/'+'Data1/'


# In[27]:

# final_path


# In[28]:

import io
df_list= []

for x in links:
    
    s=requests.get(x).content
    c=pd.read_csv(io.StringIO(s.decode('utf-8')))
    df_list.append(c)
full_df=pd.concat(df_list)


# In[11]:

# full_df.to_csv(os.path.join(final_path,'CA_140617_23155.csv'))


# In[12]:

if os.path.exists('CA_2017-06-14_23155.csv'):
    print ('it exists')
else:
    full_df.to_csv(('CA_2017-06-14_23155.csv'))


# In[13]:

full_df.head(2)


# In[14]:

# import urllib.request
# final= final_path+'CA_140617_23155.csv'
# # if os.path.exists(final):
#     print('File exists')
# else:
#     for x in links:

#         csv = urllib.request.urlopen(x).read() # returns type 'bytes'
 
        
#         with open(final, 'a', newline='') as fx:

#             # bytes, hence mode 'wb'
#             fx.write(csv)


# In[15]:

# df= pd.read_csv(final)


# In[16]:

# df.head()


# In[17]:

# df['temp_date']= df['DATE'].str[:-6]


# In[18]:

# df.head()


# In[ ]:




# In[ ]:




# In[ ]:




# In[19]:

# df_by_date = df.groupby("temp_date")
# for (date, date_df) in df_by_date:
#     #print (date)
#     filename = date.replace("/", "") + ".csv"
#     if os.path.exists(filename):
#          print(filename+' '+ 'exists')
#     else:
        
#         date_df.to_csv((filename))



# In[ ]:


        
        


# In[20]:

import boto3
s3 = boto3.resource(
    's3',
    aws_access_key_id=data["AWSAccess"],
    aws_secret_access_key=data["AWSSecret"])

client= boto3.client('s3', 
                     aws_access_key_id=data["AWSAccess"],
                    aws_secret_access_key=data["AWSSecret"])


# In[21]:

# s3.create_bucket(Bucket='adsassign1_databucket')


# In[31]:

names=[]
response = client.list_buckets()
for bucket in response["Buckets"]:
    names.append(bucket)

Bucketname= 'adsassign1_databucket'
if Bucketname in names:
    print('it exists')
else:
    s3.create_bucket(Bucket=Bucketname)   


# In[32]:

# import botocore
# from botocore.client import ClientError
# BucketName= 'adsassignment1_databucket1'
# bucket= s3.Bucket(BucketName)
# exists= True

# try:
#     s3.meta.client.head_bucket(Bucket=BucketName)
# except botocore.exceptions.ClientError as e:
#     error_code= int(e.respone['Error']['Code'])
#     if error_code == 404:
#         logger.info(BucketName+'does not exists!')
#         exists= False
        
        
#     else:
#         logger.info('adsassign1_databucket exists')
# if exists== False:
   


# In[ ]:




# In[33]:


# client.upload_file('2017-01-01.csv', 'adsassign1_databucket1', '2017-01-01.csv')
# #client.put_object('adsassign1_databucket1', '2017-01-01.csv').put(Body=open('2017-01-01.csv', 'rb'))


# In[35]:

uploadFileNames = []

for filename in glob.glob("*.csv"):
    uploadFileNames.append(filename)
    print(filename)


# In[27]:

import botocore.session
for files in uploadFileNames:
    try:
        s3.Object('adsassign1_databucket', files).load()
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            s3.Object("adsassign1_databucket", files).put(Body=open(files, 'rb'))
            print(files+' uploaded')
            
        else:
            raise
    else:
        exists = True

    print(files+''+' exists')
        


# In[ ]:




# In[44]:

# data= glob.glob('historical.csv')
# # df1= pd.read_csv('historical.csv')


# In[45]:

# for file in glob.glob("*.csv"):
#     print (file)


# In[46]:

# import os
# for root, dirs, files in os.walk("\kanakia_dhruv_spring2017"):
#     for file in files:
#         if file.endswith(".csv"):
#              print(os.path.join(root, file))


# In[47]:

# os.= '\D:\DataAnalysis4Python\kanakia_dhruv_spring2017\.csv'


# In[ ]:




# In[ ]:



