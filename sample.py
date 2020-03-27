
from ossstorage import OssStorage

#get all bucket
print(OssStorage().list_buckets())

#get all object
print(OssStorage().list_objects([bucket_name]))

#get bucket info
print(OssStorage().bucket_info([bucket_name]))




    