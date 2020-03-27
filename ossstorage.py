import os
from oss2 import SizedFileAdapter, determine_part_size
from oss2.models import PartInfo
import oss2
from itertools import islice

OSS_ACCESS_KEY_ID=[OSS_ACCESS_KEY_ID]
OSS_ACCESS_KEY_SECRET=[OSS_ACCESS_KEY_SECRET]
url = "oss-ap-southeast-1.aliyuncs.com"
auth = oss2. Auth(OSS_ACCESS_KEY_ID, OSS_ACCESS_KEY_SECRET)


class OssStorage(object):

    def __init__(self, bucket_name=None, obj_name=None, obj_file=None):
        self.bucket_name = bucket_name
        self.obj_name = obj_name
        self.obj_file = obj_file

    def create_buckets(self, bucket_name):
        bucket = oss2. Bucket(auth, url, self.bucket_name)

        # The default storage class and ACL of the created bucket are set to standard and private read respectively.
        bucket = bucket.create_bucket()
        return bucket

    def delete_bucket(self,bucket_name):
        bucket = oss2. Bucket(auth, url, bucket_name)
        try:
            # Delete the bucket.
            bucket = bucket.delete_bucket()
        except oss2.exceptions.BucketNotEmpty:
            bucket = 'bucket is not empty.'
        except oss2.exceptions.NoSuchBucket:
            bucket = 'bucket does not exist'
        return bucket

    def bucket_change_acl(self, bucket_name, acl):
        bucket = oss2. Bucket(auth, url, bucket_name)
        #acl private = oss2.BUCKET_ACL_PRIVATE
        #acl public read = oss2.BUCKET_ACL_PUBLIC_READ
        #acl public read write = oss2.BUCKET_ACL_PUBLIC_READ_WRITE
        bucket = bucket.put_bucket_acl(acl)
        return bucket

    def list_buckets(self):
        service = oss2. Service(auth, url)
        service = oss2.BucketIterator(service)
        lst = []
        for x in service:
            obj = {}
            obj['name'] = x.name
            obj['storage_class'] = x.storage_class
            obj['creation_date'] = x.creation_date
            obj['intranet_endpoint'] = x.intranet_endpoint
            obj['extranet_endpoint'] = x.extranet_endpoint
            lst.append(obj)
        return lst
    
    def bucket_info(self, bucket_name):
        bucket = oss2.Bucket(auth, url, bucket_name).get_bucket_info()
        return {
            'name': bucket.name,
            'storage_class': bucket.storage_class,
            'creation_date': bucket.creation_date,
            'intranet_endpoint':bucket.intranet_endpoint,
            'extranet_endpoint': bucket.extranet_endpoint,
            'owner_id': bucket.owner.id,
            'grant': bucket.acl.grant
        }

    def upload_single_objects(self, bucket_name, obj_name, obj_file):
        bucket = oss2. Bucket(auth,url,bucket_name)
        obj = bucket.put_object_from_file(obj_name,obj_file)
        return obj.status

    def upload_multiple_object(self, bucket_name, obj_name, obj_file):
        try:
            bucket = oss2. Bucket(auth,url,bucket_name)
            total_size = os.path.getsize(obj_file)
            # Use determine_part_size to determine the part size.
            part_size = determine_part_size(total_size, preferred_size=100 * 1024)
            # Initialize a multipart upload event.
            upload_id = bucket.init_multipart_upload(obj_name).upload_id
            parts = []
            # Upload parts one by one.
            with open(obj_file, 'rb') as fileobj:
                part_number = 1
                offset = 0
                while offset < total_size:
                    num_to_upload = min(part_size, total_size - offset)
                    # The SizedFileAdapter(fileobj, size) method generates a new object, and re-calculates the initial append location.
                    result = bucket.upload_part(obj_name, upload_id, part_number,
                                                SizedFileAdapter(fileobj, num_to_upload))
                    parts.append(PartInfo(part_number, result.etag))
                    offset += num_to_upload
                    part_number += 1

            # Complete multipart upload.
            bucket.complete_multipart_upload(obj_name, upload_id, parts)
            # Verify the multipart upload.
            with open(obj_file, 'rb') as fileobj:
                assert bucket.get_object(obj_name).read() == fileobj.read()            
            status = "success"
        except Exception as e:
            status = str(e)
        return status
    
    def download_objects(self):
        return
    
    def list_objects(self,bucket_name):
        bucket = oss2. Bucket(auth, url, bucket_name)
        lst = []
        for b in islice(oss2. ObjectIterator(bucket), 10):
            obj = {}
            obj['name'] = b.key
            obj['last_modified'] = b.last_modified
            obj['etag'] = b.etag
            obj['type'] = b.type
            obj['size'] = b.size
            obj['storage_class'] = b.storage_class
            obj['url'] =  bucket.sign_url('GET', b.key, 60) #60(s)
            lst.append(obj)
        return lst

    def delete_single_objects(self, bucket_name, obj_name):
        bucket = oss2. Bucket(auth, url, bucket_name)
        obj = bucket.delete_object(obj_name)
        return obj

    def delete_multiple_objects(self, bucket_name, obj_name):#['obj_name1','obj_name2','...']
        try:
            bucket = oss2. Bucket(auth, url, bucket_name)
            result = bucket.batch_delete_objects(obj_name)
            print('\n'.join(result.deleted_keys))
            result = 'success'
        except Exception as e:
            result = str(e)
        return result

    def object_url_authorized(self, bucket_name, obj_name, time=60):
        bucket = oss2. Bucket(auth, url, bucket_name)
        obj_url = bucket.sign_url('GET', obj_name, time)
        return obj_url



