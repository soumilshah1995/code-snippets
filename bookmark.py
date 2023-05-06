"""
Author : Soumil Nitin Shah

"""

try:
    import sys, os, ast, uuid, boto3, datetime, time, re, json, hashlib

    from ast import literal_eval
    from dataclasses import dataclass
    from datetime import datetime
    import math
    import threading
    from dateutil.parser import parse
    from datetime import datetime, timedelta
    from boto3.s3.transfer import TransferConfig
    from dateutil.tz import tzutc
    from datetime import datetime, timezone
    from dateutil.parser import parse
    from dotenv import load_dotenv
    load_dotenv("dev.env")
except Exception as e:
    print(e)


class AWSS3(object):
    """Helper class to which add functionality on top of boto3 """

    def __init__(self, bucket):

        self.BucketName = bucket
        self.client = boto3.client("s3",
                                   aws_access_key_id=os.getenv("AWS_ACCESS_KEY"),
                                   aws_secret_access_key=os.getenv("AWS_SECRET_KEY"),
                                   region_name=os.getenv("AWS_REGION")
                                   )

    def put_files(self, Response=None, Key=None):
        """
        Put the File on S3
        :return: Bool
        """
        try:
            response = self.client.put_object(
                Body=Response, Bucket=self.BucketName, Key=Key
            )
            return "ok"
        except Exception as e:
            raise Exception("Error : {} ".format(e))

    def item_exists(self, Key):
        """Given key check if the items exists on AWS S3 """
        try:
            response_new = self.client.get_object(Bucket=self.BucketName, Key=str(Key))
            return True
        except Exception as e:
            return False

    def get_item(self, Key):

        """Gets the Bytes Data from AWS S3 """

        try:
            response_new = self.client.get_object(Bucket=self.BucketName, Key=str(Key))
            return response_new["Body"].read()

        except Exception as e:
            print("Error :{}".format(e))
            return False

    def find_one_update(self, data=None, key=None):

        """
        This checks if Key is on S3 if it is return the data from s3
        else store on s3 and return it
        """

        flag = self.item_exists(Key=key)

        if flag:
            data = self.get_item(Key=key)
            return data

        else:
            self.put_files(Key=key, Response=data)
            return data

    def delete_object(self, Key):

        response = self.client.delete_object(Bucket=self.BucketName, Key=Key, )
        return response

    def get_folder_names(self, Prefix=""):
        """
        :param Prefix: Prefix string
        :return: List of folder names
        """
        try:
            paginator = self.client.get_paginator("list_objects_v2")
            pages = paginator.paginate(Bucket=self.BucketName, Prefix=Prefix, Delimiter='/')

            folder_names = []

            for page in pages:
                for prefix in page.get('CommonPrefixes', []):
                    folder_names.append(prefix['Prefix'].rstrip('/'))

            return folder_names
        except Exception as e:
            print("error", e)
            return []

    def get_ll_keys_with_meta_data_sorted(self, Prefix="", timestamp=None, sort_order='asc'):
        """
        :param Prefix: Prefix string
        :param timestamp: datetime object
        :param sort_order: 'asc' for ascending, 'desc' for descending
        :return: Sorted keys List with full S3 path
        """
        try:
            paginator = self.client.get_paginator("list_objects_v2")
            pages = paginator.paginate(Bucket=self.BucketName, Prefix=Prefix)

            tmp = []

            for page in pages:
                for obj in page["Contents"]:
                    last_modified = obj["LastModified"].replace(tzinfo=timezone.utc)
                    if timestamp is None:
                        full_path = f's3://{self.BucketName}/{obj["Key"]}'
                        obj['full_path'] = full_path
                        tmp.append(obj)
                    else:
                        """filter out keys greater than datetime provided """
                        if last_modified > timestamp:
                            # Return full S3 path
                            full_path = f's3://{self.BucketName}/{obj["Key"]}'
                            obj['full_path'] = full_path
                            tmp.append(obj)
                        else:
                            pass
            # Sort the list based on LastModified value
            sorted_tmp = sorted(tmp, key=lambda x: x['LastModified'], reverse=(sort_order == 'desc'))
            return sorted_tmp
        except Exception as e:
            print("error", e)
            return []


class Checkpoints(AWSS3):
    def __init__(self, path):
        self.path = path
        self.bucket = self.path.split("/")[2]

        AWSS3.__init__(
            self, self.bucket
        )
        self.__data = {
            "process_time": datetime.now().__str__(),
            "last_processed_file_name": None,
            "last_processed_time_stamp_of_file": None,
            "last_processed_file_path": None,
            "last_processed_partition": None
        }
        self.prefix = self.path.split(f"{self.bucket}/")[1]

        self.filename = f"{hashlib.sha256(str(self.path).encode('utf-8')).hexdigest()}.json"
        self.meta_Data = []
        self.folders = None

    def __get_objects_each_folder(self, folder, timestamp=None):
        for item in self.get_ll_keys_with_meta_data_sorted(Prefix=folder, timestamp=timestamp):
            self.meta_Data.append(item)

    def read(self):
        if self.checkpoint_exists():
            read_check_points = self.read_check_point()
            print("read_check_points", json.dumps(read_check_points, indent=3))

            timestamp = parse(read_check_points.get("last_processed_time_stamp_of_file")).replace(tzinfo=timezone.utc)

            """Get the folder Names """
            self.folders = self.get_folder_names(Prefix=self.prefix)
            if self.folders != []:
                for folder in self.folders:
                    self.__get_objects_each_folder(folder=folder, timestamp=timestamp)
            else:
                self.__get_objects_each_folder(folder=self.prefix, timestamp=timestamp)

            return self.meta_Data

        else:
            self.folders = self.get_folder_names(Prefix=self.prefix)
            if self.folders != []:
                for folder in self.folders:
                    self.__get_objects_each_folder(folder=folder)
            else:
                self.__get_objects_each_folder(folder=self.prefix)
            return self.meta_Data

    def commit(self):
        if self.meta_Data != []:
            if self.folders != []:
                self.create_check_points(
                    last_processed_time_stamp_of_file=str(self.meta_Data[-1].get("LastModified")),
                    last_processed_file_name=self.meta_Data[-1].get("Key"),
                    last_processed_file_path=self.meta_Data[-1].get("full_path"),
                    last_processed_partition=self.folders[-1]
                )
            else:
                self.create_check_points(
                    last_processed_time_stamp_of_file=str(self.meta_Data[-1].get("LastModified")),
                    last_processed_file_name=self.meta_Data[-1].get("Key"),
                    last_processed_file_path=self.meta_Data[-1].get("full_path"),
                    last_processed_partition=None
                )

    def read_check_point(self):
        return json.loads(self.get_item(Key=self.filename).decode("utf-8"))

    def checkpoint_exists(self):
        return self.item_exists(Key=self.filename)

    def create_check_points(self, last_processed_time_stamp_of_file, last_processed_file_name,
                            last_processed_file_path, last_processed_partition):
        print(self.folders)
        self.__data['last_processed_time_stamp_of_file'] = last_processed_time_stamp_of_file
        self.__data['last_processed_file_name'] = last_processed_file_name
        self.__data['last_processed_partition'] = last_processed_partition

        self.put_files(
            Key=self.filename,
            Response=json.dumps(
                self.__data
            )
        )
        return True

    def delete_checkpoints(self):
        self.delete_object(Key=self.filename)


def insert_dummy_data_no_partition():
    bucket = "delta-streamer-demo-hudi"
    helper = AWSS3(bucket)
    print("ok")
    for i in range(11, 12):
        message = {"id": str(i)}
        res = helper.put_files(Key=f"book_mark_demo/no_partition/{str(i)}.json", Response=json.dumps(message))
        print("res", res)

def insert_dummy_data_partition():
    bucket = "delta-streamer-demo-hudi"
    helper = AWSS3(bucket)
    print("ok")
    for i in range(10, 12):
        message = {"id": str(i)}
        res = helper.put_files(Key=f"book_mark_demo/partition/year=2023/{str(i)}.json", Response=json.dumps(message))
        print("res", res)


def main():
    path = "s3://delta-streamer-demo-hudi/book_mark_demo/no_partition/"
    # path = "s3://delta-streamer-demo-hudi/book_mark_demo/partition/"
    helper = Checkpoints(path=path)
    response = helper.read()

    for item in response:
        print(item.get("full_path", ""))

    # helper.delete_checkpoints()
    # helper.delete_checkpoints()
    helper.commit()
#insert_dummy_data_no_partition()
main()
