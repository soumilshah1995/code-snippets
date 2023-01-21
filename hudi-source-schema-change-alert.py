import os

try:
    import boto3
    import pandas as pd
    from dotenv import load_dotenv

    load_dotenv(".env")
    print("All modules ok...")
except Exception as e:
    print("Error : {}".format(e))


class SchemaChanges(object):
    def __init__(self,
                 catalog_id,
                 db_name,
                 table_name,
                 aws_access_key_id,
                 aws_secret_access_key,
                 region_name
                 ):
        self.catalog_id = catalog_id
        self.db_name = db_name
        self.table_name = table_name
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.region_name = region_name

        self.versions_to_compare = [0, 1 , 2 ]
        self.delete_old_versions = False
        self.number_of_versions_to_retain = 2
        self.columns_modified = []

        self.glue = boto3.client('glue',
                                 aws_access_key_id=self.aws_access_key_id,
                                 aws_secret_access_key=self.aws_secret_access_key,
                                 region_name=self.region_name
                                 )
        self.sns = boto3.client('sns',
                                aws_access_key_id=self.aws_access_key_id,
                                aws_secret_access_key=self.aws_secret_access_key,
                                region_name=self.region_name
                                )

    def send_email_alerts(self, topic_arn, message_to_send, subject="Notification: Changes in table schema"):
        try:
            response = self.sns.publish(
                TopicArn=topic_arn,
                Message=str(message_to_send.get("message")),
                Subject=subject

            )
            return response
        except Exception as e:
            print("Error ", e)

    def __get_table_versions(self):

        response = self.glue.get_table_versions(
            CatalogId=self.catalog_id,
            DatabaseName=self.db_name,
            TableName=self.table_name,
            MaxResults=100
        )
        return response

    def __version_id(self, json):

        try:
            return int(json['VersionId'])
        except KeyError:
            return 0

    def __findAddedUpdated(self, new_cols_df, old_cols_df, old_col_name_list):
        for index, row in new_cols_df.iterrows():
            new_col_name = new_cols_df.iloc[index]['Name']
            new_col_type = new_cols_df.iloc[index]['Type']

            # Check if a column with same name exist in old table but the data type has chaged
            if new_col_name in old_col_name_list:
                old_col_idx = old_cols_df.index[old_cols_df['Name'] == new_col_name][0]
                old_col_type = old_cols_df.iloc[old_col_idx]['Type']

                if old_col_type != new_col_type:
                    self.columns_modified.append(
                        f"Data type changed for '{new_col_name}' from '{old_col_type}' to '{new_col_type}'")
            # If a column is only in new column list, it a newly added column
            else:
                self.columns_modified.append(f"Added new column '{new_col_name}' with data type as '{new_col_type}'")

    def __findDropped(self, old_cols_df, new_col_name_list):
        for index, row in old_cols_df.iterrows():
            old_col_name = old_cols_df.iloc[index]['Name']
            old_col_type = old_cols_df.iloc[index]['Type']

            # check if column doesn't exist in new column list
            if old_col_name not in new_col_name_list:
                self.columns_modified.append(f"Dropped old column '{old_col_name}' with data type as '{old_col_type}'")

    def __delele_versions(self, glue_client, versions_list, number_of_versions_to_retain):
        print("deleting old versions...")
        if len(versions_list) > number_of_versions_to_retain:
            version_id_list = []
            for table_version in versions_list:
                version_id_list.append(int(table_version['VersionId']))
            # Sort the versions in descending order
            version_id_list.sort(reverse=True)
            versions_str_list = [str(x) for x in version_id_list]
            versions_to_delete = versions_str_list[number_of_versions_to_retain:]

            del_response = glue_client.batch_delete_table_version(
                DatabaseName=self.db_name,
                TableName=self.table_name,
                VersionIds=versions_to_delete
            )
            return del_response

    def IsSchemaChange(self):
        """
        Return boolean if there is schema changes
        :return: Dict
        """
        response = self.__get_table_versions()

        table_versions = response['TableVersions']
        table_versions.sort(key=self.__version_id, reverse=True)
        version_count = len(table_versions)


        if version_count > max(self.versions_to_compare):
            new_columns = table_versions[self.versions_to_compare[0]]['Table']['StorageDescriptor']['Columns']
            new_cols_df = pd.DataFrame(new_columns)

            old_columns = table_versions[self.versions_to_compare[2]]['Table']['StorageDescriptor']['Columns']
            old_cols_df = pd.DataFrame(old_columns)

            new_col_name_list = new_cols_df['Name'].tolist()
            old_col_name_list = old_cols_df['Name'].tolist()

            self.__findAddedUpdated(new_cols_df, old_cols_df, old_col_name_list)
            self.__findDropped(old_cols_df, new_col_name_list)



            if len(self.columns_modified) > 0:
                email_msg = f"Following changes are identified in '{self.table_name}' table of '{self.db_name}' database of your Datawarehouse. Please review.\n\n"
                for column_modified in self.columns_modified: email_msg += f"\t{column_modified}\n"
                _ = {
                    "statusCode": -1,
                    "message": {
                        "message": email_msg
                    }
                }

                return _

            else:
                return {
                    "statusCode": 200,
                }

        else:
            return {
                "statusCode": 200,

            }


def main():
    helper = SchemaChanges(
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY"),
        aws_secret_access_key=os.getenv("AWS_SECRET_KEY"),
        region_name="us-west-2",
        catalog_id=os.getenv("catalog_id"),
        db_name='hudidb',
        table_name='hudi_table'
    )
    response = helper.IsSchemaChange()

    if response.get("statusCode").__str__() == "200":
        print("all okay")
    else:
        print("ALERT")
        helper.send_email_alerts(
            topic_arn=os.getenv("TOPIC_ARN"),
            message_to_send=response.get("message")
        )

main()
