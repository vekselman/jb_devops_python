import boto3
from aws_app import utils

S3_CLIENT = None
S3_RESOURCE = None
FIRST_BUCKET_NAME = "leonid-first-bucket"
SECOND_BUCKET_NAME = "leonid-second-bucket"
FIRST_FILE_NAME = "firstfile.txt"
SECOND_FILE_NAME = "secondfile.txt"
THIRD_FILE_NAME = "thirdfile.txt"
# AFTER create_and_upload
FIRST_BUCKET_NAME_CR = "leonid-first-bucket-15ed1964-576d-41ed-95bc-7f751282fd4a"
SECOND_BUCKET_NAME_CR = "leonid-second-bucket-66b64841-4f13-4401-8c32-d2dd6a1f44e9"
FIRST_FILE_NAME_CR = "db3f1bfirstfile.txt"
SECOND_FILE_NAME_CR = "a0568csecondfile.txt"
THIRD_FILE_NAME_CR = "46ed4ethirdfile.txt"


def create_and_upload(s3_client, s3_resource):
    """
    Main execution function
    :return:
    """
    first_bucket_name, first_response = utils.create_bucket(
        bucket_prefix=FIRST_BUCKET_NAME,
        s3_connection=s3_resource.meta.client)

    second_bucket_name, second_response = utils.create_bucket(
        bucket_prefix=SECOND_BUCKET_NAME, s3_connection=s3_resource)

    first_file_name = utils.create_temp_file(300, FIRST_FILE_NAME, 'f')
    first_object = s3_resource.Object(
        bucket_name=first_bucket_name, key=first_file_name)
    first_bucket_again = first_object.Bucket()

    s3_resource.Object(first_bucket_name, first_file_name).upload_file(
        Filename=first_file_name)


def download_file(s3_resource):
    s3_resource.Object(FIRST_BUCKET_NAME_CR, FIRST_FILE_NAME_CR).download_file(
        f'/tmp/{FIRST_FILE_NAME}')


def acl(s3_resource):
    second_file_name = utils.create_temp_file(400, SECOND_FILE_NAME, 's')
    second_object = s3_resource.Object(FIRST_BUCKET_NAME_CR, second_file_name)
    second_object.upload_file(
        second_file_name,
        ExtraArgs={
            'ACL': 'public-read'}
    )
    print(f"second_file_name: {second_file_name}")

    second_object_acl = second_object.Acl()
    print(f"Grants public:\n{second_object_acl.grants}")

    response = second_object_acl.put(ACL='private')
    print(f"Grants private:\n{second_object_acl.grants}")


def encryption(s3_resource):
    third_file_name = utils.create_temp_file(300, THIRD_FILE_NAME, 't')
    third_object = s3_resource.Object(FIRST_BUCKET_NAME_CR, third_file_name)
    third_object.upload_file(
        third_file_name,
        ExtraArgs={
            'ServerSideEncryption': 'AES256'}
    )
    print(f"third_file_name: {third_file_name}")
    print(f"File encryption: {third_object.server_side_encryption}")


def storage(s3_resource):
    third_object = s3_resource.Object(
        FIRST_BUCKET_NAME_CR,
        THIRD_FILE_NAME_CR
    )
    third_object.upload_file(
        THIRD_FILE_NAME_CR,
        ExtraArgs={
            'ServerSideEncryption': 'AES256',
            'StorageClass': 'STANDARD_IA'}
    )
    third_object.reload()
    print(f"storage class: {third_object.storage_class}")


def versioning(s3_resource):
    utils.enable_bucket_versioning(s3_resource, FIRST_BUCKET_NAME_CR)

    s3_resource.Object(
        FIRST_BUCKET_NAME_CR,
        FIRST_FILE_NAME_CR
    ).upload_file(
        FIRST_FILE_NAME_CR)

    s3_resource.Object(
        FIRST_BUCKET_NAME_CR,
        FIRST_FILE_NAME_CR
    ).upload_file(
        THIRD_FILE_NAME_CR)

    s3_resource.Object(
        FIRST_BUCKET_NAME_CR,
        SECOND_FILE_NAME_CR
    ).upload_file(
        SECOND_FILE_NAME_CR)

    print(f"Version ID:{s3_resource.Object(FIRST_BUCKET_NAME_CR, FIRST_FILE_NAME_CR).version_id}")


if __name__ == '__main__':
    S3_CLIENT = boto3.client('s3')
    S3_RESOURCE = boto3.resource('s3')

    # Uploading a File
    # create_and_upload(S3_CLIENT, S3_RESOURCE)

    # Downloading a File
    # download_file(S3_RESOURCE)

    # Copying an Object Between Buckets
    # utils.copy_to_bucket(S3_RESOURCE, FIRST_BUCKET_NAME_CR, SECOND_BUCKET_NAME_CR, FIRST_FILE_NAME_CR)
    # OUTPUT: s3.ObjectSummary(bucket_name='leonid-second-bucket-66b64841-4f13-4401-8c32-d2dd6a1f44e9',
    #                           key='db3f1bfirstfile.txt')

    # Deleting an Object
    # S3_RESOURCE.Object(SECOND_BUCKET_NAME_CR, FIRST_FILE_NAME_CR).delete()

    # Access Control Lists
    # acl(S3_RESOURCE)
    # OUTPUT:
    # Grants public:
    # [{'Grantee': {'DisplayName': 'yaniv', 'ID': '0f0f2d9d7458b564882f5237e1f334c17e1c6fff1ec1445c0f3644ea518c7ac7',
    #   'Type': 'CanonicalUser'}, 'Permission': 'FULL_CONTROL'}, {'Grantee': {'Type': 'Group',
    #   'URI': 'http://acs.amazonaws.com/groups/global/AllUsers'}, 'Permission': 'READ'}]
    # Grants private:
    # [{'Grantee': {'DisplayName': 'yaniv', 'ID': '0f0f2d9d7458b564882f5237e1f334c17e1c6fff1ec1445c0f3644ea518c7ac7',
    #   'Type': 'CanonicalUser'}, 'Permission': 'FULL_CONTROL'}]

    # Encryption
    # encryption(S3_RESOURCE)
    # OUTPUT:
    # third_file_name: 46ed4ethirdfile.txt
    # File encryption: AES256

    # Storage
    # storage(S3_RESOURCE)
    # Output:
    # storage class: STANDARD_IA

    # Versioning
    # versioning(S3_RESOURCE)
    # Output: Version ID:1kbHA7.5fEnAf3K8Lg7VlUFzIaTELOgp

    # Bucket Traversal
    print("ALL Buckets:\n")
    utils.bucket_traversal(S3_RESOURCE)
    print("=============================\nAll objects\n")
    utils.print_objects(S3_RESOURCE.Bucket(FIRST_BUCKET_NAME_CR))


    # ===============================================================
    # LIST BUCKETS
    # response = S3_CLIENT.list_buckets()
    #
    # # Output the bucket names
    # print('Existing buckets:')
    # for bucket in response['Buckets']:
    #     print(f'  {bucket["Name"]}')

    # LIST FILES
    # for my_bucket_object in S3_RESOURCE.Bucket(SECOND_BUCKET_NAME_CR).objects.all():
    #     print(my_bucket_object)
