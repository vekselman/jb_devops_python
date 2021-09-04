import boto3
from aws_app import utils
from aws_app import pipeline as pl

S3_CLIENT = None
S3_RESOURCE = None
FIRST_BUCKET_NAME = "leonid-first-bucket"
SECOND_BUCKET_NAME = "leonid-second-bucket"
FIRST_FILE_NAME = "firstfile.txt"
SECOND_FILE_NAME = "secondfile.txt"
THIRD_FILE_NAME = "thirdfile.txt"


def main(s3_cleint, s3_resource) -> None:
    """
    Main function, for pipeline execution

    :param s3_cleint: main S3 client
    :param s3_resource: main S3 resource
    :return: None
    """
    print("+++++++++   Start   +++++++++")
    # Uploading a File
    print("Task 1. Create two buckets, and upload file")
    (first_bucket_name,
     second_bucket_name,
     first_file_name) = pl.create_and_upload(s3_cleint,
                                             s3_resource,
                                             FIRST_BUCKET_NAME,
                                             SECOND_BUCKET_NAME,
                                             FIRST_FILE_NAME)
    print("=" * 40)

    # Downloading a File
    print("Task 2. Download file from bucket ot /tmp")
    pl.download_file(s3_resource,
                     first_bucket_name,
                     first_file_name)

    print("=" * 40)

    # Copying an Object Between Buckets
    print("Task 3. Copy object Between Buckets")
    print(f"Copy {first_file_name} from\n\t{first_bucket_name}\nto\n\t{second_bucket_name}")
    utils.copy_to_bucket(S3_RESOURCE,
                         first_bucket_name,
                         second_bucket_name,
                         first_file_name)

    print("=" * 40)

    # Deleting an Object
    print("Task 4. Deleting an Object.")
    print(f"Delete {first_file_name} from\n\t{second_bucket_name}")
    s3_resource.Object(second_bucket_name, first_file_name).delete()

    print("=" * 40)

    # Access Control Lists
    print("Task 5. Create second file and grant acls.")
    second_file_name = pl.acl(s3_resource,
                              first_bucket_name,
                              SECOND_FILE_NAME)

    print("=" * 40)

    # Encryption
    print("Task 6. Add encryption")
    third_file_name = pl.encryption(s3_resource,
                                    first_bucket_name,
                                    THIRD_FILE_NAME)

    print("=" * 40)

    # Storage
    print("Task 7. Change storage type of object.")
    pl.storage(s3_resource,
               first_bucket_name,
               third_file_name)

    print("=" * 40)

    # Version
    print("Task 8. Enable versioning of object")
    pl.versioning(s3_resource)

    print("=" * 40)

    # Bucket Traversal
    print("Task 9. Traverse all buckets in resource.")
    print(f"Buckets in resource:")
    utils.bucket_traversal(s3_resource)

    print("=" * 40)

    # Object Traversal
    print("Task 10. Traverse all objects in bucket")
    utils.print_objects(s3_resource.Bucket(first_bucket_name))

    print("=" * 40)

    # Deleting a Non-empty Bucket
    print("Task 11. Delete all objects in buckets")
    utils.delete_all_objects(first_bucket_name)
    utils.delete_all_objects(second_bucket_name)

    print("=" * 40)

    # Delete buckets
    print("Task 12. Delete empty buckets")
    s3_resource.Bucket(first_bucket_name).delete()
    s3_resource.meta.client.delete_bucket(Bucket=second_bucket_name)


if __name__ == '__main__':
    S3_CLIENT = boto3.client('s3')
    S3_RESOURCE = boto3.resource('s3')

    main(S3_CLIENT, S3_RESOURCE)
