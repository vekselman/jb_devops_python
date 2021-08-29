import uuid
import boto3
from typing import Tuple


def create_bucket_name(bucket_prefix: str) -> str:
    """
    The generated bucket name must be between 3 and 63 chars long
    :param bucket_prefix:  Prefix name of bucket
    :type bucket_prefix: str
    :return: name with uuid
    :rtype: str
    """
    return '-'.join([bucket_prefix, str(uuid.uuid4())])


def create_bucket(
        bucket_prefix: str,
        s3_connection
) -> Tuple[str, dict]:
    """
    Creates Bucket in default region
    :param bucket_prefix: Prefix name of bucket
    :type bucket_prefix: str
    :param s3_connection: client connection of boto3
    :type s3_connection: boto3.client
    :return: name with uuid and dict of response
    :rtype: Tuple[str, dict]
    """
    session: boto3.session.Session = boto3.session.Session()
    current_region = session.region_name
    bucket_name = create_bucket_name(bucket_prefix)
    print(bucket_name, current_region)
    bucket_response = s3_connection.create_bucket(
        Bucket=bucket_name,
        CreateBucketConfiguration={
            'LocationConstraint': current_region}
    )
    return bucket_name, bucket_response


def create_temp_file(
        size: int,
        file_name: str,
        file_content: str
) -> str:
    """
    Create random file and returns the file name
    :param size: number of replication of file_content
    :type size: int
    :param file_name: name of file to be created
    :type file_name: str
    :param file_content: Content of file
    :type file_content: str
    :return: Create file name
    :rtype: str
    """
    random_file_name = ''.join([str(uuid.uuid4().hex[:6]), file_name])
    with open(random_file_name, 'w') as f:
        f.write(str(file_content) * size)
    return random_file_name


def copy_to_bucket(
        s3_resource,
        bucket_from_name: str,
        bucket_to_name: str,
        file_name: str
) -> None:
    """
    Copy from one bucket to another
    :param s3_resource:
    :param bucket_from_name:
    :param bucket_to_name:
    :param file_name:
    :return:
    """
    copy_source = {
        'Bucket': bucket_from_name,
        'Key': file_name
    }
    s3_resource.Object(bucket_to_name, file_name).copy(copy_source)


def enable_bucket_versioning(
        s3_resource,
        bucket_name: str) -> None:
    """
    Enables versioning to specific bucket
    :param s3_resource:
    :param bucket_name:
    :return:
    """
    bkt_versioning = s3_resource.BucketVersioning(bucket_name)
    bkt_versioning.enable()
    print(bkt_versioning.status)


def bucket_traversal(s3_resource) -> None:
    """
    Prints all buckets
    :param s3_resource:
    :return:
    """
    for bucket in s3_resource.buckets.all():
        print(bucket.name)


def print_objects(bucket) -> None:
    """
    Print all objects in bucket
    :param bucket:
    :return:
    """
    for obj in bucket.objects.all():
        print(obj.key)
