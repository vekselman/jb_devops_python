import boto3
from aws_app import utils
import typing


def create_and_upload(s3_client,
                      s3_resource,
                      first_bucket_name: str,
                      second_bucket_name: str,
                      first_file_name: str) -> typing.Tuple[str, str, str]:
    """
    Will create to buckets in S3, create some file and uploaded him to buckets

    :param s3_client: client of S3
    :param s3_resource: resource of S3
    :param first_bucket_name: Name of first bucket
    :param second_bucket_name: Name of second bucket
    :param first_file_name: Name of file name to be uploaded
    :return: uniq names of firs, second buckets and file
    :rtype: tuple(str, str, str)
    """
    print("\tCreating first bucket...")
    _first_bucket_name, _first_response = utils.create_bucket(
        bucket_prefix=first_bucket_name,
        s3_connection=s3_resource.meta.client)
    print(f"\tFirst bucket name: {first_bucket_name}")

    print("\tCreating second bucket...")
    _second_bucket_name, _second_response = utils.create_bucket(
        bucket_prefix=second_bucket_name, s3_connection=s3_resource)
    print(f"\tSecond bucket name: {_second_bucket_name}")

    print("\tCreating temp file...")
    _first_file_name = utils.create_temp_file(300, first_file_name, 'f')
    print(f"\tFirst file name: {_first_file_name}")

    first_object = s3_resource.Object(
        bucket_name=first_bucket_name, key=_first_file_name)
    first_bucket_again = first_object.Bucket()

    s3_resource.Object(_first_bucket_name, _first_file_name).upload_file(
        Filename=_first_file_name)

    return _first_bucket_name, _second_bucket_name, _first_file_name


def download_file(s3_resource,
                  bucket_name: str,
                  file_name: str,
                  path="/tmp/") -> None:
    """
    For downloading file from S3 bucket

    :param s3_resource: resource of S3
    :param bucket_name: Name of bucket
    :param file_name: Name of file
    :param path: Dir path, default /tmp/
    :return:
    """
    s3_resource.Object(bucket_name,
                       file_name
                       ).download_file(path + file_name)
    print(f"\tFile: {file_name} placed in {path}")


def acl(s3_resource,
        bucket_name,
        file_name) -> str:
    """
    Creates file in S3 and grants differ acls.

    :param s3_resource: boto3 resource of S3
    :param bucket_name: Name of bucket
    :param file_name: Name of file
    :return: unique name of created file
    """
    print(f"\tCreate and upload {file_name} ot\n\t\t{bucket_name}")
    _file_name = utils.create_temp_file(400, file_name, 's')
    _object = s3_resource.Object(bucket_name, file_name)
    _object.upload_file(
        _file_name,
        ExtraArgs={
            'ACL': 'public-read'}
    )
    print(f"second_file_name: {_file_name}")

    _object_acl = _object.Acl()
    print(f"\tGrants public:\n\t{_object_acl.grants}")

    _response = _object_acl.put(ACL='private')
    print(f"\tGrants private:\n\t{_object_acl.grants}")

    return _file_name


def encryption(s3_resource,
               bucket_name: str,
               file_name: str) -> str:
    """
    Create file in S3 bucket and encrypts him with AES256.

    :param s3_resource:
    :param bucket_name:
    :param file_name:
    :return: Name of encrypted file
    """
    print("\tCreating and uploading new file...")
    _file_name = utils.create_temp_file(300, file_name, 't')
    _object = s3_resource.Object(bucket_name, _file_name)
    _object.upload_file(
        _file_name,
        ExtraArgs={
            'ServerSideEncryption': 'AES256'}
    )
    print(f"\tFile name: {_file_name}")
    print(f"\tFile encryption: {_object.server_side_encryption}")

    return _file_name


def storage(s3_resource,
            bucket_name: str,
            file_name: str) -> None:
    """
    Reloads file with encryption and STANDARD_IA storage class

    :param s3_resource: boto3 S3 resource
    :param bucket_name: Bucket name
    :param file_name: File name
    :return:
    """
    print("\tReload file...")
    _object = s3_resource.Object(
        bucket_name,
        file_name
    )
    _object.upload_file(
        file_name,
        ExtraArgs={
            'ServerSideEncryption': 'AES256',
            'StorageClass': 'STANDARD_IA'}
    )
    _object.reload()
    print(f"\tStorage class: {_object.storage_class}")


def versioning(s3_resource,
               bucket_name: str,
               file_name_01: str,
               file_name_02: str,
               file_name_03: str) -> None:
    """
    Add's files as versions of existing files.

    :param s3_resource: boto3 S3 resource
    :param bucket_name: Name of bucket
    :param file_name_01: Name of first file
    :param file_name_02: Name of second file
    :param file_name_03: Name of third file
    :return:
    """
    print(f"\tEnabling versioning to object...")
    utils.enable_bucket_versioning(s3_resource, bucket_name)

    print(f"\tAdd version to {file_name_01}")
    s3_resource.Object(
        bucket_name,
        file_name_01
    ).upload_file(
        file_name_01)

    print(f"\tAdd version to {file_name_01}")
    s3_resource.Object(bucket_name,
                       file_name_01
                       ).upload_file(file_name_02)

    print(f"\tAdd version ot {file_name_03}")
    s3_resource.Object(bucket_name,
                       file_name_03
                       ).upload_file(file_name_03)

    print(f"Version ID:{s3_resource.Object(bucket_name, file_name_01).version_id}")
