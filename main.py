#!/usr/bin/env python
from minio import Minio
from minio.error import S3Error
from yaml import FullLoader
from yaml import load as yamlload
from argparse import ArgumentParser
from os.path import abspath, basename


class Parse:
    def __init__(self):
        self.parser = ArgumentParser()
        self.loading = self.parser.add_mutually_exclusive_group()
        self.parser.add_argument(
            "-c", "--config",
            type=str,
            help="path to configfile for overriding default name",
            default="config.yaml"
        )
        self.parser.add_argument(
            "--host",
            type=str,
            help="override hostname of MinIO server",
            default=None
        )
        self.parser.add_argument(
            "-a", "--access_key",
            type=str,
            help="override access_key of MinIO server's user",
            default=None
        )
        self.parser.add_argument(
            "-s", "--secret_key",
            type=str,
            help="override secret_key of MinIO server's user",
            default=None
        )
        self.loading.add_argument(
            "-u", "--upload",
            type=str,
            help="path to file that will be uploaded",
            default=None
        )
        self.loading.add_argument(
            "-d", "--download",
            action="store_true",
            help="name of file that will be downloaded from bucket",
            default=False
        )
        self.loading.add_argument(
            "-l", "--list_files",
            action="store_true",
            help="list of files in the bucket",
            default=False
        )
        self.parser.add_argument(
            "-b", "--bucket",
            type=str,
            help="name of the bucket",
            default=None
        )
        self.args = vars(self.parser.parse_args())


class Config:
    def config_read(self):
        with open(self.args['config'], "r") as cfgyaml:
            data = cfgyaml.read()
            json = yamlload(data, Loader=FullLoader)
            cfgyaml.close()
        json['upload'] = False
        json['list_files'] = False
        json['download'] = False
        for key in self.args:
            if self.args[key] is not None:
                json[key] = self.args[key]
        return json

    def __init__(self, args):
        self.args = args
        self.fullconfig = self.config_read()


class Client:
    def __init__(self, conf):
        self.conf = conf
        self.client = Minio(
            self.conf['host'],
            access_key=self.conf['access_key'],
            secret_key=self.conf['secret_key'],
            secure=True
        )

    def check_bucket(self):
        found = self.client.bucket_exists(self.conf['bucket'])
        if not found:
            if self.conf['list_files']:
                print("No such bucket, abort.")
                exit(0)
            else:
                self.client.make_bucket(self.conf['bucket'])

    def upload(self):
        self.check_bucket()
        try:
            filepath = abspath(self.conf['upload'])
            filename = basename(filepath)
            self.client.fput_object(
                bucket_name=self.conf['bucket'],
                object_name=filename,
                file_path=filepath
            )
            print(f"{filepath} was successfully uploaded as {filename}")
        except S3Error as exc:
            print(exc)
        except Exception as exc:
            print(exc)

    def download(self):
        objects = self.list_files()
        while True:
            try:
                choice = int(input("Choose file to download: "))
                choice -= 1                 # for indexing in list
                filename = objects[choice]
                obj = self.client.get_object(self.conf['bucket'], filename['_object_name'])
                with open(filename['_object_name'], "wb") as file:
                    file.write(obj.data)
                    file.close()
                print(f"File {filename['_object_name']} downloaded successfully.")
                break
            except KeyboardInterrupt:
                print('Cancelled.')
                break
            except IndexError:
                print('No such object, try again.')
                continue

    def list_files(self):
        self.check_bucket()
        objects = self.client.list_objects(bucket_name=self.conf['bucket'], recursive=True)
        obj_dict = []
        for obj in objects:
            obj_dict.append(obj.__dict__)
        counter = 1
        print("Files in bucket:")
        for obj in obj_dict:
            print(f"{counter} {obj['_object_name']:<60} {obj['_last_modified']} {obj['_size']}")
            counter += 1
        if self.conf['download']:
            return obj_dict


if __name__ == "__main__":
    parser = Parse()
    yaml = Config(parser.args)
    minio_cli = Client(yaml.fullconfig)
    if yaml.fullconfig['upload']:
        minio_cli.upload()
    elif yaml.fullconfig['download']:
        minio_cli.download()
    elif yaml.fullconfig['list_files']:
        minio_cli.list_files()
    else:
        print('No actions')
