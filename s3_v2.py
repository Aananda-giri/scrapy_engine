import os, sys
import boto3
import asyncio

class Ec2Functions:
        
        @staticmethod
        async def upload_file_to_s3(file_path, bucket_name, object_key):
            # Function to upload a file to S3 and return the link
            '''
            object key is the path to the file in the bucket
            '''
            
            s3_client = boto3.client('s3')
            try:
                s3_client.upload_file(file_path, bucket_name, object_key)
                # object_url = f"https://{bucket}.s3.{region_name}.amazonaws.com/{object_key}"
                object_url = s3_client.generate_presigned_url(
                    'get_object',
                    Params={'Bucket': bucket_name, 'Key': object_key},
                    ExpiresIn=172800
                )
                return object_url
            except Exception as e:
                print(f"Error uploading file '{file_path}' to S3: {e}")
                return None
        
        @staticmethod
        async def upload_folder_to_s3(folder_path = None, bucket_name=None):
            # Upload folder contents to S3
            uploaded_files = []
            if not folder_path:
                folder_path = '/home/ubuntu/saneora/cogs/downloads'
            if not bucket_name:
                bucket_name = 'discord-bot'
            for root, dirs, files in os.walk(folder_path):
                for file_name in files:
                    file_path = os.path.join(root, file_name)
                    print(file_path)
                    object_key = os.path.relpath(file_path, folder_path)
                    object_url = Ec2Functions.upload_file_to_s3(file_path, bucket_name, object_key)
                    if object_url:
                        uploaded_files.append(object_url)

            # Print the list of uploaded file links
            # for file_url in uploaded_files:
            #     print(file_url)
            return uploaded_files

        @staticmethod
        def list_files(bucket_name=None):
            if not bucket_name:
                bucket_name = '1b-bucket'
            s3 = boto3.resource('s3')
            bucket = s3.Bucket(bucket_name)
            for obj in bucket.objects.all():
                print(obj.key)
if __name__ == "__main__":
    # take filename from command line
    if len(sys.argv) >1:
        file_path = sys.argv[1]
    else:
        file_path = None


    if not os.path.exists(file_path):
        print(f'file {file_path} does not exist')
        # create test file
        with open('test.txt', 'w') as file:
            file.write('hello world')
        
        # upload test file
        # await using asyncio
        url = asyncio.run(Ec2Functions.upload_file_to_s3('test.txt', 'discord-bot', 'test.txt'))
        print(url)
    else:
        print(f'file {file_path} exists')
        url = asyncio.run(Ec2Functions.upload_file_to_s3(file_path, 'discord-bot', file_path.split('/')[-1]))
        print(url)

    Ec2Functions.list_files()
