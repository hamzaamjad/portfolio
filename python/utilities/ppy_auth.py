#!/usr/bin/env python3
"""Objects that provide an interface to various Secrets Managers.
"""

# -- Imports --------------------------------------------------------------------------------
import boto3
import base64
from botocore.exceptions import ClientError

class Auth():
    """Provides methods to retreive credentials from AWS Secrets Manager.
       
       Requires AWS CLI is configured. See https://www.notion.so/probitascap/AWS-EC2-General-eb89e3326bdf435fa81cb176c399586b for information.
    """
    def __init__(self):
        """Upon instantiation, creates variables:
            1. region_name, with a default of 'us-east-1'
            2. A boto3 Session
            3. A Secrets Manager client
        """
        self.region_name = 'us-east-1'

        # Create a Secrets Manager client
        self.session = boto3.session.Session()
        self.client = self.session.client(
            service_name = 'secretsmanager',
            region_name = self.region_name
            )
    
    def get_secret(self, secret_name : str = ''):
        """Given the name of a AWS Secrets Manager Secret, returns the Secret

           Example:
            >>> Auth.get_secret("env/service/descriptor")
        """
        # In this sample we only handle the specific exceptions for the 'GetSecretValue' API.
        # See https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
        # We rethrow the exception by default.
        try:
            get_secret_value_response = self.client.get_secret_value(
                SecretId=secret_name
            )
        except ClientError as e:
            if e.response['Error']['Code'] == 'DecryptionFailureException':
                # Secrets Manager can't decrypt the protected secret text using the provided KMS key.
                # Deal with the exception here, and/or rethrow at your discretion.
                raise e
            elif e.response['Error']['Code'] == 'InternalServiceErrorException':
                # An error occurred on the server side.
                # Deal with the exception here, and/or rethrow at your discretion.
                raise e
            elif e.response['Error']['Code'] == 'InvalidParameterException':
                # You provided an invalid value for a parameter.
                # Deal with the exception here, and/or rethrow at your discretion.
                raise e
            elif e.response['Error']['Code'] == 'InvalidRequestException':
                # You provided a parameter value that is not valid for the current state of the resource.
                # Deal with the exception here, and/or rethrow at your discretion.
                raise e
            elif e.response['Error']['Code'] == 'ResourceNotFoundException':
                # We can't find the resource that you asked for.
                # Deal with the exception here, and/or rethrow at your discretion.
                raise e
        else:
            # Decrypts secret using the associated KMS CMK.
            # Depending on whether the secret is a string or binary, one of these fields will be populated.
            if 'SecretString' in get_secret_value_response:
                secret = get_secret_value_response['SecretString']
                secret = eval(secret)
                return(secret)
            else:
                decoded_binary_secret = base64.b64decode(get_secret_value_response['SecretBinary'])
                return(decoded_binary_secret)