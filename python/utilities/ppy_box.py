import os, logging
import io
import pandas as pd

from boxsdk import JWTAuth, Client, exception

from pprint import pprint

# -- Box ------------------------------------------------------------------------------------------------
    
class ProbitasBox():
    def __init__(self):
        self.config = JWTAuth.from_settings_file(os.path.join(os.getcwd(),'prod_box_python.json'))
        self.client = Client(self.config)
        
        self.user = self.client.user().get()
        self.client.root_folder = self.client.root_folder().get()
        
        self.static_folders = {
            'data' : {
                'folder_id' : '106929200044',
                'folder_name' : 'Data',
                'sub_folders' : {
                    'apartmentsdotcom' : {
                        'folder_id' : '116337614607',
                        'folder_name' : 'ApartmentsDotCom'}
                }
            }
        }
        
        self.uploaded_files = {}
        self.updated_files = {}
        
    def get_info(self):
        print(f"Username: {self.user.name}")
        print(f"User ID : {self.user.id}")
        
    def parse_error(self, error: exception.BoxAPIException):
        error = [l.split(':',1) for l in str(error).split("\n")]
        error_keys = [l[0] for l in error]
        error_values = [l[1].strip() for l in error]
        error = dict(zip(error_keys, error_values))
        error.update({'Context Info' : eval(error.get('Context Info'))})
        error.update({'Headers' : eval(error.get('Headers'))})
        return(error)
    
    def get_folder_id(self, folder_name : str):
        folder_id = self.static_folders.get('data').get('sub_folders').get(folder_name).get('folder_id')
        return(folder_id)
    
    def get_file_list(self, folder_id : str):
        items = self.client.folder(folder_id = folder_id).get_items()
        files = [{'type' : item.type, 'id' : item.id, 'name' : item.name} for item in items]
        files = [f for f in files if f.get('type') == 'file']
        return(files)
    
    def get_file(self, file_id : str, as_bytes: bool = False):
        file_content = self.client.file(file_id).content()
        if as_bytes:
            output = file_content
        else:
            output = io.BytesIO(file_content)
        return(output)
    
    def delete_file(self, file_id : str):
        self.client.file(file_id = file_id).delete()
        
    def update_file(self, file_id : str, df, sep = ',', index = False, header = True):
        stream = io.StringIO()
        df.to_csv(stream, sep = sep, index = False, header = header)
        
        upload = self.client.file(file_id).update_contents_with_stream(stream)
        self.updated_files.update({upload.name : upload.id})
        
        print('File "{0}" uploaded to Box with file ID {1}'.format(upload.name, upload.id))
    
    def upload_file(self, folder_id : str, df, file_name : str, sep = ',', index = False, header = True, conflict_override = False):
        try:
            stream = io.StringIO()
            df.to_csv(stream, sep = sep, index = False, header = header)
            upload = self.client.folder(folder_id).upload_stream(stream, file_name)
            self.uploaded_files.update({upload.name : upload.id})
            print(f"File {upload.name} uploaded to Box with file ID {upload.id}")
        except exception.BoxAPIException as e:
            error = self.parse_error(e)
            if error.get('Code') == 'item_name_in_use':
                if conflict_override:
                    existing_file = error.get('Context Info').get('conflicts').get('id')
                    print(f"Warning -- conflict overridden. Updating file: {existing_file}")
                    self.update_file(existing_file, df, sep, index, header)
                else:
                    print("Conflict with existing file.")
                    pprint(error)
        