#Settings file
#You must fill-in a valid Azure Blob Storage account name and account key
import json
from azure.storage.blob import BlockBlobService

with open('config.json') as json_data_file:
    config = json.load(json_data_file)


URL_FIRST_PAGE = 'http://www.portaltransparencia.gov.br/despesasdiarias/resultado?'
URL_NEXT_PAGE = 'http://www.portaltransparencia.gov.br/despesasdiarias/resultado?pagina={0}#paginacao'
AZ_BLOB_SERVICE = BlockBlobService(account_name= config["azure"]["account_name"], 
                                   account_key=config["azure"]["account_key"])    
REQUESTED_DATE = None

REQ_INTERVAL = 2. #interval in seconds between HTTP requests.
                  #Can be adjusted according to throthling events 
                  #from the web server
                  
BLOBS_METADATA = frozenset()  
