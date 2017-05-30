import json
import queue
import threading
import requests
from bs4 import BeautifulSoup
import html_parser
import settings
import time
from azure.storage.blob import ContentSettings


queue_uploader = queue.Queue()
queue_blob_checker = queue.Queue()
queue_web_requester = queue.Queue()

sess = requests.session()
threads = []
if __name__ == '__main__':    
    main(args)

def main(args):
    #A consulta divulga dados cujos documentos foram emitidos a partir do dia 25 de maio de 2010 
    #e que sejam referentes às despesas realizadas por todos os órgãos e entidades da Administração 
    #Pública do Poder Executivo Federal que utilizam o SIAFI.
    if args['requestedDate'] is None:
        settings.REQUESTED_DATE = '01/01/2017'
    else:
        settings.REQUESTED_DATE = args['requestedDate']    
    
    print('{0}: Extractor started'.format(settings.REQUESTED_DATE), end = '. ')

    #Year, month, day used in Azure Storage Folder structure
    day, month, year = settings.REQUESTED_DATE.split('/')
                    
    requestResult = sess.get(settings.URL_FIRST_PAGE+ \
                    html_parser.get_querystring(settings.REQUESTED_DATE), \
                    allow_redirects=False)    
    
    if(requestResult.status_code == 302): #too bad, probably showing a captch page... must stop
        raise Exception('The requested page was redirected to {0}. Execution was terminated'
                        .format(requestResult.headers['Location']))        

    pageContents = requestResult.text
    soupObject = BeautifulSoup(pageContents, "html.parser")
    totalPagesSpan = soupObject.find('span', { 'class': 'paginaXdeN'})
    totalPages = 1
    if totalPagesSpan:
        totalPages = int(totalPagesSpan.string.strip().replace('Página 1 de ', ''))

    startFromPage = 1    
    print('Total number of pages = {1}'.
          format(settings.REQUESTED_DATE, totalPages))            
    
    threads_init()
        
    #Next pages. Reminder to myself: Python 'range' does not includ end, therefore using total+1    
    print('{0}: Preparing to load pages...'.format(settings.REQUESTED_DATE))
    for x in range(startFromPage, totalPages+1):    
        blobPath = 'despesasdiarias/{0}/{1}/{2}/despesas_{3}.json'.format(year, month, day, x)
        queue_blob_checker.put({'Item' : x,
                                              'BlobPath': blobPath,
                                              'Total' : totalPages})            
    
    queue_blob_checker.join()
    queue_web_requester.join()
    queue_uploader.join() # block until all tasks are done

    
def worker_uploader():
    while True:
        blob = queue_uploader.get()
        #print('Uploading blob: {0}'.format(blob['BlobPath']))            
        settings.AZ_BLOB_SERVICE.create_blob_from_bytes('csv',
                                    blob['BlobPath'],
                                    blob['BlobBytes'],
                                    content_settings=ContentSettings(content_type='application/json'))
        queue_uploader.task_done()

def worker_blob_checker():
    """Checks if the items present in queue_blob_checker point to existing blobs in Azure.
    If not, adds an item to queue_blob_checker so that the page is downloaded"""
    while True:
        item = queue_blob_checker.get()            
        #if not settings.AZ_BLOB_SERVICE.exists('csv', item['BlobPath']):
        if item['BlobPath'] not in settings.BLOBS_METADATA:
            queue_web_requester.put(item);
            #print('Blob already exists, skip: {0}'.format(item['BlobPath']))
        queue_blob_checker.task_done()

def threads_init():
    day, month, year = settings.REQUESTED_DATE.split('/')
    #if not settings.AZ_BLOB_SERVICE.exists('csv', item['BlobPath']):
    blobPath = 'despesasdiarias/{0}/{1}/{2}/'.format(year, month, day)
    blobList = settings.AZ_BLOB_SERVICE.list_blobs('csv',blobPath)
    blobArray = set()
    for blob in blobList:
        blobArray.add(blob.name)
    settings.BLOBS_METADATA = frozenset(blobArray) 
        
        
    if not threads:            
        num_worker_threads = 16
        for i in range(num_worker_threads):        
            t1 = threading.Thread(target=worker_uploader, daemon=True)
            t1.start()                
            t2 = threading.Thread(target=worker_blob_checker, daemon=True)        
            t2.start()
            threads.append(t1)
            threads.append(t2)
    
        #Requester must be single threaded otherwise will be throtled by the service
        #for i in range(1):        
        t3 = threading.Thread(target=worker_requester, daemon=True)
        t3.start()
        threads.append(t3)                
            
def worker_requester():    
    time_of_last_request = None
    requesters_counter = 0
    start_time = None
    while True:
        try:
            item = queue_web_requester.get()

            if start_time is None:
                start_time = time.time()
                time_of_last_request = time.time()
          
            since_last_request = time.time() - time_of_last_request
            #if since_last_request < 4.0:
             #   time.sleep(4.0 - since_last_request)
            
            since_last_request = time.time() - time_of_last_request
            time_of_last_request = time.time() 
            res = sess.get(settings.URL_NEXT_PAGE.format(item['Item']), allow_redirects=False)
            if(res.status_code == 302): #too bad, probably showing a captch page... must stop
                raise Exception('The requested page was redirected to {0}. Execution was terminated'.format(res.headers['Location']))    
            pageContents = res.text
            soupObject = BeautifulSoup(pageContents, "html.parser")
            table_as_json = html_parser.html_table_to_json(soupObject, "tabela")
            blobBytes = table_as_json.encode()                            
            queue_uploader.put({'BlobPath' : item['BlobPath'], 'BlobBytes' : blobBytes})            
            requesters_counter += 1


            total_execution_time = (time.time() - start_time) + .01

            print('{0}: Page {1}/{2} Requests: {3} Total time: {4:.2f} Last request: {5:.2f} Requests/minute: {6:.2f}'
                  .format(settings.REQUESTED_DATE, item['Item'],
                          item['Total'], requesters_counter,total_execution_time,
                          since_last_request,
                          ( requesters_counter/total_execution_time) * 60)) 
        except Exception as err:
            print('An error occurred while requesting page {0} of {1}, trying again later. {2}'.format(item['Item'], item['Total'], err))
            time.sleep(30)
            queue_web_requester.put({ 'Item' : item['Item'], 'BlobPath': item['BlobPath'], 'Total' : item['Total']})
        queue_web_requester.task_done()