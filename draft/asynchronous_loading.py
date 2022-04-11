import urllib
from threading import Thread
from queue import Queue
import requests

NUM_WORKERS = 3


class Download:
    def __init__(self):
        self.Q = Queue()
        for i in range(NUM_WORKERS):
            t = Thread(target=self.worker)
            t.setDaemon(True)
            t.start()

    def worker(self):
        while 1:
            url, Q = self.Q.get()
            try:
                f_worker = urllib.request.urlopen(url)
                Q.put(('ok', url, f_worker.read()))
                f_worker.close()
            except(
                    requests.exceptions.ConnectionError,
                    requests.exceptions.HTTPError,
                    requests.exceptions.Timeout,
                    requests.exceptions.RequestException,
                    requests.exceptions.TooManyRedirects
            ) as e:
                Q.put(('error', url, e))
                try:
                    f_worker.close()
                except:
                    pass

    def download_urls(self, L):
        Q = Queue() # Create a second queue so the worker
                    # threads can send the data back again
        for url in L:
            # Add the URLs in `L` to be downloaded asynchronously
            self.Q.put((url, Q))

        rtn = []
        for i in range(len(L)):
            # Get the data as it arrives, raising
            # any exceptions if they occur
            status, url, data = Q.get()
            if status == 'ok':
                rtn.append((url, data))
            else:
                raise data
        return rtn


base_url = 'http://localhost:5000/'
endpoint = 'events?'
inst = Download()

# data need search
for url, data in inst.download_urls([base_url+endpoint+'start_date=2022-04-08&end_date=2022-04-09']*4):
    print(url, data)