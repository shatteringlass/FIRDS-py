import loguru
import requests

from firds.util.misc import retry


class Firds(object):

    URL = 'https://registers.esma.europa.eu/solr/esma_registers_firds_files/select'
    ISOfmt = "%Y-%m-%dT%H:%M:%SZ"

    def __init__(self, session=None, retry_count=1, retry_delay=0,
                 proxies=None):

        if session is None:
            session = requests.Session()
        self.session = session
        self.proxies = proxies
        self.retry_count = retry_count
        self.retry_delay = retry_delay

    @retry
    def base_request(self, *args, **kwargs):

        base_params = {
            'q': '*',  # tells the response to return all columns for a given result if one exists
            'fq': f"publication_date:[{kwargs.get('start_date')} TO {kwargs.get('end_date')}]", # publication date interval
            'wt': 'json',  # xml / json format of response
            'indent': True,  # make response more readable
            'start': kwargs.get('start'),  # response offset
            'rows': kwargs.get('max_rows')
        }

        response = self.session.get(url=self.URL, params=base_params, proxies=self.proxies)
        try:
            response.raise_for_status()
        except requests.HTTPError as e:
            raise e
        else:
            return response

    def get_list(self, dt_from, dt_to, start=0, max_rows=100):
        response = self.base_request(start_date=dt_from, end_date=dt_to, start=start, max_rows=max_rows).json()['response']
        
        links = dict()

        leftovers = max(0,response['numFound'] - max_rows)
        body = response['docs']

        if leftovers > 0:
            self.get_list(dt_from, dt_to, start+max_rows)
        
        return links

        """
        fulins = [x for x in body if x['file_type'] == 'FULINS']

        if len(fulins) > 0:
            for prod in prods:
                p_prods = [x for x in fulins if hasProduct(x, prod)]
                try:
                    p_newestFUL = get_newest(p_prods)
                except(ValueError):
                    p_newestFUL = lastRun
                newestFUL = p_newestFUL if not newestFUL or p_newestFUL > newestFUL else newestFUL
                ls.append([f for f in p_prods if datetime.strptime(
                    f['publication_date'], ISOfmt) == p_newestFUL])

        DLT = [x for x in body if x['file_type'] == 'DLTINS' and isNewerThan(x, newestFUL)]
        ls.append(DLT)
        newestDLT = get_newest(DLT)
        return ls, newestFUL, newestDLT

    def get_newest(l):
        return max([datetime.strptime(x['publication_date'], ISOfmt) for x in l])

    def downloadLinks(list, destPath):
        for sublist in list:
            for file in sublist:
                link = file['download_link']
                downloadZip(link, destPath + getFilename(link))

    def hasProduct(r, p):
        return r['file_name'].find('_{}_'.format(p)) != -1

    def isNewerThan(r, dt):
        return datetime.strptime(r['publication_date'], ISOfmt) > dt

    def getFilename(link):
        return link[link.rfind("/") + 1:]

    def downloadZip(link, dest):
        response = requests.get(link, stream=True)
        # Throw an error for bad status codes
        response.raise_for_status()

        # Write chunks to file
        os.makedirs(os.path.dirname(dest), exist_ok=True)
        with open(dest, 'wb') as handle:
            for block in response.iter_content(1024):
                handle.write(block)
"""