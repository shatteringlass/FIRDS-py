import datetime
import enum
import json
import jsonpath_rw
import os
import re
import requests
import tempfile

from firds.util.misc import retry


class CFI(enum.Enum):
    EQUITY = 'E'
    DEBT = 'D'
    COLLECTIVE_INVESTMENT_VEHICLES = 'C'
    ENTITLEMENTS = 'R'
    LISTED_OPTIONS = 'O'
    FUTURES = 'F'
    SWAPS = 'S'
    NON_LISTED_OPTIONS = 'H'
    SPOT = 'I'
    FORWARDS = 'J'
    STRATEGIES = 'K'
    REFERENCE_INSTRUMENTS = 'T'
    OTHERS = 'M'


class FirdsQuery:

    search_endpoint = 'https://registers.esma.europa.eu/publication/searchRegister/doMainSearch'

    headers = {
        'Origin': 'https://registers.esma.europa.eu',
        'Referer': 'https://registers.esma.europa.eu/publication/searchRegister?core=esma_registers_firds',
        'Accept-Encoding': 'gzip, deflate, br',
        'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.169 Safari/537.36',
        'Content-Type': 'application/json',
        'Accept': 'application/json, text/javascript, */*; q=0.01',
        'X-Requested-With': 'XMLHttpRequest',
        'Connection': 'keep-alive',
    }

    def build_query(self, start=0, criteria=list()):
        return json.dumps({
            "core": "esma_registers_firds",
            "pagingSize": "1000",
            "start": start,
            "keyword": "",
            "sortField": "isin asc",
            "criteria": criteria,
            "wt": "json"
        })

    def get(self, criteria):
        query = self.build_query(criteria=criteria)
        return requests.post(self.search_endpoint,
                             headers=self.headers,
                             data=query
                             ).json()


class FirdsDB:

    URL = 'https://registers.esma.europa.eu/solr/esma_registers_firds_files/select'
    today = datetime.date.today()
    amnight = datetime.datetime.min.time()
    bmnight = datetime.datetime.max.time()
    utc_tz = datetime.timezone.utc
    today_ts = datetime.datetime.combine(today, bmnight, utc_tz)

    def __init__(self, date=today_ts, session=None, retry_count=1, retry_delay=0, proxies=None):
        if session is None:
            session = requests.Session()
        self.session = session
        self.proxies = proxies
        self.retry_count = retry_count
        self.retry_delay = retry_delay
        self.start_date = self.zulu_dt(self.calc_start_date(self.today_ts))
        self.end_date = self.zulu_dt(self.today_ts)
        self._links = self.get_list(
            dt_from=self.start_date, dt_to=self.end_date)
        self.tmpfolder = tempfile.TemporaryDirectory()

    @classmethod
    def zulu_dt(cls, dt):
        return dt.isoformat().replace('+00:00', 'Z')

    @classmethod
    def calc_start_date(cls, end_date):
        """
        The files published by ESMA on its website are generated:
        a. on a weekly basis for the Full File – on Sunday morning by 09:00 CET;
        b. on a daily basis for the Delta File – every morning by 09:00 CET.
        """
        wd = end_date.weekday()
        idx = (wd + 1) % 7
        return datetime.datetime.combine(end_date - datetime.timedelta(1 + idx), cls.amnight, cls.utc_tz)

    @retry
    def base_request(self, *args, **kwargs):

        base_params = {
            'q': '*',  # tells the response to return all columns for a given result if one exists
            # publication date interval
            'fq': f"publication_date:[{kwargs.get('start_date')} TO {kwargs.get('end_date')}]",
            'wt': 'json',  # xml / json format of response
            'indent': True,  # make response more readable
            'start': kwargs.get('start'),  # response offset
            'rows': kwargs.get('max_rows')
        }
        response = self.session.get(
            url=self.URL, params=base_params, proxies=self.proxies)
        try:
            response.raise_for_status()
        except requests.HTTPError as e:
            raise e
        else:
            return response

    def get_list(self,
                 dt_from,
                 dt_to,
                 start=0,
                 max_rows=100,
                 lst=dict(FULINS=dict(), DLTINS=list())):

        response = self.base_request(
            start_date=dt_from, end_date=dt_to, start=start, max_rows=max_rows).json()
        num_found = response['response']['numFound']

        jpath = jsonpath_rw.parse('response.docs[*].download_link')
        prd_re = re.compile('^.*?_([A-Z]{1})_.*?$')

        for k in jpath.find(response):
            if 'FULINS' in k.value:
                p = CFI(re.match(prd_re, k.value).group(1)).name
                lst['FULINS'].setdefault(p, []).append(k.value)
            elif 'DLTINS' in k.value:
                lst.setdefault('DLTINS', []).append(k.value)
            else:
                # unhandled file type
                continue

        if max(0, num_found - max_rows) > 0:
            self.get_list(dt_from=dt_from, dt_to=dt_to,
                          start=start + max_rows, lst=lst)

        return lst

    def get_zip(self, link):

        def get_filename_from_cd(r):
            """
            Get filename from content-disposition
            """
            cd = r.headers.get('content-disposition')
            if not cd:
                return None
            fname = re.findall('filename=(.+)', cd)
            if len(fname) == 0:
                return None
            return fname[0]

        response = requests.get(link, stream=True)
        fname = get_filename_from_cd(response) or link.rsplit('/', 1)[1]
        # Throw an error for bad status codes
        response.raise_for_status()

        # Write chunks to file
        path = os.path.join(self.tmpfolder.name, fname)
        with open(path, 'wb') as handle:
            for block in response.iter_content(1024):
                handle.write(block)
        return path

    def download_delta(self):
        for l in self.links['DLTINS']:
            print(self.get_zip(l))

    def download_all(self):
        pass
        self.download_delta()

    @property
    def links(self):
        return self._links
