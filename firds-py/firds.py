import pandas as pd
import requests

from util import worker
from util.misc import retry


class Firds(object):

    URL = 'https://registers.esma.europa.eu/solr/esma_registers_firds_files/select'

    def __init__(self, session=None, retry_count=1, retry_delay=0,
                 proxies=None):
        """
        Parameters
        ----------
        api_key : str
        session : requests.Session
        retry_count : int
            number of times to retry the call if the connection fails
        retry_delay: int
            amount of seconds to wait between retries
        proxies : dict
            requests proxies
        """
        if session is None:
            session = requests.Session()
        self.session = session
        self.proxies = proxies
        self.retry_count = retry_count
        self.retry_delay = retry_delay

        @retry
        def base_request(self, start_date: pd.Timestamp, end_date: pd.Timestamp) -> requests.Response:
            start_str = self._datetime_to_str(start)
            end_str = self._datetime_to_str(end)

            base_params = {
                'q': '*', # tells the response to return all columns for a given result if one exists
                'fq': f"publication_date:[{start_date} TO {end_date}]", # publication date interval
                'wt': 'json', # xml / json format of response
                'indent': True, # make response more readable
                'start': 0, # response offset
                'rows': rows
            }
            params.update(base_params)

            response = self.session.get(url=URL, params=params,
                                        proxies=self.proxies)
            try:
                response.raise_for_status()
            except requests.HTTPError as e:
                raise e
            else:
                return response

    """
    def __init__():

        # set working directory
        # set files to be cleaned up after
        # set cutoff date
        # set db hostname
        # set db name
        # set db username
        # set db password
        # do stuff
    """
    pass
