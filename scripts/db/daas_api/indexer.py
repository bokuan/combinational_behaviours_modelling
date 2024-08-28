"""
Group of functions that are used to query overlap database and files database.
"""
import os
from urllib.parse import urljoin
import requests


class IndexerApi:
    """
    Class that defines calls for overlap and files databases.

    Group of functions that are used to query overlap database and files database.
    Origin: AVL
    Modified by Volvo team.
    """

    def __init__(self, url):

        os.environ['SSL_CERTIFICATE_PATH'] = '/etc/ssl/certs/ca-bundle.trust.crt'
        os.environ['PREDECESSOR'] = 'True'

        self.base_url = url
        self.predecessor = os.getenv('PREDECESSOR')

    def get_overlap_index_data(self, joined_query: str, testdrive_list: list) -> requests.Response:
        """
        Query the overlap database for a certain query and list of test drive ids,

        Certain syntax needs to be applied to the query, for example:
        "'Primary highway' and 'Day'"  or "'Primary highway' and not 'Day'"

        Parameters
        ----------
        joined_query : str
            String with query for overlaps.
        testdrive_list : list
            List with strings of all testdrive ids/recording ids to query on.

        Returns
        -------
        requests.Response
            Response of the API request.
        """

        headers = None
        data = {
            'query': joined_query,
            'testdrive_id_list': testdrive_list,
            'predecessor': self.predecessor,
        }
        url = urljoin(self.base_url, '/overlaps/query')

        return requests.post(
            url=url,
            json=data,
            headers=headers,
            verify=os.getenv('SSL_CERTIFICATE_PATH'),
        )

    def get_file_index_data(self, payload: dict) -> requests.Response:
        """
        Query the file database for testdrives

        Parameters
        ----------
        payload : dict
            Dictionary with key 'testdrive_ids' and a list of files

        Returns
        -------
        requests.Response
            Response of the API request.
        """

        headers = None
        url = urljoin(self.base_url, '/index/list_query')

        return requests.post(
            url=url,
            json=payload,
            headers=headers,
            verify=os.getenv('SSL_CERTIFICATE_PATH'),
        )
