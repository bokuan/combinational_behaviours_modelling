import requests
import logging
import pandas as pd

log = logging.getLogger()


class TagFactoryApi:
    """
    Class defines behaviour of Tag Factory API functions.

    Group of functions that are used to query tag factory.
    Origin: AVL, modified by Volvo team
    """

    def __init__(self, url: str, max_trial: int = 3):
        """
        Init for the Tag Factory API functions.
        """
        self.base_url = url
        self.max_trial = max_trial

    # Query, Filters and Search

    def tags_query(self, payload: dict, timeout: float = 100) -> requests.Response:
        """
        Query for tags specified in payload with four filters, and return the response.

        Payload schema is a dictionary with optional keys types, sub_types, names, testdrives.
        Corresponding items should be lists with strings.
        Types and sub_types refers to the "parent type" and "sub types" of the avl tags, for example Environment and Weather.
        Names are the tag names, such as "Day", "Night", ...
        Testdrives are recording ids.


        Parameters
        ----------
        payload : dict
            Dictionary with payload for the query.
        timeout : float, optional
            Seconds for timeout of the request, by default 100.

        Returns
        -------
        requests.Response
            The response of the API query.
        """

        return requests.post(self.base_url + '/tags/q', json=payload, timeout=timeout)

    def limited_tag_query(self, payload: dict, offset: int, limit: int, timeout: float = 100) -> requests.Response:
        """
        Query for tags specified in payload with two filters, a limit and an offset, and return the response.

        Payload schema is a dictionary with optional keys names and testdrives.
        Corresponding items should be lists with strings.
        Types and sub_types refers to the "parent type" and "sub types" of the avl tags, for example Environment and Weather.
        Names are the tag names, such as "Day", "Night", ...
        Testdrives are recording ids.

        Query is limited by the offset and the limit, returning a smaller part of the output. 
        Response is also limited to contain only the most important informations about the tags.


        Parameters
        ----------
        payload : dict
            Dictionary with payload for the query.
        offset: int
            Offset for paginated query.
        limit: int
            Limit for paginated query.
        timeout : float, optional
            Seconds for timeout of the request, by default 100.

        Returns
        -------
        requests.Response
            The response of the API query.
        """

        return requests.post(self.base_url + f'/tags/query?limit={limit}&offset={offset}',
                             json=payload, timeout=timeout)

    def paginated_tag_query(self, tag_names: list = [], recording_ids: list = []) -> pd.DataFrame:
        """
        Perform paginated query for tags.

        Parameters
        ----------
        tag_names : list, optional
            List with tag names to query for, by default [].
        recording_ids : list, optional
            List with recording_ids to query for, by default [].


        Returns
        -------
        pd.DataFrame
            Dataframe with normalized output of the tags.
        """

        if tag_names and not recording_ids:
            payload = {'names': tag_names}
        elif recording_ids and not tag_names:
            payload = {'testdrives': recording_ids}
        else:
            payload = {'names': tag_names, 'testdrives': recording_ids}

        # limits and variables with saved output
        offset = 0
        limit = 5000
        tags = []
        num_trial = 0
        tag_count = 0

        while True:
            response_returned = self.limited_tag_query(payload, offset, limit)

            if response_returned.status_code == 200:
                data = response_returned.json()

                total_tag_count = data['total']
                tag_count += len(data['items'])

                offset += limit

                log.info('Tag API: There are {} tags in the database.'.format(
                    total_tag_count))
                log.info('Tag API: The query returned {} results. Remaining tags: {}.'.format(
                    len(data['items']), total_tag_count-tag_count))

                tags.extend(data['items'])

                if offset >= total_tag_count:
                    break

            else:
                log.info('Failed tag API call with code {}'.format(
                    response_returned.status_code))
                num_trial += 1

            if num_trial >= self.max_trial:
                log.warning('Tag API: Too many failed calls - Terminating')
                break

        if tags:
            df = pd.json_normalize(tags)
            return df
        else:
            log.info('Tag API: No tags extracted, returning empty dataframe.')
            return pd.DataFrame([])
