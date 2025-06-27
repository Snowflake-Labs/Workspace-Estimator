import os

import requests

from tqdm.notebook import tqdm_notebook

from workspace_extractor.exceptions.no_cluster_events_error import NoClusterEventsError
from workspace_extractor.utils.util import Util


class Manager:
    results_count = {}

    def __init__(self, input_url=None, input_token=None, input_output="./output"):
        if input_url:
            input_url = input_url[:-1] if input_url.endswith("/") else input_url
        self.url = input_url
        self.token = input_token
        self.output = input_output
        self.api_utl = Util()
        os.makedirs(self.output, exist_ok=True)

    def show_results(self, days):
        print(f"The following information was collected by the Workspace Estimator for the last {days} days")
        for key, value in self.results_count.items():
            print(f"{key.upper().ljust(10)}:\t{value}")

    def generator(self):
        while True:
            yield

    def get_and_save(
        self,
        path=None,
        name_output="default_output",
        array_field=None,
        suffix="",
        default_params=None,
        use_paging=False,
        post=False,
        body=None,
        url_api="",
        cloud_provider="",
        pb=None,
        pb_message=None,
        paging_pb=False,
        full_response=False,
    ) -> tuple[bool, str]:
        """Fetch data from an API endpoint with pagination support and save results to a file.

        This method handles API requests (GET/POST), automatic pagination, progress tracking,
        data aggregation, and file output. It's designed to work with various cloud provider
        APIs and can handle different pagination mechanisms.

        Args:
            path (str, optional): API endpoint path to append to the base URL. If None,
                uses url_api directly. Defaults to None.
            name_output (str): Base name for the output file (without extension).
                Defaults to "default_output".
            array_field (str, optional): JSON field name containing the array data to extract.
                If None, uses the entire response. Defaults to None.
            suffix (str): Suffix to append to the output filename. Defaults to "".
            default_params (dict, optional): Default query parameters for the API request.
                Defaults to None (empty dict).
            use_paging (bool): Whether to use pagination for the API requests.
                Defaults to False.
            post (bool): Whether to use POST method instead of GET. Defaults to False.
            body (dict, optional): Request body for POST requests. Defaults to None (empty dict).
            url_api (str): Full URL for the API endpoint. Used when path is None or
                as base URL when path is provided. Defaults to "".
            cloud_provider (str): Cloud provider identifier for response processing.
                Defaults to "".
            pb (tqdm, optional): Progress bar instance for tracking overall progress.
                Defaults to None.
            pb_message (str, optional): Custom message for the progress bar. If None,
                uses default message. Defaults to None.
            paging_pb (bool): Whether to show a separate progress bar for pagination.
                Defaults to False.
            full_response (bool): Whether to save the complete API response or just
                the extracted data. Defaults to False.

        Returns:            tuple[bool, str]: A tuple containing:
                - bool: True if an error occurred, False if successful
                - str: Success message or error description

        Raises:
            NoClusterEventsError: When the API response indicates cluster events don't exist
            Exception: For other API errors or connection failures

        Side Effects:
            - Creates output files in the configured output directory
            - Updates self.results_count with the number of records processed
            - Updates progress bars if provided
            - Writes error logs to the output directory if exceptions occur

        Note:
            The method automatically handles:
            - Multiple pagination mechanisms (tokens, offsets, skip parameters)
            - Rate limiting and error handling
            - Data validation and file integrity checks
            - Progress tracking for long-running operations

        Example:
            # Simple GET request
            error, message = manager.get_and_save(
                path="api/v2/clusters",
                name_output="clusters",
                use_paging=True
            )

            # POST request with custom parameters
            error, message = manager.get_and_save(
                path="api/v2/sql/history/queries",
                name_output="query_history",
                post=True,
                body={"max_results": 1000},
                use_paging=True,
                paging_pb=True
            )

        """
        has_more = True
        counter = 0
        next_page_token = ""
        new_url = ""
        response = []
        query = ""
        paging = {}
        offset = -1
        full_json = []
        if body is None:
            body = {}
        if default_params is None:
            default_params = {}
        skip = 0
        if pb:
            (pb.set_description(f"{pb_message}") if pb_message else pb.set_description(f"Processing {path}"))
        result = False, "Data fetched and saved successfully"
        try:
            new_params = default_params.copy()
            pb_paging = None
            file_output = f"{name_output}{suffix}"
            gen = self.generator()
            if paging_pb:
                pb_paging = tqdm_notebook(gen, desc=f"Pages in {path}")
            for _ in gen:
                new_params = self.api_utl.get_params(counter, new_params, next_page_token, offset, use_paging, skip)
                response = self.get_response(body, new_params, path, post, url_api)
                json_data = self.api_utl.get_full_json(
                    array_field,
                    full_json,
                    name_output,
                    path,
                    response,
                    full_response,
                    cloud_provider=cloud_provider,
                )
                paging = self.api_utl.get_paging(json_data)
                offset = self.api_utl.get_offset(json_data, offset)
                skip = paging.get("has_skip")
                has_more = self.api_utl.get_has_more(json_data, offset)
                next_page_token = self.api_utl.get_page_token(paging)
                counter += 1
                if paging_pb and pb_paging:
                    pb_paging.update(1)
                if not has_more:
                    break
            if paging_pb and pb_paging:
                pb_paging.close()
            Util.write_file_request_(self.output, file_output, full_json)
            Util.check_file_request_(self.output, file_output, full_json)
        except Exception as e:
            local_vars = locals().copy()
            Util.write_log(self.output, e, local_vars)
            error_message = f"Error while processing url: {url_api}. {str(e)}"
            result = True, error_message
            print(f"Error fetching {name_output.replace('_', ' ')}: {error_message}")
        if pb:
            pb.update(1)
        self.results_count[name_output] = len(full_json)

        return result

    def get_response(self, body, new_params, path, post, url):
        query = self.api_utl.get_query(new_params)
        url_path = f"{url}/{path}" if path else url
        url_not_query = url_path if path else url
        new_url = f"{url_path}?{query}" if query else url_not_query
        headers = {"Authorization": f"Bearer {self.token}"}
        if post:
            response = requests.post(new_url, headers=headers, json=body)
        else:
            response = requests.get(new_url, headers=headers)
        if response.status_code != 200:
            error = f"Failed connection - {response.content}"
            if "does not exist" in error:
                raise NoClusterEventsError(response.content)
            raise Exception(error)
        return response
