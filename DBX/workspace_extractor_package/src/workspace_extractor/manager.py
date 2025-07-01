import os

from collections.abc import Generator
from typing import Any

import requests

from tqdm.notebook import tqdm_notebook

from workspace_extractor.exceptions.no_cluster_events_error import NoClusterEventsError
from workspace_extractor.utils.util import Util


class Manager:
    results_count: dict[str, int] = {}

    def __init__(
        self, input_url: str | None = None, input_token: str | None = None, input_output: str = "./output"
    ) -> None:
        """Initialize the Manager instance for API data extraction.

        Sets up the workspace estimator with connection details and output configuration.
        Creates the output directory if it doesn't exist.

        Args:
            input_url (Optional[str]): Base URL for the API endpoint. Trailing slash will be
                removed if present. Defaults to None.
            input_token (Optional[str]): Authentication token for API access.
                Defaults to None.
            input_output (str): Directory path for saving output files. Directory will be
                created if it doesn't exist. Defaults to "./output".

        Returns:
            None

        Side Effects:
            - Creates output directory if it doesn't exist
            - Initializes internal utility instance
            - Stores configuration for subsequent API calls

        """
        if input_url:
            input_url = input_url[:-1] if input_url.endswith("/") else input_url
        self.url = input_url
        self.token = input_token
        self.output = input_output
        self.api_utl = Util()
        os.makedirs(self.output, exist_ok=True)

    def show_results(self, days: int | str) -> None:
        """Display a summary of data collection results.

        Prints a formatted report showing the number of records collected for each
        data type during the specified time period.

        Args:
            days (int | str): Number of days for which data was collected.
                Used in the display message.

        Returns:
            None

        Side Effects:
            - Prints collection summary to standard output
            - Formats and displays results_count data

        Example:
            manager.show_results(30)
            # Output:
            # The following information was collected by the Workspace Estimator for the last 30 days
            # CLUSTERS  :    15
            # RUNS      :    42
            # JOBS      :    8

        """
        print(f"The following information was collected by the Workspace Estimator for the last {days} days")
        for key, value in self.results_count.items():
            print(f"{key.upper().ljust(10)}:\t{value}")

    def generator(self) -> Generator[None, None, None]:
        """Create an infinite generator for pagination loops.

        Provides a simple infinite iterator used in pagination loops to continue
        fetching data until a break condition is met. This allows for clean
        pagination handling without explicit while loops.

        Args:
            None

        Returns:
            Generator[None, None, None]: An infinite generator that yields None values.

        Yields:
            None: Continuously yields None to enable infinite iteration.

        Example:
            for _ in manager.generator():
                # Process page
                if not has_more_pages:
                    break

        """
        while True:
            yield

    def get_and_save(
        self,
        path: str | None = None,
        name_output: str = "default_output",
        array_field: str | None = None,
        suffix: str = "",
        default_params: dict[str, Any] | None = None,
        use_paging: bool = False,
        post: bool = False,
        body: dict[str, Any] | None = None,
        url_api: str = "",
        cloud_provider: str = "",
        pb: Any | None = None,
        pb_message: str | None = None,
        paging_pb: bool = False,
        full_response: bool = False,
    ) -> tuple[bool, str]:
        """Fetch data from an API endpoint with pagination support and save results to a file.

        This method handles API requests (GET/POST), automatic pagination, progress tracking,
        data aggregation, and file output. It's designed to work with various cloud provider
        APIs and can handle different pagination mechanisms.

        Args:
            path (str | None): API endpoint path to append to the base URL. If None,
                uses url_api directly. Defaults to None.
            name_output (str): Base name for the output file (without extension).
                Defaults to "default_output".
            array_field (str | None): JSON field name containing the array data to extract.
                If None, uses the entire response. Defaults to None.
            suffix (str): Suffix to append to the output filename. Defaults to "".
            default_params (dict[str, Any] | None): Default query parameters for the API request.
                Defaults to None (empty dict).
            use_paging (bool): Whether to use pagination for the API requests.
                Defaults to False.
            post (bool): Whether to use POST method instead of GET. Defaults to False.
            body (dict[str, Any] | None): Request body for POST requests. Defaults to None (empty dict).
            url_api (str): Full URL for the API endpoint. Used when path is None or
                as base URL when path is provided. Defaults to "".
            cloud_provider (str): Cloud provider identifier for response processing.
                Defaults to "".
            pb (Any | None): Progress bar instance for tracking overall progress.
                Defaults to None.
            pb_message (str | None): Custom message for the progress bar. If None,
                uses default message. Defaults to None.
            paging_pb (bool): Whether to show a separate progress bar for pagination.
                Defaults to False.
            full_response (bool): Whether to save the complete API response or just
                the extracted data. Defaults to False.

        Returns:
            tuple[bool, str]: A tuple containing:
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
        response = []
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

    def get_response(
        self, body: dict[str, Any], new_params: dict[str, Any], path: str | None, post: bool, url: str
    ) -> requests.Response:
        """Execute HTTP request to API endpoint and handle response validation.

        Constructs the full URL with query parameters, sets up authentication headers,
        performs the HTTP request (GET or POST), and validates the response status.
        Raises appropriate exceptions for error conditions.

        Args:
            body (dict[str, Any]): Request body data for POST requests. Can be empty dict for GET.
            new_params (dict[str, Any]): Query parameters to append to the URL.
            path (str | None): API endpoint path to append to base URL. If None, uses url directly.
            post (bool): Whether to use POST method. If False, uses GET method.
            url (str): Base URL or full URL for the API endpoint.

        Returns:
            requests.Response: HTTP response object from the API call.

        Raises:
            NoClusterEventsError: When API response indicates the requested cluster events don't exist.
            Exception: For HTTP errors (non-200 status codes) or connection failures.

        Side Effects:
            - Makes HTTP request to external API
            - May trigger rate limiting or authentication challenges

        Example:
            response = manager.get_response(
                body={},
                new_params={"limit": 100, "offset": 0},
                path="api/v2/clusters",
                post=False,
                url="https://api.databricks.com"
            )

        """
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
