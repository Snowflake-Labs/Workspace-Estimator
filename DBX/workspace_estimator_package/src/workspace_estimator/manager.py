import os

import requests
from tqdm.notebook import tqdm_notebook

from workspace_estimator.exceptions.no_cluster_events_error import NoClusterEventsError
from workspace_estimator.utils.util import Util


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
        print(
            f"The following information was collected by the Workspace Estimator for the last {days} days"
        )
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
    ):
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
            pb.set_description(f"{pb_message}") if pb_message else pb.set_description(
                f"Processing {path}"
            )
        try:
            new_params = default_params.copy()
            pb_paging = None
            file_output = f"{name_output}{suffix}"
            gen = self.generator()
            if paging_pb:
                pb_paging = tqdm_notebook(gen, desc=f"Pages in {path}")
            for _ in gen:
                new_params = self.api_utl.get_params(
                    counter, new_params, next_page_token, offset, use_paging, skip
                )
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

        if pb:
            pb.update(1)
        self.results_count[name_output] = len(full_json)

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
