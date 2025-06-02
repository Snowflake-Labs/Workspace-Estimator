import os
from datetime import datetime, timedelta

from tqdm.notebook import tqdm_notebook

from workspace_estimator.manager import Manager
from workspace_estimator.mapping import Mapping


class Sizing(Manager):
    def __init__(self, input_url, input_token=None, input_output="./output"):
        super().__init__(input_url, input_token=input_token, input_output=input_output)
        self.token = input_token
        self.output = input_output
        os.makedirs(self.output, exist_ok=True)

    def get_clusters_events(self, timestamp, pb=None):
        events_path = "api/2.0/clusters/events"
        if pb:
            pb.set_description(f"Processing {events_path}")

        cluster_list = Mapping.get_clusters_ids(self.output)
        with tqdm_notebook(
            range(len(cluster_list)), desc="Fetching Cluster Events"
        ) as pb2:
            for cluster in cluster_list:
                self.get_and_save(
                    path=events_path,
                    name_output="events",
                    suffix=f"_{cluster}",
                    post=True,
                    default_params={
                        "cluster_id": f"{cluster}",
                        "limit": 250,
                        "start_time": timestamp,
                    },
                    body={
                        "event_types": (
                            "CREATING",
                            "STARTING",
                            "RESTARTING",
                            "TERMINATING",
                            "RUNNING",
                            "RESIZING",
                            "UPSIZE_COMPLETED",
                            "EDITED",
                        )
                    },
                    use_paging=True,
                    url_api=self.url if self.url else "",
                    pb=pb2,
                    pb_message=f"Fetching {cluster} events",
                )
        if pb:
            pb.update(1)

    def get_runs_details(self, pb=None):
        runs_details_path = "api/2.1/jobs/runs/get"
        if pb:
            pb.set_description(f"Processing {runs_details_path}")

        runs_list = Mapping.get_runs_ids(self.output)
        with tqdm_notebook(range(len(runs_list)), desc="Fetching Runs Details") as pb2:
            for run_id in runs_list:
                self.get_and_save(
                    path=runs_details_path,
                    name_output="runs_details",
                    suffix=f"_{run_id}",
                    default_params={"run_id": run_id},
                    url_api=self.url if self.url else "",
                    pb=pb2,
                    pb_message=f"Fetching details of run:{run_id}",
                    full_response=True,
                )
        if pb:
            pb.update(1)

    def get_metadata(self, days=60):
        with tqdm_notebook(range(9), desc="Processing...") as pb:
            date_days_ago = datetime.now() - timedelta(days=days)
            timestamp = int(date_days_ago.timestamp() * 1000)
            self.get_and_save(
                path="api/2.0/clusters/list-node-types",
                name_output="node_types",
                url_api=self.url if self.url else "",
                pb=pb,
            )
            self.get_and_save(
                path="api/2.0/clusters/list",
                name_output="clusters",
                use_paging=True,
                url_api=self.url if self.url else "",
                pb=pb,
            )
            self.get_and_save(
                path="api/2.0/jobs/list",
                name_output="jobs",
                use_paging=True,
                url_api=self.url if self.url else "",
                default_params={"expand_tasks": "true"},
                pb=pb,
            )
            self.get_and_save(
                path="api/2.1/jobs/runs/list",
                name_output="runs",
                use_paging=True,
                default_params={"expand_tasks": "true"},
                url_api=self.url if self.url else "",
                pb=pb,
                paging_pb=True,
            )
            self.get_and_save(
                path="api/2.0/sql/warehouses",
                name_output="warehouses",
                use_paging=True,
                url_api=self.url if self.url else "",
                pb=pb,
            )
            self.get_and_save(
                path="api/2.0/pipelines",
                name_output="pipelines",
                array_field="statuses",
                use_paging=True,
                default_params={"max_results": 100},
                url_api=self.url if self.url else "",
                pb=pb,
            )
            self.get_and_save(
                path="api/2.0/sql/history/queries",
                name_output="queries",
                array_field="res",
                use_paging=True,
                url_api=self.url if self.url else "",
                pb=pb,
            )
            self.get_clusters_events(timestamp, pb=pb)
            self.get_runs_details(pb=pb)
