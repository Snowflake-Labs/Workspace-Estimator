import os
from datetime import datetime, timedelta

from tqdm.notebook import tqdm_notebook

from workspace_extractor.manager import Manager
from workspace_extractor.mapping import Mapping


class Sizing(Manager):
    def __init__(self, input_url, input_token=None, input_output="./output"):
        super().__init__(input_url, input_token=input_token, input_output=input_output)
        self.token = input_token
        self.output = input_output
        os.makedirs(self.output, exist_ok=True)

    def get_clusters_events(self, timestamp, pb=None):
        """
        Fetch cluster lifecycle events for all clusters from a specified timestamp.
        
        This method retrieves detailed event information for each cluster in the workspace,
        including creation, startup, restart, termination, resizing, and editing events.
        It processes each cluster individually and saves events to separate files.
        
        Args:
            timestamp (int): Unix timestamp in milliseconds specifying the start time
                for event collection. Events from this timestamp onwards will be fetched.
                Typically calculated as: int(datetime.timestamp() * 1000)
            pb (tqdm, optional): Progress bar instance for tracking overall progress.
                If provided, will be updated to show processing status. Defaults to None.
        
        Returns:
            tuple[bool, str]: A tuple containing:
                - bool: True if an error occurred during processing, False if successful
                - str: Success message or error description
        
        Side Effects:
            - Reads cluster IDs from the output directory using Mapping.get_clusters_ids()
            - Creates individual event files for each cluster in format: "events_{cluster_id}"
            - Updates progress bars to show current processing status
            - Displays a secondary progress bar showing individual cluster processing
        
        API Details:
            - Endpoint: "api/2.0/clusters/events"
            - Method: POST
            - Parameters:
                - cluster_id: Individual cluster identifier
                - limit: 250 events per request
                - start_time: Provided timestamp parameter
            - Event Types Collected:
                - CREATING: Cluster creation events
                - STARTING: Cluster startup events  
                - RESTARTING: Cluster restart events
                - TERMINATING: Cluster shutdown events
                - RUNNING: Cluster running state events
                - RESIZING: Cluster scaling events
                - UPSIZE_COMPLETED: Cluster upscaling completion events
                - EDITED: Cluster configuration change events
        
        Error Handling:
            - Catches and logs all exceptions during processing
            - Returns error status and message if any cluster processing fails
            - Individual cluster failures don't stop processing of remaining clusters
        
        Dependencies:
            - Requires cluster list file to exist in output directory
            - Uses Mapping.get_clusters_ids() to retrieve cluster identifiers
            - Leverages inherited get_and_save() method for API calls and file management
        
        Note:
            This method uses pagination automatically through the get_and_save() method
            to ensure all events are collected regardless of the number of events per cluster.
            Each cluster's events are saved to a separate file for easier analysis.
        
        Example:
            # Fetch events from 30 days ago
            from datetime import datetime, timedelta
            
            date_30_days_ago = datetime.now() - timedelta(days=30)
            timestamp = int(date_30_days_ago.timestamp() * 1000)
            
            error, message = sizing.get_clusters_events(timestamp)
            if error:
                print(f"Error occurred: {message}")
            else:
                print("Cluster events fetched successfully")
        """
        events_path = "api/2.0/clusters/events"
        if pb:
            pb.set_description(f"Processing {events_path}")

        result = False, "Data fetched and saved successfully"

        try:
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
        except Exception as e:
            result = True, f"Error while processing url: {events_path}. {str(e)}"

        if pb:
            pb.update(1)

        return result

    def get_runs_details(self, pb=None) -> tuple[bool, str]:
        """
        Fetch detailed information for all job runs in the workspace.
        
        This method retrieves comprehensive details for each job run identified in the workspace,
        including execution statistics, configuration, cluster information, and task details.
        Each run's details are saved to individual files for analysis and reporting.
        
        Args:
            pb (tqdm, optional): Progress bar instance for tracking overall progress.
                If provided, will be updated to show processing status. Defaults to None.
        
        Returns:
            tuple[bool, str]: A tuple containing:
                - bool: True if an error occurred during processing, False if successful
                - str: Success message or error description
        
        Side Effects:
            - Reads job run IDs from the output directory using Mapping.get_runs_ids()
            - Creates individual detail files for each run in format: "runs_details_{run_id}"
            - Updates progress bars to show current processing status
            - Displays a secondary progress bar showing individual run processing
            - Saves complete API responses including all nested details
        
        API Details:
            - Endpoint: "api/2.1/jobs/runs/get"
            - Method: GET
            - Parameters:
                - run_id: Individual job run identifier
            - Response Data Includes:
                - Run execution details (start time, end time, duration)
                - Job configuration and settings
                - Cluster information and specifications
                - Task execution details and status
                - Error information and logs (if applicable)
                - Resource usage metrics
                - Notebook/JAR/Python file paths and parameters
        
        Data Collection:
            - Fetches complete run details rather than summary information
            - Includes all nested objects and arrays in the response
            - Preserves full API response structure for comprehensive analysis
            - Captures both successful and failed run information
        
        Error Handling:
            - Catches and logs all exceptions during processing
            - Returns error status and message if any run processing fails
            - Individual run failures don't stop processing of remaining runs
            - Continues processing even if some runs are inaccessible
        
        Dependencies:
            - Requires job runs list file to exist in output directory
            - Uses Mapping.get_runs_ids() to retrieve run identifiers
            - Leverages inherited get_and_save() method for API calls and file management
            - Depends on prior execution of job runs list collection
        
        Performance Notes:
            - Processes runs sequentially to avoid API rate limiting
            - Uses full_response=True to capture complete run details
            - May take significant time for workspaces with many job runs
            - Progress tracking helps monitor long-running operations
        
        Use Cases:
            - Analyzing job execution patterns and performance
            - Troubleshooting failed job runs
            - Generating detailed execution reports
            - Calculating resource utilization metrics
            - Auditing job configurations and settings
        
        Example:
            # Fetch details for all job runs
            error, message = sizing.get_runs_details()
            if error:
                print(f"Error occurred: {message}")
            else:
                print("Job run details fetched successfully")
                
            # With progress tracking
            from tqdm.notebook import tqdm_notebook
            with tqdm_notebook(range(1), desc="Main Progress") as main_pb:
                error, message = sizing.get_runs_details(pb=main_pb)
        """
        runs_details_path = "api/2.1/jobs/runs/get"
        if pb:
            pb.set_description(f"Processing {runs_details_path}")
        result = False, "Data fetched and saved successfully"
        try:
            runs_list = Mapping.get_runs_ids(self.output)
            with tqdm_notebook(
                range(len(runs_list)), desc="Fetching Runs Details"
            ) as pb2:
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
        except Exception as e:
            result = True, f"Error while processing url: {runs_details_path}. {str(e)}"

        if pb:
            pb.update(1)

        return result

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
                path="api/2.2/jobs/list",
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
