import os

from datetime import datetime, timedelta
from typing import Any

from tqdm.notebook import tqdm_notebook

from workspace_extractor.manager import Manager
from workspace_extractor.mapping import Mapping


class Sizing(Manager):
    def __init__(self, input_url: str, input_token: str | None = None, input_output: str = "./output") -> None:
        """Initialize the Sizing instance for workspace resource estimation.

        Extends the Manager class with specialized functionality for collecting
        detailed workspace metrics, cluster events, and job run information
        needed for sizing analysis and resource estimation.

        Args:
            input_url (str): Base URL for the Databricks workspace API endpoint.
                Should be the full workspace URL (e.g., 'https://dbc-12345678-9abc.cloud.databricks.com').
                Trailing slash will be removed if present.
            input_token (str | None): Personal access token for authentication.
                Must have appropriate permissions to read cluster, job, and workspace metadata.
                Defaults to None.
            input_output (str): Directory path for saving collected data files.
                Directory will be created if it doesn't exist. All output files
                will be saved in JSON format within this directory. Defaults to "./output".

        Returns:
            None

        Side Effects:
            - Calls parent Manager.__init__() with provided parameters
            - Creates output directory if it doesn't exist
            - Stores configuration for subsequent data collection operations

        Inherited Attributes:
            - self.url: Processed base URL for API calls
            - self.token: Authentication token for API requests
            - self.output: Output directory path
            - self.api_utl: Utility instance for API operations
            - self.results_count: Dictionary tracking collected record counts

        Example:
            # Initialize for a Databricks workspace
            sizing = Sizing(
                input_url="https://dbc-12345678-9abc.cloud.databricks.com",
                input_token="dapi1234567890abcdef",
                input_output="./workspace_data"
            )

        """
        super().__init__(input_url, input_token=input_token, input_output=input_output)
        self.token = input_token
        self.output = input_output
        os.makedirs(self.output, exist_ok=True)

    def get_clusters_events(self, timestamp: int, pb: Any | None = None) -> tuple[bool, str]:
        """Fetch cluster lifecycle events for all clusters from a specified timestamp.

        This method retrieves detailed event information for each cluster in the workspace,
        including creation, startup, restart, termination, resizing, and editing events.
        It processes each cluster individually and saves events to separate files.

        Args:
            timestamp (int): Unix timestamp in milliseconds specifying the start time
                for event collection. Events from this timestamp onwards will be fetched.
                Typically calculated as: int(datetime.timestamp() * 1000)
            pb (Any | None): Progress bar instance for tracking overall progress.
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
            with tqdm_notebook(range(len(cluster_list)), desc="Fetching Cluster Events") as pb2:
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

    def get_runs_details(self, pb: Any | None = None) -> tuple[bool, str]:
        """Fetch detailed information for all job runs in the workspace.

        This method retrieves comprehensive details for each job run identified in the workspace,
        including execution statistics, configuration, cluster information, and task details.
        Each run's details are saved to individual files for analysis and reporting.

        Args:
            pb (Any | None): Progress bar instance for tracking overall progress.
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
        except Exception as e:
            result = True, f"Error while processing url: {runs_details_path}. {str(e)}"

        if pb:
            pb.update(1)

        return result

    def get_metadata(self, days: int | float = 60) -> None:
        """Collect comprehensive workspace metadata for sizing analysis.

        This method orchestrates the collection of all necessary workspace data
        for resource estimation and analysis. It gathers cluster information,
        job configurations, execution history, and detailed events from the
        specified time period.

        Args:
            days (int | float): Number of days to look back for historical data.
                Used to calculate the timestamp for cluster events and other
                time-based data collection. Must be a positive number.
                Defaults to 60 days.

        Returns:
            None

        Data Collection Sequence:
            1. **Node Types**: Available cluster node types and specifications
            2. **Clusters**: All cluster configurations and current state
            3. **Jobs**: Job definitions with expanded task configurations
            4. **Job Runs**: Historical job execution records with task details
            5. **SQL Warehouses**: SQL compute endpoint configurations
            6. **Pipelines**: Delta Live Tables pipeline statuses
            7. **SQL Queries**: SQL query execution history
            8. **Cluster Events**: Detailed lifecycle events for all clusters
            9. **Run Details**: Comprehensive details for each job run

        API Endpoints Used:
            - api/2.0/clusters/list-node-types: Available node types
            - api/2.0/clusters/list: Cluster configurations
            - api/2.2/jobs/list: Job definitions (with expanded tasks)
            - api/2.1/jobs/runs/list: Job run history (with expanded tasks)
            - api/2.0/sql/warehouses: SQL warehouse configurations
            - api/2.0/pipelines: Delta Live Tables pipelines
            - api/2.0/sql/history/queries: SQL query execution history
            - api/2.0/clusters/events: Cluster lifecycle events (via get_clusters_events)
            - api/2.1/jobs/runs/get: Individual run details (via get_runs_details)

        Side Effects:
            - Creates multiple JSON files in the output directory
            - Updates self.results_count with record counts for each data type
            - Displays progress bars for tracking collection status
            - May take significant time depending on workspace size and history

        Progress Tracking:
            - Main progress bar shows overall collection progress (9 steps)
            - Individual operations may show additional progress bars
            - Cluster events and run details show nested progress for individual items

        File Outputs:
            - node_types.json: Available cluster node types
            - clusters.json: Cluster configurations
            - jobs.json: Job definitions
            - runs.json: Job run history
            - warehouses.json: SQL warehouse configurations
            - pipelines.json: Pipeline configurations
            - queries.json: SQL query history
            - events_{cluster_id}.json: Events for each cluster
            - runs_details_{run_id}.json: Details for each job run

        Performance Considerations:
            - Uses pagination for large datasets
            - Sequential processing to respect API rate limits
            - Progress tracking for long-running operations
            - Automatic retry and error handling for individual API calls

        Time Range:
            - Most data collection is current state (clusters, jobs, warehouses)
            - Historical data (events, runs) uses the specified days parameter
            - Timestamp calculation: datetime.now() - timedelta(days=days)

        Example:
            # Collect 30 days of workspace data
            sizing.get_metadata(days=30)

            # Collect 90 days of data for comprehensive analysis
            sizing.get_metadata(days=90)

            # Use default 60-day collection period
            sizing.get_metadata()

        Note:
            This method is typically the primary entry point for workspace
            data collection and should be called after proper initialization
            of the Sizing instance with valid URL and authentication token.

        """
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
