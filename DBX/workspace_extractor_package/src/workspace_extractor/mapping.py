import glob
import json
import os

import pandas as pd

from workspace_extractor.utils.util import Util


class Mapping:
    @staticmethod
    def get_clusters_ids(output: str | None = None) -> list[str | int]:
        """Get all cluster IDs from both runs and clusters data.

        Combines cluster IDs extracted from job runs and cluster configurations
        to provide a comprehensive list of all clusters in the workspace.

        Args:
            output (str | None): Path to the directory containing JSON files.
                                  If None, returns an empty list.

        Returns:
            list[str | int]: Combined list of cluster IDs from runs and clusters.
                                 May contain duplicates if a cluster appears in both sources.

        Example:
            # Get all cluster IDs from default output directory
            cluster_ids = Mapping.get_clusters_ids("./output")

            # Returns empty list if no output directory specified
            empty_list = Mapping.get_clusters_ids(None)

        """
        results = []
        results.extend(Mapping.get_clusters_ids_from_runs(output))
        results.extend(Mapping.get_clusters_ids_from_clusters(output))
        return results

    @staticmethod
    def get_clusters_ids_from_runs(output: str | None = None) -> set[str | int]:
        """Extract cluster IDs from job run data files.

        Processes job run data to identify clusters used by successful runs,
        filtering to include only runs with positive duration and successful completion.
        For each job name, selects the most recent successful run.

        Args:
            output (str | None): Path to the directory containing run JSON files.
                                  If None, returns an empty set.

        Returns:
            set[str | int]: Set of unique cluster IDs from successful job runs.
                                Empty set if no output directory or no successful runs found.

        Processing Logic:
            1. Loads all run data using get_runs()
            2. Calculates run duration (end_time - start_time)
            3. Filters for successful runs with positive duration
            4. Ranks runs by end_time within each run_name group
            5. Selects the most recent run for each job
            6. Extracts cluster IDs from selected runs

        Example:
            # Extract cluster IDs from job runs
            run_cluster_ids = Mapping.get_clusters_ids_from_runs("./output")
            print(f"Found {len(run_cluster_ids)} unique clusters from job runs")

        """
        if not output:
            return set()
        runs_list = Mapping.get_runs(output)
        if len(runs_list) > 0:
            result_df = pd.DataFrame(runs_list)
            result_df["duration"] = result_df["end_time"] - result_df["start_time"]
            result_df = result_df[(result_df["duration"] > 0) & (result_df["result_state"] == "SUCCESS")]
            result_df["rank"] = result_df.groupby(["run_name"])["end_time"].rank("first", ascending=False)
            rank_first_df = result_df[result_df["rank"] == 1]
            rank_first_df = rank_first_df[rank_first_df["cluster_id"].notna()]
            cluster_ids = rank_first_df["cluster_id"].values
            return set(cluster_ids)
        return set()

    @staticmethod
    def get_clusters_ids_from_clusters(output: str | None = None) -> list[str | int]:
        """Extract cluster IDs from cluster configuration data files.

        Processes cluster configuration data to identify both UI-created clusters
        and job-created clusters. For job clusters, applies the same filtering
        logic as run-based extraction to ensure consistency.

        Args:
            output (str | None): Path to the directory containing cluster JSON files.
                                  If None, returns an empty list.

        Returns:
            list[str | int]: List of cluster IDs from both UI and job clusters.
                                 Includes all UI clusters and filtered job clusters.

        Processing Logic:
            1. Loads all cluster data using get_clusters()
            2. Separates UI clusters (cluster_source != "JOB") - includes all
            3. For job clusters (cluster_source == "JOB"):
               - Filters for successful clusters with positive duration
               - Ranks by end_time within each run_name group
               - Selects most recent cluster for each job
            4. Combines both UI and filtered job cluster IDs

        Example:
            # Extract cluster IDs from cluster configurations
            config_cluster_ids = Mapping.get_clusters_ids_from_clusters("./output")
            print(f"Found {len(config_cluster_ids)} clusters from configurations")

        """
        if not output:
            return []
        cluster_list = Mapping.get_clusters(output)
        if len(cluster_list) > 0:
            new_clusters = []
            result_df = pd.DataFrame(cluster_list)
            ui_clusters = result_df[result_df["cluster_source"] != "JOB"]["cluster_id"].values
            new_clusters.extend(set(ui_clusters))
            result_df["duration"] = result_df["end_time"] - result_df["start_time"]
            result_df = result_df[
                (result_df["duration"] > 0)
                & (result_df["cluster_source"] == "JOB")
                & (result_df["result_state"] == "SUCCESS")
            ]
            result_df["rank"] = result_df.groupby(["run_name"])["end_time"].rank("first", ascending=False)
            rank_first_df = result_df[result_df["rank"] == 1]
            rank_first_df = rank_first_df[rank_first_df["cluster_id"].notna()]
            cluster_ids = rank_first_df["cluster_id"].values
            new_clusters.extend(set(cluster_ids))
            return new_clusters
        return []

    @staticmethod
    def get_runs_ids(output: str | None = None) -> set[str | int]:
        """Extract job run IDs from run data files.

        Processes job run data to identify successful runs, applying the same
        filtering and ranking logic as cluster extraction to ensure consistency.

        Args:
            output (str | None): Path to the directory containing run JSON files.
                                  If None, returns an empty set.

        Returns:
            set[str | int]: Set of unique run IDs from successful job runs.
                                Empty set if no output directory or no successful runs found.

        Processing Logic:
            1. Loads all run data using get_runs()
            2. Calculates run duration (end_time - start_time)
            3. Filters for successful runs with positive duration
            4. Ranks runs by end_time within each run_name group
            5. Selects the most recent run for each job
            6. Extracts run IDs from selected runs

        Use Cases:
            - Identifying successful job runs for detailed analysis
            - Creating lists for further API calls to get run details
            - Filtering runs for performance analysis

        Example:
            # Extract run IDs for successful jobs
            successful_runs = Mapping.get_runs_ids("./output")
            print(f"Found {len(successful_runs)} successful job runs")

        """
        if not output:
            return set()
        runs_list = Mapping.get_runs(output)
        if len(runs_list) > 0:
            result_df = pd.DataFrame(runs_list)
            result_df["duration"] = result_df["end_time"] - result_df["start_time"]
            result_df = result_df[(result_df["duration"] > 0) & (result_df["result_state"] == "SUCCESS")]
            result_df["rank"] = result_df.groupby(["run_name"])["end_time"].rank("first", ascending=False)
            rank_first_df = result_df[result_df["rank"] == 1]
            rank_first_df = rank_first_df[rank_first_df["run_id"].notna()]
            runs_ids = rank_first_df["run_id"].values
            return set(runs_ids)
        return set()

    @staticmethod
    def get_clusters(output: str) -> list[dict[str, str | int | None]]:
        """Parse cluster data from JSON files and extract relevant information.

        Reads all cluster JSON files in the specified directory and extracts
        cluster metadata including IDs, sources, tags, and timing information.
        Processes both UI-created and job-created clusters.

        Args:
            output (str): Path to the directory containing cluster JSON files.
                        Must be a valid directory path containing "clusters*.json" files.

        Returns:
            ist[dict[str, str | int | None]]: List of dictionaries containing
            cluster information with the following keys:
                - cluster_id: Unique cluster identifier
                - cluster_source: Source of cluster creation (UI, JOB, etc.)
                - run_name: Associated job run name (cleaned)
                - job_id: Associated job ID or "NO_ID_FOUND"/"NO_TAG_FOUND"
                - start_time: Cluster start timestamp
                - end_time: Cluster end timestamp
                - result_state: Cluster termination state

        Data Processing:
            - Reads all files matching "clusters*.json" pattern
            - Extracts default_tags for job and run information
            - Processes termination_reason for result state
            - Applies name cleaning via Util.get_clean_name()
            - Handles missing or null values with appropriate defaults

        File Format Expected:
            JSON files containing arrays of cluster objects with standard
            Databricks cluster API response format.

        Error Handling:
            - Safely handles missing or null cluster objects
            - Provides default values for missing tags or metadata
            - Gracefully processes malformed cluster data

        Example:
            # Parse cluster data from output directory
            clusters = Mapping.get_clusters("./output")
            for cluster in clusters:
                print(f"Cluster {cluster['cluster_id']}: {cluster['cluster_source']}")

        """
        result = []
        files = glob.glob(os.path.join(output, "clusters*.json"))
        for f in files:
            with open(f) as file:
                clusters = json.load(file)
                for cluster in clusters:
                    cluster_id = cluster.get("cluster_id", None) if cluster else None
                    cluster_source = cluster.get("cluster_source", None) if cluster else None
                    tags = cluster.get("default_tags", None) if cluster else None
                    job_id = tags.get("JobId", "NO_ID_FOUND") if tags else "NO_TAG_FOUND"
                    run_name = tags.get("RunName", "NO_NAME_FOUND") if tags else "NO_TAG_FOUND"
                    start_time = cluster.get("start_time", 0)
                    end_time = cluster.get("end_time", 0)
                    termination_reason = cluster.get("termination_reason")
                    result_state = termination_reason.get("type") if termination_reason else None
                    result.append(
                        {
                            "cluster_id": cluster_id,
                            "cluster_source": cluster_source,
                            "run_name": Util.get_clean_name(run_name),
                            "job_id": job_id,
                            "start_time": start_time,
                            "end_time": end_time,
                            "result_state": result_state,
                        }
                    )
        return result

    @staticmethod
    def get_runs(output: str) -> list[dict[str, str | int | dict | None]]:
        """Parse job run data from JSON files and extract relevant information.

        Reads all job run JSON files in the specified directory and extracts
        run metadata including IDs, names, timing, and associated cluster information.
        Handles both regular job runs and ADF (Azure Data Factory) runs with
        special name processing.

        Args:
            output (str): Path to the directory containing run JSON files.
                        Must be a valid directory path containing "run*.json" files.

        Returns:
            list[dict[str, str | int | dict | None]]: List of dictionaries containing
            run information with the following keys:
                - run_name: Job run name (cleaned, ADF suffix removed if present)
                - run_id: Unique run identifier
                - start_time: Run start timestamp
                - end_time: Run end timestamp
                - cluster_instance: Complete cluster instance object (if available)
                - cluster_id: Associated cluster identifier
                - result_state: Run execution result state

        Data Processing:
            - Reads all files matching "run*.json" pattern
            - Processes task-level information for cluster associations
            - Handles both existing_cluster_id and cluster_instance patterns
            - Special handling for ADF runs (removes 37-character suffix)
            - Applies name cleaning via Util.get_clean_name()
            - Extracts result_state from nested task state objects

        Task Processing:
            For each run, iterates through all tasks to extract:
            - Cluster associations (existing or new cluster instances)
            - Task execution states and results
            - Creates separate entries for each task in multi-task jobs

        ADF Integration:
            - Detects Azure Data Factory runs by "ADF" in run name
            - Removes 37-character suffix from ADF run names for consistency
            - Preserves original run structure while cleaning names

        File Format Expected:
            JSON files containing arrays of run objects with standard
            Databricks Jobs API response format including nested tasks.

        Error Handling:
            - Safely handles missing or null run objects
            - Provides default values for missing metadata
            - Gracefully processes runs without tasks
            - Handles missing cluster information

        Example:
            # Parse run data from output directory
            runs = Mapping.get_runs("./output")
            for run in runs:
                print(f"Run {run['run_id']}: {run['run_name']} -> Cluster {run['cluster_id']}")

        """
        result = []
        files = glob.glob(os.path.join(output, "run*.json"))
        for f in files:
            with open(f) as file:
                runs = json.load(file)
                for run in runs:
                    run_id = run.get("run_id", None) if run else None
                    run_name = run.get("run_name", None) if run else None
                    start_time = run.get("start_time", None) if run else None
                    end_time = run.get("end_time", None) if run else None
                    if "tasks" in run:
                        tasks = run["tasks"]
                        for task in tasks:
                            existing_cluster_id = task.get("existing_cluster_id", None)
                            cluster_instance = task.get("cluster_instance", None)
                            cluster_id = (
                                cluster_instance.get("cluster_id", None) if cluster_instance else existing_cluster_id
                            )
                            state = task.get("state", None)
                            result_state = state.get("result_state", None) if state else None
                            if run_name:
                                run_name = run_name[:-37] if "ADF" in run_name else run_name
                            result.append(
                                {
                                    "run_name": Util.get_clean_name(run_name),
                                    "run_id": run_id,
                                    "start_time": start_time,
                                    "end_time": end_time,
                                    "cluster_instance": cluster_instance,
                                    "cluster_id": cluster_id,
                                    "result_state": result_state,
                                }
                            )
        return result
