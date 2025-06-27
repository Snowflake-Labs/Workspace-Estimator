import glob
import json
import os

import pandas as pd

from workspace_extractor.utils.util import Util


class Mapping:
    @staticmethod
    def get_clusters_ids(output=None):
        results = []
        results.extend(Mapping.get_clusters_ids_from_runs(output))
        results.extend(Mapping.get_clusters_ids_from_clusters(output))
        return results

    @staticmethod
    def get_clusters_ids_from_runs(output=None):
        if not output:
            return []
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
        return []

    @staticmethod
    def get_clusters_ids_from_clusters(output=None):
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
    def get_runs_ids(output=None):
        if not output:
            return []
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
        return []

    @staticmethod
    def get_clusters(output):
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
    def get_runs(output):
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
