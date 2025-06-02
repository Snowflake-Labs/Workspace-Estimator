import json
import math
import os
import re
import shutil
import zipfile
from datetime import datetime

from workspace_estimator.exceptions.no_cluster_events_error import NoClusterEventsError


class UtilFile:
    email_pattern = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,7}")
    url_parameters_pattern = re.compile(
        "^(?:https?:\/\/)?(?:[^@\/\n]+@)?(?:www\.)?([^:?\n]+)"
    )
    url_pattern = re.compile(r"https?://\S+|www\.\S+")
    jwt_pattern = re.compile(r"[A-Za-z0-9_-]{4,}(?:\.[A-Za-z0-9_-]{4,}){2}")
    dbx_pattern = re.compile(
        r"https?://adb-\d{4,16}\.\d{0,2}|https?://dbc-.{4,12}-.{2,4}"
    )

    @staticmethod
    def check_file_request_(output, name_output, json_data_check):
        os.makedirs(output, exist_ok=True)
        file_path = os.path.join(output, f"{name_output}.json")
        if os.path.exists(file_path):
            size = os.path.getsize(file_path)
            size_max_unit_value = "10 MB"
            array_units = ("B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB")
            size_bites_unit_value = UtilFile.convert_size_to_mb(size)
            size_correct = UtilFile.has_correct_size(
                size_bites_unit_value, size_max_unit_value, array_units
            )
            if size_correct:
                return True
            else:
                current_size_in_mb = UtilFile.convert_size_to_mb(size)
                size_value = int(current_size_in_mb.split(" ")[0])
                size_max_value = int(size_max_unit_value.split(" ")[0])
                parts = math.ceil(size_value / size_max_value)
                count = UtilFile.get_count(json_data_check)
                size_part = math.ceil(count / parts)
                end = 0
                for i in range(parts):
                    start = end + 1 if end != 0 else 0
                    end = end + size_part if i != parts else count - 1
                    json_data_part = json_data_check[start:end]
                    # the following line is to activate the cleanup of the json files.
                    # json_data_part = UtilFile.filter_data(json_data_part, [])
                    UtilFile.write_file_request_(
                        output, f"{name_output}_{i + 1:02d}", json_data_part
                    )
                file_path = os.path.join(output, f"{name_output}.json")
                os.remove(file_path)
                return True
        else:
            return False

    @staticmethod
    def convert_size_to_mb(size_bytes):
        s = UtilFile.convert_size_to_mb_number(size_bytes)
        return "%s %s" % (s, "MB")

    @staticmethod
    def convert_size_to_mb_number(size_bytes):
        if size_bytes == 0:
            return 0

        p = math.pow(1024, 2)
        s = int(round(size_bytes / p, 0))
        return s

    @staticmethod
    def compress_folder_to_zip(output, output_zip_file, extension="zip"):
        try:
            if not os.path.exists(output):
                raise Exception(f"The source folder '{output}' does not exist.")

            with zipfile.ZipFile(
                f"{output_zip_file}.{extension}", "w", zipfile.ZIP_DEFLATED
            ) as zipf:
                for root, dirs, files in os.walk(output):
                    for file in files:
                        file_path = os.path.join(root, file)
                        relative_path = os.path.relpath(file_path, output)
                        zipf.write(file_path, relative_path)

            zip_file_size_bytes = os.path.getsize(f"{output_zip_file}.{extension}")

            if zip_file_size_bytes > 199 * 1024 * 1024:
                UtilFile.split_zip_file(f"{output_zip_file}.{extension}")

        except Exception as e:
            print(f"Error: {e}")

    @staticmethod
    def split_zip_file(file_path: str, part_size_mb: int = 195):
        if not file_path.lower().endswith(".zip"):
            raise ValueError(
                f"File '{file_path}' is not a zip file. Splitting is only supported for .zip files."
            )

        part_size = part_size_mb * 1024 * 1024
        part_num = 1

        with open(file_path, "rb") as f:
            while True:
                chunk = f.read(part_size)
                if not chunk:
                    break
                part_filename = f"{file_path}.{part_num:03d}"
                with open(part_filename, "wb") as pf:
                    pf.write(chunk)
                # print(f"Created: {part_filename}")
                part_num += 1

        # we_data_8_parts

        base_path = os.path.dirname(file_path)
        merged_parts_zip_file_name = f"{base_path}/we_data_{part_num}_parts.zip"

        with zipfile.ZipFile(
            merged_parts_zip_file_name, "w", zipfile.ZIP_DEFLATED
        ) as zipf:
            for root, dirs, files in os.walk(base_path):
                for file in files:
                    if re.match(r".*\.zip\.\d{3}$", file):
                        part_file_path = os.path.join(root, file)
                        relative_path = os.path.relpath(part_file_path, base_path)
                        zipf.write(part_file_path, relative_path)
                        os.remove(part_file_path)

    @staticmethod
    def clean_output(output):
        shutil.rmtree(output)

    @staticmethod
    def get_count(json_data_count):
        if isinstance(json_data_count, list):
            count = len(json_data_count)
        elif isinstance(json_data_count, dict):
            count = len(json_data_count)
        else:
            count = 0
        return count

    @staticmethod
    def get_file_name(workspace):
        workspace = workspace.replace(" ", "_")
        workspace = workspace.replace(" ", "")
        workspace = workspace.replace(" ", "_")
        workspace = re.sub(r"[^A-Za-z0-9 ]+", "", workspace)
        workspace = "Default_wkp" if workspace == "" else workspace
        now = datetime.now()
        date_part = now.strftime("%m%d")
        name = f"{workspace}_{date_part}"
        return name

    @staticmethod
    def has_correct_size(size_bites_unit_value, size_max_unit_value, array_units):
        size_unit = size_bites_unit_value.split(" ")[1]
        size_index = array_units.index(size_unit)
        size_max_unit = size_max_unit_value.split(" ")[1]
        size_max_index = array_units.index(size_max_unit)
        if size_index < size_max_index:
            return True
        elif size_index > size_max_index:
            return False
        else:
            size_value = int(size_bites_unit_value.split(" ")[0])
            size_max_value = int(size_max_unit_value.split(" ")[0])
            if size_value > size_max_value:
                return False
            else:
                return True

    @staticmethod
    def read_config(config_path):
        with open(config_path, "r") as file:
            config_values = json.load(file)
        if "url" not in config_values:
            raise ValueError("Missing 'url' in configuration file.")
        if "token" not in config_values:
            raise ValueError("Missing 'token' in configuration file.")
        return config_values

    @staticmethod
    def write_log(output, e, local_vars):
        log_path = os.path.join(output, "log.txt")
        with open(log_path, "a") as log_file:
            log_file.write("An error occurred:\n")
            log_file.write(f"{str(e)}\n")
            if not isinstance(e, NoClusterEventsError):
                variables_message = "Local Variables at the point of exception:\n"
                items = local_vars.items()
                log_file.write(variables_message)
                not_include = ["self", "file", "e", "log_file"]
                variables_info = "   ".join(
                    [
                        f"{key}: {value}\n"
                        for key, value in items
                        if key not in not_include
                    ]
                )
                log_file.write(f"   {variables_info}")

    @staticmethod
    def write_file_request_(output, name_output, json_data):
        data = json.dumps(json_data)
        os.makedirs(output, exist_ok=True)
        file_path = os.path.join(output, f"{name_output}.json")
        with open(file_path, "w") as file:
            file.write(data)

    @staticmethod
    def filter_data(json_data, keys):
        if isinstance(json_data, dict):
            new_data = UtilFile.clean_dictionary(json_data, keys, {})
        elif isinstance(json_data, list):
            new_data = UtilFile.clean_list(json_data, keys, [])
        else:
            new_data = json_data

        return new_data

    @staticmethod
    def clean_list(source, keys, new_list):
        if len(source) == 0:
            return new_list
        new_data = []
        for value in source:
            if isinstance(value, str):
                new_data.append(UtilFile.clean_str(value))
            elif isinstance(value, dict):
                new_data.append(UtilFile.clean_dictionary(value, keys, {}))
            elif isinstance(value, list):
                new_data.append(UtilFile.clean_list(value, keys, []))
            else:
                new_data.append(value)

        return new_data

    @staticmethod
    def clean_dictionary(source, keys, new_dict):
        if len(source) == 0:
            return new_dict
        for key, value in source.items():
            if len(keys) > 0 and key in keys:
                continue
            elif isinstance(value, str):
                new_dict[key] = UtilFile.clean_str(value)
            elif isinstance(value, dict):
                new_dict[key] = UtilFile.clean_dictionary(value, keys, {})
            elif isinstance(value, list):
                new_dict[key] = UtilFile.clean_list(value, keys, [])

            else:
                new_dict[key] = value

        return new_dict

    @staticmethod
    def clean_str(txt):
        clean_string = UtilFile.replace_emails(txt)
        clean_string = UtilFile.replace_adb_url(clean_string)
        clean_string = UtilFile.remove_url_parameters(clean_string)
        clean_string = UtilFile.remove_jwt(clean_string)
        return clean_string

    @staticmethod
    def replace_emails(text):
        return UtilFile.email_pattern.sub("[EMAIL_REMOVED]", text)

    @staticmethod
    def remove_url_parameters(text):
        matches = UtilFile.url_parameters_pattern.match(text)
        if matches:
            return matches.group(1)
        else:
            return text

    @staticmethod
    def remove_url(text):
        return UtilFile.url_pattern.sub("[URL_REMOVED]", text)

    @staticmethod
    def replace_adb_url(text):
        return UtilFile.dbx_pattern.sub("https://adb-0000000000000.00", text)

    @staticmethod
    def remove_jwt(txt):
        return UtilFile.jwt_pattern.sub("[TOKEN_REMOVED]", txt)
