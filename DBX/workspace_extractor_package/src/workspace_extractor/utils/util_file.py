import json
import math
import os
import re
import shutil
import zipfile

from datetime import datetime

from workspace_extractor.exceptions.no_cluster_events_error import NoClusterEventsError


class UtilFile:
    email_pattern = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,7}")
    url_parameters_pattern = re.compile("^(?:https?:\\/\\/)?(?:[^@\\/\n]+@)?(?:www\\.)?([^:?\n]+)")
    url_pattern = re.compile(r"https?://\S+|www\.\S+")
    jwt_pattern = re.compile(r"[A-Za-z0-9_-]{4,}(?:\.[A-Za-z0-9_-]{4,}){2}")
    dbx_pattern = re.compile(r"https?://adb-\d{4,16}\.\d{0,2}|https?://dbc-.{4,12}-.{2,4}")

    @staticmethod
    def check_file_request_(output, name_output, json_data_check):
        os.makedirs(output, exist_ok=True)
        file_path = os.path.join(output, f"{name_output}.json")
        if os.path.exists(file_path):
            size = os.path.getsize(file_path)
            size_max_unit_value = "10 MB"
            array_units = ("B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB")
            size_bites_unit_value = UtilFile.convert_size_to_mb(size)
            size_correct = UtilFile.has_correct_size(size_bites_unit_value, size_max_unit_value, array_units)
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
                    UtilFile.write_file_request_(output, f"{name_output}_{i + 1:02d}", json_data_part)
                file_path = os.path.join(output, f"{name_output}.json")
                os.remove(file_path)
                return True
        else:
            return False

    @staticmethod
    def convert_size_to_mb(size_bytes):
        s = UtilFile.convert_size_to_mb_number(size_bytes)
        return "{} {}".format(s, "MB")

    @staticmethod
    def convert_size_to_mb_number(size_bytes):
        if size_bytes == 0:
            return 0

        p = math.pow(1024, 2)
        s = int(round(size_bytes / p, 0))
        return s

    @staticmethod
    def compress_folder_to_zip(
        source_folder_path: str,
        output_zip_file_no_extension: str,
        extension: str = "zip",
        split_size_mb: int = 195,
    ) -> str | None:
        """Compresses a specified folder into a zip archive, optionally splitting it.

        This function takes a source folder and compresses its contents into a
        zip file. The zip file is created using the ZIP_DEFLATED compression method.
        If the resulting zip file's size exceeds a specified threshold,
        the function will attempt to split it into multiple smaller parts using
        `UtilFile.split_zip_file`.

        Args:
            source_folder_path (str): The absolute or relative path to the folder
                that needs to be compressed.
            output_zip_file_no_extension (str): The base name for the output zip
                file, without the file extension. For example, if "archive" is
                provided, the output might be "archive.zip".
            extension (str, optional): The file extension for the output zip file.
                Defaults to "zip".
            split_size_mb (int, optional): The maximum size in megabytes for each
                part if the zip file needs to be split. If the total size of the
                created zip file exceeds this value, it will be split into
                multiple files (e.g., archive.z01, archive.z02, ...).
                Defaults to 195 MB.

        Raises:
            Exception: If the `source_folder_path` does not exist.
                Other exceptions encountered during the zipping or splitting
                process are caught, and their error messages are printed to the
                standard output; these exceptions are not re-raised by this function.

        """
        try:
            if not os.path.exists(source_folder_path):
                raise Exception(f"The source folder '{source_folder_path}' does not exist.")

            with zipfile.ZipFile(f"{output_zip_file_no_extension}.{extension}", "w", zipfile.ZIP_DEFLATED) as zipf:
                for root, _dirs, files in os.walk(source_folder_path):
                    for file in files:
                        file_path = os.path.join(root, file)
                        relative_path = os.path.relpath(file_path, source_folder_path)
                        zipf.write(file_path, relative_path)

            zip_file_size_bytes = os.path.getsize(f"{output_zip_file_no_extension}.{extension}")

            if zip_file_size_bytes > split_size_mb * 1024 * 1024:
                return UtilFile.split_zip_file(
                    zip_file_path=f"{output_zip_file_no_extension}.{extension}",
                    part_size_mb=split_size_mb,
                )
            return f"{output_zip_file_no_extension}.{extension}"

        except Exception as e:
            print(f"Error: {e}")
            return None

    @staticmethod
    def split_zip_file(zip_file_path: str, part_size_mb: int = 195) -> str | None:
        """Split a large .zip file into smaller parts and create a new archive.

        The original .zip file is read and split into chunks based on the
        `part_size_mb` parameter. Each chunk is saved as a separate file in the
        same directory as the original file, with a suffix like '.001', '.002',
        etc. (e.g., 'original_filename.zip.001').

        After all parts are created, these part files are then bundled into a
        new .zip archive. This new archive is named 'we_data_X_parts.zip',
        where X is the total number of parts created plus one. This new archive
        is saved in the same directory as the original .zip file.

        The individual part files (e.g., 'original_filename.zip.001') are
        deleted after being successfully added to the new 'we_data_X_parts.zip'
        archive. The original input .zip file remains untouched.

        Args:
            zip_file_path (str): The absolute or relative path to the .zip file
                that needs to be split.
            part_size_mb (int, optional): The maximum size for each part file
                in megabytes (MB). Defaults to 195 MB.

        Raises:
            ValueError: If the `zip_file_path` does not point to a file with
                a '.zip' extension.

        Side effects:
            - Creates temporary part files (e.g., 'filename.zip.001') in the
              same directory as the input `zip_file_path`. These are deleted
              once they are archived into the new zip file.
            - Creates a new zip file named 'we_data_X_parts.zip' (where X is
              the number of parts + 1) in the same directory as the input
              `zip_file_path`. This new zip file contains all the generated
              part files.

        """
        if not zip_file_path.lower().endswith(".zip"):
            raise ValueError(f"File '{zip_file_path}' is not a zip file. Splitting is only supported for .zip files.")

        try:
            part_size = part_size_mb * 1024 * 1024
            part_num = 1

            with open(zip_file_path, "rb") as f:
                while True:
                    chunk = f.read(part_size)
                    if not chunk:
                        break
                    part_filename = f"{zip_file_path}.{part_num:03d}"
                    with open(part_filename, "wb") as pf:
                        pf.write(chunk)
                    part_num += 1

            return UtilFile.rezip_zip_parts(zip_file_path, part_num)
        except Exception as _:
            return None

    @staticmethod
    def rezip_zip_parts(zip_file_path: str, part_num: int) -> str:
        r"""Consolidate multiple split zip parts into a single zip archive.

        This method searches for and combines split zip file parts (e.g., file.zip.001, file.zip.002)
        that were created by the split_zip_file method, then creates a new consolidated zip archive
        containing all the parts. The original part files are deleted after consolidation.

        Args:
            zip_file_path (str): Path to the original zip file (base name for the parts).
                Used to identify and match the corresponding split parts with pattern
                {basename}.001, {basename}.002, etc.
            part_num (int): Number of parts to expect/consolidate. Used in the output
                filename to indicate how many parts were merged.

        Returns:
            str: Path to the newly created consolidated zip file with format:
                "{base_path}/we_{filename_without_extension}_data_{part_num}_parts.zip"

        Side Effects:
            - Creates a new consolidated zip file in the same directory as the original
            - Deletes all original split part files after successful consolidation
            - Uses ZIP_DEFLATED compression for the consolidated archive

        File Processing:
            - Searches for parts matching pattern: {basename}.\\d{3} (e.g., file.zip.001)
            - Walks through the base directory to find all matching parts
            - Adds each part file to the new zip with its relative path preserved
            - Removes each part file immediately after adding to the new zip

        Error Handling:
            - If part files don't exist or can't be accessed, the method may fail
            - If the output directory is not writable, file creation will fail
            - No explicit error handling - exceptions will propagate to caller

        Use Cases:
            - Reassembling large data exports that were split for transfer
            - Consolidating workspace estimator output files after processing
            - Preparing split archives for final distribution or storage
            - Cleaning up temporary split files while preserving data

        File Naming Convention:
            Input parts: original_file.zip.001, original_file.zip.002, ...
            Output: we_original_file_data_{part_num}_parts.zip

        Example:
            # Consolidate 3 parts of a split zip file
            original_zip = "/path/to/workspace_data.zip"
            # Assumes parts exist: workspace_data.zip.001, workspace_data.zip.002, workspace_data.zip.003

            consolidated_path = UtilFile.rezip_zip_parts(original_zip, 3)
            print(f"Consolidated zip created: {consolidated_path}")
            # Output: "/path/to/we_workspace_data_data_3_parts.zip"

            # Original part files (.001, .002, .003) are automatically deleted

        Note:
            This method is typically used after split_zip_file has created multiple parts
            and you want to recombine them into a single archive. The part numbering
            parameter should match the actual number of parts created during splitting.

        """
        base_path, zip_file_basename, filename_without_extension, _ = UtilFile.get_path_separated(zip_file_path)

        merged_parts_zip_file_name = f"{base_path}/we_{filename_without_extension}_data_{part_num}_parts.zip"

        with zipfile.ZipFile(merged_parts_zip_file_name, "w", zipfile.ZIP_DEFLATED) as zipf:
            for root, _, files in os.walk(base_path):
                for file in files:
                    zip_file_basename = os.path.basename(zip_file_path)
                    part_file_pattern = f"^{re.escape(zip_file_basename)}\\.\\d{{3}}$"
                    if re.match(part_file_pattern, file):
                        part_file_path = os.path.join(root, file)
                        relative_path = os.path.relpath(part_file_path, base_path)
                        zipf.write(part_file_path, relative_path)
                        os.remove(part_file_path)

        return merged_parts_zip_file_name

    @staticmethod
    def get_path_separated(file_path: str) -> tuple[str, str, str, str]:
        """Separates a file path into its directory, basename, filename without extension, and extension.

        Args:
            file_path (str): The full path to the file.

        Returns:
            tuple[str, str, str, str]: A tuple containing:
                - base_path (str): The directory path of the file.
                - zip_file_basename (str): The name of the file including its extension (basename).
                - filename_without_extension (str): The name of the file without its extension.
                - file_extension (str): The file extension (e.g., '.txt', '.zip').

        """
        base_path = os.path.dirname(file_path)
        zip_file_basename = os.path.basename(file_path)
        filename_without_extension, file_extension = os.path.splitext(zip_file_basename)

        return base_path, zip_file_basename, filename_without_extension, file_extension

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
        workspace = re.sub(r"\s+", "_", workspace)
        workspace = re.sub(r"[^A-Za-z0-9]+", "", workspace)
        workspace = "Default_wkp" if workspace == "" else workspace
        now = datetime.now()
        date_part = now.strftime("%m%d")
        name = f"{workspace}_{date_part}"
        return name

    @staticmethod
    def has_correct_size(size_bytes_unit_value, size_max_unit_value, array_units):
        size_unit = size_bytes_unit_value.split(" ")[1]
        size_index = array_units.index(size_unit)
        size_max_unit = size_max_unit_value.split(" ")[1]
        size_max_index = array_units.index(size_max_unit)
        if size_index < size_max_index:
            return True
        elif size_index > size_max_index:
            return False
        else:
            size_value = int(size_bytes_unit_value.split(" ")[0])
            size_max_value = int(size_max_unit_value.split(" ")[0])
            if size_value > size_max_value:
                return False
            else:
                return True

    @staticmethod
    def read_config(config_path):
        with open(config_path) as file:
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
                variables_info = "   ".join([f"{key}: {value}\n" for key, value in items if key not in not_include])
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
