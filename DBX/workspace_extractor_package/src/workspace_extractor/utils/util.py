from urllib.parse import urlencode

from workspace_extractor.utils.util_file import UtilFile


class Util(UtilFile):
    @staticmethod
    def get_url(input_url):
        return input_url[:-1] if input_url.endswith("/") else input_url

    @staticmethod
    def get_full_json(
        array_field,
        full_json,
        name_output,
        path,
        response,
        full_response,
        cloud_provider="",
    ):
        json_data = response.json()
        field_out = array_field if array_field else name_output
        if field_out in json_data:
            if path or cloud_provider == "azure":
                data = json_data[field_out]
            else:
                data = json_data[field_out].items()
            full_json.extend(data)
        if full_response:
            if isinstance(json_data, list):
                full_json.extend(json_data)
            else:
                full_json.append(json_data)

        return json_data

    @staticmethod
    def get_has_more(json_data, offset):
        return (
            json_data.get("has_more")
            or json_data.get("has_next_page")
            or offset
            or json_data.get("NextPageLink")
        )

    @staticmethod
    def get_page_token(paging):
        return paging.get("next_page_token")

    @staticmethod
    def get_params(counter, new_params, next_page_token, offset, use_paging, skip):
        if use_paging and counter > 0:
            if next_page_token:
                new_params["page_token"] = next_page_token
            elif skip:
                new_params["$skip"] = skip
            elif offset and offset >= 0:
                new_params["offset"] = offset
        return new_params

    @staticmethod
    def get_paging(json_data):
        return {
            "has_more": json_data.get("has_more"),
            "next_page_token": json_data.get("next_page_token"),
            "has_next_page": json_data.get("has_next_page"),
            "has_skip": Util.get_skip(json_data),
        }

    @staticmethod
    def get_offset(json_data, offset):
        next_data = json_data.get("next_page")
        offset = json_data.get("next_page")["offset"] if next_data else None
        return offset

    @staticmethod
    def get_query(params):
        query = ""
        return urlencode(params) if params else query

    @staticmethod
    def get_skip(json_data):
        next_page_link = json_data.get("NextPageLink", None)
        if next_page_link:
            skip = json_data.get("NextPageLink").split("$skip=")[1]
            return skip
        else:
            return ""

    @staticmethod
    def get_clean_name(run_name):
        return run_name[:-37] if run_name.startswith("ADF_") else run_name
