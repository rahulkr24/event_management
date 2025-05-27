import requests, json, os
from datetime import datetime
from zoneinfo import ZoneInfo
from common_constant import *
pytest_api_count = 0

timezone = os.getenv("TZ", "Asia/Kolkata")
server_pytrace_enabled = os.getenv("SERVER_PYTRACE_ENABLED", "NO") == "YES"

def get_now():
    value = datetime.now(ZoneInfo(timezone))
    return (value)

def call_restapi(request_type: str, endpoint: str = None, data: dict = None, params: dict = None, files: dict = None, token: str = None, content_type: str = 'application/json', timeout:int=10):
    global pytest_api_count
    ts = get_now()
    pytest_api_count += 1

    print("----- PYTEST API CALL : Count=[" + str(pytest_api_count) + "] -----")
    if server_pytrace_enabled:
        print("   request_type       = [" + str(request_type) + "]")
        print("   endpoint           = [" + str(endpoint) + "]")
        print("   params             = [" + str(params) + "]")
        print("   data               = [" + str(data) + "]")
        print("   files              = [" + str(files) + "]")
        print("   content_type       = [" + str(content_type) + "]")
        print("   token              = [" + str(token) + "]")
        
        if files is not None and token is not None:
            headers = {'Authorization': 'Bearer {}'.format(token)}
        elif token is not None:
            headers = {'Content-Type': content_type, 'Authorization': 'Bearer {}'.format(token)}
        else:
            headers = {'Content-Type': content_type}
        try:
            if request_type == s_get:
                response = requests.get(url=endpoint, params=params, data=data, files=files, headers=headers, timeout=timeout)
            elif request_type == s_post:
                response = requests.post(url=endpoint, params=params, data=data, files=files, headers=headers, timeout=timeout)
            elif request_type == s_patch:
                response = requests.patch(url=endpoint, params=params, data=data, files=files, headers=headers, timeout=timeout)
            elif request_type == s_delete:
                response = requests.delete(url=endpoint, params=params, headers=headers, timeout=timeout)
            else:
                print("ERROR ***: no valid request_type specified.")
        except (ConnectionError, Exception) as error:
            response = None
            pass
        result = {}
        if ((response is not None) and (response.status_code == HTTP_STATUS_CODE_200)):
            result = response.json()
            result[s_status] = result[s_status]
            result[s_message] = result[s_message]
            result[s_status_code] = HTTP_STATUS_CODE_200

        elif (response is not None) and (
                (response.status_code == HTTP_STATUS_CODE_404) or (response.status_code == HTTP_STATUS_CODE_401)):
            result = response.json()
            result[s_status] = s_failure
            result[s_status_code] = response.status_code
        else:
            result[s_status] = s_failure
            result[s_status_code] = HTTP_STATUS_CODE_500
            result[s_message] = 'pytest_call_rest_api likely was not invoked correctly or server not accessible.'
            result['statusbool'] = False
        result[s_timestamp] = get_now()

    te = get_now()
    test_processing_time = round((te - ts).total_seconds() * 1000, 1)
    result_str_trimmed = str(result)[0:500]
    print("   result             = [" + str(result_str_trimmed) + "]")
    print(" test_processing_time = [" + str(test_processing_time) + " ms]")
    return result


call_restapi(request_type="get")