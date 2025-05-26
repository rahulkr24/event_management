from aap_main_helper import *


app = FastAPI()

@app.post("/users", tags=["Users"])
def create_user(user_name:str, user_phone:str, user_email:str, password:str, user_permission:UserRole):
    if not user_phone.isdigit() or len(user_phone) != 10:
        return {s_status: s_failure, s_status_code: HTTP_STATUS_CODE_401, s_message: "Invalid phone number. It must be 10 digits."}

    if not is_valid_email(user_email):
        return {s_status: s_failure, s_status_code: HTTP_STATUS_CODE_401, s_message: "Invalid email format."}

    return add_user(user_name=user_name, user_phone=user_phone, user_email=user_email, password=password,  user_role=user_permission)
