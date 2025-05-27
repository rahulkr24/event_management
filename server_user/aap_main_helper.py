import re

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import psycopg2.sql
from typing import Optional
import bcrypt,pytz
from datetime import datetime, timedelta
import random,hashlib
from dbpool_main import *

TABLE_NAME_USERS = "users"

def hash_password(plain_password: str) -> str:
    salt = bcrypt.gensalt()
    return bcrypt.hashpw(plain_password.encode('utf-8'), salt).decode('utf-8')

def generate_otp() -> str:
    return str(random.randint(100000, 999999))

def is_valid_email(email):
    pattern = r'^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$'
    match = re.match(pattern, email)
    return bool(match)



def get_expiry_date_as_str() -> str:
    tz = pytz.timezone("Asia/Kolkata")
    expiry_date = datetime.now(tz) + timedelta(days=30)
    return expiry_date.strftime('%Y-%m-%d %H:%M:%S')

@db_wrapper_connect_and_exceptions
def add_user(dbh: db_handler_standard, user_name: str, user_phone: str, user_email: str, password: str,  user_role: UserRole = None):
    h_password = hash_password(plain_password=password)
    expiry_date = get_expiry_date_as_str()
    if user_role:
        user_role = user_role.value

    data = { "user_name": user_name, "user_password": h_password, "user_permission": user_role, "user_phone": user_phone, "user_email": user_email,
             "user_password_expiry": expiry_date, "user_otp": generate_otp(), "status": "active"}
    response = dbh.insert_record(table_name=TABLE_NAME_USERS, record_dict=data)
    if ((response[s_status] == s_failure) or (response[s_count] == 0)):
        return { s_status: s_failure, s_status_code: HTTP_STATUS_CODE_422, s_message: "failed to create user" }
    return response

def update_user(dbh: db_handler_standard, record_dict:dict, record_id:dict)-> dict:
    return dbh.update_record(table_name="users", record_dict=record_dict, record_id=record_id)


def delete_user(dbh: db_handler_standard, record_id:dict)-> dict:
    return dbh.delete_record(table_name="users", record_id=record_id)