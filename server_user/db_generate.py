import os, sys
from dbpool_main import *

db_host = os.getenv('SERVER_DB_HOST', "")
db_port = int(str(os.getenv('SERVER_DB_PORT', None)))
db_name = os.getenv('SERVER_DB_NAME', "")
db_user = os.getenv('SERVER_DB_USER', "")
db_password = os.getenv('SERVER_DB_PASSWORD', "")
db_admin_default_db_name = os.getenv('SERVER_DB_DEFAULT_DB_NAME', "")


def delete_existing_and_create_new_db():
    print("\n----  db_delete_existing_and_create_new  ------")
    dbh = db_handler_admin(db_host=db_host, db_port=db_port, db_name=db_name, db_user=db_user, db_password=db_password, db_admin_default_db_name=db_admin_default_db_name,db_setup_pool=False)

    result = dbh.db_superadmin_delete_existing_db()
    print(f"db_superadmin_delete_existing_db = [{result}]")
    result = dbh.db_superadmin_create_new_db()
    print(f"db_superadmin_create_new_db = [{result}]")
    del dbh
    return


def create_schema_in_db():

    if (db_host is None) or (db_name is None) or (db_user is None) or (db_password is None) or (db_port is None):
        print("ERROR: ns_create_new_schema_in_db - db params error so cannot connect to db at all")
        exit(-1)

    dbh = db_handler_admin(db_host=db_host, db_port=db_port, db_name=db_name, db_user=db_user,db_password=db_password, db_admin_default_db_name=db_admin_default_db_name)
    if (not (result := dbh.db_connect())):
        print(f"db_connect=[{result}]")
        exit(-1)

    dbh.add_standard_elements_to_db()
    enum_list = ["admin", "read_write", "read"]
    FIELD_TYPE_ENUM_USER_TYPE = dbh.create_type_enum(type_name="USER_TYPE", enum_list=enum_list) + f" NOT NULL"


    TABLE_NAME = "users"
    dbh.create_table(TABLE_NAME)
    dbh.add_field(table_name=TABLE_NAME, field_name="user_name", field_type=FIELD_TYPE_STRING)
    dbh.add_field(table_name=TABLE_NAME, field_name="user_phone", field_type=FIELD_TYPE_STRING_NOT_NULL, constraint_str=CONSTRAINT_UNIQUE)
    dbh.add_field(table_name=TABLE_NAME, field_name="user_email", field_type=FIELD_TYPE_STRING)
    dbh.add_field(table_name=TABLE_NAME, field_name="user_password", field_type=FIELD_TYPE_STRING)
    dbh.add_field(table_name=TABLE_NAME, field_name="user_password_expiry", field_type=FIELD_TYPE_STRING)
    dbh.add_field(table_name=TABLE_NAME, field_name="user_permission", field_type=FIELD_TYPE_ENUM_USER_TYPE)
    dbh.add_field(table_name=TABLE_NAME, field_name="user_otp", field_type=FIELD_TYPE_STRING)

    dbh.grant_permission_all_users()
    dbh.db_disconnect()

    exit(0)

if __name__ == "__main__":
    print("\nStarted ...")

    delete_existing_and_create_new_db()
    create_schema_in_db()

    print("\nFinished ...")
    exit(0)