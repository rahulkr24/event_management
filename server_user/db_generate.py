import os, sys
from dbpool_main import *

db_host = os.getenv('SERVER_DB_HOST', "")
db_port = int(str(os.getenv('SERVER_DB_PORT', None)))
db_name = os.getenv('SERVER_DB_NAME', "")
db_user = os.getenv('SERVER_DB_USER', "")
db_password = os.getenv('SERVER_DB_PASSWORD', "")
db_admin_default_db_name = os.getenv('SERVER_DB_DEFAULT_DB_NAME', "")

print("================== DB Handler ======================")
print(f"db_host					: [ {db_host} ]")
print(f"db_user					: [ {db_user} ]")
print(f"db_name					: [ {db_name} ]")
print(f"db_port					: [ {db_port} ]")
print(f"db_password				: [ ** Reacted ** ]")
print(f"db_admin_default_db_name		: [ {db_admin_default_db_name} ]\n")
input("Press Enter to Continue...!\n")

if (db_host is None) or (db_name is None) or (db_user is None) or (db_password is None) or (db_port is None) or (db_admin_default_db_name is None):
    print("ERROR: db params error")
    exit(-1)

dbh = db_handler(db_host=db_host, db_port=db_port, db_name=db_name, db_user=db_user, db_password=db_password, db_admin_default_db_name=db_admin_default_db_name)

try:
    dbh.db_connect()
    print("✅ Database connection successful.")
except Exception as e:
    print(f"❌ Database connection failed: {e}")
    sys.exit(1)


# Create the DB first before connecting to it
dbh.db_superadmin_delete_existing_db(db_name=db_name)
dbh.db_superadmin_create_new_db(db_new_name=db_name)

# Now connect to the newly created database
dbh.db_connect()
print("✅ Database connection successful.")


enum_list = ["admin", "read_write", "read"]
FIELD_TYPE_ENUM_USER_TYPE = dbh.create_type_enum(type_name="USER_TYPE", enum_list=enum_list) + f" NOT NULL"

dbh.add_standard_elements_to_db()

TABLE_NAME = "users"
dbh.create_table(TABLE_NAME)
dbh.add_field(table_name=TABLE_NAME, field_name="user_name", field_type=FIELD_TYPE_STRING)
dbh.add_field(table_name=TABLE_NAME, field_name="user_phone", field_type=FIELD_TYPE_STRING)
dbh.add_field(table_name=TABLE_NAME, field_name="user_email", field_type=FIELD_TYPE_STRING)
dbh.add_field(table_name=TABLE_NAME, field_name="user_password", field_type=FIELD_TYPE_STRING)
dbh.add_field(table_name=TABLE_NAME, field_name="user_password_expiry", field_type=FIELD_TYPE_STRING)
dbh.add_field(table_name=TABLE_NAME, field_name="user_permission", field_type=FIELD_TYPE_ENUM_USER_TYPE)
dbh.add_field(table_name=TABLE_NAME, field_name="user_otp", field_type=FIELD_TYPE_STRING)

dbh.grant_permission_all_users()

dbh.db_disconnect()

exit(0)
