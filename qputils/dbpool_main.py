import time, json, os, sys, subprocess
from time import perf_counter
import psycopg2
import psycopg2.extras
from psycopg2 import sql, pool, OperationalError, DatabaseError, IntegrityError
from common_constant import *

MIN_POOL_CONNS = 1
MAX_POOL_CONNS = 10

num_total_dbopens = 0
num_total_queries = 0
num_total_errors = 0
num_total_records = 0
MAX_QUERY_RECORDS = 1000
DEBUG = True


def db_wrapper_connect_and_exceptions(func):
    def wrapper(*args, **kwargs):
        if DEBUG:
            print(f"")
            print(f"function     : [ {func.__name__} ]")  # Log the function name
            print(f"arguments    : [ {kwargs} ]")  # Log the arguments passed
        try:
            dbh = kwargs.get("dbh", None)
            if dbh is None:
                dbh_new = db_handler_standard()  # Create a new database handler
                kwargs['dbh'] = dbh_new  # Pass 'dbh' as a keyword argument to the function
                if DEBUG: print("dbh_new created")
            else:
                dbh_new = dbh
            response = func(*args, **kwargs)  # Call the decorated function with 'dbh'
            if ((response != False) and (DEBUG == True)):  # Log the response if it's not False
                if DEBUG: print(f"response     : [ {response} ]")
                pass
            if (dbh_new is not None):  # if we created dbh_new, then we should cleanup by deleting it
                del dbh_new  # Clean up by deleting the dbh object
            return response  # Return the response from the decorated function
        except Exception as e:
            if DEBUG: print(f"{func.__name__}: Error - {e}")  # Log the error if an exception occurs
            return {s_status: s_failure, s_status_code: 500}  # Return None if an error occurs

    return wrapper


def log_psycopg2_exception(err):
    err_type, err_obj, traceback = sys.exc_info()

    line_num = traceback.tb_lineno
    print(f"\npsycopg2 ERROR:{err} on line number:{line_num}")
    print(f"psycopg2 traceback:{traceback} -- type:{err_type}")
    print(f"\nextensions.Diagnostics:{err.diag}")
    print(f"pgerror:{err.pgerror}")
    print(f"pgcode:{err.pgcode}\n")
    return


class dbpool_handler():

    def __init__(self, db_host: str = None, db_port: int = None, db_user: str = None, db_password: str = None, db_name: str = None):
        self.db_pg_pool = None
        self.table_columns = {}

        if (db_host is None): db_host = os.getenv('SERVER_DB_HOST', "")
        if (db_port is None): db_port = int(str(os.getenv('SERVER_DB_PORT', 5432)))
        if (db_user is None): db_user = os.getenv('SERVER_DB_USER', "")
        if (db_password is None): db_password = os.getenv('SERVER_DB_PASSWORD', "")
        if (db_name is None): db_name = os.getenv('SERVER_DB_NAME', "")

        if (db_host is None) or (db_name is None) or (db_user is None) or (db_password is None) or (db_port is None):
            print("ERROR: dbpool_handler - db params error so cannot connect to db at all")
            exit(-1)

        print(f"dbpool_handler: calling SimpleConnectionPool host={db_host}, port={db_port}, user={db_user}, password={db_password}, database={db_name}")
        self.db_pg_pool = psycopg2.pool.SimpleConnectionPool(MIN_POOL_CONNS, MAX_POOL_CONNS, host=db_host, port=db_port, user=db_user, password=db_password, database=db_name)
        if (self.db_pg_pool):
            # print("dbpool_handler: ***** added pg pool handle")
            pass
        else:
            global num_total_errors
            num_total_errors += 1
        return

    def __del__(self):
        if (self.db_pg_pool is not None):
            self.db_pg_pool.closeall()
            self.db_pg_pool = None
            # print("dbpool_handler: ***** closed pg pool handle")
        return

    def db_connect(self):
        global num_total_errors
        global num_total_dbopens

        db_conn = None
        while True:
            try:
                num_total_dbopens += 1
                db_conn = self.db_pg_pool.getconn()
                if (not db_conn):
                    print("FAILED - could not get a db_conn")  # was ic
                break
            except psycopg2.OperationalError as error:
                print( f"exception in db_connect: psycopg2.OperationalError=[{psycopg2.OperationalError}] Exception=[{error}]")
                num_total_errors += 1
            except (Exception, psycopg2.DatabaseError) as error:
                print( f"exception in db_connect: psycopg2.DatabaseError=[{psycopg2.DatabaseError}] Exception=[{error}]")
                num_total_errors += 1
        return db_conn

    def db_disconnect(self, db_conn, rollback: bool = False):
        if db_conn is not None:
            if rollback:  # some error has occurred
                db_conn.rollback()
            else:  # no errors, commit transaction
                db_conn.commit()
            self.db_pg_pool.putconn(db_conn)
            db_conn = None
        return None


class db_handler_admin:

    def __init__(self, db_host: str = None, db_port: int = None, db_name: str = None, db_user: str = None, db_password: str = None, db_admin_default_db_name: str = None, db_setup_pool: bool = True):
        self.db_conn = None
        self.stop_at = self.start_at = None
        self.call_rollback = False

        if (db_host is None): db_host = os.getenv('SERVER_DB_HOST', "")
        if (db_port is None): db_port = int(str(os.getenv('SERVER_DB_PORT', 5432)))
        if (db_user is None): db_user = os.getenv('SERVER_DB_USER', "")
        if (db_password is None): db_password = os.getenv('SERVER_DB_PASSWORD', "")
        if (db_name is None): db_name = os.getenv('SERVER_DB_NAME', "")
        if (db_admin_default_db_name is None): db_name = os.getenv('SERVER_DB_DEFAULT_DB_NAME', "")

        if (db_host is None) or (db_name is None) or (db_user is None) or (db_password is None) or (db_port is None) or (db_admin_default_db_name is None):
            print("ERROR: db params error - cannot connect to db at all")
            exit(-1)

        self.db_host = db_host
        self.db_port = db_port
        self.db_name = db_name
        self.db_user = db_user
        self.db_password = db_password
        self.db_admin_default_db_name = db_admin_default_db_name
        # self.start_at = time.time()
        if (db_setup_pool == True):
            self.db_pool = dbpool_handler(db_host=db_host, db_port=db_port, db_user=db_user,db_password=db_password,db_name=db_name)
        else:
            self.db_pool = None
        return

    def __del__(self):
        if (self.db_conn is not None):
            self.db_conn.close()
            self.db_conn = None
        if (self.db_pool is not None):
            del self.db_pool
        return

    def db_superadmin_execute_sql(self, sql_str: str) -> bool:
        try:
            sql_str = sql.SQL(sql_str)
            admin_con = psycopg2.connect(database=self.db_admin_default_db_name, user=self.db_user, password=self.db_password, host=self.db_host, port=self.db_port, connect_timeout=60)
            admin_con.autocommit = True
            db_cur = admin_con.cursor()
            response = db_cur.execute(sql_str)
        except Exception as err:
            log_psycopg2_exception(err)
            return False
        finally:
            if (db_cur is not None): db_cur.close(); db_cur = None
            if (admin_con is not None): admin_con.close(); admin_con = None
        return True

    def db_superadmin_create_new_db(self, db_new_name: str = None) -> bool:
        try:
            if (db_new_name is None):
                db_new_name = self.db_name
            sql_str = f"CREATE DATABASE {db_new_name};"
            if (not self.db_superadmin_execute_sql(sql_str)): return False
            sql_str = f"ALTER DATABASE {db_new_name} SET timezone TO 'Asia/Calcutta';"
            if (not self.db_superadmin_execute_sql(sql_str)): return False
        except Exception as err:
            log_psycopg2_exception(err)
            return False
        return True



    def db_superadmin_delete_existing_db(self, db_name: str = None) -> bool:
        try:
            if (db_name is None):
                db_name = self.db_name
            sql_str = f"SELECT pg_terminate_backend(pg_stat_activity.pid) FROM pg_stat_activity WHERE pg_stat_activity.datname = '{db_name}';"
            if (not self.db_superadmin_execute_sql(sql_str)): return False
            sql_str = f"DROP DATABASE IF EXISTS {db_name};"
            if (not self.db_superadmin_execute_sql(sql_str)): return False
        except Exception as err:
            log_psycopg2_exception(err)
            return False
        return True



    def db_connect(self) -> bool:
        while True:
            try:
                self.start_at = time.time()
                self.call_rollback = False
                print(
                    f"\tdb_connect: host=[{self.db_host}] port=[{self.db_port}] dbname=[{self.db_name}] user=[{self.db_user}] password=[REDACTED]")
                self.db_conn = psycopg2.connect(host=self.db_host, port=self.db_port, dbname=self.db_name,
                                                user=self.db_user, password=self.db_password, connect_timeout=60)
                # self.db_conn.autocommit = False
            except (Exception, psycopg2.DatabaseError) as error:
                print(f"\texception in db_connect psycopg2.DatabaseError error=[{error}]")
                return False
            except psycopg2.OperationalError as error:
                print(f"\texception in db_connect psycopg2.OperationalError error=[{error}]")
                return False
            break
        return True



    def db_disconnect(self):
        """
            This function commits/rollbacks the transaction and closes DB connection
        """
        self.stop_at = time.time()
        process_time = (self.stop_at - self.start_at) * 1000
        if self.call_rollback:  # some error has occurred
            if (self.db_conn is not None):
                # print("***** TX ***** - db_disconnect - rollback")
                self.db_conn.rollback()
        else:  # no errors, commit transaction
            if (self.db_conn != None):
                # print("***** TX ***** - db_disconnect - commit")
                self.db_conn.commit()
        if (self.db_conn is not None): self.db_conn.close()
        if (self.db_conn is None): return False
        self.db_conn.close()
        self.db_conn = None
        return True

    def execute_sql(self, sql_str: str):
        bReturn = True
        if (self.db_conn is None):
            return (False)
        db_cur = None
        try:
            db_cur = self.db_conn.cursor(cursor_factory=psycopg2.extras.NamedTupleCursor)
            db_cur.execute(sql_str)
            self.db_conn.commit()  # commit before closing cursor or after, both ok
        except Exception as err:
            log_psycopg2_exception(err)
            self.db_conn.rollback()
            bReturn = False
        finally:
            if db_cur is not None: db_cur.close()
        return (bReturn)

    def add_standard_elements_to_db(self):
        self.add_db_procedure_on_create(field_name="created_at")
        self.add_db_procedure_on_update(field_name="updated_at")
        return

    def create_type_enum(self, type_name: str, enum_list: list) -> str:
        enum_tuple_str = ', '.join([f"'{value}'" for value in enum_list])
        sql_str = f"CREATE TYPE {type_name} AS ENUM ({enum_tuple_str});"
        self.execute_sql(sql_str)
        # print(f"create_type_enum: sql_str=[{sql_str}]")
        return (type_name)

    def create_table(self, table_name: str, add_standard_triggers: bool = True, add_standard_fields: bool = True):
        sql_str = sql.SQL(f"CREATE TABLE IF NOT EXISTS {table_name}()")
        self.execute_sql(sql_str)
        if (add_standard_fields is True):
            self.add_table_trigger(table_name=table_name, trigger_name=TRIGGER_NO_UPDATE_ON_CREATE, event=BEFORE_UPDATE,
                                   procedure=PROCEDURE_RETAIN_CREATED_DATETIME)
            self.add_table_trigger(table_name=table_name, trigger_name=TRIGGER_SET_TIMESTAMP, event=BEFORE_UPDATE,
                                   procedure=PROCEDURE_SET_TIMESTAMP)
        if (add_standard_fields is True):
            self.add_field(table_name=table_name, field_name="id", field_type=FIELD_TYPE_INTEGER_IDENTITY,
                           constraint_name=table_name + "id_constraint", constraint_str=CONSTRAINT_UNIQUE)
            self.add_field(table_name=table_name, field_name="status", field_type=FIELD_TYPE_STRING)
            self.add_field(table_name=table_name, field_name="created_at", field_type=FIELD_TYPE_DATETIME)
            self.add_field(table_name=table_name, field_name="updated_at", field_type=FIELD_TYPE_DATETIME)
            self.add_field(table_name=table_name, field_name="details", field_type=FIELD_TYPE_JSONB)
        return

    def add_constraint_to_field(self, table_name: str, field_name: str, constraint_name: str = None,
                                constraint_str: str = None):
        if (constraint_str is not None):
            if (constraint_name is None):
                constraint_name = str(field_name + str(constraint_str))
            sql_text = sql.SQL(
                f"ALTER TABLE {table_name} ADD CONSTRAINT {constraint_name} {constraint_str} ({field_name})")
            self.execute_sql(sql_text)
        return

    def add_field(self, table_name: str, field_name: str, field_type: str, constraint_name: str = None,
                  constraint_str: str = None):
        sql_str = sql.SQL(f"ALTER TABLE {table_name} ADD {field_name} {field_type}")
        self.execute_sql(sql_str)
        self.add_constraint_to_field(table_name=table_name, field_name=field_name, constraint_name=constraint_name,
                                     constraint_str=constraint_str)
        return

    def add_db_procedure_on_create(self, field_name: str):
        sql_str = "CREATE OR REPLACE FUNCTION public." + str(
            PROCEDURE_RETAIN_CREATED_DATETIME) + "() RETURNS TRIGGER AS $$ BEGIN NEW." + str(
            field_name) + ":= OLD.created_at; RETURN NEW; END; $$ LANGUAGE plpgsql"
        self.execute_sql(sql_str)
        return

    def add_db_procedure_on_update(self, field_name: str):
        sql_str = "CREATE OR REPLACE FUNCTION public." + str(
            PROCEDURE_SET_TIMESTAMP) + "() RETURNS TRIGGER AS $$ BEGIN NEW." + str(
            field_name) + "= NOW(); RETURN NEW; END; $$ LANGUAGE plpgsql"
        self.execute_sql(sql_str)
        return

    def add_table_trigger(self, table_name: str, trigger_name: str, event: str, procedure: str):
        sql_str = "CREATE TRIGGER " + str(trigger_name) + " " + str(event) + " ON public." + str(
            table_name) + " FOR EACH ROW EXECUTE PROCEDURE " + str(procedure) + "('" + str(table_name) + "')"
        self.execute_sql(sql_str)
        return


    def grant_permission_all_users(self):
        db_cur = self.db_conn.cursor(cursor_factory=psycopg2.extras.NamedTupleCursor)

        # admin_user
        sql_str = "GRANT ALL PRIVILEGES ON DATABASE " + str(self.db_name) + " TO admin_user;"
        # --- ADDED RECENTLY
        sql_str = sql_str + "GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO admin_user;"
        db_cur.execute(sql_str)

        # read_write_user
        sql_str = ""
        sql_str = sql_str + "GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO read_write;"
        db_cur.execute(sql_str)

        # read_only
        sql_str = ""
        sql_str = sql_str + "GRANT CONNECT ON DATABASE " + str(self.db_name) + " TO read_only;"
        sql_str = sql_str + "GRANT SELECT ON ALL SEQUENCES IN SCHEMA public TO read_only;"
        sql_str = sql_str + "GRANT SELECT ON ALL TABLES IN SCHEMA public TO read_only;"
        db_cur.execute(sql_str)
        db_cur.close()
        self.db_conn.commit()
        return

    def add_constrain(self, table_name: str):
        sql_text = "ALTER TABLE IF EXISTS public." + str(table_name) + " ADD CONSTRAINT " + str(
            table_name) + "id_constraint UNIQUE (id);"
        self.execute_sql(sql_text)
        return

    def add_sequence(self, table_name: str):
        sql_text = "CREATE SEQUENCE IF NOT EXISTS public." + str(
            table_name) + "_id_seq INCREMENT 1 START 1 MINVALUE 1 MAXVALUE 2147483647 CACHE 1;"
        self.execute_sql(sql_text)
        return

    def add_foreign_constraint_to_field(self, table_name: str, field_name: str, ref_table: str, ref_field_name: str,
                                        constraint_name: str = None):
        if (constraint_name is None):
            constraint_name = field_name + "_in_" + table_name + "_must_exist_as_" + ref_field_name + "_in_" + ref_table
        sql_text = "ALTER TABLE ONLY public." + str(table_name) + " ADD CONSTRAINT " + str(
            constraint_name) + " FOREIGN KEY (" + str(field_name) + ") REFERENCES public." + str(ref_table) + "(" + str(
            ref_field_name) + ") NOT VALID;"
        self.execute_sql(sql_text)
        return

class db_handler_standard:
    def __init__(self):
        try:
            self.db_pool = dbpool_handler()

            self.db_conn = None
            self.db_cur = None
            self.call_rollback = False

            self.db_conn = self.db_pool.db_pg_pool.getconn()
            if ((self.db_conn is None) or (self.db_conn.closed == True)):
                print("db_open_transaction: **** ERROR **** in db_conn")
                self.db_conn = None
                return

            self.db_cur = self.db_conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            if (self.db_cur is None):
                print("db_open_transaction: **** ERROR **** in db_conn.cursor")
                return

        except (Exception) as error:
            print(f"exception in db_open_transaction error=[{error}]")
            self.db_conn = None
            self.db_cur = None
            self.db_pool = None
            pass
        finally:
            pass
        return

    def __del__(self):
        self.close()
        return

    def enable_rollback(self) -> bool:
        self.call_rollback = True
        return True

    def close(self) -> bool:
        breturn = True
        try:
            if (self.db_cur is not None):
                self.db_cur.close()
                self.db_cur = None
            if (self.db_conn is not None):
                if (not self.call_rollback):
                    self.db_conn.commit()
                else:
                    self.db_conn.rollback()
            pass
        except (Exception) as error:
            self.call_rollback = True
            print(f"exception in close error=[{error}]")
            breturn = False
            if self.call_rollback: self.db_conn.rollback()
            pass
        finally:
            if (self.db_pool is not None):
                self.db_pool.db_pg_pool.putconn(self.db_conn)
            self.db_conn = None
            if (self.db_pool is not None):
                del self.db_pool
                self.db_pool = None
            pass
        return breturn

    def execute_sql(self, sql_str: psycopg2.sql.SQL = None, data: tuple = None, num_records: int = None) -> dict:
        if (self.db_cur is None):
            response = {s_status: s_failure}
            return response

        records = []
        response = {s_status: s_success}
        # if the user is asking for no records, then we can respond with an empty query without even querying the db
        if (num_records == 0):
            response = {s_status: s_success, s_count: 0, s_records: []}
            return response

        try:
            if (sql_str is not None):
                self.db_cur.execute(sql_str, data)
            if ((self.db_cur.rowcount > 0) and (self.db_cur.description is not None)):
                if ((self.db_cur.rowcount > 0) and (
                        self.db_cur.description is not None)):  # rowcount will be 1 or greater if delete was successful
                    records = self.db_cur.fetchall()
                else:
                    # any other non zero positive number will be passed to the fetch query
                    records = self.db_cur.fetchmany(num_records)
            if (self.db_cur.rowcount == 0):  # No matching record found
                response = {s_status: s_success, s_count: 0, s_records: []}
                return response
            if (self.db_cur.rowcount < 0):  # No matching record found
                response = {s_status: s_failure}
                return response
            response.update({s_count: len(records)})
            response.update({s_rowcount: self.db_cur.rowcount})
            response.update({s_records: records})
            response.update({s_status: s_success})
            pass

        except psycopg2.Error as error:
            print(f"Error occurred: {error}")
            response = {s_status: s_failure}
            pass

        except (OperationalError, DatabaseError) as error:
            # Catch specific database-related errors
            print(f"Error occurred: {error}")
            response = {s_status: s_failure}
            pass

        except (Exception) as error:
            print(f"exception in db_execute_sql error=[{error}]")
            response = {s_status: s_failure}
            pass

        finally:
            pass
        final_response = self.unpack_json(response)
        return final_response

    def insert_record(self, table_name: str, record_dict: dict, fields_list: list = None):
        if ((record_dict is None) or (len(record_dict) <= 0)):
            response = {'success': False, 'total_count': 0, 'count': 0, 'records': []}
            return response
        packed_record_dict = self.pack_details_in_record_dict(record_dict=record_dict, table_name=table_name)
        fields = sql.SQL(', ').join(sql.Identifier(k) for k in list(packed_record_dict))
        num_vals = sql.SQL(', ').join(sql.Placeholder() * len(packed_record_dict))

        if ('details' in packed_record_dict):
            packed_record_dict['details'] = json.dumps(packed_record_dict['details'])
        if (fields_list is not None):  # get all records but filter out fields
            sql_str = sql.SQL("INSERT INTO {table}({cols}) VALUES ({vals}) RETURNING {f}").format(
                table=sql.Identifier(table_name), cols=fields, vals=num_vals,
                f=sql.SQL(', ').join(sql.Composed([sql.Identifier(k)]) for k in fields_list))
        else:
            sql_str = sql.SQL("INSERT INTO {table}({cols}) VALUES ({vals}) RETURNING *").format(
                table=sql.Identifier(table_name), cols=fields, vals=num_vals)
        response = self.execute_sql(sql_str=sql_str, data=tuple(packed_record_dict.values()), num_records=None)
        response = self.pack_json(response)
        if ((response[s_status] == s_success) and (response[s_count] != 0)):
            return response
        return {s_status: s_failure, s_status_code: HTTP_STATUS_CODE_422}



    def delete_record(self, table_name: str, record_id: int) -> dict:
        msg = f"Delete request failed. Check table_name=[{table_name}] and if record_id=[{record_id}] exists."
        response = {s_status: s_failure, s_status_code: HTTP_STATUS_CODE_404, s_message: msg}
        if ((table_name is None) or (record_id is None)):
            return response

        self.call_rollback = False
        try:
            if ((self.db_conn is None) or (self.db_conn.closed is True)):
                print("**** ERROR **** in db_conn")
                return response
            sql_str = sql.SQL("DELETE FROM {table} WHERE id = %s").format(table=sql.Identifier(table_name))
            data = (str(record_id),)
            self.db_cur.execute(query=sql_str, vars=data)
            if (self.db_cur.rowcount > 0):  # Get the number of rows affected by the DELETE
                msg = f"Delete successful. # records deleted=[{self.db_cur.rowcount}]"
                response = {s_status: s_success, s_count: self.db_cur.rowcount, s_message: msg}
            pass

        except psycopg2.Error as error:
            print(f"log_exception in db_execute_sql error=[{error}]")
            response = {s_status: s_failure}
            pass
        except (Exception) as error:
            print(f"log_exception in db_execute_sql error=[{error}]")
            self.call_rollback = True
            pass
        finally:
            pass
        return response

    def update_record(self, table_name: str, record_id: int, record_dict: dict, return_record: bool = True) -> dict:
        if ((table_name is None) or (record_id is None)):
            response = {s_status: s_failure, s_status_code: HTTP_STATUS_CODE_404,
                        s_message: "Check if tablename and fieldname are valid or if table is empty."}
            return response

        response = {s_status: s_success}
        packed_record_dict = self.pack_details_in_record_dict(record_dict=record_dict, table_name=table_name)

        try:
            if ((response[s_status] == s_success) and ('details' in packed_record_dict)):
                sql_str = sql.SQL("SELECT details FROM {table} WHERE id = %s").format(table=sql.Identifier(table_name))
                self.db_cur.execute(sql_str, (record_id,))
                if (self.db_cur.rowcount == 0):  # rowcount will be 1 if update was successful
                    response = {s_status: s_failure, s_message: "Update of record to db failed."}

                if (response[s_status] == s_success):
                    records = self.db_cur.fetchall()
                    existing_details = records[0]['details']  # retrieve existing details json field
                    if existing_details is None:
                        existing_details = {}
                    status, message = self.modify_json_fields(record_dict=packed_record_dict,
                                                              existing_details_dict=existing_details)
                    if status == s_failure:
                        response = {s_status: s_failure}

                if (response[s_status] == s_success):
                    # update 'details' fields separately since it requires to be json dumped format
                    sql_str = sql.SQL("UPDATE {table} SET details=%s WHERE id = %s  RETURNING *").format(
                        table=sql.Identifier(table_name))
                    self.db_cur.execute(sql_str, (json.dumps(existing_details), record_id))
                    if self.db_cur.rowcount == 0:  # rowcount will be 1 if update was successful
                        response = {s_status: s_failure}

                if (response[s_status] == s_success):
                    packed_record_dict.pop(s_details)  # remove the details key from dict since it has been updated

            if (response[s_status] == s_success) and (
                    len(packed_record_dict) > 0):  # check if there are any other fields that need to be updated
                sql_str = sql.SQL("UPDATE {table} SET {new_vals} WHERE id = {id}  RETURNING *").format(
                    table=sql.Identifier(table_name),
                    new_vals=sql.SQL(', ').join(
                        sql.Composed([sql.Identifier(k), sql.SQL(" = "), sql.Placeholder(k)]) for k in
                        packed_record_dict.keys()),
                    id=sql.Placeholder(s_id))

                packed_record_dict.update({s_id: record_id})

                res = self.db_cur.execute(sql_str, packed_record_dict)

                if (self.db_cur.rowcount == 0):  # rowcount will be 1 if update was successful
                    response = {s_status: s_failure, s_status_code: HTTP_STATUS_CODE_404,
                                s_message: "Check if record_id are valid or if table is empty."}

            if (response[s_status] == s_success) and return_record:
                updated_row = self.db_cur.fetchall()
                response.update({s_count: self.db_cur.rowcount})
                response.update({s_records: updated_row})
            pass

        except (psycopg2.ProgrammingError, psycopg2.DataError, psycopg2.IntegrityError) as error:
            print(f"exception in db_update_record error=[{error}]")
            response = {s_status: s_failure,
                        s_message: "Check if tablename and fieldname are valid or if table is empty."}
            pass
        except psycopg2.Error as error:
            print(f"exception in db_update_record error=[{error}]")
            response = {s_status: s_failure}
            pass
        finally:
            if (self.db_conn is not None):
                if (response[s_status] == s_success):
                    self.call_rollback = False
                else:
                    self.call_rollback = True
            pass
        packed_response = self.pack_json(response)
        return packed_response

    def pack_json(self, response: dict):
        if s_records not in response: return response
        new_records = []
        for record in response[s_records]:
            if (record.get(s_details, None) is None):
                new_records.append(record)
            else:
                new_record = {}
                for key, val in record[s_details].items():
                    new_record.update({key: val})
                del record['details']
                new_record.update(record)
                new_records.append(new_record)
        response[s_records] = new_records
        return response

    def unpack_json(self, response: dict):
        if s_records not in response: return response
        new_records = []
        for record in response[s_records]:
            if ('details' not in record):
                new_records.append(record)
            else:
                # if 'details' field is present in record
                new_record = {}
                if (record.get('details') is not None):
                    for key, val in record['details'].items():
                        new_record.update({key: val})
                del record['details']
                new_record.update(record)
                new_records.append(new_record)
        response[s_records] = new_records
        return response

    def modify_json_fields(self, record_dict: dict, existing_details_dict: dict) -> tuple:
        """
            record_dict can be of type
                {"some_key": "val", "details": {"remove": "some_field_name", "existing_key": "new_value", "new_key": "some_value" }} OR
                {"some_key": "val", "details": {"remove": ["field1", "field2"], "existing_key": "new_value", "new_key": "some_value" }}
            This function will remove all fields mentioned as values in the "remove" key and update remaining keys in the existing details json field.
        """
        if ('details' in record_dict):
            if (record_dict.get('details') != {}):
                existing_details_dict.update(record_dict['details'])
            status = s_success
            message = "No error"
        else:
            status = s_failure
            message = "Key [details] missing"
        return status, message

    def populate_table_column_lists(self, table_name: str):
        # we have previously iterated thru this table for it's column names so reuse that now
        if ((table_name is not None) and (table_name in self.db_pool.table_columns)):
            return self.db_pool.table_columns[table_name]

        # we have not yet iterated thru this table for it's column names
        sql_str = sql.SQL(
            "SELECT column_name FROM information_schema.columns WHERE table_schema = 'public' AND table_name = %s;")
        response = self.execute_sql(sql_str=sql_str, data=(str(table_name),))
        col_names = []
        if (s_records in response):
            for col in response[s_records]:
                for key, col_name in col.items():
                    col_names.append(col_name)
        self.db_pool.table_columns[table_name] = col_names
        return self.db_pool.table_columns[table_name]

    def pack_details_in_record_dict(self, record_dict: dict, table_name: str) -> dict:
        table_columns = self.populate_table_column_lists(table_name=table_name)
        details_dict = {}
        new_record_dict = {}
        for key, val in record_dict.items():
            if ((key != s_details) and (key not in table_columns)):
                details_dict.update({key: val})
                continue
            if type(val) is dict:
                val = json.dumps(val)
            new_record_dict.update({key: val})
        if (len(details_dict) != 0):
            new_record_dict.update({s_details: details_dict})
        return (new_record_dict)






