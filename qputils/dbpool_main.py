import time, json, os, sys, subprocess
from time import perf_counter
import psycopg2
import psycopg2.extras
from psycopg2 import sql
from psycopg2 import pool
from common_constant import *

MIN_POOL_CONNS = 1
MAX_POOL_CONNS = 12


MAX_QUERY_RECORDS = 1000
MAX_QUERY_RECORDS_NOLIMIT = 999999999



# -----------

num_total_dbopens = 0
num_total_queries = 0
num_total_errors = 0
num_total_records = 0

iCount = 0

db_pool = None


# print("**************** LOADED py_dbpools_main **************")


# define a function that handles and parses psycopg2 exceptions
def print_psycopg2_exception(err):
    # get details about the exception
    err_type, err_obj, traceback = sys.exc_info()

    line_num = traceback.tb_lineno
    print(f"\npsycopg2 ERROR:{err} on line number:{line_num}")
    print(f"psycopg2 traceback:{traceback} -- type:{err_type}")
    print(f"\nextensions.Diagnostics:{err.diag}")
    print(f"pgerror:{err.pgerror}")
    print(f"pgcode:{err.pgcode}\n")
    return


def db_populate_table_column_lists(table_name: str):
    global db_pool

    # we have previously iterated thru this table for it's column names so reuse that now
    if (table_name is not None) and (table_name in db_pool.table_columns):
        return db_pool.table_columns[table_name]

    # we have not yet iterated thru this table for it's column names
    sql_str = sql.SQL(
        "SELECT column_name FROM information_schema.columns WHERE table_schema = 'public' AND table_name = %s;")
    response = db_execute_sql_new(sql_str=sql_str, data=(str(table_name),))
    col_names = []
    if s_records in response:
        for col in response[s_records]:
            for key, col_name in col.items():
                col_names.append(col_name)
    db_pool.table_columns[table_name] = col_names
    return db_pool.table_columns[table_name]

class dbpool_handler():

    def __init__(self, host: str, port: int, user: str, password: str, database: str):
        self.db_pg_pool = None
        self.table_columns = {}


        # , sslmode='require'
        self.db_pg_pool = psycopg2.pool.SimpleConnectionPool(MIN_POOL_CONNS, MAX_POOL_CONNS, host=host, port=port,
                                                             user=user, password=password, database=database)
        if (self.db_pg_pool):
            pass
        else:
            global num_total_errors
            num_total_errors += 1
        return

    def __del__(self):
        if self.db_pg_pool is not None:
            self.db_pg_pool.closeall()
            self.db_pg_pool = None
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
                print(
                    f"exception in db_connect: psycopg2.OperationalError=[{psycopg2.OperationalError}] Exception=[{error}]")
                num_total_errors += 1
            except (Exception, psycopg2.DatabaseError) as error:
                print(
                    f"exception in db_connect: psycopg2.DatabaseError=[{psycopg2.DatabaseError}] Exception=[{error}]")
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


class dbconn_handler():

    def __init__(self, db_pool):
        self.db_pool = db_pool
        self.db_pg_conn = None
        self.rollback = False
        if (self.db_pool is None):
            print("db_pool is none - bad!!! Error!")  # was ic
            return
        self.db_pg_conn = db_pool.db_connect()
        return

    def setrollback(self, rollback: bool):
        self.rollback = rollback
        return

    def __del__(self):
        if (self.db_pool is None) or (self.db_pg_conn is None):
            print(
                "connected already disconnected or not created yet or db_pool is none!!! Error!")  # was ic
            return
        self.db_pool.db_disconnect(self.db_pg_conn, rollback=self.rollback)
        self.db_pg_conn = None
        return


# -----------------------


class db_handler:
    """
        class for handling db_connections, commits and rollbacks
    """

    def __init__(self, db_host: str, db_port: int, db_name: str, db_user: str, db_password: str,
                 db_admin_default_db_name: str):
        self.db_conn = None
        self.stop_at = self.start_at = None
        self.call_rollback = False

        self.db_host = db_host
        self.db_port = db_port
        self.db_name = db_name
        self.db_user = db_user
        self.db_password = db_password
        self.db_admin_default_db_name = db_admin_default_db_name
        return

    def __del__(self):
        if self.db_conn is None: return
        self.db_conn.close()
        return

    def db_superadmin_execute_sql(self, sql_str):
        admin_con = None
        db_cur = None

        try:
            if not isinstance(sql_str, sql.Composable):
                sql_str = sql.SQL(sql_str)

            admin_con = psycopg2.connect( database=self.db_admin_default_db_name, user=self.db_user, password=self.db_password, host=self.db_host, port=self.db_port, connect_timeout=60 )
            admin_con.autocommit = True
            db_cur = admin_con.cursor()
            db_cur.execute(sql_str)

        except Exception as err:
            print(f"❌ psycopg2 ERROR: {err}")
            print(f"❌ psycopg2 type: {type(err)}")
            try:
                print(f"🔍 psycopg2 diag: {err.diag}")  # Only for psycopg2 errors
            except AttributeError:
                pass

        finally:
            if db_cur is not None:
                db_cur.close()
            if admin_con is not None:
                admin_con.close()

        return

    def db_superadmin_create_new_db(self, db_new_name=None):
        if db_new_name is None:
            db_new_name = self.db_name
        sql_str = f"CREATE DATABASE {db_new_name};"
        self.db_superadmin_execute_sql(sql.SQL(sql_str))
        sql_str = f"ALTER DATABASE {db_new_name} SET timezone TO 'Asia/Calcutta';"
        self.db_superadmin_execute_sql(sql_str)
        return

    def db_superadmin_terminate_connections(self, db_name=None):
        if db_name is None: db_name = self.db_name
        sql_str = f"SELECT * FROM pg_stat_activity WHERE datname = '{db_name}';"
        self.db_superadmin_execute_sql(sql_str)
        sql_str = f"SELECT pg_terminate_backend (pid) FROM pg_stat_activity WHERE datname = '{db_name}';"
        self.db_superadmin_execute_sql(sql_str)
        return

    def db_superadmin_rename(self, db_cur_name, db_new_name):
        sql_str = f"ALTER DATABASE {db_cur_name} RENAME TO {db_new_name};"
        self.db_superadmin_execute_sql(sql_str)
        return


    def db_superadmin_delete_existing_db(self, db_name=None):
        if db_name is None: db_name = self.db_name
        sql_str = f"SELECT pg_terminate_backend(pg_stat_activity.pid) FROM pg_stat_activity WHERE pg_stat_activity.datname = '{db_name}';"
        self.db_superadmin_execute_sql(sql_str)
        sql_str = f"DROP DATABASE IF EXISTS {db_name};"
        self.db_superadmin_execute_sql(sql_str)
        return

    def db_superadmin_create_user(self, user_name, user_password):
        sql_str = f"CREATE USER {user_name} WITH PASSWORD '{user_password}';"
        self.db_superadmin_execute_sql(sql_str)
        return

    def db_connect(self):
        while True:
            try:
                self.start_at = time.time()
                self.call_rollback = False
                print(f"\tdb_connect: Loading DB VARs ...")
                print(f"\tdb_host     =[{self.db_host}]")
                print(f"\tdb_port     =[{self.db_port}]")
                print(f"\tdb_name     =[{self.db_name}]")
                print(f"\tdb_user     =[{self.db_user}]")
                print(f"\tdb_password =[{self.db_password}]")
                # , sslmode='require'
                self.db_conn = psycopg2.connect(host=self.db_host, port=self.db_port, dbname=self.db_name,
                                                user=self.db_user, password=self.db_password, connect_timeout=60)
                # self.db_conn.autocommit = True
                self.db_dumpinfo()
                break
            except (Exception, psycopg2.DatabaseError) as error:
                print(f"\texception in db_connect psycopg2.DatabaseError error=[{error}]")
            except psycopg2.OperationalError as error:
                print(f"\texception in db_connect psycopg2.OperationalError error=[{error}]")
            break
        return

    def db_dumpinfo(self):
        if self.db_conn is None: return
        db_cur = None

        print(f"\tdb_host     =[{self.db_host}]")
        print(f"\tdb_port     =[{self.db_port}]")
        print(f"\tdb_name     =[{self.db_name}]")
        print(f"\tdb_user     =[{self.db_user}]")
        print(f"\tdb_password =[{self.db_password}]")

        try:
            db_cur = self.db_conn.cursor(cursor_factory=psycopg2.extras.NamedTupleCursor)
            # execute a statement
            sql_str = f"SELECT version()"
            db_cur.execute(sql_str)
            # display the PostgreSQL database server_user version
            db_version = db_cur.fetchone()
            print(f"\tdb_version  =[{db_version}]")

        except (Exception, psycopg2.DatabaseError) as error:
            print(f"\texception in db_dumpinfo psycopg2.DatabaseError error=[{error}]")
        finally:
            if (db_cur is not None): db_cur.close()
        return

    def db_disconnect(self):
        """
            This function commits/rollbacks the transaction and closes DB connection
        """
        self.stop_at = time.time()
        process_time = (self.stop_at - self.start_at) * MAX_QUERY_RECORDS

        if self.call_rollback:  # some error has occurred
            if (self.db_conn is not None):
                self.db_conn.rollback()
        else:  # no errors, commit transaction
            if (self.db_conn != None):
                self.db_conn.commit()

        if self.db_conn is not None: self.db_conn.close()
        if self.db_conn is None: return
        self.db_conn.close()
        self.db_conn = None
        return

    def execute_sql(self, sql_str):
        bReturn = True
        if self.db_conn is None: return (False)
        db_cur = None

        try:
            db_cur = self.db_conn.cursor(cursor_factory=psycopg2.extras.NamedTupleCursor)
            db_cur.execute(sql_str)
            db_cur.close()
            self.db_conn.commit()
        except Exception as err:
            # pass exception to function
            print_psycopg2_exception(err)
            self.db_conn.rollback()
            bReturn = False
        finally:
            if (db_cur is not None): db_cur.close()
        return (bReturn)

    def add_standard_elements_to_db(self):
        self.add_db_procedure_on_create(field_name="created_at")
        self.add_db_procedure_on_update(field_name="updated_at")
        return

    def create_sequence(self, seq_name, start="1", max_val="2147483647"):
        ##sql_str = "CREATE SEQUENCE IF NOT EXISTS public." + str(seq_name) + " INCREMENT 1" + " START " + str(start) + " MINVALUE 1" + " MAXVALUE " + str(max_val) + " CACHE 1;"
        sql_str = sql.SQL(
            f"CREATE SEQUENCE IF NOT EXISTS {seq_name} INCREMENT 1 START {start} MINVALUE 1 MAXVALUE {max_val} CACHE 1;")
        self.execute_sql(sql_str)
        return

    def create_table(self, table_name, add_standard_triggers=True, add_standard_fields=True):
        ##sql_str = "CREATE TABLE IF NOT EXISTS public." + str(table_name) + "()"
        sql_str = sql.SQL(f"CREATE TABLE IF NOT EXISTS {table_name}()")
        self.execute_sql(sql_str)

        if add_standard_fields is True:
            # self.add_field(table_name=table_name, field_name="id", field_type=FIELD_TYPE_INTEGER_NOT_NULL, constraint_name=table_name+"id_constraint", constraint_str=CONSTRAINT_UNIQUE)
            self.add_table_trigger(table_name=table_name, trigger_name=TRIGGER_NO_UPDATE_ON_CREATE, event=BEFORE_UPDATE, procedure=PROCEDURE_RETAIN_CREATED_DATETIME)
            self.add_table_trigger(table_name=table_name, trigger_name=TRIGGER_SET_TIMESTAMP, event=BEFORE_UPDATE, procedure=PROCEDURE_SET_TIMESTAMP)

        if add_standard_fields is True:
            self.add_field(table_name=table_name, field_name="id", field_type=FIELD_TYPE_INTEGER_IDENTITY,  constraint_name=table_name + "id_constraint", constraint_str=CONSTRAINT_UNIQUE)
            self.add_field(table_name=table_name, field_name="status", field_type=FIELD_TYPE_STRING)
            self.add_field(table_name=table_name, field_name="created_at", field_type=FIELD_TYPE_DATETIME)
            self.add_field(table_name=table_name, field_name="updated_at", field_type=FIELD_TYPE_DATETIME)
            self.add_field(table_name=table_name, field_name="details", field_type=FIELD_TYPE_JSONB)
        return

    def add_field(self, table_name, field_name, field_type, constraint_name=None, constraint_str=None):
        sql_str = sql.SQL(f"ALTER TABLE {table_name} ADD {field_name} {field_type}")
        self.execute_sql(sql_str)
        self.add_constraint_to_field(table_name=table_name, field_name=field_name, constraint_name=constraint_name,
                                     constraint_str=constraint_str)
        return

    def add_constraint_to_field(self, table_name, field_name, constraint_name=None, constraint_str=None):
        if constraint_str is not None:
            if (constraint_name is None):
                constraint_name = str(field_name + str(constraint_str))
            sql_text = sql.SQL(
                f"ALTER TABLE {table_name} ADD CONSTRAINT {constraint_name} {constraint_str} ({field_name})")
            self.execute_sql(sql_text)
        return

    def add_db_procedure_on_create(self, field_name):
        sql_str = "CREATE OR REPLACE FUNCTION public." + str(
            PROCEDURE_RETAIN_CREATED_DATETIME) + "() RETURNS TRIGGER AS $$ BEGIN NEW." + str(
            field_name) + ":= OLD.created_at; RETURN NEW; END; $$ LANGUAGE plpgsql"
        self.execute_sql(sql_str)
        return

    def add_db_procedure_on_update(self, field_name):
        sql_str = "CREATE OR REPLACE FUNCTION public." + str(
            PROCEDURE_SET_TIMESTAMP) + "() RETURNS TRIGGER AS $$ BEGIN NEW." + str(
            field_name) + "= NOW(); RETURN NEW; END; $$ LANGUAGE plpgsql"
        self.execute_sql(sql_str)
        return

    def add_table_trigger(self, table_name, trigger_name, event, procedure):
        sql_str = "CREATE TRIGGER " + str(trigger_name) + " " + str(event) + " ON public." + str(
            table_name) + " FOR EACH ROW EXECUTE PROCEDURE " + str(procedure) + "('" + str(table_name) + "')"
        self.execute_sql(sql_str)
        return

    def grant_sequence_permission(self, sequence):
        sql_str = "GRANT USAGE, SELECT ON SEQUENCE " + str(sequence) + " TO read_write_user;"
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

        # read_only_user
        sql_str = ""
        sql_str = sql_str + "GRANT CONNECT ON DATABASE " + str(self.db_name) + " TO read_only;"
        sql_str = sql_str + "GRANT SELECT ON ALL SEQUENCES IN SCHEMA public TO read_only;"
        sql_str = sql_str + "GRANT SELECT ON ALL TABLES IN SCHEMA public TO read_only;"
        db_cur.execute(sql_str)
        db_cur.close()
        self.db_conn.commit()
        return

    def add_constrain(self, table_name):
        sql_text = "ALTER TABLE IF EXISTS public." + str(table_name) + " ADD CONSTRAINT " + str(
            table_name) + "id_constraint UNIQUE (id);"
        self.execute_sql(sql_text)
        return

    def add_sequence(self, table_name):
        sql_text = "CREATE SEQUENCE IF NOT EXISTS public." + str(
            table_name) + "_id_seq INCREMENT 1 START 1 MINVALUE 1 MAXVALUE 2147483647 CACHE 1;"
        self.execute_sql(sql_text)
        return

    def add_foreign_constraint_to_field(self, table_name, field_name, ref_table, ref_field_name, constraint_name=None):
        if (constraint_name is None):
            constraint_name = field_name + "_in_" + table_name + "_must_exist_as_" + ref_field_name + "_in_" + ref_table
        sql_text = "ALTER TABLE ONLY public." + str(table_name) + " ADD CONSTRAINT " + str(
            constraint_name) + " FOREIGN KEY (" + str(field_name) + ") REFERENCES public." + str(ref_table) + "(" + str(
            ref_field_name) + ") NOT VALID;"
        self.execute_sql(sql_text)
        return

    def create_type_enum (self, type_name: str, enum_list: list) -> str:
        enum_tuple_str = ', '.join([f"'{value}'" for value in enum_list])
        sql_str = f"CREATE TYPE {type_name} AS ENUM ({enum_tuple_str});"
        self.execute_sql(sql_str)
        #log_pyutils.debug(f"create_type_enum: sql_str=[{sql_str}]")
        return(type_name)






    def pack_details_in_record_dict (self, record_dict: dict, table_name: str) -> dict:
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
        return(new_record_dict)

    def insert_record(table_name: str, record_dict: dict):
        if not record_dict:
            return {'success': False, 'total_count': 0, 'count': 0, 'records': []}

        packed_record_dict = pack_details_in_record_dict(record_dict=record_dict, table_name=table_name)

        fields = sql.SQL(', ').join(sql.Identifier(k) for k in packed_record_dict)
        num_vals = sql.SQL(', ').join(sql.Placeholder() * len(packed_record_dict))

        if 'details' in packed_record_dict:
            packed_record_dict['details'] = json.dumps(packed_record_dict['details'])

        sql_str = sql.SQL( "INSERT INTO {table} ({cols}) VALUES ({vals}) RETURNING *" ).format( table=sql.Identifier(table_name), cols=fields, vals=num_vals )

        # Execute insert
        response = db_execute_sql_new(sql_str=sql_str, data=tuple(packed_record_dict.values()), num_records=None)
        response = self.pack_json(response)

        if response.get(s_status) == s_success and response.get(s_count, 0) > 0:
            return response

        return {s_status: s_failure, s_status_code: HTTP_STATUS_CODE_422}



    # --- UPDATE FUNCTION ---
    def update_record(table: str, record_dict: dict, record_id: dict) -> dict:
        if len(record_id) != 1:
            raise ValueError("Only single primary key supported in record_id")

        set_clauses = [sql.SQL("{} = %s").format(sql.Identifier(k)) for k in record_dict.keys()]
        set_clause = sql.SQL(', ').join(set_clauses)

        pk_field, pk_value = list(record_id.items())[0]
        values = tuple(record_dict.values()) + (pk_value,)

        query = sql.SQL("UPDATE {table} SET {set_clause} WHERE {pk_field} = %s RETURNING id").format(
            table=sql.Identifier(table),
            set_clause=set_clause,
            pk_field=sql.Identifier(pk_field)
        )

        return db_execute_sql_new(query, values, 1)


    # --- DELETE FUNCTION ---
    def delete_record(table: str, record_id: dict) -> dict:
        if len(record_id) != 1:
            raise ValueError("Only single primary key supported in record_id")

        pk_field, pk_value = list(record_id.items())[0]

        query = sql.SQL("DELETE FROM {table} WHERE {pk_field} = %s RETURNING id").format(
            table=sql.Identifier(table),
            pk_field=sql.Identifier(pk_field)
        )

        return db_execute_sql_new(query, (pk_value,), 1)


    def db_get_records_by_key_val(table_name: str, query_dict: dict = None):
        query_dict = {key: value for key, value in query_dict.items() if value is not None}
        num_records = MAX_QUERY_RECORDS
        if "num_records" in query_dict.keys():
            num_records = query_dict.pop("num_records")
        if num_records == 0:
            response = {s_status: s_success, "count": 0, s_status_code: HTTP_STATUS_CODE_200,
                        s_message: "Zero records requsted. "}
            return response
        if query_dict:
            base_query = sql.SQL( "SELECT * FROM public.{table} WHERE {params} ORDER BY updated_at ASC LIMIT " + str(num_records)).format( table=sql.Identifier(table_name), params=sql.SQL(' AND ').join(
                    sql.Composed([sql.Identifier(k), sql.SQL(" = "), sql.Placeholder(k)]) for k in query_dict.keys()))
        else:
            base_query = sql.SQL("SELECT * FROM public.{table} LIMIT " + str(num_records)).format(
                table=sql.Identifier(table_name))

        response = db_execute_sql_new(sql_str=base_query, data=dict(query_dict.items()))
        if (response[s_status] == s_success) and (response[s_count] != 0):
            return response
        return {s_status: s_failure, s_status_code: HTTP_STATUS_CODE_404, s_message: 'details not found'}

def db_startup_dbpools():
    global db_pool

    print("startup_dbpools: started")
    if db_pool is None:
        print("DB_POOL STARTED *******************")
        db_profile_name = "QIKPOD"
        db_host = os.getenv(db_profile_name + '_DB_HOST', None)
        db_port = int(str(os.getenv(db_profile_name + '_DB_PORT', "25060")))
        db_user = os.getenv(db_profile_name + '_DB_USER', None)
        db_password = os.getenv(db_profile_name + '_DB_PASSWORD', None)
        db_name = os.getenv(db_profile_name + '_DB_NAME', None)
        # if py_utils is being used where db is not needed then do not auto instantiate db_pool
        if db_host is not None:
            db_pool = dbpool_handler(host=db_host, port=db_port, user=db_user, password=db_password, database=db_name)
            print(
                f"Loading Pool DB: host={db_host}, port={db_port}, user={db_user}, password={db_password}, database={db_name}")
        else:
            print("DB POOL SKIPPED- ENV VARS MISSING *******************")
    print("startup_dbpools: completed")
    return


# ----- START OF NEW SECTION TO SUPPORT AD HOC DB TRANSACTION COMMIT/ROLLBACK -----


def db_execute_sql_open_transaction() -> object:
    global db_pool

    db_conn = None
    db_cur = None
    try:
        db_conn = db_pool.db_pg_pool.getconn()
        if ((db_conn is None) or (db_conn.closed == True)):
            print("db_execute_sql_transaction_open: **** ERROR **** in db_conn")
            db_conn = None
        if (db_conn is not None):
            db_cur = db_conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    except (Exception) as error:
        print(f"exception in db_execute_sql_transaction_open error=[{error}]")
        db_conn = None
        db_cur = None
        pass

    finally:
        pass

    return db_conn, db_cur


def db_execute_sql_close_transaction(db_conn: object, db_cur: object, commit_to_db: bool = True) -> bool:
    global db_pool

    breturn = True
    try:
        if db_cur is not None:
            db_cur.close()
            db_cur = None
        if db_conn is not None:
            if commit_to_db:
                db_conn.commit()
            else:  # no errors, commit transaction
                db_conn.rollback()
            db_pool.db_pg_pool.putconn(db_conn)
        pass

    except (Exception) as error:
        print(f"exception in db_execute_sql_transaction_close error=[{error}]")
        breturn = False
        pass

    finally:
        pass

    return breturn


def db_unpack_json (response):
    if s_records not in response:
        return response

    new_records = []
    for record in response[s_records]:
        if 'details' not in record:
            new_records.append(record)
        else:
            # if 'details' field is present in record
            new_record = {}
            if record.get('details') is not None:
                for key, val in record['details'].items():
                    new_record.update({key: val})
            del record['details']
            new_record.update(record)
            new_records.append(new_record)
    response[s_records] = new_records
    return response

def db_execute_sql_with_transaction(db_cur: object, sql_str: psycopg2.sql.SQL = None, data: tuple = None,
                                    num_records: int = None) -> dict:
    global db_pool

    if (db_cur is None):
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
            db_cur.execute(sql_str, data)

        if ((db_cur.rowcount > 0) and (db_cur.description is not None)):
            if ((db_cur.rowcount > 0) and (
                    db_cur.description is not None)):  # rowcount will be 1 or greater if delete was successful
                records = db_cur.fetchall()
            else:
                # any other non zero positive number will be passed to the fetch query
                records = db_cur.fetchmany(num_records)

        if (db_cur.rowcount == 0):  # No matching record found
            response = {s_status: s_success, s_count: 0, s_records: []}
            return response

        if (db_cur.rowcount < 0):  # No matching record found
            response = {s_status: s_failure}
            return response

        # here total_count returns the count as per db_cur query response.
        # count is the number of records fetched - which could be < total_count since the user
        # may have requested fewer than all the records be returned or pagination maybe implemented
        response.update({s_count: len(records)})
        response.update({s_rowcount: db_cur.rowcount})
        response.update({s_records: records})
        response.update({s_status: s_success})
        pass

    except (Exception) as error:
        print(f"exception in db_execute_sql error=[{error}]")
        response = {s_status: s_failure}
        pass

    finally:
        pass

    final_response = db_unpack_json(response)
    return final_response


def db_execute_sql_new(sql_str: psycopg2.sql.SQL = None, data: tuple = None, num_records: int = MAX_QUERY_RECORDS) -> dict:
    global db_pool
    commit_to_db = True
    db_conn = None
    response = {s_status: s_success}

    # if the user is asking for no records, then we can respond with an empty query without even querying the db
    if (num_records == 0):
        response = {s_status: s_success, s_count: 0, s_records: []}
        return response

    try:
        db_conn, db_cur = db_execute_sql_open_transaction()
        if ((db_cur is not None) and (sql_str is not None)):
            response = db_execute_sql_with_transaction(db_cur=db_cur, sql_str=sql_str, data=data)
        pass
    except (Exception) as error:
        print(f"exception in db_execute_sql_new error=[{error}]")
        commit_to_db = False
        response = {s_status: s_failure}
        pass
    finally:
        db_execute_sql_close_transaction(db_conn=db_conn, db_cur=db_cur, commit_to_db=commit_to_db)
        pass

    return response

# ----- END OF NEW SECTION TO SUPPORT AD HOC DB TRANSACTION COMMIT/ROLLBACK -----


