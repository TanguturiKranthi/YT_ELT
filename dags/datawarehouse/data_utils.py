from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import RealDictCursor

table = "yt_api"

def get_conn_cursor():
    hook = PostgresHook(postgres_conn_id="postgres_db_yt_elt")

    conn = hook.get_conn()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    return conn,cur

def close_conn_cursor(conn,cur):
    cur.close()
    conn.close()

def create_schema(schema):
    conn, cur = get_conn_cursor()
    schema_query = f"create schema if not exists {schema};"
    cur.execute(schema_query)
    conn.commit()
    close_conn_cursor(conn,cur)


def create_table(schema):
    conn, cur = get_conn_cursor()
    if schema == "staging":
        table_sql = f"""
        create table if not exists {schema}.{table}(
            "Video_Id" varchar(11) primary key not null,
            "Video_Title" text not null,
            "Upload_Date" timestamp not null,
            "Duration" varchar(20) not null,
            "Video_Views" int,
            "Likes_Count" int,
            "Comments_Count" int
        );
        """
    else:
        table_sql = f"""
        create table if not exists {schema}.{table}(
            "Video_Id" varchar(11) primary key not null,
            "Video_Title" text not null,
            "Upload_Date" timestamp not null,
            "Duration" time not null,
            "Video_Type" varchar(100) not null,
            "Video_Views" int,
            "Likes_Count" int,
            "Comments_Count" int
        );
        """
    cur.execute(table_sql)
    conn.commit()
    close_conn_cursor(conn,cur)

def get_video_ids(cur,schema):
    cur.execute(f'select "Video_Id" from {schema}.{table};')
    video_ids = [row["Video_Id"] for row in cur.fetchall()]
    return video_ids





