def establish_connection():
    from psycopg2 import connect, DatabaseError
    from os import getenv
    from dotenv import load_dotenv

    load_dotenv()

    try:

        u = getenv("dbuser")
        p = getenv("dbpassword")
        h = getenv("dbhost")
        r = getenv("dbport")
        d = getenv("dbdb")

        connection = connect(f"postgres://{u}:{p}@{h}:{r}/{d}")
        connection.autocommit = True
        return connection

    except DatabaseError:
        raise RuntimeError("Could not connect to databse")


def query(statement, vars=None, single=False):
    from psycopg2.extras import RealDictCursor as cursor_type

    with establish_connection() as conn:
        with conn.cursor(cursor_factory=cursor_type) as cursor:
            cursor.execute(statement, vars)
            print(cursor.query.decode("UTF-8"))
            if single:
                return cursor.fetchone()

            return cursor.fetchall()