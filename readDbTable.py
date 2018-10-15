from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
import psycopg2


sc = SparkContext('local')
spark = SparkSession(sc)
spark.conf.set("spark.sql.crossJoin.enabled", "true")

def get_cursor():
    conn = psycopg2.connect(
        " user=postgres host=X.X.X.X password=123456")
    cur = conn.cursor()
    return conn, cur

def get_content():
    sql = """select image_name,info_type,tl_x,tl_y,br_x,br_y from <schema>.<table> """
    try:
        conn, cur = get_cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        cur.close()

        return rows
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()

result = get_content()

df1 = spark.createDataFrame(result,["image_name","info_type","tl_x","tl_y","br_x","br_y"])

df1.show(5)
