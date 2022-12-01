import mysql.connector as database
# Creating connection object

def del_data():
    try:       
        connection = database.connect(user="root", password="@Umurinzi#BKT!", host="127.0.0.1", database="sns_report")
        cursor = connection.cursor()
        
        cursor.execute("TRUNCATE TABLE farmer_fert")
        connection.commit()
        count = cursor.rowcount
        print(count, "Record deleted successfully into farmer_fert")
    except database.Error as e:
        print("Failed to insert record into table", e)
    finally:
        cursor.close()
        connection.close()

def insert_data(business_name,type_org,count_type):
    try:
       
        connection = database.connect(user="root", password="@Umurinzi#BKT!", host="127.0.0.1", database="sns_report")
        cursor = connection.cursor()
        
        #cursor.execute("TRUNCATE TABLE farmer_fert")
        postgres_insert_query = "INSERT INTO sns_report.farmer_fert (business_name, type_org, count_type) VALUES (%s,%s,%s)"
        record_to_insert = (business_name,type_org,count_type)
        cursor.execute(postgres_insert_query, record_to_insert)
        connection.commit()
        count = cursor.rowcount
        print(count, "Record inserted successfully into farmer_fert")
    except database.Error as e:
        print("Failed to insert record into table", e)
    finally:
        cursor.close()
        connection.close()
        
def get_data():
    try:
        connection = database.connect(user="root", password="@Umurinzi#BKT!", host="127.0.0.1", database="sns")
        cursor = connection.cursor()
        
        del_data()

        statement = "SELECT business_name, type_org, COUNT(*) AS count_type FROM sns.organizations WHERE type_org = 8 OR type_org = 8.1  GROUP BY business_name, type_org ORDER BY count_type DESC LIMIT 50"
        cursor.execute(statement)
        for (business_name, type_org, count_type) in cursor:
            insert_data(business_name,type_org,count_type)
            print(f"Successfully retrieved {business_name},{type_org}, {count_type}")
    except database.Error as e:
        print(f"Error retrieving entry from database: {e}")
    finally:
        connection.close()

get_data()

