import mysql.connector
import pandas as pd

try:
    mydb = mysql.connector.connect(host="host",user="user",passwd="password",database="db")
    mycursor = mydb.cursor()

    sql = "SELECT * FROM table"
    mycursor.execute(sql)
    sql_data = pd.DataFrame(mycursor.fetchall())
    sql_data.columns = mycursor.column_names

    sql_data.to_csv("table.csv",index = False)
    #If False do not print fields for index names.

except mysql.connector.Error as err:
    print("Something went wrong: {}".format(err))

finally:
    mycursor.close()
    mydb.close()
    #Reduce IO resources usage
