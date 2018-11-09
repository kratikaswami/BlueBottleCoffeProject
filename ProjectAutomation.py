import mysql.connector
import csv
import os
from datetime import datetime

# opening the given csv file for sales data and converting it into a list.
print("loading the given csv file into a temporary table.")
with open("morse.csv", "r") as data_file:
        next(data_file, None)
        reader = csv.reader(data_file, delimiter=',')
        data_list = list(reader)
        print(data_list)

# for security purpose, taking the username and password from environment variables.
db_user = os.getenv('DB_USER', 'root')
db_password = os.getenv('DB_PASS', 'root')
print("Connecting to local instance of mysql")

# creating mysql connection.
mydb = mysql.connector.connect(
     host='localhost',
     user=db_user,
     passwd=db_password,
     database='cafedb'
)
cursor = mydb.cursor()
print(mydb)

# creating the table for sales data.
# drop query is used to avoid duplication.
print("Creating table to insert morse.csv data now")
drop_query = ("DROP TABLE IF EXISTS cafedb.salesdata;")
create_query = ("CREATE TABLE cafedb.salesdata ("
                "`created_at` DATETIME DEFAULT NULL,"
                "`item_name` VARCHAR(30) DEFAULT NULL,"
                "`quantity` INT(11))"
                "ENGINE = INNODB DEFAULT CHARSET = utf8;")
cursor.execute(drop_query)
mydb.commit()
cursor.execute(create_query)
mydb.commit()
print("Temporary table was created.")

# performing insertion operation into the table via the list created earlier.
print("Populating table now.")
for i, item in enumerate(data_list):
    insert_query = ("INSERT INTO cafedb.salesdata VALUES (%s, %s, %s)")
    dateobtained = datetime.strptime(item[0], "%m/%d/%Y %H:%M:%S")
    created_at = dateobtained.replace(minute=0, second=0, microsecond=0)
    item_name = item[1]
    quantity = int(item[2])

    cursor.execute(insert_query, (created_at, item_name, quantity))

mydb.commit()
print("Added data to the table")

# Creating the temperature table here via the data obtained from weather api.
print("creating the temperature table now.")
with open("date_temp_file.csv", "r") as temp_file:
    next(temp_file, None)
    reader = csv.reader(temp_file, delimiter=',')
    temp_list = list(reader)

# creating the table for temperature data.
# drop query is used to avoid duplication.
drop_temp_query = ("DROP TABLE IF EXISTS cafedb.tempdata;")
create_temp_query = ("CREATE TABLE cafedb.tempdata ("
                     "`date` DATETIME DEFAULT NULL,"
                     "`temperature` INT(11))"
                     "ENGINE = INNODB DEFAULT CHARSET = utf8;")
cursor.execute(drop_temp_query)
mydb.commit()
cursor.execute(create_temp_query)
mydb.commit()
# performing insertion operation into the table via the list created earlier.
print("Populating temperature table now.")
for item in enumerate(temp_list):
    insert_temp_query = ("INSERT INTO cafedb.tempdata VALUES (%s, %s)")
    date = datetime.strptime(str(item[1][0]), "%m/%d/%Y %H:%M:%S")
    temperature = float(item[1][1])

    cursor.execute(insert_temp_query, (date, temperature))
    mydb.commit()

# SQL query for report 1 here.
# The output obtained from the query is inserted in a csv file.
print("Generating report 1 now.")
report1 = ("SELECT \
  tempdata.temperature AS Temperature,\
  salesdata.item_name AS ItemName, \
  SUM(salesdata.quantity) AS Quantity \
  FROM salesdata \
  INNER JOIN tempdata ON salesdata.created_at = tempdata.date \
  GROUP BY tempdata.temperature, salesdata.item_name, salesdata.quantity \
  ORDER BY SUM(salesdata.quantity) DESC;")
cursor.execute(report1)
# generating a csv file for the output of report1 query.
with open("report1.csv", "w") as report1:
    writer = csv.writer(report1, quoting=csv.QUOTE_NONNUMERIC)
    writer.writerow(col[0] for col in cursor.description)
    for row in cursor:
        writer.writerow(row)

# SQL query for report 2 here.
# The output obtained from the query is inserted in a csv file.
print("Generating report 2 now.")
report2 = ("SELECT lq.nameA item_name, AVG(lq.case1) avg_sale_when_colder, AVG(lq.case2) avg_sale_when_warmer \
   FROM \
   (SELECT nameA,\
   CASE  WHEN (tempB - tempA) < -1 THEN quanB - quanA ELSE NULL END AS case1,\
   CASE WHEN (tempB - tempA) > 1  THEN quanB - quanA ELSE NULL END AS case2\
   FROM (\
   SELECT date(A.created1) createdA, A.name1 nameA, A.quan1 quanA, A.temp1 tempA,\
   date(B.created2) createdB, B.name2 nameB, B.quan quanB, B.temp2 tempB \
   FROM  (\
   SELECT date(s1.created_at) created1, s1.quantity quan1, s1.item_name name1, date(t1.date) date1, t1.temperature temp1 \
   FROM salesdata s1, tempdata t1 \
   WHERE date(s1.created_at) = date(t1.date)) AS A \
   INNER JOIN  (\
   SELECT date(s2.created_at) created2, s2.item_name name2, s2.quantity quan, date(t2.date) date2, t2.temperature temp2 \
   FROM salesdata s2, tempdata t2 \
   WHERE date(s2.created_at) = date(t2.date)) AS B\
   ON A.name1 = B.name2\
   WHERE DATEDIFF(date(B.created2), date(A.created1)) = 1) as C) AS lq GROUP BY lq.nameA;        ")
cursor.execute(report2)
# generating a csv file for the output of report1 query.
with open("report2.csv", "w") as report2:
    writer = csv.writer(report2, quoting=csv.QUOTE_NONNUMERIC)
    writer.writerow(col[0] for col in cursor.description)
    for row in cursor:
        writer.writerow(row)

mydb.close()
cursor.close()
