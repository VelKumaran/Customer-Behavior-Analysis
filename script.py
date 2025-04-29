import pandas as pd

#read the files
f=pd.read_csv('customer_journey.csv')
f1=pd.read_csv('customer_reviews.csv')
f2=pd.read_csv('customers.csv')
f3=pd.read_csv('engagement_data.csv')
f4=pd.read_csv('geography.csv')
f5=pd.read_csv('products.csv')

#checking for null values in the given data sets
print(f.isnull().sum())
print(f1.isnull().sum())
print(f2.isnull().sum())
print(f3.isnull().sum())
print(f4.isnull().sum())
print(f5.isnull().sum())

#handling the null values
f.fillna(format(f['Duration'].mean(),".2f"),inplace=True)

import mysql
from mysql.connector import connect
#establishing coonection with sql
conn_mysql = connect(
    host="localhost",
    user="root",
    password="velkumaran"
)
cursor = conn_mysql.cursor()
print("MySQL connection established!")

#creating databae in the sql
cursor.execute("CREATE DATABASE IF NOT EXISTS customer_db;")
print("MySQL database 'customer_db' created successfully!")

#creating tables
cursor.execute("USE customer_db;") 
cursor.execute("""
    CREATE TABLE IF NOT EXISTS customer_journey (
        JourneyID INT,
        CustomerID INT,
        ProductID INT,
        VisitDate DATE,
        Stage VARCHAR(50),
        Action VARCHAR(50),
        Duration FLOAT
    );
""")

cursor.execute("""
    CREATE TABLE IF NOT EXISTS customer_reviews (
        ReviewID INT,
        CustomerID INT,
        ProductID INT,
        ReviewDate DATE,
        Rating INT,
        ReviewText VARCHAR(255)
    );
""")

cursor.execute("""
    CREATE TABLE IF NOT EXISTS customers (
        CustomerID INT,
        CustomerName VARCHAR(50),
        Email VARCHAR(50),
        Gender VARCHAR(100),
        Age INT,
        GeographyID INT
        
    );
""")

cursor.execute("""
    CREATE TABLE IF NOT EXISTS geography (
       GeographyID INT(50),
       Country VARCHAR(50),
       City VARCHAR(50)
    );
""")

cursor.execute("""
    CREATE TABLE IF NOT EXISTS engagement_data (
        EngagementID INT,
        ContentID INT,
        ContentType VARCHAR(50),
        Likes INT,
        EngagementDate Date,
        CampaignID INT ,
        ProductID  INT,
        ViewsClicksCombined VARCHAR(50)
        
    );
""")

cursor.execute("""
    CREATE TABLE IF NOT EXISTS products (
        ProductID INT,
        ProductName VARCHAR(50),
        Category VARCHAR(50) ,
        Price INT
        
    );
""")

print("Table 'customer' created successfully in MySQL!")
conn_mysql.commit()

#data insertion
data_list = f.values.tolist()
query = """
    INSERT INTO customer_journey (JourneyID,CustomerID,ProductID,VisitDate,Stage,Action,Duration)
    VALUES (%s, %s, %s, %s, %s, %s, %s);
"""
cursor.executemany(query, data_list)
conn_mysql.commit()
print("Data inserted using to_list()")

data_list = f1.values.tolist()
query = """
    INSERT INTO customer_reviews (ReviewID,CustomerID,ProductID,ReviewDate,Rating,ReviewText)
    VALUES (%s, %s, %s, %s, %s, %s);
"""

cursor.execute("ALTER TABLE customer_reviews MODIFY ReviewText TEXT;")
conn_mysql.commit()
print("Data inserted using to_list()")


data_list = f2.values.tolist()
query = """
    INSERT INTO customers(CustomerID,CustomerName,Email,Gender,Age,GeographyID)
    VALUES ( %s, %s, %s, %s, %s, %s);
"""
cursor.executemany(query, data_list)
conn_mysql.commit()
print("Data inserted using to_list()")


data_list = f5.values.tolist()
query = """
    INSERT INTO products (ProductID,ProductName,Category,Price)
    VALUES (%s, %s, %s, %s);
"""
cursor.executemany(query, data_list)
conn_mysql.commit()
print("Data inserted using to_list()")

data_list = f4.values.tolist()
query = """
    INSERT INTO geography (GeographyID, Country, City)
    VALUES (%s, %s, %s);
"""
cursor.executemany(query, data_list)
conn_mysql.commit()
print("Data inserted into 'geography' using to_list()")

data_list = f3.values.tolist()
query = """
    INSERT INTO engagement_data (EngagementID,ContentID,ContentType,Likes,EngagementDate,CampaignID,ProductID,ViewsClicksCombined)
    VALUES (%s, %s, %s,%s, %s, %s,%s, %s);
"""
cursor.executemany(query, data_list)
conn_mysql.commit()
print("Data inserted into 'engagement_data ' using to_list()")
