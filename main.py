import psycopg2
import csv

def main():
    host = "postgres"
    database = "postgres"
    user = "postgres"
    pas = "postgres"
    conn = psycopg2.connect(host=host, database=database, user=user, password=pas)
    
    # Створення нових таблиць
    cursor = conn.cursor()
    cursor.execute('DROP TABLE IF EXISTS transactions')
    cursor.execute('DROP TABLE IF EXISTS products')
    cursor.execute('DROP TABLE IF EXISTS accounts')
    with open('sql/accounts.sql', 'r') as file:
        sql_script = file.read()
        cursor.execute(sql_script)
    with open('sql/products.sql', 'r') as file:
        sql_script = file.read()
        cursor.execute(sql_script)
    with open('sql/transactions.sql', 'r') as file:
        sql_script = file.read()
        cursor.execute(sql_script)
    
    # додавання даних в таблиці
    with open('data/accounts.csv', 'r') as file:
        csv_reader = list(csv.reader(file))[1:]      
        cursor.executemany('''
            INSERT INTO accounts 
                (account_id,first_name,last_name,address_1,address_2,city,state,zip_code,join_date) 
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)''', csv_reader)        
    with open('data/products.csv', 'r') as file:
        csv_reader = list(csv.reader(file))[1:]        
        cursor.executemany('''
            INSERT INTO products 
                (product_id,product_code,product_description) 
            VALUES (%s, %s, %s)''', csv_reader)    
    with open('data/transactions.csv', 'r') as file:
        csv_reader = list(csv.reader(file))[1:]
        mydata = [[row[0],row[1],row[2],row[6],row[5]] for row in csv_reader]
        cursor.executemany('''
            INSERT INTO transactions 
                (transaction_id,transaction_date,product_id,account_id,quantity) 
            VALUES (%s, %s, %s, %s, %s)''', mydata)

    # print в консоль вибраних даних
    print("---------acc---------")
    cursor.execute('SELECT * FROM accounts')
    print(cursor.fetchall())
    print("---------prod---------")
    cursor.execute('SELECT * FROM products')
    print(cursor.fetchall())
    print("---------trans---------")
    cursor.execute('SELECT * FROM transactions')
    print(cursor.fetchall())

    conn.commit()
    cursor.close()
    conn.close()
    print("Script finished successfuly")

if __name__ == "__main__":
    main()
