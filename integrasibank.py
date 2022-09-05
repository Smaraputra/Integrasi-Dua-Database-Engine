import pymysql
import time

def connect_db_toko():
    conn = pymysql.connect(host='remotemysql.com', user='qH4xaikDfx', password='dOhkDO4T7h', db='qH4xaikDfx')
    return conn

def connect_db_bank():
    conn = pymysql.connect(host='remotemysql.com', user='IzvgfOVDyn', password='9Ab9vkzPAt', db='IzvgfOVDyn')
    return conn

def update(table, data, cursor, db):
    try:
        sql = "UPDATE " + table + " SET user_id = '%s', id_produk = '%s', jumlah = '%s' , total = '%s' , status = '%s' WHERE id_invoice = %s"
        val = (data[1], data[2], data[3], data[4], data[6], data[0])
        cursor.execute(sql, val)
        db.commit()

    except (pymysql.Error, pymysql.Warning) as e:
        print(e)

    return 1

def insert(table, data, cursor, db):
    sql = "INSERT INTO " + table + "(id_invoice, user_id,id_produk,jumlah,total,date,status) VALUES (%s, %s, %s, %s, %s, %s, %s)"
    val = (data [0], data[1], data[2], data[3], data[4], data[5], data[6])

    cursor.execute(sql, val)
    db.commit()

    return 1

def delete(table, data, cursor, db):
    sql = "DELETE FROM " + table + " WHERE id_invoice = %s"
    val = (data[0])

    cursor.execute(sql, val)
    db.commit()

    return 1


def select(table, cursor):
    sql_select = "SELECT * FROM " + table
    cursor.execute(sql_select)

    results = cursor.fetchall()

    return results

tables_1 = ("tb_invoice","")
histories_1 = ("tb_integrasi_invoice","")

delay_engine = int(input('Masukkan Lama Delay Engine : '))

while (1):
    try:
        db_toko = connect_db_toko()
        cursor_toko = db_toko.cursor()

        db_bank = connect_db_bank()
        cursor_bank = db_bank.cursor()

        result = select(tables_1[0],cursor_bank)
        history = select(histories_1[0],cursor_bank)

        print("Result len = %d | History len = %d" % ( len(result),len(history) ))

        #insert listener
        if(len(result) > len(history)):
            print("-- INSERT DETECTED --")
            for data in result:
                a = 0
                for dataHistory in history:
                    if (data[0] == dataHistory[0]):
                        a = 1
                if (a == 0):
                    print("-- RUN INSERT FOR ID = %d" % (data[0]))
                    insert(histories_1[0],data,cursor_bank,db_bank)
                    insert(histories_1[0],data,cursor_toko,db_toko)
                    insert(tables_1[0],data,cursor_toko,db_toko)

        #delete listener
        if(len(result) < len(history)):
            print("-- DELETE DETECTED --")
            for dataHistory in history:
                a = 0
                for data in result:
                    if (dataHistory[0] == data[0]):
                        a = 1
                if (a == 0):
                    print("-- RUN DELETE FOR ID = %d" % (dataHistory[0]))
                    delete(histories_1[0],dataHistory,cursor_bank,db_bank)
                    delete(histories_1[0],dataHistory,cursor_toko,db_toko)
                    delete(tables_1[0],dataHistory,cursor_toko,db_toko)

        #update listener
        if(result != history):
            print("-- EVENT SUCCESS OR UPDATE DETECTED --")
            for data in result:
                for dataHistory in history:
                    if (data[0] == dataHistory[0]):
                        if(data != dataHistory):
                            update(histories_1[0],data,cursor_bank,db_bank)
                            update(histories_1[0],data,cursor_toko,db_toko)
                            update(tables_1[0],data,cursor_toko,db_toko)
                
    except (pymysql.Error, pymysql.Warning) as e:
        print(e)

    # Untuk delay
    time.sleep(delay_engine)
