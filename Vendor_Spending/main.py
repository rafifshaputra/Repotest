import sqlalchemy as db
from sqlalchemy import text
import pandas as pd
import mysql.connector
import json
import time

database_source='127.0.0.1:3316'
database_destination='127.0.0.1:3306'

username='rafif'
password='Jiidai3caa1phai'

source = 'borobudur'
destination = 'bridestory'

database = 'mysql://' + username + ':' + password + '@'
database1 = database + database_source + '/' + source
database2 ='mysql://root:@127.0.0.1/bridestory'

engine = db.create_engine(database1)
engine2 = db.create_engine(database2)

def get_current_state():
    con=engine.connect()
    current_state= con.execute(
        text("""
        SELECT id
        from {source}.payments_transactions
        ORDER BY id desc limit 1""". format(source=source))
        ).fetchone()[0]
    
    return current_state

def get_last_state():
    con2 = engine2.connect()
    last_state = con2.execute(
            text("""
                SELECT state
                FROM {destination}.smt_job_states
                WHERE name = 'vendor-spending'
                """.format(destination=destination))
        ).fetchone()
    return last_state
    
def update_state(state):
    con2 = engine2.connect()
    data = {"last_id": state}
    file = json.dumps(data)
    con2.execute(
        text("""
            INSERT INTO {destination}.smt_job_states (name, state)
            VALUES (:name, :state)
            ON DUPLICATE KEY
            UPDATE state = VALUES(state)
            """.format(destination=destination)),
        dict(name='vendor-spending', state=file)
    )
    print("state updated")
    print(state)
    return

def get_list_id(last_id):
    con=engine.connect()
    query_id ="""
        SELECT id
        from borobudur.payments_transactions
        where id > {idx}
    
        """.format(idx=last_id)
    
    df_id = pd.read_sql_query(query_id,con)

    list_id= df_id['id'].tolist()
    return list_id


def get_data(list_id):
    con = engine.connect()
    data="""
        select pt.vendor_id, SUBSTRING_INDEX(pt.paymentable_type,'\\\\',-1) as type_of_payment, sum(pt.total_amount) as total_spending
        from {source}.payments_transactions pt
        where pt.vendor_id is not null and
        pt.status="approved" and
        pt.deleted_at is null and
        pt.id in %(list_id)s
        group by pt.vendor_id,pt.paymentable_type
        """.format(source=source)

    df_data=pd.read_sql_query(data,con,params=dict(list_id=list_id))
    print('ambil_berhasil')    
    return df_data

def upload_data(data):
    con2 = engine2.connect()
    update_query = """
                    INSERT INTO {destination}.smt_vendor_spending
                    (new_id,vendor_id, type_of_payment, total_spending)
                    VALUES (:new_id,:vendor_id, :type, :total)
                    ON DUPLICATE KEY UPDATE total_spending=values(total_spending)
                    """.format(destination=destination)

    for index,row in data.iterrows():
        new_id = str(row['vendor_id'])+row['type_of_payment']
        con2.execute(
            text(update_query),
            dict(new_id=new_id,vendor_id=row['vendor_id'], type=row['type_of_payment'], total=row['total_spending'])
        )
   
    print("data uploaded")
    return

def main():
    if get_last_state() is None:
        last_id = 0
    else:
        last_queried = json.loads(get_last_state()[0])
        last_id = last_queried['last_id']
        # last_id=0

    print('Last id:', last_id)
    
    if get_list_id(last_id)== []:
        print('Belum Ada Data Baru')
    else:
        print(get_list_id(last_id))
        data = get_data(get_list_id(last_id))
        upload_data(data)
        update_state(get_current_state())
        print("done")
    return
   
if __name__ == '__main__':
    while True:
        try:
            main()
        except Exception as e:
            print(repr(e))
            print("\n failed")
        time.sleep(300)
