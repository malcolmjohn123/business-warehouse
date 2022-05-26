import os
import json
import db_credentials 
import psycopg2 as db
from psycopg2 import Error
from sql import dummy_data_insert


os.chdir('./file')
files_list = os.listdir()


#variables
l = {"<class 'str'>":'VARCHAR',"<class 'float'>":'DECIMAL',"<class 'int'>":'INTEGER',"<class 'dict'>":"JSON"}
table_creation_queries = []
table_drop_queries = []
table_insert_queries = {}

#json file parsing and creating query
for files in files_list:
    with open(files,'rb') as file:
        flag = 1
        while(flag):
            data = file.readline()
            jsondata = json.loads(data)

            drop_query = 'DROP TABLE IF EXISTS '+(files.split('_')[3]).split('.')[0]+';'
            create_query = 'CREATE TABLE '+(files.split('_')[3]).split('.')[0]+' ( '
            insert_query = "insert into "+(files.split('_')[3]).split('.')[0]+" select * from json_populate_recordset(null::"+(files.split('_')[3]).split('.')[0]+", (select json_agg(rawdata) from raw_table where sourcefilename = '"+files+"'));"
            for i,j in jsondata.items():
                if j:
                    create_query += i+' '+l[str(type(j))]+','
                    flag = 0
                else:
                    flag = 1
        create_query  = create_query[:len(create_query)-1] + ' );'
        table_creation_queries.append(create_query)
        table_drop_queries.append(drop_query)
        table_insert_queries[files]=insert_query

#raw table drop and creation
with db.connect(**db_credentials.server_params) as conn:
    with conn.cursor() as curs:
        
        #raw table creating and inserting data
        curs.execute('DROP TABLE IF EXISTS raw_table');
        curs.execute('CREATE TABLE raw_table (rawdata json,sourcefilename varchar(100) DEFAULT NULL);');
        
        for files in files_list:
            try:
                curs.execute("copy raw_table(rawdata) from 'C:\\temp\\"+files+"' csv quote e'\\x01' delimiter e'\\x02';");
                curs.execute("update raw_table set sourcefilename='"+files+"' where sourcefilename is NULL;")
            except Error as e:
                print("Error({}): {}".format(e.pgcode, e.pgerror))
            conn.commit()

        #staging table drop
        for i in table_drop_queries:
            try:
                curs.execute(i)
                conn.commit()
                print(i)
            except Error as e:
                print("Error({}): {}".format(e.pgcode, e.pgerror))

        #staging table create
        for i in table_creation_queries:
            try:
                curs.execute(i)
                conn.commit()
                print(i)
            except Error as e:
                print("Error({}): {}".format(e.pgcode, e.pgerror))
        
#stage table insert
for i,j in table_insert_queries.items():
    try:
        with db.connect(**db_credentials.server_params) as conn:
            with conn.cursor() as curs:
                curs.execute(j)
                conn.commit()
    except :
        with db.connect(**db_credentials.server_params) as conn:
            with conn.cursor() as curs:
                curs.callproc('insert_data',((i.split('_')[3]).split('.')[0],))
                result = curs.fetchall()
                for x in result:
                    for y in x:
                        table_fields = y.split('-')
                table_fields.pop()
                f = open(i,'r',encoding = 'utf-8')
                lines = f.readlines()
                f.close()
                for line in lines:
                    s = json.loads(line)
                    query = "insert into "+(i.split('_')[3]).split('.')[0]+" values("
                    for field in table_fields:
                        if "__**" in str(field):
                            field = str(field)
                            field = field.rstrip("__**")
                            query = query + "'"+(str(s[field])).replace("'","''")+"',"
                        else:
                            query = query + "'"+str(s[field])+"',"
                    query = query.rstrip(',') + ");"
                    curs.execute(query)
                    conn.commit()


#stage table dummy data insert
with db.connect(**db_credentials.server_params) as conn:
    with conn.cursor() as curs:
        for query in dummy_data_insert:
            curs.execute(query)
            conn.commit()






    



















