#code for extraction
import pandas as pd
import json
from sqlalchemy import create_engine,text

# create mysql database commection
mysql_engine = create_engine('mysql+pymysql://root:Admin%40143@localhost:3308/enterpriseretaildwh')


# Create Oracle engine
oracle_engine = create_engine('oracle+cx_oracle://system:admin@localhost:1521/xe')


def load_csv_mysql(file_path,table_name):
    df = pd.read_csv(file_path)
    df.to_sql(table_name,mysql_engine,if_exists='replace',index=False)

def load_xml_mysql(file_path,table_name):
    df = pd.read_xml(file_path,xpath='.//item')
    df.to_sql(table_name,mysql_engine,if_exists='replace',index=False)

def load_json_mysql(file_path,table_name):
    df = pd.read_json(file_path)
    df.to_sql(table_name,mysql_engine,if_exists='replace',index=False)

def load_oracle_to_mysql(query,table_name):
    df = pd.read_sql(query,oracle_engine)
    df.to_sql(table_name,mysql_engine,if_exists='replace',index=False)
    

if __name__=="__main__":
    load_csv_mysql('sales_data.csv','staging_sales')
    load_csv_mysql('product_data.csv','staging_product')
    load_xml_mysql('inventory_data.xml','staging_inventory')
    load_json_mysql('supplier_data.json','staging_supplier')
    load_oracle_to_mysql("select * from stores",'staging_store')
    
