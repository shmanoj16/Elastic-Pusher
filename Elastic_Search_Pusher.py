import pandas as pd
import numpy as np
import os
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
import json
import sys
def elastic_kibana_push(filename,sheet_name=None,column_upto=None,index_name=None,host='localhost',port=9200):
    """
    filenmae --> input_file (xlsx or csv)
    """
    if filename.lower().endswith('.csv'):
        df=pd.read_csv(filename,encoding='iso8859')
    if filename.lower().endswith('.xlsx') or filename.lower().endswith('.xls'):
        df = pd.read_excel(filename,sheet_name=sheet_name)
    print('col_upto',column_upto)
    if column_upto:
        df=df.iloc[:,:column_upto]
    
    valid=[x for x in df.columns if not str(x).startswith('Unnamed')]

    df.columns

    df=df[valid]

    df=df.dropna(how='all')
    
    #get numeric columns
    numeric_columns=[]
    object_columns=[]
    for col in df.columns:
        try:
            print(col)
            df[col]=df[col].replace('blanks',np.nan)
            df[col]=pd.to_numeric(df[col])
            print(f'{col} is numeric')
            numeric_columns.append(col)
        except Exception:
            object_columns.append(col)


    df=df.dropna(subset=numeric_columns)

    df=df.fillna('blanks')
    #df to dict
    records=df.to_dict(orient='records')


    map_dtype={'object':'keyword','float64':'float'}

    type_df=df.dtypes.to_frame()
    type_df=type_df.rename(columns={0:'type'})
    type_df['type']=type_df['type'].astype(str).map(map_dtype)

    #dtypes df to dict
    d=type_df.astype(str).to_dict(orient='index')

    mapping = {

            "mappings": {
                "type1": {
                    "properties":{}
                }
            }
        }

    mapping['mappings']['type1']['properties']=d

    #convert to json
    mapping=json.dumps(mapping,indent=4)

    #pushing to elastic search
    es = Elasticsearch([{'host':host,'port':port}])
    es.indices.create(index=index_name,body=mapping, ignore=400 )
    
    bulk(es,records, index=index_name, doc_type='type1')
    return "Success"


if __name__=='__main__':
    filename=sys.argv[1]
    sheet_name=sys.argv[2]
    index_name=sys.argv[3]
    elastic_kibana_push(filename,sheet_name=sheet_name,index_name=index_name)
    
