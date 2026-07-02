import requests 
import json
from datetime import datetime

headers = {
    'Content-Type':'application/json'
}

def insert_values(table_name,values,extra_queries = ""):
    columns = list(values[0].keys())

    col_clause = '( ' + ', '.join([f" '{item}' " for item in columns]) + ' )'
    
    row_clause = [ 
            '(' + ', '.join([ f"{row[key]}" for key in columns]) + ' )'
            for row in values
    ]
    return (col_clause,row_clause)

res,res2 = insert_values("T",
    [
        {"A":100,"B":300,"TEST":None},
        {"A":400,"B":700,"TEST":None}
    ])
    
cur= datetime.now()
print(cur)
# print(res2)