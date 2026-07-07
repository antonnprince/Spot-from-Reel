test = [{"a":1,"b":2},{"c":3,"d":4}]

def some_func(schema_name,table_name, values, extra_queries = ""):
        
        if type(values)==list:
                actual_schema = values[0]
        

                res = ", ".join(
                        f"( { ', '.join(map(str, item.values()))}   )"
                        for item in test
                )
                        

                # query_string = f"""
                #         INSERT INTO {schema_name}.{table_name}
                #         {   
                #                 '( '
                #         +' ,'.join([f" {key}" for key in values.keys()])
                #         +' )'
                #         }
                #         VALUES
                #         {
                #         '( ' + 
                #         ' ,'.join([
                #                 f" '{values[key]}'" for key in values.keys()
                #         ])
                #         + ' )'
                #         }
                # """

        else:
                print("HI")
        
        print(res)

some_func("schema","table",test)
