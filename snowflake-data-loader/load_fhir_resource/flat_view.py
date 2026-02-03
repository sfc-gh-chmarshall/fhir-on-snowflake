import snowflake.snowpark as snowpark

sql_reserved_default = ['ALL', 'ALTER', 'AND', 'ANY', 'AS', 'BETWEEN', 'BY', 'CHECK', 'COLUMN', 'CONNECT', 'CREATE', 'CURRENT', 'DELETE', 'DISTINCT', 'DROP', 'ELSE', 'EXISTS', 'FOLLOWING', 'FOR', 'FROM', 'GRANT', 'GROUP', 'HAVING', 'IN', 'INSERT', 'INTERSECT', 'INTO', 'IS', 'LIKE', 'NOT', 'NULL', 'OF', 'ON', 'OR', 'ORDER', 'REVOKE', 'ROW', 'ROWS', 'SAMPLE', 'SELECT', 'SET', 'START', 'TABLE', 'TABLESAMPLE', 'THEN', 'TO', 'TRIGGER', 'UNION', 'UNIQUE', 'UPDATE', 'VALUES', 'WHENEVER', 'WHERE', 'WITH']

def sql_reserved_replace(string):
    if string.upper() in sql_reserved_default:
        return string + "_"
    else:
        return string

path_name = "regexp_replace(regexp_replace(f.path,'\\\\[(.+)\\\\]'),'(\\\\w+)','\"\\\\1\"')"
attribute_type = "DECODE (substr(typeof(f.value),1,1),'A','ARRAY','B','BOOLEAN','I','FLOAT','D','FLOAT','STRING')"
alias_name = "regexp_replace(regexp_replace(f.path,'\\\\[(.+)\\\\]'),'[^a-zA-Z0-9]','_')"


def test_local(session: snowpark.Session):
    # This code is just here so the python block can be run in a python worksheet. 
    # Use this function as the handler to test the code
    main(session, 'event_heartbeat_raw_green', 'data', 'EVENT_HEARTBEAT_SF6')

def main(session: snowpark.Session, TABLE_NAME, COL_NAME, VIEW_NAME): 
    col_list = ""
    element_query = f"SELECT DISTINCT \n" \
                f"{path_name} AS path_name, \n" \
                f"{attribute_type} AS attribute_type, \n" \
                f"{alias_name} AS alias_name \n" \
                f"FROM \n" \
                f"{TABLE_NAME}, \n" \
                f"LATERAL FLATTEN({COL_NAME}, RECURSIVE=>true) f \n" \
                f"WHERE TYPEOF(f.value) != 'OBJECT' \n" \
                f"AND NOT contains(f.path,'[') "      # This prevents traversal down into arrays;

    
    # Run the query...
    element_res = session.sql(element_query).collect()
    print(element_query)
    
    # ...And loop through the list that was returned
    for row in element_res:
        # Add elements and datatypes to the column list
        # They will look something like this when added:
        # col_name:"name"."first"::STRING as name_first,
        # col_name:"name"."last"::STRING as name_last
        # print(row.as_dict())
        # print(row[1])
        # print(row[3])

        if col_list != "":
            col_list = col_list + ", \n"
        col_list = col_list + COL_NAME + ":" + row[0]                   # Start with the element path name
        col_list = col_list + "::" + row[1]                             # Add the datatype
        col_list = col_list + " as " + sql_reserved_replace(row[2])                           # And finally the element alias 
    
    # Now build the CREATE VIEW statement
    view_ddl = f"CREATE OR REPLACE SECURE VIEW {VIEW_NAME} AS \n" \
               f"SELECT \n{col_list}\n" \
               f"FROM {TABLE_NAME};"

    print(view_ddl)
    
    # Now run the CREATE VIEW statement
    df_create = session.sql(view_ddl).collect()
    return 'ok'

if __name__ == '__main__':
    import os, sys
    from snowflake.snowpark import Session
    
    connection_name = os.getenv("SNOWFLAKE_CONNECTION_NAME", "default")
    session = Session.builder.config("connection_name", connection_name).create()

    if len(sys.argv) > 3:
        print(main(session, sys.argv[1], sys.argv[2], sys.argv[3]))
    else:
        print(main(session, 'RAW_FHIR_RESOURCES', 'RESOURCE', 'FLAT_FHIR_VIEW'))

    session.close()