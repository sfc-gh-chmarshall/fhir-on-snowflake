from __future__ import annotations
from snowflake.snowpark import Session
import sys
from operator import and_
from functools import reduce
import snowflake.snowpark as snowpark
from snowflake.snowpark import Window
from snowflake.snowpark.functions import col, when_matched, when_not_matched, row_number, desc, get_ignore_case, lit
from snowflake.snowpark.functions import json_extract_path_text, to_variant, get_path, is_object, is_array, not_, any_value
from snowflake.snowpark.functions import charindex, substr, regexp_replace, replace, trim, regexp_extract, length, regexp_count
from snowflake.snowpark.functions import concat, upper
from snowflake.snowpark.exceptions import SnowparkSQLException

def main(session: snowpark.Session):
    
    dict_params = {
        "source_stream_database": "HL7"
        ,"source_stream_schema": "FHIRDEMO"
        ,"source_stream": "RAW_FHIR_CLAIMS_STREAM"
        ,"target_tables": [
            {
                "target_database": "HL7"
                ,"target_schema": "FHIRDEMO"
                ,"target_table": "CLAIMS"
                ,"json_path": ""
                ,"primary_keys": ['FULLURL', 'RESPONSETIME']
                ,"ordering_columns": ["RESPONSETIME"]
                ,"recursive": True
            }
        ]
    }


    source_stream_database = dict_params["source_stream_database"]
    source_stream_schema = dict_params["source_stream_schema"]
    source_stream = dict_params["source_stream"]

    for dict_target_table_params in dict_params["target_tables"]:
        target_database = dict_target_table_params["target_database"]
        target_schema = dict_target_table_params["target_schema"]
        target_table = dict_target_table_params["target_table"]

        json_path = dict_target_table_params['json_path']
        primary_keys = dict_target_table_params['primary_keys']
        ordering_columns = dict_target_table_params['ordering_columns']
        recursive = dict_target_table_params['recursive']
        
        
        df_stream = session.table([source_stream_database, source_stream_schema, source_stream])

        df_stream_flatten = df_stream.join_table_function("flatten", input = df_stream["RESOURCE"], path = lit(json_path), recursive = lit(recursive), mode = lit('Both')).drop(["THIS", "SEQ", "INDEX"])
        
        # return df_stream_flatten

        # Counting brackets
        df_stream_flatten_bracket_count = df_stream_flatten.with_column('bracket_count', regexp_count(df_stream_flatten.path, '\\['))
        max_bracket_count = df_stream_flatten_bracket_count.group_by().max(df_stream_flatten_bracket_count.bracket_count).collect()[0][0]
        print('max_bracket_count', max_bracket_count)

        # Generate regex for brackets since we can't pick matching groups out of a match on multiple groups (POSIX ERE Regex Implementation)
        regex_pattern_brackets = r'(\.\w+)'.join([r'(\[\d*?\])' for x in range(max_bracket_count)])
        len_regex_pattern_brackets = len(regex_pattern_brackets)
        # Save regex for each path length into each record
        df_stream_flatten_bracket_count_regex = df_stream_flatten_bracket_count.with_column('regex_pattern', lit(regex_pattern_brackets).substr(1, len_regex_pattern_brackets-lit(17)*(lit(max_bracket_count)-df_stream_flatten_bracket_count.bracket_count)))
        print(regex_pattern_brackets)
        
        # start with our table we're going to add i number of elem_index to.  Have to do odd columns thanks to POSIX regex not supporting non-matched groups.
        df_stream_flatten_indexed = df_stream_flatten_bracket_count_regex
        for i in range(1, 2*max_bracket_count+1):
            if (i % 2) == 0:
                continue
            df_stream_flatten_indexed = df_stream_flatten_indexed.with_column(f'ELEM_INDEX_{i}', regexp_extract(df_stream_flatten_indexed.path, df_stream_flatten_indexed.regex_pattern, i))

        # Concatenate ELEM_INDEX_* and drop individual indices
        print('df_stream_flatten_indexed.columns', df_stream_flatten_indexed.columns)

        # return df_stream_flatten_indexed
        
        if [col(x) for x in df_stream_flatten_indexed.columns if x.startswith('ELEM_INDEX_')]:
            df_stream_flatten_indexed = df_stream_flatten_indexed.with_column('ELEM_INDICES', concat(*[col(x) for x in df_stream_flatten_indexed.columns if x.startswith('ELEM_INDEX_')]))
        else:
            df_stream_flatten_indexed = df_stream_flatten_indexed.with_column('ELEM_INDICES', lit(None))

        df_stream_flatten_indexed = df_stream_flatten_indexed.drop([col(x) for x in df_stream_flatten_indexed.columns if x.startswith('ELEM_INDEX_')])
        # Drop rows that are objects or arrays (we already have flattened key/value pairs that we will PIVOT later)
        df_stream_flatten_filtered = df_stream_flatten_indexed.where( not_(is_object(df_stream_flatten_indexed.value)) & not_(is_array(df_stream_flatten_indexed.value)) )

        # return df_stream_flatten_filtered

        # Clean up our path to remove numbers so we can GROUP (implicit) before the PIVOT properly.
        df_stream_flatten_filtered_clean_path = df_stream_flatten_filtered.with_column('CLEAN_PATH', upper(replace(trim(replace(regexp_replace(df_stream_flatten_filtered.path, "\\[\\d*\\]", ""), json_path, ""), lit('.')), lit('.'), lit("_"))))

        # return df_stream_flatten_filtered_clean_path
        
        # Build list of columns I need in final result.
        print('df_stream_flatten_filtered_clean_path.columns', df_stream_flatten_filtered_clean_path.columns)
        mycols = [df_stream_flatten_filtered_clean_path['RESOURCE'][primary_key].alias(primary_key) for primary_key in primary_keys] \
            + [col for col in df_stream_flatten_filtered_clean_path.columns if col.startswith('ELEM_INDICES')] \
            + [df_stream_flatten_filtered_clean_path.clean_path, df_stream_flatten_filtered_clean_path.value]
        print('mycols', mycols)
        # SELECT list of columns we want in final result
        df_stream_pk_path_value = df_stream_flatten_filtered_clean_path.select(mycols)

        #return df_stream_pk_path_value
        
        print('df_stream_pk_path_value', df_stream_pk_path_value.columns)
        path_cols = [c[0] for c in df_stream_pk_path_value.group_by("CLEAN_PATH").agg(any_value("CLEAN_PATH")).collect()]
        print('path_cols', path_cols)
        # Build aliases to get rid of quotes around "cols" for the pivot.
        #cols_alias = [col(json_path_pk).alias(json_path_pk) for json_path_pk in json_path_pks] + [col("'" + c + "'").alias(c) for c in cols]
        cols_alias = [col(primary_key).alias(primary_key.upper()) for primary_key in primary_keys if primary_key.upper() not in path_cols] + [col("'" + c + "'").alias(c.upper()) for c in path_cols]
        print('cols_alias', cols_alias)
        
        # Pivot Key/Value pairs.  NB! Pivot implicitly GROUP_BY() on all other columns.
        df_source = df_stream_pk_path_value \
            .pivot("CLEAN_PATH", path_cols).min("VALUE") \
            .drop('ELEM_INDICES') \
            .select(cols_alias)
        
        # return df_source
        #.where(df_source.umvid == 58372830)

        # Create our Window to grab latest record based on a primary_key and timestamp
        w = Window.partitionBy([x.upper() for x in primary_keys]).orderBy(*[desc(c) for c in ordering_columns])
    
        # # Use row_number() and our Window to get a Dataframe with the latest primary_key
        df_source_latest = df_source.withColumn("rn",row_number().over(w)).filter(col("rn") ==1).drop("rn")
    
        df_source_latest.write.mode("append").save_as_table(target_table,column_order="name")
        print("New table:", session.table(target_table).show())

        # return source
        
        
    #session.sql('''COMMIT''').collect()
    return 'Done!'

# For local debugging. Be aware you may need to type-convert arguments if
# you add input parameters
if __name__ == "__main__":
    # Add the utils package to our path and import the snowpark_utils function
    import os, sys
    current_dir = os.getcwd()
    parent_parent_dir = os.path.dirname(os.path.dirname(current_dir))
    sys.path.append(parent_parent_dir)
    parent_dir = os.path.dirname(current_dir)
    sys.path.append(parent_dir)
    sys.path.append(current_dir)

    from utils import snowpark_utils
    session = snowpark_utils.get_snowpark_session()
    if len(sys.argv) > 1:
        print(main(session, *sys.argv[1:]))  # type: ignore
    else:
        print(main(session))  # type: ignore
    session.close()
