def upsert_df(df: "pd.DataFrame", table_name: str, pk: str, user: str, password: str, host: str, port: str, database: str,schema: str):
    """Upserts a pandas df into a specified table
    Args:
        df (pd.DataFrame): DataFrame to upsert into
        table_name (str): Name of table to upsert into
        pk (str): Primary key name
        user (str): Database username
        password (str): Database password
        host (str): Database host
        port (str): Database port
        database (str): Database name
    """
    from sqlalchemy import create_engine, Table, MetaData
    from sqlalchemy.dialects.postgresql import insert
    import numpy as np

    engine = create_engine(f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}")

    # Creating a MetaData object and binding it to the specified table
    metadata = MetaData(schema=schema)
    orders_table = Table(table_name, metadata, autoload=True, autoload_with=engine)

    columns = list(df.columns)

    # Replacing nans to None (null in the db) then converting to list of dicts
    # Nulls will not overwrite existing values
    records = df.replace({np.nan:None}).to_dict(orient='records')

    # Initiate insert operation
    insert_stmt = insert(orders_table).values(records)

    # Mapping conflicting columns to excluded (new) values
    update_dict = {c: insert_stmt.excluded[c] for c in columns if c != pk}

    if type(pk) is not list:
        pk = [pk]

    # Adding conflict map to the insert operation
    update_stmt = insert_stmt.on_conflict_do_update(
        index_elements=pk,
        set_=update_dict
    )

    # Executing and committing operation
    with engine.connect() as conn:
        conn.execute(update_stmt)

    print(f"{len(records)} upserted into {table_name}")

def replace_table(user:str,password:str,host:str,port:str,database:str,schema:str,df,table_name:str):
    """Replaces a table in the warehouse using sqlalchemy and inserts a dataframe into it.
    Args:
        table_name (str): Name of table to clear
    """
    from sqlalchemy import create_engine, Table, MetaData
    from sqlalchemy.dialects.postgresql import insert
    import numpy as np

    engine = create_engine(f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}")

    # Creating a MetaData object and binding it to the specified table
    metadata = MetaData(schema=schema)
    orders_table = Table(table_name, metadata, autoload_with=engine)

    # Replacing nans to None (null in the db) then converting to list of dicts
    # Nulls will not overwrite existing values
    records = df.replace({np.nan:None}).to_dict(orient='records')

    # Replacing nans to None (null in the db) then converting to list of dicts
    records = df.replace({np.nan:None}).to_dict(orient='records')

    # Initiate insert operation
    insert_stmt = insert(orders_table).values(records)

    # Clear the table
    with engine.connect() as conn:
        conn.execute(orders_table.delete())
        print(f"Table {table_name} cleared")
        conn.execute(insert_stmt)
        print(f"{len(records)} inserted into {table_name}")
        conn.commit()