class DataLake:
    """
    A class used to represent a datalake for raw storage of unstructured data
    ...

    Attributes
    ----------
    _ak : str
        The access key for s3 bucket configuration
    _sak : str
        The secret access key for s3 bucket configuration

    Methods
    -------
    connect_to_s3()
        Connects to a bucket in S3
    extract_json(object)
        Extracts the json data from file
    connect_to_datalake()
        Gets the mongo client
    create_db(name, conn)
        Creates a database in the mongo client connection
    create_collection(name, db)
        creates a collection in the previously created database in the mongo client connection
    insert_data(collection, data)
        Inserts data into the mongo collection
    initial_config_mongo()
        Makes the initial configuration (creation of db, collection) for the mongo datalake
    execute()
        Executes all the steps for the datalake
    """
    def __init__(self, access_key, secret_access_key):
        self._ak = access_key
        self._sak = secret_access_key

    def connect_to_s3(self):
        """
        Connects to a bucket in S3

        Returns
        ----------
        bucket
            Bucket object from s3
        """
        import boto3

        session = boto3.Session(
            aws_access_key_id=self._ak,
            aws_secret_access_key=self._sak
        )

        s3 = session.resource('s3')

        bucket = s3.Bucket('de-tech-assessment-2022')
        return bucket

    def extract_json(self, object):
        """
        Extracts the json data from file

        Parameters
        ----------
        object
            file to be loaded as json

        Returns
        ----------
        bucket
            Bucket object from s3
        """
        import json
        data = json.load(object)
        return data

    def connect_to_datalake(self):
        """
        Gets the mongo client

        Returns
        ----------
        conn
            Mongo client
        """
        from pymongo import MongoClient
        conn = MongoClient()
        return conn

    def create_db(self, name, conn):
        """
        Creates a database in the mongo client connection

        Parameters
        ----------
        name
            name of the database
        conn
            connection object to mongo

        Returns
        ----------
        db
            database object from mongo
        """
        db = conn.name
        return db

    def create_collection(self, name, db):
        """
        creates a collection in the previously created database in the mongo client connection

        Parameters
        ----------
        name
            name of the database
        db
            database object

        Returns
        ----------
        collection
            collection object from mongo
        """
        collection = db.name
        return collection

    def insert_data(self, collection, data):
        """
        Inserts data into the mongo collection

        Parameters
        ----------
        collection
            name of the collection
        data
            the data to be loaded

        """
        collection.insert_one(data)

    def initial_config_mongo(self):
        """
        Makes the initial configuration (creation of db, collection) for the mongo datalake

        Returns
        ----------
        conn
            connection to mongo datalake
        db
            database object
        """
        conn = self.connect_to_datalake()
        db = self.create_db("gps", conn)
        collection = self.create_collection("raw", db)
        return collection, db

    def execute(self):
        """
        Executes all the steps for the datalake
        """
        bucket = self.connect_to_s3()
        collection, db = self.initial_config_mongo()

        for objects in bucket.objects.filter(Prefix="data/"):
            data = self.extract_json(objects)
            self.insert_data(collection, data)


class DataWarehouse(DataLake):
    """
    A class used to represent a data warehouse for processed storage of structured data
    Is a child class of DataLake
    ...

    Methods
    -------
    read_from_db(database, collection, filters)
        Reads from mongo database
    connect_to_mysql()
        Creates connection to mysql
    create_db_mysql(cursor, data, name)
        Creates database for mysql
    get_mysql_field(type, length)
        Gets the type of field from data
    create_query(df, table_name)
        Creates a creation query given the data and data types from a dataframe
    load_transformed(conncursor, data)
        Inserts data into the mysql table
    execute()
        Executes all the steps for the data warehouse
    """

    def read_from_db(self, database, collection, filters):
        """
        Reads from mongo database

        Parameters
        ----------
        database
            name of the database
        collection
            name of the colleciton

        filters
            filters for the find method for the mongo extraction

        Returns
        ----------
        collection
            dataframe which contains the information read in mongo
        """
        import pandas as pd

        def from_mongo(database, collection: str, filters: dict = {}) -> list:
            return [
                item for item in database[collection].find(filters)
            ]

        return pd.DataFrame(from_mongo(database, collection))

    def connect_to_mysql(self):
        """
        Creates connection to mysql

        Returns
        ----------
        cursor
            connection cursor to mysql
        """
        import mysql

        connection = mysql.connector.connect(
            host='localhost',
            user='root',
            password='root'
        )

        cursor = connection.cursor()
        return cursor

    def create_db_mysql(self, cursor, data, name):
        """
        Creates database for mysql

        Parameters
        ----------
        cursor
            connection cursor to mysql
        data
            data to be inserted
        name
            name of the table

        """
        creation_query = self.create_query(data, name)
        cursor.execute(creation_query)

    def get_mysql_field(self, type, length: int = 0):
        """
        Gets the type of field from data

        Parameters
        ----------
        type
            type of data
        length
            length for the column to be created in mysql

        Returns
        ----------
        field
            name of field type
        """
        type = str(type).lower()
        if 'int' in type:
            field = 'INT'
        elif 'float' in type:
            field = 'DOUBLE'
        elif 'object' in type:
            if length == 0:
                length = 100
            elif length > int(10e3):
                length = int(10e3)
            field = f'VARCHAR({length})'

        elif 'datetime' in type:
            field = 'DATETIME'
        elif 'date' in type:
            field = 'DATE'
        elif 'bool' in type:
            field = 'BOOL'
        else:
            raise (ValueError(f"Unknown type '{type}'"))

        return field

    def create_query(self, df, table_name: str) -> str:
        """
        Creates a creation query given the data and data types from a dataframe

        Parameters
        ----------
        df
            data to be inserted
        table_name
            name of the tabl

        Returns
        ----------
        query_format
            Creation quety
        """
        types = ",\n".join(
            [
                '{field} {type}'.format(
                    field=field,
                    type=self.get_mysql_field(type, length=df[field].astype(str).str.len().max())
                ) for field, type in df.dtypes.items()
            ]
        )
        query_format = 'CREATE TABLE IF NOT EXISTS {table} ({fields});'

        return query_format.format(table=table_name, fields=types)

    def load_transformed(self, conncursor, data):
        """
        Inserts data into the mysql table

        Parameters
        ----------
        conncursor
            cursor connection to the mysql table
        data
            data to be inserted
        """
        insert_query = f"""
            INSERT INTO {table_name} ({columns})
            VALUES ({values})
            ON DUPLICATE KEY UPDATE 
            disbursement_uid=disbursement_uid;
        """
        with conncursor as cursor:
            cursor.executemany(
                insert_query.format(
                    table="gps_data"
                ),
                list(
                    data.itertuples(
                        index=False,
                        name=None
                    )
                )

            )

        conncursor.commit()

    def execute(self):
        """
        Executes all the steps for the data warehouse
        """
        collection, db = self.initial_config_mongo()
        data = self.read_from_db(db, collection, filters={})
        cursor = self.connect_to_mysql()
        self.create_db_mysql()
        self.load_transformed(cursor, data)


def pipeline(access_key, secret_access_key):
    gps_datalake = DataLake(
        "access key",
        "secret access key"
    )

    gps_data_warehouse = DataWarehouse(
        "access key",
        "secret access key"
    )

    gps_datalake.execute()
    gps_data_warehouse.execute()


if __name__ == '__main__':

    pipeline("ak", "sak")

