import sqlite3

# redefine sqlite3 commends to use them simplier later
# also allows better determing of occured errors
# more possibilities to handle exceptions 
class DatabaseInterface:
    def __init__(self, filepath: str,
                 connection: sqlite3.Connection = None,
                 cursor: sqlite3.Cursor = None):
        
        self._filepath = filepath
        self._connection = connection
        self._cursor = cursor
    
    def connect(self):
        ''' Creates connection to database. Cursor will be created automatically '''
        self._connection = sqlite3.connect(self._filepath)
        self._cursor = self._connection.cursor()

    def connection(self) -> bool:
        ''' Check if connection already exists '''
        return bool(self._connection)
    
    def disconnect(self):
        ''' Close Cursor and Connection '''
        self._cursor.close()
        self._connection.close()
        self._cursor = None
        self._connection = None

    # decorator to do all with new connection
    def _on_connection(func):
        ''' Decorator for functions, that have to work only with connection to database 
            Here is used to call combination of functions, f.e. execute() + commit()
            that represent any SQL-query together
        '''
        def inner(self, command):
            if not self.connection():
                self.connect()
                res = func(self, command)
                self.disconnect()
                return res
            else:   # unexpected connection
                return None
        return inner
    
    def execute(self, command: str, values: tuple = None) -> bool:
        ''' Execute any SQL query, but doesn't change the original database.
            If you want to see changes in the original table or get data from it
            Use commit() or fetch- functions after this one.

            Ensure the connection is on (exists) before using this function.
            After connection is on and execution is done it has to be turned off

            Better to use any of these funcitons:
            - execute_and_commit()
            - execute_and_fetchone()
            - execute_and_fetchall()
        '''
        try:
            if values:
                self._cursor.execute(command, values)
            else:
                self._cursor.execute(command)
            return True
        
        except Exception as ex:
            print(ex)
            return False
        
    def commit(self):
        ''' Saves changes in database after execute() '''
        self._connection.commit()

    def fetchone(self):
        ''' Returns the first found row (column of first found row) as tuple 
            after any SELECT query
        '''
        return self._cursor.fetchone()
    
    def fetchall(self):
        ''' Returns all found rows (column of all found rows) as tuple 
            after any SELECT query
        '''
        return self._cursor.fetchall()

    @_on_connection
    def execute_and_commit(self, command) -> bool:
        ''' Execute any SQL query and save changes in database.
            Turns on and off connection automatically
        '''
        if self.execute(command):
            self.commit()
            return True
    
        return False

    @_on_connection
    def execute_and_fetchone(self, command):
        ''' Execute any SQL SELECT-query and return selected data.
            Turns on and off connection automatically
        '''
        if self.execute(command):
            data = self.fetchone()
            return data
        
        return None
    
    @_on_connection
    def execute_and_fetchall(self, command):
        ''' Execute any SQL SELECT-query and return selected data.
            Turns on and off connection automatically
        '''
        if self.execute(command):
            data = self.fetchall()
            return data
        
        return None
    
class Table(DatabaseInterface):
    ''' This class represents SQL table and supports usual SQL commands to work with it.
        Use [] - operator to get any column value from table(check description how to use)
        Use [] - operator to set(update) any column in the table

    '''
    def __init__(self, filepath,
                 table_name: str ='',
                 columns: tuple[str] = (),
                 primary_key: str = ''):
        super().__init__(filepath)

        self.name = table_name
        self._columns = columns
        self._primary_key = primary_key if primary_key else columns[0]  # eventually corrections because columns[0] may contain datatype

    def insert(self, *values):
        ''' Insert values into table with self._columns '''
        command = f'''INSERT INTO {self.name} {self._columns} VALUES {values}'''
        self.execute_and_commit(command)

    # not *args because magic method __getitem__ takes only 2 arguments
    # the rest will be wrapped to tuple automatically
    def __getitem__(self, args):
        ''' :args - tuple of arguments: key and columns (args == (key, columns))
            Returns values stored in columns of row where self._primary_key == key 
            Give only primary_key value or set only one column = '*' to get the whole row
        '''
        if type(args) is not tuple:  # key and to return column are given
            key = args
            columns = '*'
        else:
            key, *columns = args

        if type(key) is str:
            key = "'" + key + "'"

        condition = f'''{self._primary_key} = {key}'''
        command = f'''SELECT {columns} FROM {self.name} WHERE '''
        
        # delete all troubling symbols from command
        # these hav occured while formatting values to f-string
        command = command.replace('[', '')
        command = command.replace(']', '')
        command = command.replace('(', '')
        command = command.replace(')', '')
        command = command.replace("'", '')

        # add select condition
        command += condition

        data = self.execute_and_fetchone(command)

        return data[0] if data and len(data) == 1 else data
    
    def __setitem__(self, args, new_value):
        ''' :args - key and column where is to update property
            :new_value - new property value

            Sets new value in selected column for row 
            where self._primary_key == key
        '''
        key, column = args

        if type(new_value) is str:
            new_value = "'" + new_value + "'" 

        command = f'''UPDATE {self.name} SET {column} = ({new_value}) WHERE {self._primary_key} = ({key})'''

        self.execute_and_commit(command)       

    def get_columns(self) -> tuple[str]:
        return self._columns
    
    def get_primary_key(self) -> str:
        return self._primary_key

    def get_by(self, key_property, key_value, column = '*'):
        ''' Returns value stored in column of 
            row where key_property == key_value 
            Call function without column param or
            set column = '*' to get the whole row
        '''

        if type(key_value) is str:
            key_value = "'" + key_value + "'" 

        command = f'''SELECT {column} FROM {self.name} WHERE {key_property} = ({key_value})'''

        data = self.execute_and_fetchall(command)[-1]
        return data if column == '*' or data == None else data[0]

    def update_column(self, key_value, update_property, new_value):
        ''' Updates update_property with new_value in
            row found by self._primary_key with key_value
        '''
        if type(new_value) is str:
            new_value = "'" + new_value + "'" 

        command = f'''UPDATE {self.name} SET {update_property} = {new_value} WHERE {self._primary_key} = {key_value}'''
        self.execute_and_commit(command)

    def delete_row(self, key_property, compare_value) -> bool:
        if type(compare_value) is str:
            compare_value = "'" + compare_value + "'" 

        command = f'''DELETE FROM {self.name} WHERE {key_property} = {compare_value}'''
        
        self.execute_and_commit(command)


class Database(DatabaseInterface):

    # -------- Constructor, Destructor and overloaded operators ----------------------------------------------------------------------------------------------- #

    def __init__(self, filepath, clear = False):
        super().__init__(filepath)
        self._tables: dict = {}
        
        # User has 2 opportunities when creating new database:
        # 1) Find all already existing tables and work later with them
        # 2) Create completely new database even if smth already exists there
        if clear:
            tables = self.get_existing_tables()
            for table in tables:
                self.delete_table(table)
        else:
            self._define_existing_tables()

    def __getitem__(self, table_name: str) -> Table:
        if self._tables:
            return self._tables[table_name] if table_name in self._tables else None
        return None

    def __del__(self):
        if self.connection():   # error case 
            self.disconnect()

    # -------- PUBLIC METHODS --------------------------------------------------------------------------------------------------------------------------------- #

    def create_table(self, table_name: str, columns_str: str) -> Table:
        '''  Creates new Table in Database if it not exists.
             It is not the best way to create tables for bot. Better to use
             create_users_table() or create_orders_table()

            :table_name - Name for new Table
            :columns_str - Part of SQL-command to create new Table, that contains
             column names with datatype specifications

            - Example to call: 
             database.create_table('New Table', 'arg1 VARCHAR(20) PRIMARY KEY, arg2 INT, arg3 BOOL')
        '''
        table_name = table_name.strip()

        if table_name not in self._tables: # If table not exists
            columns_str = columns_str.strip()

            command = f'''CREATE TABLE IF NOT EXISTS {table_name} ({columns_str})'''
            self.execute_and_commit(command)

            columns, primary_key = self.__get_column_names_from_sql(columns_str)
            
            new_table = Table(self._filepath,
                              table_name, columns, primary_key)

            # append table to tables list
            self._tables[table_name] = new_table
        return self._tables[table_name]

    def get_existing_tables(self):
        ''' Returns tuple with names of tables, that already exist in the Database '''

        command = '''SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' '''
        res = self.execute_and_fetchall(command)    # returns list of tuples each with only one element(name of table)
                                                    # [('name1',), ('name2',), ('name3',), etc]
                                                    # but we want only list of names
        tables = []
        for table in res:
            tables.append(table[0])
        
        return tuple(tables)
    
    def get_table_columns(self, table_name: str):
        ''' Returns tuple with name of columns, that table with table_name has
            and name of the main column for this table (primary key)
        '''
        # if database is already initialised (call from existing object)
        if table_name in self._tables:
            table = self[table_name]
            return table.get_columns(), table.get_primary_key() if table else None, None
        
        # if new object is creating we check for existing tables and columns via sql_master
        else:   
            command = f'''SELECT sql FROM sqlite_master WHERE type='table' AND name='{table_name}';'''
            
            sql_query = self.execute_and_fetchone(command)
            if sql_query:
                sql_query = sql_query[0]    # if fetchone has smth found it returns tuple with only one value == sql

                # sql_query is always looks like: CREATE TABLE table_name (arg1 type, arg2 type, arg3 type, etc)
                # we need only part of string, that is in brackets after table_name, but without these brackets
                columns_str = sql_query.split(table_name)[-1].strip()[1:-1] # to avoid first and last bracket

                columns, primary_key = self.__get_column_names_from_sql(columns_str)

            else:
                columns = None
                primary_key = None

            return columns, primary_key

    def delete_table(self, table_name):
        command = f'''DROP TABLE {table_name};'''
        self.execute_and_commit(command)

    def clear_all(self):
        ''' Deletes all tables in the database '''
        for table in self._tables:
            self.delete_table(table)

    # -------- PROTECTED METHODS ------------------------------------------------------------------------------------------------------------------------------ #

    def _define_existing_tables(self):
        existing_tables = self.get_existing_tables()
        for table in existing_tables:
            columns, primary_key = self.get_table_columns(table)
            new_table = Table(self._filepath,
                              table, columns, primary_key)

            # append table to tables list
            self._tables[table] = new_table
            
    # -------- PRIVATE METHODS -------------------------------------------------------------------------------------------------------------------------------- #
        
    @staticmethod
    def __get_column_names_from_sql(columns_str):
        ''' Create tuple with column names from part of SQL-query.

            :columns_str - Part of SQL-command to create new Table, that contains
             column names with datatype specifications

            :Example of columns_str - 'arg1 VARCHAR(20) PRIMARY KEY, arg2 INT, arg3 BOOL'

        '''
        columns = []
        primary_key = ''
        for column in columns_str.split(','):
            column = column.strip()
            if 'primary key' in column or 'PRIMARY KEY' in column:
                primary_key = column.split()[0]

            if 'autoincrement' not in column and 'AUTOINCREMENT' not in column:
                columns.append(column.split()[0])
        columns = tuple(columns)
        return columns, primary_key
