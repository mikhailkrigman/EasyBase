from database import Database

bank = Database('test.sql')
bank.delete_table('table1')
table1 = bank.create_table('table1', 'num INTEGER PRIMARY KEY AUTOINCREMENT, name VARCHAR(20), age INT, height FLOAT, health BOOL')
table1.insert('Mikhail', 20, 185.0, False)
table1.insert('Max', 20, 184.2, True)
table1.insert('Andrii', 20, 183.3, True)
table1.insert('Vovanchik', 21, 175, True)

print(table1[1])
print(table1[2, 'height'])
print(table1[3, 'health', 'age', 'name'])
print(table1[4, 'name', 'age', 'height'])
print(table1[2, '*'])

table2 = bank.create_table('table2', 'num INTEGER PRIMARY KEY, v1 INT, v2 FLOAT, v3 BOOL, v4 VARCHAR(50)')
table3 = bank.create_table('table3', 'a0 INTEGER PRIMARY KEY, a1 INT, a2 FLOAT, a3 BOOL, a4 VARCHAR(50)')

table3.insert(777, 0, 1.1, True, '1st row of table3')

tables = bank.find_existing_tables()
print(tables)
for table in tables:
    print(bank.get_table_columns(table))

bank2 = Database('test.sql')
table_from_bank_2 = bank2['table3']

print(table_from_bank_2[777])
print(table3[777])
print(table_from_bank_2 is table3)

