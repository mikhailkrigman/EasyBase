from database import Database

bank = Database('main.sql')
table = bank.create_table('myTable', 'num INTEGER PRIMARY KEY AUTOINCREMENT, name VARCHAR(20), age int, height float')
table.insert('Mika', 20, 185.0)
table.insert('MaX', 20, 183.5)
table.insert('Serj', 26, 184.3)

print(table[1])
print(table[2, 'height'])
print(table[3, 'age', 'height', 'name'])
print(table[2, '*'])