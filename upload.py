import sqlite3 as sq
async def upload_keys(k):
    with open('keys.txt', 'r') as file:
        keys = file.readlines()
        for key in keys:
            if key.startswith('NS7'):
                key = key.split(' ')[1]
                with sq.connect('database.db') as con:
                    cur = con.execute('INSERT INTO keys (key, duration, SOLD, buyer_id) VALUES (?, ?, ?, ?)', (key, 7, 0, None))
                    con.commit()

