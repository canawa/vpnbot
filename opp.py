import sqlite3 as sq
from datetime import datetime
import asyncio
import time
with sq.connect('test.db') as con:
    cur = con.cursor()
    today = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    cur.execute('CREATE TABLE IF NOT EXISTS test (id INTEGER PRIMARY KEY, date TEXT)')
    cur.execute('INSERT INTO test (date) VALUES (?)', (today,))
    con.commit()
    time.sleep(3)
    cur.execute('SELECT date FROM test ORDER BY rowid DESC LIMIT 1')
    result = cur.fetchall()
    today = datetime.now()
    if result[0] <= today:
        print('Сравнение успешно')
    else:
        print('Сравнение не успешно')