from flask import Flask, render_template
import sqlite3
app = Flask(__name__)
conn = sqlite3.connect('cafes.db')
c = conn.cursor()
c.execute('''
    CREATE TABLE cafes (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL,
        wifi BOOLEAN,
        power BOOLEAN
    )
''')
conn.commit()
conn.close()
@app.route('/')
def index():
    conn = sqlite3.connect('cafes.db')
    c = conn.cursor()
    c.execute('SELECT * FROM cafes')
    cafes = c.fetchall()
    conn.close()
    return render_template('index.html', cafes=cafes)
