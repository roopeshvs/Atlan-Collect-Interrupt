# External Package(s) Used: Flask
# Sample Data Used: Sentimental Analysis on Quality of Stack Overflow Questions
# Sample Data URL: https://www.kaggle.com/imoore/60k-stack-overflow-questions-with-quality-rate

# Although these long tasks seem to ask for distributed task queues like Celery, 
# It is possible to solve by only using the inbuilt Python modules.
# time.sleep(x) values are used to simulate longer tasks.

# This implementation is focused on an event-driven approach.
# Have tried to limit DB hits but yes, further optimizations and code cleaning are possible.

# Note: SQLite supports only one write request at a time, 
# That is the need to maintain transaction states in a separate DB,
# And connections to be opened and closed more frequently.

from flask import Flask, request, render_template, url_for, jsonify
import sqlite3 as sql
import csv, uuid, io
import time


app = Flask(__name__)

@app.route('/create', methods=['POST'])
def file_read():
    """
    Gets a CSV File as input and Creates Rows in DB based on them.
    Supports Pause, Resume, and Stop Operations.
    Simulates Example 1 & 3.
    """
    if request.method == 'POST':
        file = request.files.get('file')
        id = uuid.uuid4()
        print(id)
        row = (str(id), 'PROGRESS', 'WRITE')
        co5 = sql.connect('states.db')
        c5 = co5.cursor()
        c5.execute("""CREATE TABLE IF NOT EXISTS transactions (id varchar NOT NULL PRIMARY KEY, state varchar, db_operation varchar)""")
        c5.execute("""insert into transactions values (?,?,?);""", row)
        c5.close()
        co5.commit()
        co5.close()
        co = sql.connect("database.db")
        c1 = co.cursor() 
        c1.execute("""CREATE TABLE IF NOT EXISTS stackoverflow(id NUMBER NOT NULL PRIMARY KEY, title varchar, body varchar, tags varchar, created_on varchar, quality varchar)""")
        co.commit()
        co.close()
        stream = io.StringIO(file.stream.read().decode("UTF8"), newline=None)
        reader = csv.reader(stream)
        co3 = sql.connect('database.db')
        c3 = co3.cursor()
        for index, field in enumerate(reader):
            time.sleep(0.001)
            c3.execute("INSERT INTO stackoverflow VALUES (?,?,?,?,?,?);", field)
            if index % 500 == 0:
                co6 = sql.connect('states.db')
                c6 = co6.cursor()
                c6.execute("SELECT state from transactions where id = (?)", (str(id),))
                for tup in c6:
                    for state in tup:
                        if state == 'STOPPED':
                            co3.rollback()
                            co6.close()
                            co3.close()
                            response = {'Task ID' : id, 'State': 'STOPPED', 'Info': 'Interrupted by User'}
                            return jsonify(response)
                        elif state == 'PAUSED':
                            while True:
                                time.sleep(1)
                                c6.execute("SELECT state from transactions where id = (?)", (str(id),))
                                new_state = 'PAUSED'
                                for tup in c6:
                                    for state in tup:
                                        if state == 'PROGRESS':
                                            new_state = 'PROGRESS'
                                        elif state == 'STOPPED':
                                            co3.close()
                                            co6.close()
                                            response = {'Task ID' : id, 'State': 'STOPPED', 'Info': 'Interrupted by User'}
                                            return jsonify(response)
                                if new_state == 'PROGRESS':
                                    break
        co6.close()
        co3.commit()
        co3.close()
        co4 = sql.connect('states.db')
        c4 = co4.cursor()
        c4.execute("update transactions set state='COMPLETED' where id = (?)", (str(id),))
        co4.commit()
        co4.close()
        response = {'Task ID' : id, 'State': 'COMPLETED'}
        return jsonify(response)

@app.route('/create')
def uploadfile():
    """
    File Upload via Browser
    """
    return render_template('upload_files.html')

@app.route('/export')
def export():
    """
    Exports the data in stackoverflow table as a CSV File.
    Supports Pause, Resume, and Stop Operations.
    Simulates Example 2.
    """
    co = sql.connect('database.db')
    c1 = co.cursor()
    c1.execute('SELECT * FROM stackoverflow')
    c = list(c1)
    if len(c) == 0:
        return jsonify({'Error':'No data in DB!'})
    c1.close()
    co.close()
    co2 = sql.connect('states.db')
    c2 = co2.cursor()
    c2.execute("""CREATE TABLE IF NOT EXISTS transactions (id varchar NOT NULL PRIMARY KEY, state varchar, db_operation varchar)""")
    c2.close()
    co2.commit()
    co2.close()
    id = uuid.uuid4()
    print(id)
    row = (str(id), 'PROGRESS', 'READ')
    co5 = sql.connect('states.db')
    c5 = co5.cursor()
    c5.execute("""insert into transactions values (?,?,?);""", row)
    c5.close()
    co5.commit()
    co5.close()
    file = 'export-' + str(id) + '.csv'
    with open(file,'w', encoding='utf8', newline='') as out_csv_file:
        csv_out = csv.writer(out_csv_file)               
        for index, result in enumerate(c):
            time.sleep(0.001)
            if index % 500 == 0:
                co3 = sql.connect('states.db')
                c3 = co3.cursor()
                c3.execute("SELECT state from transactions where id = (?)", (str(id),))
                for tup in c3:
                    for state in tup:
                        if state == 'STOPPED':
                            c3.close()
                            co3.close()
                            response = {'Task ID' : id, 'State': 'STOPPED', 'Info': 'Interrupted by User'}
                            return jsonify(response)
                        elif state == 'PAUSED':
                            while True:
                                time.sleep(1)
                                c3.execute("SELECT state from transactions where id = (?)", (str(id),))
                                new_state = 'PAUSED'
                                for tup in c3:
                                    for state in tup:
                                        if state == 'PROGRESS':
                                            new_state = 'PROGRESS'
                                        elif state == 'STOPPED':
                                            c3.close()
                                            co3.close()
                                            response = {'Task ID' : id, 'State': 'STOPPED', 'Info': 'Interrupted by User'}
                                            return jsonify(response)
                                if new_state == 'PROGRESS':
                                    break
            csv_out.writerow(result)
    co4 = sql.connect('states.db')
    c4 = co4.cursor()
    c4.execute("update transactions set state='COMPLETED' where id = (?)", (str(id),))
    c4.close()
    co4.commit()
    co4.close()
    response = {'Task ID' : id, 'State': 'COMPLETED'}
    return jsonify(response)

@app.route('/<task_id>/stop', methods=['POST'])
def stoptask(task_id):
    """
    Changes Task State to STOPPED.
    """
    co = sql.connect('states.db')
    c = co.cursor()
    c.execute("select state from transactions where id = (?)", (task_id,))
    row = list(c)
    if len(row) > 0:
        for tup in row:
            for state in tup:
                if state == 'COMPLETED':
                    return jsonify({'Error':'Task Completed Already'})
    else:
        return jsonify({'Error':'Given Task Not Found'})

    c.execute("update transactions set state='STOPPED' where id = (?)", (task_id,))
    c.close()
    co.commit()
    co.close()
    response = {'Task ID' : task_id, 'State': 'STOPPED'}
    return jsonify(response)

@app.route('/<task_id>/pause', methods=['POST'])
def pausetask(task_id):
    """
    Changes Task State to PAUSED.
    """
    co = sql.connect('states.db')
    c = co.cursor()
    c.execute("select state from transactions where id = (?)", (task_id,))
    row = list(c)
    if len(row) > 0:
        for tup in row:
            for state in tup:
                if state == 'COMPLETED' or state == 'STOPPED':
                    return jsonify({'Error':'Task Completed Already'})
    else:
        return jsonify({'Error':'Given Task Not Found'})

    c.execute("update transactions set state='PAUSED' where id = (?)", (task_id,))
    c.close()
    co.commit()
    co.close()
    response = {'Task ID' : task_id, 'State': 'PAUSED'}
    return jsonify(response)

@app.route('/<task_id>/resume', methods=['POST'])
def resumetask(task_id):
    """
    Changes Task State to PROGRESS.
    """
    co = sql.connect('states.db')
    c = co.cursor()
    c.execute("select state from transactions where id = (?)", (task_id,))
    row = list(c)
    if len(row) > 0:
        for tup in row:
            for state in tup:
                if state == 'COMPLETED' or state == 'STOPPED':
                    return jsonify({'Error':'Task Completed Already'})
    else:
        return jsonify({'Error':'Given Task Not Found'})
    c.execute("update transactions set state='PROGRESS' where id = (?)", (task_id,))
    c.close()
    co.commit()
    co.close()
    response = {'Task ID' : task_id, 'State': 'PROGRESS'}
    return jsonify(response)

@app.route('/<task_id>/status')
def taskstatus(task_id):
    """
    Returns Status of a Task.
    """
    co = sql.connect('states.db')
    c = co.cursor()
    c.execute("select * from transactions where id = (?)", (task_id,))
    response = {}
    for tup in c:
        for i, item in enumerate(tup):
            if i == 0:
                response['Task ID'] = item
            if i == 1:
                response['State'] = item
    c.close()
    co.commit()
    co.close()
    return jsonify(response)

if __name__ == '__main__':
    app.run(host='0.0.0.0', debug=True) 
