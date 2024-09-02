from flask import Flask, request, render_template
import ijson
import mysql.connector
import requests
import concurrent.futures

app = Flask(__name__)

# Kobo API details
url = "https://kf.kobotoolbox.org/api/v2/assets/aW9w8jHjn4Cj8SSQ5VcojK/data.json"
headers = {
    'Authorization': 'Token f24b97a52f76779e97b0c10f80406af5e9590eaf',
    'Cookie': 'django_language=en'
}

# MySQL connection details
mydb = mysql.connector.connect(
    host="3306",
    user="root",
    password="r00tme",
    database="kobotoolbdb"
)

# Define a function to insert records in parallel
def insert_records(records):
    mycursor = mydb.cursor()

    for record in records:
        # Process each record here
        # ...

        # Insert the record into the MySQL table
        mycursor.execute("INSERT INTO table_name (column1, column2) VALUES (%s, %s)", (value1, value2))

    # Commit the transaction and close the cursor
    mydb.commit()
    mycursor.close()

@app.route('/')
def index():
    # Fetch data from Kobo API and insert into MySQL
    response = requests.get(url, headers=headers)

    # Parse JSON data in chunks
    parsed_json_data = []
    for record in ijson.items(response.iter_content(4096), '.'):
        parsed_json_data.append(record)

    # Prepare the records for insertion
    chunked_json_data = [parsed_json_data[i:i + 1000] for i in range(0, len(parsed_json_data), 1000)]

    # Use parallel processing to insert records
    with concurrent.futures.ThreadPoolExecutor() as executor:
        for result in executor.map(insert_records, chunked_json_data):
            pass

    # Close the MySQL connection
    mydb.close()

    # Render a template with the data
    return render_template('index.html', data=parsed_json_data)

@app.route('/create', methods=['GET', 'POST'])
def create():
    if request.method == 'POST':
        # Get the form data
        data = request.form.to_dict()

        # Insert the data into the MySQL table
        mycursor = mydb.cursor()
        mycursor.execute("INSERT INTO table_name (column1, column2) VALUES (%s, %s)", (data['column1'], data['column2']))
        mydb.commit()
        mycursor.close()

        # Redirect to the index page
        return redirect(url_for('index'))

    # Render the create template
    return render_template('create.html')

@app.route('/read/<id>')
def read(id):
    # Fetch the data from the MySQL table
    mycursor = mydb.cursor()
    mycursor.execute("SELECT * FROM table_name WHERE id = %s", (id,))
    data = mycursor.fetchone()
    mycursor.close()

    # Render the read template with the data
    return render_template('read.html', data=data)

@app.route('/update/<id>', methods=['GET', 'POST'])
def update(id):
    if request.method == 'POST':
        # Get the form data
        data = request.form.to_dict()

        # Update the data in the MySQL table
        mycursor = mydb.cursor()
        mycursor.execute("UPDATE table_name SET column1 = %s, column2 = %s WHERE id = %s", (data['column1'], data['column2'], id))
        mydb.commit()
        mycursor.close()

        # Redirect to the index page
        return redirect(url_for('index'))

    # Fetch the data from the MySQL table
    mycursor = mydb.cursor()
    mycursor.execute("SELECT * FROM table_name WHERE id = %s", (id,))
    data = mycursor.fetchone()
    mycursor.close()

    # Render the update template with the data
    return render_template('update.html', data=data)

@app.route('/delete/<id>', methods=['POST'])
def delete(id):
    # Delete the data from the MySQL table
    mycursor = mydb.cursor()
    mycursor.execute("DELETE FROM table_name WHERE id = %s", (id,))
    mydb.commit()
    mycursor.close()

    # Redirect to the index page
    return redirect(url_for('index'))

if __name__ == '__main__':
    app.run()