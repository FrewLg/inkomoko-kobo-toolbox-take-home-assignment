import requests
import sqlite3
from datetime import datetime

# API details
api_url = "https://kf.kobotoolbox.org/api/v2/assets/aW9w8jHjn4Cj8SSQ5VcojK/data.json"
headers = {
    'Authorization': 'Token f24b97a52f76779e97b0c10f80406af5e9590eaf',
    'Cookie': 'django_language=en'
}

# Database connection
conn = sqlite3.connect('survey_data.db')
c = conn.cursor()

def create_tables():
    c.execute('''CREATE TABLE IF NOT EXISTS users
                 (user_id INTEGER PRIMARY KEY, username TEXT, email TEXT, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)''')

    c.execute('''CREATE TABLE IF NOT EXISTS surveys
                 (survey_id INTEGER PRIMARY KEY, title TEXT, description TEXT, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, user_id INTEGER, FOREIGN KEY (user_id) REFERENCES users(user_id))''')

    c.execute('''CREATE TABLE IF NOT EXISTS responses
                 (response_id INTEGER PRIMARY KEY, survey_id INTEGER, respondent_id INTEGER, submitted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, FOREIGN KEY (survey_id) REFERENCES surveys(survey_id))''')

    c.execute('''CREATE TABLE IF NOT EXISTS questions
                 (question_id INTEGER PRIMARY KEY, survey_id INTEGER, question_text TEXT, question_type TEXT, FOREIGN KEY (survey_id) REFERENCES surveys(survey_id))''')

    c.execute('''CREATE TABLE IF NOT EXISTS options
                 (option_id INTEGER PRIMARY KEY, question_id INTEGER, option_text TEXT, FOREIGN KEY (question_id) REFERENCES questions(question_id))''')

    c.execute('''CREATE TABLE IF NOT EXISTS answers
                 (answer_id INTEGER PRIMARY KEY, response_id INTEGER, question_id INTEGER, selected_option_id INTEGER, text_answer TEXT, FOREIGN KEY (response_id) REFERENCES responses(response_id), FOREIGN KEY (question_id) REFERENCES questions(question_id), FOREIGN KEY (selected_option_id) REFERENCES options(option_id))''')

def save_extracted_data():
    response = requests.get(api_url, headers=headers)
    data = response.json()

    for record in data:
        if isinstance(record, dict):
            # Check if user already exists
            c.execute("SELECT user_id FROM users WHERE username = ? AND email = ?", (record.get('user', {}).get('username'), record.get('user', {}).get('email')))
            user = c.fetchone()
            if user:
                user_id = user[0]
            else:
                c.execute("INSERT INTO users (username, email) VALUES (?, ?)", (record.get('user', {}).get('username'), record.get('user', {}).get('email')))
                user_id = c.lastrowid

            # Check if survey already exists
            c.execute("SELECT survey_id FROM surveys WHERE title = ? AND description = ? AND user_id = ?", (record.get('survey', {}).get('title'), record.get('survey', {}).get('description'), user_id))
            survey = c.fetchone()
            if survey:
                survey_id = survey[0]
            else:
                c.execute("INSERT INTO surveys (title, description, user_id) VALUES (?, ?, ?)", (record.get('survey', {}).get('title'), record.get('survey', {}).get('description'), user_id))
                survey_id = c.lastrowid

            # Save responses
            for response in record.get('responses', []):
                c.execute("SELECT response_id FROM responses WHERE survey_id = ? AND respondent_id = ?", (survey_id, response.get('respondent_id')))
                existing_response = c.fetchone()
                if not existing_response:
                    c.execute("INSERT INTO responses (survey_id, respondent_id) VALUES (?, ?)", (survey_id, response.get('respondent_id')))
                    response_id = c.lastrowid

                    # Save questions and options
                    for question in response.get('questions', []):
                        c.execute("SELECT question_id FROM questions WHERE survey_id = ? AND question_text = ?", (survey_id, question.get('text')))
                        existing_question = c.fetchone()
                        if existing_question:
                            question_id = existing_question[0]
                        else:
                            c.execute("INSERT INTO questions (survey_id, question_text, question_type) VALUES (?, ?, ?)", (survey_id, question.get('text'), question.get('type')))
                            question_id = c.lastrowid

                        for option in question.get('options', []):
                            c.execute("SELECT option_id FROM options WHERE question_id = ? AND option_text = ?", (question_id, option))
                            existing_option = c.fetchone()
                            if not existing_option:
                                c.execute("INSERT INTO options (question_id, option_text) VALUES (?, ?)", (question_id, option))

                    # Save answers
                    for answer in response.get('answers', []):
                        c.execute("SELECT answer_id FROM answers WHERE response_id = ? AND question_id = ? AND selected_option_id = ? AND text_answer = ?",
                                 (response_id, answer.get('question_id'), answer.get('selected_option_id'), answer.get('text_answer')))
                        existing_answer = c.fetchone()
                        if not existing_answer:
                            c.execute("INSERT INTO answers (response_id, question_id, selected_option_id, text_answer) VALUES (?, ?, ?, ?)",
                                     (response_id, answer.get('question_id'), answer.get('selected_option_id'), answer.get('text_answer')))

    conn.commit()
    conn.close()

create_tables()
save_extracted_data()