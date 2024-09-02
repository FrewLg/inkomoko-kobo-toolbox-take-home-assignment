from flask_sqlalchemy import SQLAlchemy
from datetime import datetimeapp = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///kobotoolboxremotedb.db'
db = SQLAlchemy(app)


class Todo(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    content = db.Column(db.String(200), nullable=False)
    date_created = db.Column(db.DateTime, default=datetime.utcnow)
def __repr__(self):
    return '<Task %r>' % self.id
