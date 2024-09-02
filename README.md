# inkomoko-kobo-toolbox-take-home-assignment

#installation proceedure and Environment setup

virtualenv venv
source venv/bin/activate

pip install Flask if didn't exist

pip3 install aiohttp
pip install ijson
pip install mysql-connector-python
flask run --host=127.0.0.1:2002

#configuration
host= 3306
database= kobotoolbdb
username= root
password=r00tme

1. To maximize efficiency and performance, we can add an exception or error handling and pagination   methods for the requests and responses. 

THe file named SCHEMA.py is the database schema.

The main file consists the codes for the entire assignment is main.py