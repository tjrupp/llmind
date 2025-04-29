apt-get update
apt-get -y install curl
apt-get install apt-transport-https
curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -
curl https://packages.microsoft.com/config/ubuntu/20.04/prod.list | tee /etc/apt/sources.list.d/msprod.list
apt-get update
apt-get -y install mssql-tools unixodbc-dev
apt-get -y install libgssapi-krb5-2
apt-get -y install apt-utils
apt-get -y install unixodbc
apt-get install -y msodbcsql
apt-get install -y mssql-tools
apt-get install -y unixodbc-dev
pip install --upgrade pip
pip install --user -r requirements.txt
pip install --user pandas
pip install --user pyodbc
pip install --user requests
pip install --user pdfplumber
python "./src/db_config.py"
python "./src/1. DBicddownloader.py"
python "./src/2. DBICD_processing.py"
python "./src/3. DBdsmsplit.py"
python "./src/6. KGFileHandler.py"