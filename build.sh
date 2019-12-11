pip3 install --target ./package psycopg2 --upgrade
cd package
zip -r9 ${OLDPWD}/function.zip .
cd $OLDPWD
zip -g function.zip creds.py
