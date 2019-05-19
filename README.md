# quandl_scrap


The quandl scrap is a python program coded to download End Of Days prices of Instruments on CME. It works with other
exchanges and instruments.
It records the data into a mysql database.

It is necessary to have a folder with credentials, in the code called credentials/.

The database_credentials.txt should have:

db_host=192.168.xx.xx  
db_user=yourusername  
db_pass=yourpassword  
db_name=yourDB  
host_port=yourport  

the quandl_credentials.txt should have:

quandl_authtoken=XXXXXXXXXXXXXXXXXXXX