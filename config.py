
###########################################################################################################
# Configuration parameters for connecting to the MySQL database
###########################################################################################################

# Machine name where the database is running
DBSRV = 'localhost'

# Login name used to gain access
DBUSER = 'root'

# Password used to gain access
#todo: for production server data that also includes confidential information, this should be moved to a safer place outside the source code
DBPASS = '1234'

# Default database name (e.g. used to store certain settings)
DB='datasetindex'

# Command to invoke the MySQL prompt
mysqlcommand = '/usr/local/mysql/bin/mysql'
#mysqlcommand = 'mysql'

###########################################################################################################
# Server side file structure location
###########################################################################################################

# Root directory of the server side file structure
BASEDIR = '/Users/pvaut/Documents/Genome'
#BASEDIR = '/mnt/storage/webapps'


