#/usr/bin/python2.7
import sys
sys.path.append('/usr/local/lib/python2.7/site-packages')
import psycopg2
import os
import pandas as pd
import traceback

usr = os.environ["DWH_DB_USER"]
pwd = os.environ["DWH_DB_PWD"]
db = os.environ["DWH_PSQL_DB"]
host = os.environ["DWH_RDS_HOST95"]

def main():
	conn_string = "host='"+host+"' dbname='"+db+"' user='"+usr+"' password='"+pwd+"'"
	print "Connection String: %s\n" % (conn_string)

	try:
		# raise TypeError("Naah, yo.") # Use this to test flow of program and ensure NOT UnboundLocalError: local variable 'cur' referenced before assignment
		connection = psycopg2.connect(conn_string)
		print "Connected!\n"
		cur = connection.cursor()
	except Exception, err:
		print "Unable to connect to the database.\n"
		traceback.print_exc()

	try:
		df1 = pd.read_sql_query("""SELECT drupal_user_id, qualified_view FROM common.video_views_fact LIMIT 25""", connection)
		df2 = pd.read_sql_query("""SELECT uid, tid FROM common.user_onboards LIMIT 25""", connection)
		df3 = pd.read_sql_query("""SELECT drupal_user_id, onboarding_term, user_score FROM common.user_onboarding_fact WHERE user_score IS NOT NULL LIMIT 25""", connection)
		print df1
		print df2
		print df3
	except Exception, err:
		print "Unable to read_sql_query.\n"
		traceback.print_exc()

	# try:
	# 	cur.execute("""SELECT * FROM common.video_views_fact LIMIT 100""")
	# 	rows = cur.fetchall()
	# 	print "\nvideo_views_fact:\n"
	# 	for row in rows:
	# 		print "   ", row[0]
	# except Exception, err:
	# 	print "Cannot retreive.\n"
	# 	traceback.print_exc()
	connection.close()

if __name__ == "__main__":
	main()
