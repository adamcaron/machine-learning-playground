#/usr/bin/python2.7
import sys
sys.path.append('/usr/local/lib/python2.7/site-packages')

import psycopg2
import os
import pandas as pd
import traceback

from sklearn import preprocessing

import pyspark
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.mllib.recommendation import ALS
sc = SparkContext(appName="BuildRecommendations")
sqlContext = SQLContext(sc)

import math

usr = os.environ["DWH_DB_USER"]
pwd = os.environ["DWH_DB_PWD"]
db = os.environ["DWH_PSQL_DB"]
host = os.environ["DWH_RDS_HOST95"]

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
	# df1 = pd.read_sql_query("""SELECT drupal_user_id, media_nid, qualified_view FROM common.video_views_fact LIMIT 25""", connection)
	# df2 = pd.read_sql_query("""SELECT drupal_user_id, onboarding_term FROM common.user_onboarding_fact WHERE user_score IS NOT NULL LIMIT 25""", connection)
	# print df2

	views_df = pd.read_sql_query("""SELECT drupal_user_id, media_nid, qualified_view FROM common.video_views_fact WHERE qualified_view > 0 LIMIT 500""", connection)
	# views_df.replace('1', 1)
	# views_df.columns = ['drupal_user_id', 'media_nid', 'views']
	# print views_df.head()
	# print views_df

	user_id_le = preprocessing.LabelEncoder()
	media_nid_le = preprocessing.LabelEncoder()
	user_id_le.fit(views_df.drupal_user_id)
	media_nid_le.fit(views_df.media_nid)

	# print 'Number of unique users: ', str(len(user_id_le.classes_))
	# print 'Number of unique videos: ', str(len(media_nid_le.classes_))

	n_views_df = views_df
	n_views_df.drupal_user_id = user_id_le.transform(views_df.drupal_user_id)
	n_views_df.media_nid = media_nid_le.transform(views_df.media_nid)
	# print n_views_df.head()
	# print n_views_df

	n_views_rdd = sqlContext.createDataFrame(n_views_df).rdd
	training_rdd, validation_rdd, test_rdd = n_views_rdd.randomSplit([6, 2, 2], 1345)
	validation_for_predict_rdd = validation_rdd.map(lambda x: (x[0], x[1]))
	test_for_predict_rdd = test_rdd.map(lambda x: (x[0], x[1]))
	
	# print 'Training RDD\n', training_rdd.take(5)
	# print '\nValidation for Prediction RDD\n', validation_for_predict_rdd.take(5)
	# print '\nTest for Prediction RDD\n', test_for_predict_rdd.take(5)

	seed = 49247
	iterations = 10
	lambdas = [0.01, 0.1]
	ranks = [16]
	alphas = [1.0, 2.0, 4.0, 8.0, 16.0, 32.0, 40.0, 80.0]
	errors = [0 for x in range(len(alphas) * len(ranks) * len(lambdas))]
	err_index = 0

	for lambda_ in lambdas:
		for rank in ranks:
			for alpha in alphas:
				model = ALS.trainImplicit(training_rdd, rank, seed=seed, iterations=iterations, lambda_=lambda_, alpha=alpha)
				predictions = model.predictAll(validation_for_predict_rdd).map(lambda r: ((r[0], r[1]), r[2]))
				views_and_preds = validation_rdd.map(lambda r: ((int(r[0]), int(r[1])), float(r[2]))).join(predictions)
				error = math.sqrt(views_and_preds.map(lambda r: (r[1][0] - r[1][1])**2).mean())
				errors[err_index] = error
				err_index += 1
				print('For rank {0} at alpha: {1} and lambda: {2}, the RMSE is {3}'.format(rank, alpha, lambda_, error))
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
