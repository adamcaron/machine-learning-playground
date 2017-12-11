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
from pyspark.mllib.recommendation import ALS, MatrixFactorizationModel
import math
import time
# import boto3

# USR = os.environ["DWH_DB_USER"]
# PWD = os.environ["DWH_DB_PWD"]
# DB = os.environ["DWH_PSQL_DB"]
# HOST = os.environ["DWH_RDS_HOST95"]
# CONN_STRING = "host='"+HOST+"' dbname='"+DB+"' user='"+USR+"' password='"+PWD+"'"
# print "Connection String: %s\n" % (CONN_STRING)

def main(sc):
	# USR = os.environ["DWH_DB_USER"]
	# PWD = os.environ["DWH_DB_PWD"]
	# DB = os.environ["DWH_PSQL_DB"]
	# HOST = os.environ["DWH_RDS_HOST95"]
	# USR = sc.getConf.get("spark.poc.DWH_DB_USER")
	# PWD = sc.getConf.get("spark.poc.DWH_DB_PWD")
	# DB = sc.getConf.get("spark.poc.DWH_PSQL_DB")
	# HOST = sc.getConf.get("spark.poc.DWH_RDS_HOST95")
	# CONN_STRING = "host='"+HOST+"' dbname='"+DB+"' user='"+USR+"' password='"+PWD+"'"
	# try:
	# 	# raise TypeError("Naah, yo.") # Use this to test flow of program and ensure NOT UnboundLocalError: local variable 'cur' referenced before assignment
	# 	connection = psycopg2.connect(CONN_STRING)
	# 	print "Connected!\n"
	# 	# cur = connection.cursor()
	# except Exception, err:
	# 	print "Unable to connect to the database.\n"
	# 	traceback.print_exc()

	try:
		# s3 = boto3.client('s3')
		# s3.download_file(
		# 	'gaia-poc-spark',
		# 	'SELECT_drupal_user_id_media_nid_qualified_view_FROM_common_video_201711281500.csv',
		# 	'/tmp/views.csv'
		# )
		# views_df = pd.read_csv('/tmp/views.csv', index_col=None, header=None)
		views_df = pd.read_csv('SELECT_drupal_user_id_media_nid_qualified_view_FROM_common_video_201711281500.csv', index_col=None)
		# print views_df.head()

		# df1 = pd.read_sql_query("""SELECT drupal_user_id, media_nid, qualified_view FROM common.video_views_fact LIMIT 25""", connection)
		# df2 = pd.read_sql_query("""SELECT drupal_user_id, onboarding_term FROM common.user_onboarding_fact WHERE user_score IS NOT NULL LIMIT 25""", connection)
		# print df2

		# views_df = pd.read_sql_query("SELECT drupal_user_id, media_nid, qualified_view FROM common.video_views_fact WHERE qualified_view > 0 LIMIT 8000", connection)
		# views_df = pd.read_sql_query("SELECT drupal_user_id, media_nid, qualified_view FROM common.video_views_fact LIMIT 57000", connection)
		# connection = psycopg2.connect(CONN_STRING)
		# views_df = pd.read_sql_query("SELECT drupal_user_id, media_nid, qualified_view FROM common.video_views_fact LIMIT 57000", connection)
		views_df = views_df.replace('1', 1)
		views_df.columns = ['drupal_user_id', 'media_nid', 'views']
		# print views_df.head()
		# print views_df
		# views_df.info() # https://jessesw.com/Rec-System/

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

		sqlContext = SQLContext(sc)
		n_views_rdd = sqlContext.createDataFrame(n_views_df).rdd
		training_rdd, validation_rdd, test_rdd = n_views_rdd.randomSplit([6, 2, 2], 1345)
		validation_for_predict_rdd = validation_rdd.map(lambda x: (x[0], x[1]))
		test_for_predict_rdd = test_rdd.map(lambda x: (x[0], x[1]))

		# print 'Training RDD\n', training_rdd.take(5)
		# print '\nValidation for Prediction RDD\n', validation_for_predict_rdd.take(5)
		# print '\nTest for Prediction RDD\n', test_for_predict_rdd.take(5)

		seed = 49247
		iterations = 10
		# lambdas = [0.01, 0.1]
		lambdas = [0.01, 0.1, 10.0] # regularizations
		# ranks = [16]
		ranks = [16] #latent_factors
		# alphas = [1.0, 2.0, 4.0, 8.0, 16.0, 32.0, 40.0, 80.0]
		alphas = [80.0]
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

		# print model.recommendProductsForUsers(2).collect()[:5]
		print model.recommendProducts(1216, 5)
		timestamp = time.strftime("%Y%m%d-%H%M%S")
		model.save(sc, './tmp/collaborative_filter_' + timestamp)

		same_model = MatrixFactorizationModel.load(sc, './tmp/collaborative_filter_' + timestamp)
		print 'again: ', same_model.recommendProducts(1216, 5)
	except Exception, err:
		# print "Unable to read_sql_query.\n"
		traceback.print_exc()
	# connection.close()

if __name__ == "__main__":
	sc = SparkContext(appName="BuildRecommendations")
	sc.setLogLevel("WARN")
	main(sc)
	sc.stop()