%pyspark
import math
import time
import traceback
import s3fs
import pandas as pd
import pyspark
from sklearn import preprocessing
from pyspark.sql import SQLContext
from pyspark.mllib.recommendation import ALS, MatrixFactorizationModel

S3_BUCKET = "<bucket-location-here>"
QUALIFIED_VIEWS ="<your-data.csv"

def main():
	try:
		views_df = pd.read_csv(S3_BUCKET + QUALIFIED_VIEWS, index_col=None)
		views_df = views_df.replace('1', 1)
		views_df.columns = ['drupal_user_id', 'media_nid', 'views']

		user_id_le = preprocessing.LabelEncoder()
		media_nid_le = preprocessing.LabelEncoder()
		user_id_le.fit(views_df.drupal_user_id)
		media_nid_le.fit(views_df.media_nid)
s
		n_views_df = views_df
		n_views_df.drupal_user_id = user_id_le.transform(views_df.drupal_user_id)
		n_views_df.media_nid = media_nid_le.transform(views_df.media_nid)

		sqlContext = SQLContext(sc)
		n_views_rdd = sqlContext.createDataFrame(n_views_df).rdd
		training_rdd, validation_rdd, test_rdd = n_views_rdd.randomSplit([6, 2, 2], 1345)
		validation_for_predict_rdd = validation_rdd.map(lambda x: (x[0], x[1]))
		test_for_predict_rdd = test_rdd.map(lambda x: (x[0], x[1]))

		seed = 49247
		iterations = 10
		# lambdas = [0.01, 0.1]
		lambdas = [0.01, 0.1, 10.0]
		# ranks = [16]
		ranks = [16]
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

		timestamp = time.strftime("%Y%m%d-%H%M%S")
		model.save(sc, S3_BUCKET + '/models/' + timestamp)
	except Exception, err:
		traceback.print_exc()

if __name__ == "__builtin__":
	main()