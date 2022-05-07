# Databricks notebook source
# MAGIC %run ./includes/utilities

# COMMAND ----------

# MAGIC %run ./includes/configuration

# COMMAND ----------

# Grab the global variables
wallet_address,start_date = Utils.create_widgets()
print(wallet_address,start_date)

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Your Code starts here...

# COMMAND ----------

# Read the silver wallet table
df = spark.table("G05_db.SilverTable_Wallets")

# COMMAND ----------

# cache
df.cache()
df.printSchema()

# Display the unique number of wallets and ERC20 tokens
unique_wallets = df.select('wallet_address').distinct().count()
unique_tokens = df.select('token_address').distinct().count()
print(f'Number of unique user wallets: {unique_wallets}')
print(f'Number of unique ERC20 tokens: {unique_tokens}')

# COMMAND ----------

# MAGIC %md
# MAGIC ### EDA for modeling

# COMMAND ----------

# MAGIC %md
# MAGIC Let's check the distribution of the number of ERC20 token transfers between the considered wallets

# COMMAND ----------

# Import necessary libraries
import matplotlib.pyplot as plt
import matplotlib.cm as cm
import numpy as np

# Define a function to plot the histogram of number of transactions
# This function was taken from the Music Recommender System Project
def prepareSubplot(xticks, yticks, figsize=(10.5, 6), hideLabels=False, gridColor='#999999',
                gridWidth=1.0, subplots=(1, 1)):
    """Template for generating the plot layout."""
    plt.close()
    fig, axList = plt.subplots(subplots[0], subplots[1], figsize=figsize, facecolor='white',
                               edgecolor='white')
    if not isinstance(axList, np.ndarray):
        axList = np.array([axList])
        
    for ax in axList.flatten():
        ax.axes.tick_params(labelcolor='#999999', labelsize='10')
        for axis, ticks in [(ax.get_xaxis(), xticks), (ax.get_yaxis(), yticks)]:
            axis.set_ticks_position('none')
            axis.set_ticks(ticks)
            axis.label.set_color('#999999')
            if hideLabels: axis.set_ticklabels([])
        ax.grid(color=gridColor, linewidth=gridWidth, linestyle='-')
        map(lambda position: ax.spines[position].set_visible(False), ['bottom', 'top', 'left', 'right'])
        
    if axList.size == 1:
        axList = axList[0]  # Just return a single axes object for a regular plot
    return fig, axList

# COMMAND ----------

# MAGIC %md
# MAGIC Plot histogram of the number of token transfers

# COMMAND ----------

# count total entries
total_entries = df.count()

# find percentage listens by number of tokens transfered
number_transfers = []
for i in range(10):
  number_transfers.append(float(df.filter(df.transactions == i+1).count())/total_entries*100)

# create bar plot
bar_width = 0.7
colorMap = 'Set1'
cmap = cm.get_cmap(colorMap)

fig, ax = prepareSubplot(np.arange(0, 10, 1), np.arange(0, 80, 5))
plt.bar(np.linspace(1,10,10), number_transfers, width=bar_width, color=cmap(0))
plt.xticks(np.linspace(1,10,10) + bar_width/2.0, np.linspace(1,10,10))
plt.xlabel('Number of Transfers'); plt.ylabel('%')
plt.title('Percentage Number of Transfers of tokens')
display(fig)

# COMMAND ----------

# MAGIC %md
# MAGIC Plot the cumulative distribution of number of token transfers

# COMMAND ----------

#find cumulative sum
cumsum_number_transfers = np.cumsum(number_transfers)
cumsum_number_transfers = np.insert(cumsum_number_transfers, 0, 0)
print(cumsum_number_transfers)

fig, ax = prepareSubplot(np.arange(0, 10, 1), np.arange(0, 100, 10))
plt.plot(np.linspace(0,10,11), cumsum_number_transfers, color=cmap(1))
plt.xticks(np.linspace(0,10,11), np.linspace(0,10,11))
plt.xlabel('Number of Transfers'); plt.ylabel('Cumulative sum')
plt.title('Cumulative disribution of Number of Token Transfers')
display(fig)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Collaborative Filtering

# COMMAND ----------

# MAGIC %md
# MAGIC - Collaborative filtering requires integer IDs
# MAGIC - Hence encode the wallet address and token address as integers 

# COMMAND ----------

# Encoding the address variables to numeric IDs

from pyspark.sql import Window
from pyspark.sql.functions import dense_rank

df=df.withColumn("new_tokenid",dense_rank().over(Window.orderBy("token_address")))
df=df.withColumn("new_walletid",dense_rank().over(Window.orderBy("wallet_address")))

df = df.withColumn("transactions", df["transactions"].cast(DoubleType()))

# COMMAND ----------

# Import the necessary modules
from pyspark.sql.types import *
from pyspark.sql import functions as F

import mlflow
import mlflow.spark
from mlflow.tracking import MlflowClient
from mlflow.models.signature import infer_signature
from mlflow.models.signature import ModelSignature
from mlflow.types.schema import Schema, ColSpec

from pyspark.ml.recommendation import ALS
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder

# COMMAND ----------

# MAGIC %md
# MAGIC Split the data into train and test sets 

# COMMAND ----------

# We'll hold out 80% for training, 20% of our data for validation
seed = 42
(split_80_df, split_20_df) = df.randomSplit([0.8, 0.2], seed = seed)

# Let's cache these datasets for performance
training_df = split_80_df.cache()
validation_df = split_20_df.cache()

print('Training: {0}, validation: {1} \n'.format(
  training_df.count(), validation_df.count())
)

# COMMAND ----------

# MAGIC %md
# MAGIC Begin with ML Experiments

# COMMAND ----------

# Create/set experiment for group
MY_EXPERIMENT = "/Users/pthulasi@ur.rochester.edu/G05_Final_Project"
mlflow.set_experiment(MY_EXPERIMENT)

# COMMAND ----------

# Check experiment
experiment = mlflow.get_experiment_by_name("/Users/pthulasi@ur.rochester.edu/G05_Final_Project")
print("Experiment_id: {}".format(experiment.experiment_id))
print("Artifact Location: {}".format(experiment.artifact_location))
print("Tags: {}".format(experiment.tags))
print("Lifecycle_stage: {}".format(experiment.lifecycle_stage))

# COMMAND ----------

# MAGIC %md
# MAGIC Define the signature/schema of the model

# COMMAND ----------

from pyspark.sql import functions as F
from mlflow.models.signature import ModelSignature
from mlflow.types.schema import Schema, ColSpec

input_schema = Schema([
    ColSpec("integer", "wallet_address"),
    ColSpec("integer", "token_address"),
])

output_schema = Schema([ColSpec("double","predictions")])
signature = ModelSignature(inputs=input_schema, outputs=output_schema)

# COMMAND ----------

# MAGIC %md
# MAGIC Begin model training
# MAGIC - SparkTrials is designed to parallelize computations for single-machine ML models such as scikit-learn. For models created with distributed ML algorithms such as MLlib or Horovod, do not use SparkTrials. In this case the model building process is automatically parallelized on the cluster and you should use the default Hyperopt class Trials.
# MAGIC - ALS algorith used in our project is a part of MLlib. Hence we used Trials() instead of SparkTrials.

# COMMAND ----------

from pyspark.ml.recommendation import ALS
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder

# Let's initialize our ALS learner
als = ALS()

# Now set the parameters for the method
als.setMaxIter(5)\
   .setSeed(seed)\
   .setItemCol("new_tokenid")\
   .setRatingCol("transactions")\
   .setUserCol("new_walletid")\
   .setColdStartStrategy("drop")

# Now let's compute an evaluation metric for our test dataset
# We create an RMSE evaluator using the label and predicted columns
reg_eval = RegressionEvaluator(predictionCol="prediction", labelCol="transactions", metricName="rmse")

import hyperopt
from hyperopt import fmin, hp, tpe, STATUS_OK, SparkTrials,Trials
import builtins as py_builtin 

space = {
  "rank": hp.quniform("rank", 14, 19, 1),
  "regParam": hp.quniform("regParam", 0.15, 0.50, 0.05)
 }

spark_trials = Trials()

def train(params):

  rank=int(params['rank'])
  regParam = params['regParam']
  
  als.setParams(rank = rank, regParam = regParam)
  
  model = als.fit(training_df)
  
  predict_df = model.transform(validation_df)

  # Remove NaN values from prediction (due to SPARK-14489)
  predicted_transfers_df = predict_df.filter(predict_df.prediction != float('nan'))
  predicted_transfers_df = predicted_transfers_df.withColumn("prediction", F.abs(F.round(predicted_transfers_df["prediction"],0)))
  # Run the previously created RMSE evaluator, reg_eval, on the predicted_ratings_df DataFrame
  error = reg_eval.evaluate(predicted_transfers_df)
  obj_metric = error 
  return {"loss": obj_metric, "status": STATUS_OK}


with mlflow.start_run() as run:
  mlflow.set_tags({"group": 'G05', "class": "DSCC202-402"})
  best_hyperparam = fmin(fn=train, 
                         space=space, 
                         algo=tpe.suggest, 
                         max_evals=32, 
                         trials=spark_trials)
  # Log model and metrics
  mlflow.log_params(best_hyperparam)
  als.setParams(rank = best_hyperparam['rank'], regParam = best_hyperparam['regParam'])
  best_model_1 = als.fit(training_df)
  
  mlflow.log_metric('rmse', py_builtin.min(spark_trials.losses())) 
  
  mlflow.spark.log_model(spark_model=best_model_1, signature = signature, artifact_path='als-model', registered_model_name='ALS_G05')

print(hyperopt.space_eval(space, best_hyperparam))

mlflow.end_run()

run_Id = run.info.run_id

# COMMAND ----------

# Print the best hyper parameters:
best_hyperparam

# COMMAND ----------

# Register model in Staging
from mlflow.tracking import MlflowClient

client = MlflowClient()

model_versions = []

filter_string = "run_id='{}'".format(run_Id)

for mv in client.search_model_versions(filter_string):
    model_versions.append(dict(mv)['version'])
    if dict(mv)['current_stage'] == 'Staging':
        print("Archiving: {}".format(dict(mv)))
        # Archive the currently staged model
        client.transition_model_version_stage(
            name="ALS_G05",
            version=dict(mv)['version'],
            stage="Archived"
            )

client.transition_model_version_stage(
    name="ALS_G05",
    version=model_versions[0],  # this model (current build)
    stage="Staging"
)

# COMMAND ----------

# client.transition_model_version_stage(
#     name="ALS_G05",
#     version=model_versions[0],  # this model (current build)
#     stage="Production"
# )

# COMMAND ----------

# Predict for a user  
tokensDF=sqlContext.sql("select token_address,name,symbol,links,image from G05_db.GoldTable_Wallet_details")
try:
  UserID=df.filter(df.wallet_address==wallet_address).select('new_walletid').toPandas().iloc[0,0]
except:
  UserID=0
UserID=int(UserID)
print(UserID)
#UserID = 1049154 
users_tokens = df.filter(df.new_walletid == UserID).join(tokensDF, on= 'token_address').select('new_tokenid', 'name', 'symbol','links','image').distinct()                                   
# generate list of tokens held 
tokens_held_list = [] 
# for tok in users_tokens.collect():   
#     tokens_held_list.append(tok['name'])  
print('Tokens user has held:') 
users_tokens.select('name').show()  
    
# generate dataframe of tokens user doesn't have 
tokens_not_held = df.filter(~ df['new_tokenid'].isin([token['new_tokenid'] for token in users_tokens.collect()])).select('new_tokenid').withColumn('new_walletid', F.lit(UserID)).distinct()

# Print prediction output and save top predictions (recommendations) to table in DB
model = mlflow.spark.load_model('models:/ALS_G05/Staging') ##Uncomment this with name of our best model
predicted_toks = model.transform(tokens_not_held)
    
print('Predicted Tokens:')

predicted_toks =predicted_toks.orderBy('prediction',ascending =False).limit(10)
toppredictions = predicted_toks.join(df, 'new_tokenid')
tokensDF=tokensDF.distinct()
toppredictions=toppredictions.select('token_address').withColumn('wallet_address',F.lit(wallet_address)).distinct()
toppredictions = toppredictions.join(tokensDF,on='token_address').select('wallet_address','token_address','name','image', 'symbol','links')
print(toppredictions.show(10))
spark.sql("""DROP TABLE IF EXISTS G05_db.GoldTable_Recommendations """)
toppredictions.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("G05_db.GoldTable_Recommendations")

# COMMAND ----------

# Return Success
dbutils.notebook.exit(json.dumps({"exit_code": "OK"}))
