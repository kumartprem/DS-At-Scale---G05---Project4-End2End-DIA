# Databricks notebook source
# MAGIC %md
# MAGIC ## Rubric for this module
# MAGIC - Implement a routine to "promote" your model at **Staging** in the registry to **Production** based on a boolean flag that you set in the code.
# MAGIC - Using wallet addresses from your **Staging** and **Production** model test data, compare the recommendations of the two models.

# COMMAND ----------

# MAGIC %run ./includes/utilities

# COMMAND ----------

# MAGIC %run ./includes/configuration

# COMMAND ----------

# Grab the global variables
wallet_address,start_date = Utils.create_widgets()
print(wallet_address,start_date)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Your Code Starts Here...

# COMMAND ----------

# MAGIC %md
# MAGIC - Promote the staging model to Production if its performance is better
# MAGIC - When the experiment is run for the first time, and there is no model in 'Production', the staging model will be pushed to Production by default

# COMMAND ----------


# Compare staging and production model predictions for a user
# MAGIC Changing the wallet address widget to compare different users


from mlflow.tracking import MlflowClient
client = MlflowClient()

from pyspark.sql import Window
from pyspark.sql.functions import dense_rank
df=sqlContext.sql("select * from G05_db.SilverTable_Wallets")
df=df.withColumn("new_tokenid",dense_rank().over(Window.orderBy("token_address")))
df=df.withColumn("new_walletid",dense_rank().over(Window.orderBy("wallet_address")))

# Predict for a user
from pyspark.sql import functions as F
# Predict for a user  
tokensDF = sqlContext.sql("select token_address,name,symbol,links,image from G05_db.GoldTable_Wallet_details")
try:
    UserID = df.filter(df.wallet_address==wallet_address).select('new_walletid').toPandas().iloc[0,0]
except:
    UserID = 0

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

# COMMAND ----------

# Print prediction output for staging model
model = mlflow.spark.load_model('models:/ALS_G05/Staging')
predicted_toks = model.transform(tokens_not_held)
print('Predicted Tokens:')

predicted_toks =predicted_toks.orderBy('prediction',ascending =False).limit(10)

toppredictions = predicted_toks.join(df, 'new_tokenid')

tokensDF=tokensDF.distinct()

toppredictions=toppredictions.select('token_address').withColumn('wallet_address',F.lit(wallet_address)).distinct()
toppredictions = toppredictions.join(tokensDF,on='token_address').select('wallet_address','token_address','name','image', 'symbol','links')

print(toppredictions.show(10))

# COMMAND ----------

# Print prediction output for production model
try:
    model = mlflow.spark.load_model('models:/ALS_G05/Production')
except:
    run_stage = client.get_latest_versions('ALS_G05',["Staging"])[0]
    client.transition_model_version_stage(
        name="ALS_G05",
        version=run_stage.version,  # this model (current build)
        stage="Production"
    )
    model = mlflow.spark.load_model('models:/ALS_G05/Production')

predicted_toks = model.transform(tokens_not_held)
print('Predicted Tokens:')

predicted_toks =predicted_toks.orderBy('prediction',ascending =False).limit(10)

toppredictions = predicted_toks.join(df, 'new_tokenid')

tokensDF=tokensDF.distinct()

toppredictions=toppredictions.select('token_address').withColumn('wallet_address',F.lit(wallet_address)).distinct()
toppredictions = toppredictions.join(tokensDF,on='token_address').select('wallet_address','token_address','name','image', 'symbol','links')

print(toppredictions.show(10))

# COMMAND ----------

# Promote staging model to production if it's better
# Get staging and production models
run_stage = client.get_latest_versions('ALS_G05', ["Staging"])[0]
run_prod = client.get_latest_versions('ALS_G05', ["Production"])[0]

# Gets version of staged model
run_staging=run_stage.version

# Gets version of production model
run_production=run_prod.version

# Visualize difference in RMSE
rmse_ = [client.get_metric_history(run_id=run_stage.run_id, key='rmse')[0].value, client.get_metric_history(run_id=run_prod.run_id, key='rmse')[0].value]
stage = ['Staging', 'Production']

import matplotlib.pyplot as plt
plt.bar(stage,rmse_)
plt.ylabel("RMSE")
plt.show()

# If the RMSE of staging model is less than that of production, promote staging model and archive production model
if client.get_metric_history(run_id=run_stage.run_id, key='rmse')[0].value < client.get_metric_history(run_id=run_prod.run_id, key='rmse')[0].value:
    filter_string = "name='{}'".format('ALS_G05')
    for mv in client.search_model_versions(filter_string):
         if dict(mv)['current_stage'] == 'Production':
             # Archive the current model in production
             client.transition_model_version_stage(
                name="ALS_G05",
                version=dict(mv)['version'],
                stage="Archived"
            )
    version_fin=int(run_staging)
    client.transition_model_version_stage(
    name='ALS_G05',
    version=version_fin,
    stage='Production'
)

# COMMAND ----------

# Return Success
dbutils.notebook.exit(json.dumps({"exit_code": "OK"}))
