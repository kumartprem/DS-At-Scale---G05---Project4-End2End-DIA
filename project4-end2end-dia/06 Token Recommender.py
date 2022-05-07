# Databricks notebook source
# MAGIC %md
# MAGIC ## Token Recommendation
# MAGIC <table border=0>
# MAGIC   <tr><td><img src='https://data-science-at-scale.s3.amazonaws.com/images/rec-application.png'></td>
# MAGIC     <td>Your application should allow a specific wallet address to be entered via a widget in your application notebook.  Each time a new wallet address is entered, a new recommendation of the top tokens for consideration should be made. <br> **Bonus** (3 points): include links to the Token contract on the blockchain or etherscan.io for further investigation.</td></tr>
# MAGIC   </table>

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
# MAGIC ## Your code starts here...

# COMMAND ----------

# Idenitfy the recommendation
recommendations = spark.sql("select * from G05_db.GoldTable_Recommendations where wallet_address = '${wallet.address}'").toPandas().head(5)
recommendations_cs = spark.table("G05_db.GoldTable_ColdStart").toPandas().head(5)

if(recommendations.shape[0] == 0):
    recommendations_html = ""
    for index, row in recommendations_cs.iterrows():
        recommendations_html += """
        <tr>
          <td style="padding:15px">{5}</td>
          <td style="padding:15px"><a href="{0}"><img src="{1}"></a></td>
          <td style="padding:15px">{2} ({3})</td>
          <td style="padding:15px">
            <a href="https://etherscan.io/address/{4}">
              <img src="https://upload.wikimedia.org/wikipedia/commons/a/a2/External_link_green_icon.svg" width="25" height="25">
            </a>
          </td>
        </tr>
        """.format(row["links"], row["image"], row["name"], row["symbol"].upper(), row["token_address"], index+1)
else:
    recommendations_html = ""
    for index, row in recommendations.iterrows():
        recommendations_html += """
        <tr>
          <td style="padding:15px">{5}</td>
          <td style="padding:15px"><aedi href="{0}"><img src="{1}"></a></td>
          <td style="padding:15px">{2} ({3})</td>
          <td style="padding:15px">
            <a href="https://etherscan.io/address/{4}">
              <img src="https://upload.wikimedia.org/wikipedia/commons/a/a2/External_link_green_icon.svg" width="25" height="25">
            </a>
          </td>
        </tr>
        """.format(row["links"], row["image"], row["name"], row["symbol"].upper(), row["token_address"], index+1)

output_content_str = """
<h2>Recommend Tokens for user address:</h2>
<p style="color:666666;font-size:1.25em">{0}</p>
<table border=0>
{1}
</table>
""".format(str(wallet_address), recommendations_html)

# COMMAND ----------

# Return Success
dbutils.notebook.exit(json.dumps({"exit_code": "OK",
                                  "output_content": output_content_str
                                 }))
