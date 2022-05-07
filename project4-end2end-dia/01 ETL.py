# Databricks notebook source
# MAGIC %md
# MAGIC ## Ethereum Blockchain Data Analysis - <a href=https://github.com/blockchain-etl/ethereum-etl-airflow/tree/master/dags/resources/stages/raw/schemas>Table Schemas</a>
# MAGIC - **Transactions** - Each block in the blockchain is composed of zero or more transactions. Each transaction has a source address, a target address, an amount of Ether transferred, and an array of input bytes. This table contains a set of all transactions from all blocks, and contains a block identifier to get associated block-specific information associated with each transaction.
# MAGIC - **Blocks** - The Ethereum blockchain is composed of a series of blocks. This table contains a set of all blocks in the blockchain and their attributes.
# MAGIC - **Receipts** - the cost of gas for specific transactions.
# MAGIC - **Traces** - The trace module is for getting a deeper insight into transaction processing. Traces exported using <a href=https://openethereum.github.io/JSONRPC-trace-module.html>Parity trace module</a>
# MAGIC - **Tokens** - Token data including contract address and symbol information.
# MAGIC - **Token Transfers** - The most popular type of transaction on the Ethereum blockchain invokes a contract of type ERC20 to perform a transfer operation, moving some number of tokens from one 20-byte address to another 20-byte address. This table contains the subset of those transactions and has further processed and denormalized the data to make it easier to consume for analysis of token transfer events.
# MAGIC - **Contracts** - Some transactions create smart contracts from their input bytes, and this smart contract is stored at a particular 20-byte address. This table contains a subset of Ethereum addresses that contain contract byte-code, as well as some basic analysis of that byte-code. 
# MAGIC - **Logs** - Similar to the token_transfers table, the logs table contains data for smart contract events. However, it contains all log data, not only ERC20 token transfers. This table is generally useful for reporting on any logged event type on the Ethereum blockchain.
# MAGIC 
# MAGIC In Addition, there is a price feed that changes daily (noon) that is in the **token_prices_usd** table
# MAGIC 
# MAGIC ### Rubric for this module
# MAGIC - Transform the needed information in ethereumetl database into the silver delta table needed by your modeling module
# MAGIC - Clearly document using the notation from [lecture](https://learn-us-east-1-prod-fleet02-xythos.content.blackboardcdn.com/5fdd9eaf5f408/8720758?X-Blackboard-Expiration=1650142800000&X-Blackboard-Signature=h%2FZwerNOQMWwPxvtdvr%2FmnTtTlgRvYSRhrDqlEhPS1w%3D&X-Blackboard-Client-Id=152571&response-cache-control=private%2C%20max-age%3D21600&response-content-disposition=inline%3B%20filename%2A%3DUTF-8%27%27Delta%2520Lake%2520Hands%2520On%2520-%2520Introduction%2520Lecture%25204.pdf&response-content-type=application%2Fpdf&X-Amz-Security-Token=IQoJb3JpZ2luX2VjEAAaCXVzLWVhc3QtMSJHMEUCIQDEC48E90xPbpKjvru3nmnTlrRjfSYLpm0weWYSe6yIwwIgJb5RG3yM29XgiM%2BP1fKh%2Bi88nvYD9kJNoBNtbPHvNfAqgwQIqP%2F%2F%2F%2F%2F%2F%2F%2F%2F%2FARACGgw2MzU1Njc5MjQxODMiDM%2BMXZJ%2BnzG25TzIYCrXAznC%2BAwJP2ee6jaZYITTq07VKW61Y%2Fn10a6V%2FntRiWEXW7LLNftH37h8L5XBsIueV4F4AhT%2Fv6FVmxJwf0KUAJ6Z1LTVpQ0HSIbvwsLHm6Ld8kB6nCm4Ea9hveD9FuaZrgqWEyJgSX7O0bKIof%2FPihEy5xp3D329FR3tpue8vfPfHId2WFTiESp0z0XCu6alefOcw5rxYtI%2Bz9s%2FaJ9OI%2BCVVQ6oBS8tqW7bA7hVbe2ILu0HLJXA2rUSJ0lCF%2B052ScNT7zSV%2FB3s%2FViRS2l1OuThecnoaAJzATzBsm7SpEKmbuJkTLNRN0JD4Y8YrzrB8Ezn%2F5elllu5OsAlg4JasuAh5pPq42BY7VSKL9VK6PxHZ5%2BPQjcoW0ccBdR%2Bvhva13cdFDzW193jAaE1fzn61KW7dKdjva%2BtFYUy6vGlvY4XwTlrUbtSoGE3Gr9cdyCnWM5RMoU0NSqwkucNS%2F6RHZzGlItKf0iPIZXT3zWdUZxhcGuX%2FIIA3DR72srAJznDKj%2FINdUZ2s8p2N2u8UMGW7PiwamRKHtE1q7KDKj0RZfHsIwRCr4ZCIGASw3iQ%2FDuGrHapdJizHvrFMvjbT4ilCquhz4FnS5oSVqpr0TZvDvlGgUGdUI4DCdvOuSBjqlAVCEvFuQakCILbJ6w8WStnBx1BDSsbowIYaGgH0RGc%2B1ukFS4op7aqVyLdK5m6ywLfoFGwtYa5G1P6f3wvVEJO3vyUV16m0QjrFSdaD3Pd49H2yB4SFVu9fgHpdarvXm06kgvX10IfwxTfmYn%2FhTMus0bpXRAswklk2fxJeWNlQF%2FqxEmgQ6j4X6Q8blSAnUD1E8h%2FBMeSz%2F5ycm7aZnkN6h0xkkqQ%3D%3D&X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Date=20220416T150000Z&X-Amz-SignedHeaders=host&X-Amz-Expires=21600&X-Amz-Credential=ASIAZH6WM4PLXLBTPKO4%2F20220416%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Signature=321103582bd509ccadb1ed33d679da5ca312f19bcf887b7d63fbbb03babae64c) how your pipeline is structured.
# MAGIC - Your pipeline should be immutable
# MAGIC - Use the starting date widget to limit how much of the historic data in ethereumetl database that your pipeline processes.

# COMMAND ----------

# MAGIC %run ./includes/utilities

# COMMAND ----------

# MAGIC %run ./includes/configuration

# COMMAND ----------

# Grab the global variables
wallet_address,start_date = Utils.create_widgets()
print(wallet_address,start_date)
spark.conf.set('wallet.address',wallet_address)
spark.conf.set('start.date',start_date)

spark.conf.set("hive.exec.dynamic.partition", "true")
spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")

# COMMAND ----------

# MAGIC %md
# MAGIC ## YOUR SOLUTION STARTS HERE...

# COMMAND ----------

# MAGIC %md
# MAGIC ### Creation of silver tables
# MAGIC Creation of tables required for training a recommender system
# MAGIC - The silver tables are created and saved in the database 'g05_db'.
# MAGIC - The silver tables will be dropped after the entire process is run.
# MAGIC - The silver tables are created with a date filter (using the widget 'start_date').

# COMMAND ----------

# MAGIC %md
# MAGIC ### 'Contracts' silver table
# MAGIC - The 'g05_db.SilverTable_ERC20_ethereum_tokens' table is a subset of 'ethereumetl.token_prices_usd'
# MAGIC - The table is created by:
# MAGIC     - identifying Ethereum tokens (from token_prices_usd where asset_platform_id = 'ethereum')
# MAGIC     - identifying ERC20 tokens (from silver_contracts where 'is_erc20 = True' and 'is_erc721 = False')
# MAGIC - This gives us a total of 248 tokens which will be our focus for the recommender system
# MAGIC - Note: There are 21 tokens with 'True' for both 'is_erc20' & 'is_erc721'. We chose to exclude them in our analysis

# COMMAND ----------

# Load the required bronze tables
eth_sc = spark.table("ethereumetl.silver_contracts")
eth_tpu = spark.table("ethereumetl.token_prices_usd")

# Create the silver table dataframe
g05_tpu = (
    eth_tpu.alias("tpu")
    .join(eth_sc.alias("sc"), (eth_tpu.contract_address == eth_sc.address), 'inner')
    .filter( (col("asset_platform_id") == 'ethereum') & (col("is_erc20") == True) & (col("is_erc721") == False) )
    .select("tpu.*")
    .dropDuplicates()
)

# Save as a Delta table
spark.sql("drop table if exists g05_db.SilverTable_ERC20_ethereum_tokens")
(
  g05_tpu.write
    .format("delta")
    .mode("overwrite")
    .option("mergeSchema", "true")
    .saveAsTable("g05_db.SilverTable_ERC20_ethereum_tokens")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 'Blocks' silver table
# MAGIC - The silver table 'g05_db.SilverTable_Blocks' is a subset of 'ethereumetl.blocks'
# MAGIC - The silver table contains only the blocks that were created on or after the input 'start_date'

# COMMAND ----------

# Load the required bronze tables
eth_b = spark.table("ethereumetl.blocks")

# Create the silver table dataframe
g05_b = (
    eth_b
    .select("*")
    .filter( to_date(to_timestamp(col("timestamp")),"yyyy-MM-dd") >= (spark.conf.get('start.date')) )
)

# Save as a Delta table
spark.sql("drop table if exists g05_db.SilverTable_Blocks")
(
  g05_b.write
    .format("delta")
    .mode("overwrite")
    .partitionBy("start_block", "end_block")
    .option("mergeSchema", "true")
    .saveAsTable("g05_db.SilverTable_Blocks")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 'Token Transfers' silver table
# MAGIC - The silver table 'g05_db.SilverTable_Token_transfer' is a subset of 'ethereumetl.token_transfers'
# MAGIC - The silver table contains only the transfers that happened on or after the 'start_date' (input widget)
# MAGIC - Also, any token transfer from or to the null/vanity address on Ethereum blockchain is excluded from the analysis

# COMMAND ----------

# Load the required bronze and silver tables
eth_tt = spark.table("ethereumetl.token_transfers")
eth_st_tok = spark.table("g05_db.SilverTable_ERC20_ethereum_tokens")
eth_st_b = spark.table("g05_db.SilverTable_Blocks")

# Create the silver table dataframe
g05_tt = (
    eth_tt.alias("tt")
    .join(eth_st_b.alias("b"), ((eth_tt.start_block == eth_st_b.start_block) & (eth_tt.end_block == eth_st_b.end_block) & (eth_tt.block_number == eth_st_b.number)), 'inner')
    .select("tt.*")
    .dropDuplicates()
)

g05_tt_v2 = (
    g05_tt.alias("tt")
    .join(eth_st_tok.alias("st"), (g05_tt.token_address == eth_st_tok.contract_address), 'inner')
    .select("tt.*")
    .filter( 
        (col("from_address") != '0x0000000000000000000000000000000000000000') & 
        (col("to_address") != '0x0000000000000000000000000000000000000000') & 
        (col("to_address") != col("token_address")) 
    )
    .dropDuplicates()
)

# Save as a Delta table
spark.sql("drop table if exists g05_db.SilverTable_filtered_token_transfers")
(
  g05_tt_v2.write
    .format("delta")
    .mode("overwrite")
    .partitionBy("token_address")
    .option("mergeSchema", "true")
    .saveAsTable("g05_db.SilverTable_filtered_token_transfers")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 'Wallet vs Token' Silver table
# MAGIC - The silver table 'g05_db.SilverTable_Wallets' contains information about the number of transfers (of the identified 248 tokens) between user wallets on or after the start date (input)
# MAGIC - We compute the total number of transactions to and from every wallet.
# MAGIC   - Sending and/or receiving a token more implies a higher preference for that token.

# COMMAND ----------

# Load the required silver tables
eth_tt = spark.table("g05_db.SilverTable_filtered_token_transfers")

# Create the silver table dataframe
g05_silver_wallets = (
    (
        ( eth_tt.select(col("from_address").alias("wallet_address"), col("token_address")) )
        .union( eth_tt.select(col("to_address").alias("wallet_address"), col("token_address")) )
    )
    .groupBy('wallet_address', 'token_address')
    .agg(count('*').alias('transactions'))
    .dropDuplicates()
)

# Save as a Delta table
spark.sql("drop table if exists g05_db.SilverTable_Wallets")
(
  g05_silver_wallets.write
    .format("delta")
    .mode("overwrite")
    .partitionBy("token_address")
    .option("mergeSchema", "true")
    .saveAsTable("g05_db.SilverTable_Wallets")
)

# Peform Z-order optimization with the other non-partitioned dimension 'wallet_address'
spark.sql("optimize g05_db.SilverTable_Wallets zorder by (wallet_address)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 'Wallet vs Token' Gold table
# MAGIC - The gold table 'g05_db.GoldTable_Wallet_details' contains the same information as the silver table created above, and also the name, image, link, and symbol of every token (token_prices_usd table)

# COMMAND ----------

eth_sw = spark.table("g05_db.SilverTable_Wallets")
eth_st = spark.table("g05_db.SilverTable_ERC20_ethereum_tokens")

# Create the gold table dataframe
g05_gold = (
    eth_sw.alias("w")
    .join(eth_st.alias("t"), eth_sw.token_address == eth_st.contract_address)
    .select("w.*", "t.name", "t.image", "t.links", "t.symbol")
    .dropDuplicates()
)

# Save as a Delta table
spark.sql("drop table if exists g05_db.GoldTable_Wallet_details")
(
  g05_gold.write
    .format("delta")
    .mode("overwrite")
    .partitionBy("token_address")
    .option("mergeSchema", "true")
    .saveAsTable("g05_db.GoldTable_Wallet_details")
)

# Peform Z-order optimization with the other non-partitioned dimension 'wallet_address'
spark.sql("optimize g05_db.GoldTable_Wallet_details zorder by (wallet_address)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Tables for EDA
# MAGIC Creation of tables required for EDA
# MAGIC - These tables are created and saved in the database 'g05_db'.
# MAGIC - The table creation codes are commented out, as they take hours to partition the tables.
# MAGIC - We used spark's sql context to create these EDA related tables.

# COMMAND ----------

# Create a table with ERC20 contracts only
#  -- The table 'g05_db.contracts_erc20' is a subset of 'ethereumetl.silver_contracts'.
#  -- identifying ERC20 tokens (from silver_contracts where 'is_erc20 = True' and 'is_erc721 = False')
#  -- This gives us a total of 248 tokens which will be our focus for the recommender system
#  -- Note: There are 21 tokens with 'True' for both 'is_erc20' & 'is_erc721'. We chose to exclude them in our analysis

# spark.sql("drop table if exists g05_db.contracts_erc20")

# spark.sql("""
# create table g05_db.contracts_erc20 as
# select * 
# from ethereumetl.silver_contracts
# where is_erc20 = True and is_erc721 = False
# """)

# COMMAND ----------

# Create a table with valid token transfers only
#  -- The table 'g05_db.token_transfers_valid' is a subset of 'ethereumetl.token_transfers'.
#  -- This table is used to answer a few questions in EDA.
#  -- The address '0x0000000000000000000000000000000000000000' is not a valid user wallet address. It's the null/vanity address in ethereum blockchian, and any transfer to or from this address is excluded from our analysis. 

# spark.sql("drop table if exists g05_db.token_transfers_valid")
# spark.sql("create table g05_db.token_transfers_valid LIKE ethereumetl.token_transfers")

# spark.sql("""
# insert into g05_db.token_transfers_valid
# partition(start_block, end_block)
# select *
# from ethereumetl.token_transfers
# where from_address != '0x0000000000000000000000000000000000000000' and to_address != '0x0000000000000000000000000000000000000000'
# """)

# COMMAND ----------

# Create a table with ERC20 token transfers only
#  -- The table 'g05_db.token_transfers_erc20' is a subset of 'ethereumetl.token_transfers'.
#  -- This table is used to answer a few questions in EDA.
#  -- Also, we consider only the valid token transfers (from the table 'g05_db.token_transfers_valid' created above)

# spark.sql("drop table if exists g05_db.token_transfers_erc20")
# spark.sql("create table g05_db.token_transfers_erc20 LIKE ethereumetl.token_transfers")

# spark.sql("""
# insert into g05_db.token_transfers_erc20
# partition(start_block, end_block) 
# select * 
# from g05_db.token_transfers_valid
# where token_address in (select address from g05_db.contracts_erc20)
# """)

# COMMAND ----------

wallet_check = spark.sql("select count(*) as cnt from g05_db.GoldTable_Wallet_details where wallet_address = '${wallet.address}'")
test_value = wallet_check.select('cnt').first()[0]

# COMMAND ----------

# Return Success
dbutils.notebook.exit(json.dumps({"exit_code": "OK",
                                  "test_val": test_value,
                                  "message_to_user": "Warning: The given wallet address is not available in the gold table (g05_db.GoldTable_Wallet_details). So, a generic recommendation (not user specific) will be made."
                                 }))
