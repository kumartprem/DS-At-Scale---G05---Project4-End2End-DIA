-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Ethereum Blockchain Data Analysis - <a href=https://github.com/blockchain-etl/ethereum-etl-airflow/tree/master/dags/resources/stages/raw/schemas>Table Schemas</a>
-- MAGIC - **Transactions** - Each block in the blockchain is composed of zero or more transactions. Each transaction has a source address, a target address, an amount of Ether transferred, and an array of input bytes. This table contains a set of all transactions from all blocks, and contains a block identifier to get associated block-specific information associated with each transaction.
-- MAGIC - **Blocks** - The Ethereum blockchain is composed of a series of blocks. This table contains a set of all blocks in the blockchain and their attributes.
-- MAGIC - **Receipts** - the cost of gas for specific transactions.
-- MAGIC - **Traces** - The trace module is for getting a deeper insight into transaction processing. Traces exported using <a href=https://openethereum.github.io/JSONRPC-trace-module.html>Parity trace module</a>
-- MAGIC - **Tokens** - Token data including contract address and symbol information.
-- MAGIC - **Token Transfers** - The most popular type of transaction on the Ethereum blockchain invokes a contract of type ERC20 to perform a transfer operation, moving some number of tokens from one 20-byte address to another 20-byte address. This table contains the subset of those transactions and has further processed and denormalized the data to make it easier to consume for analysis of token transfer events.
-- MAGIC - **Contracts** - Some transactions create smart contracts from their input bytes, and this smart contract is stored at a particular 20-byte address. This table contains a subset of Ethereum addresses that contain contract byte-code, as well as some basic analysis of that byte-code. 
-- MAGIC - **Logs** - Similar to the token_transfers table, the logs table contains data for smart contract events. However, it contains all log data, not only ERC20 token transfers. This table is generally useful for reporting on any logged event type on the Ethereum blockchain.
-- MAGIC 
-- MAGIC ### Rubric for this module
-- MAGIC Answer the quetions listed below.

-- COMMAND ----------

-- MAGIC %run ./includes/utilities

-- COMMAND ----------

-- MAGIC %run ./includes/configuration

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC # Grab the global variables
-- MAGIC wallet_address,start_date = Utils.create_widgets()
-- MAGIC print(wallet_address,start_date)
-- MAGIC spark.conf.set('wallet.address',wallet_address)
-- MAGIC spark.conf.set('start.date',start_date)
-- MAGIC 
-- MAGIC sqlContext.setConf('spark.sql.shuffle.partitions', 'auto')
-- MAGIC sqlContext.setConf('spark.sql.adaptive.skewJoin.enabled', 'true')

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### NOTE:

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Q: What is the maximum block number and date of block in the database

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC df_q01 = spark.sql("""
-- MAGIC select number as maximum_block_number, to_date(CAST(`timestamp` as TIMESTAMP)) as date_of_max_block
-- MAGIC from ethereumetl.blocks
-- MAGIC where number in (select max(number) from ethereumetl.blocks)
-- MAGIC """)
-- MAGIC 
-- MAGIC results = df_q01.select('maximum_block_number', 'date_of_max_block').first()
-- MAGIC maximum_block_number = results[0]
-- MAGIC date_of_max_block = results[1]
-- MAGIC 
-- MAGIC print(f"The maximum block number in the database is {maximum_block_number} and its date is {date_of_max_block}.")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Q: At what block did the first ERC20 transfer happen?

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC # Note:
-- MAGIC #   -- The 'g05_db.token_transfers_erc20' table is a subset of 'ethereumetl.token_transfers'
-- MAGIC #   -- The table was created:
-- MAGIC #        1) With only ERC20 tokens (from silver_contracts where 'is_erc20 = True' and 'is_erc721 = False')
-- MAGIC #        2) Without any transfers where from_address or to_address = '0x0000000000000000000000000000000000000000' (null/vanity address on Ethereum blockchain)
-- MAGIC 
-- MAGIC df_q02 = spark.sql("""
-- MAGIC select min(block_number) as block_number
-- MAGIC from g05_db.token_transfers_erc20
-- MAGIC """)
-- MAGIC 
-- MAGIC block_num = df_q02.select('block_number').first()[0]
-- MAGIC 
-- MAGIC print(f"The first ERC20 transfer happened in the block {block_number}.")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Q: How many ERC20 compatible contracts are there on the blockchain?

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC # Note:
-- MAGIC #   -- The 'g05_db.contracts_erc20' table is a subset of 'ethereumetl.silver_contracts'
-- MAGIC #   -- The table was created with filters 'is_erc20 = True' and 'is_erc721 = False'
-- MAGIC #   -- There are 21 tokens with True for both 'is_erc20' & 'is_erc721'. We chose to exclude them in our analysis
-- MAGIC 
-- MAGIC df_q03 = spark.sql("""
-- MAGIC select count(distinct address) as erc20_contracts 
-- MAGIC from g05_db.contracts_erc20
-- MAGIC """)
-- MAGIC 
-- MAGIC erc20_contracts = df_q03.select('erc20_contracts').first()[0]
-- MAGIC 
-- MAGIC print(f"There are {erc20_contracts} ERC20 compatible contracts on the blockchain.")

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ## Q: What percentage of transactions are calls to contracts

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC # Note:
-- MAGIC #   -- If the receiver address of a transaction is a contract, then that transaction is a call to contract.
-- MAGIC 
-- MAGIC df_q04 = spark.sql("""
-- MAGIC select round((sum(cast((sc.address is not null) as integer))/count(1))*100, 2) as percentage_
-- MAGIC from ethereumetl.transactions t
-- MAGIC left join ethereumetl.silver_contracts sc on t.to_address = sc.address
-- MAGIC """)
-- MAGIC 
-- MAGIC percentage_ = df_q04.select('percentage_').first()[0]
-- MAGIC 
-- MAGIC print(f"{percentage_}% of transactions are calls to contracts.")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Q: What are the top 100 tokens based on transfer count?

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC # Note:
-- MAGIC #   -- The 'g05_db.token_transfers_valid' table is a subset of 'ethereumetl.token_transfers'
-- MAGIC #   -- The table was created:
-- MAGIC #        1) Without any transfers where from_address or to_address = '0x0000000000000000000000000000000000000000' (null/vanity address on Ethereum blockchain)
-- MAGIC #   -- In an ideal case, the keyword 'distinct' is redundant. But we chose to use it just to avoid any duplicate entries.
-- MAGIC 
-- MAGIC df_q05 = spark.sql("""
-- MAGIC select
-- MAGIC token_address, count(distinct transaction_hash) as transfer_count
-- MAGIC from g05_db.token_transfers_valid
-- MAGIC group by token_address
-- MAGIC order by transfer_count desc
-- MAGIC limit 100
-- MAGIC """)
-- MAGIC 
-- MAGIC df_q05.show(100, truncate=False)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Q: What fraction of ERC-20 transfers are sent to new addresses
-- MAGIC (i.e. addresses that have a transfer count of 1 meaning there are no other transfers to this address for this token this is the first)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC # Note:
-- MAGIC #   -- Every first transfer of an ERC20 token to an address is considered. Even if transfer count to an address is greater than 1, the first transfer to that address is considered a transfer to a new address.
-- MAGIC #   -- The 'g05_db.token_transfers_erc20' table is a subset of 'ethereumetl.token_transfers'
-- MAGIC #   -- The table was created:
-- MAGIC #        1) With only ERC20 tokens (from silver_contracts where 'is_erc20 = True' and 'is_erc721 = False')
-- MAGIC #        2) Without any transfers where from_address or to_address = '0x0000000000000000000000000000000000000000' (null/vanity address on Ethereum blockchain)
-- MAGIC 
-- MAGIC spark.sql("use ethereumetl")
-- MAGIC 
-- MAGIC df_q06 = spark.sql("""
-- MAGIC select round((count(distinct concat(token_address, to_address))/count(1))*100, 2) as percentage_
-- MAGIC from g05_db.token_transfers_erc20
-- MAGIC """)
-- MAGIC 
-- MAGIC percentage_ = df_q06.select('percentage_').first()[0]
-- MAGIC 
-- MAGIC print(f"{percentage_}% of ERC20 transfers are sent to new addresses.")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Q: In what order are transactions included in a block in relation to their gas price?
-- MAGIC - hint: find a block with multiple transactions 

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC spark.sql("use ethereumetl")
-- MAGIC 
-- MAGIC # ---STEP 1------------------------------------
-- MAGIC # The table 'blocks' is partitioned based on the columns 'start_block' & 'end_block'
-- MAGIC # spark.sql("show partitions ethereumetl.blocks").show(1000, truncate=False)
-- MAGIC # ---------------------------------------------
-- MAGIC 
-- MAGIC # ---STEP 2------------------------------------
-- MAGIC # Let's find a block with more than 1 transaction in the last partition.
-- MAGIC # In the first partition, a few blocks had multple transactions with the same gas_price
-- MAGIC # df_q07_temp = spark.sql("""
-- MAGIC # select number, transaction_count
-- MAGIC # from blocks
-- MAGIC # where start_block >= 14030000 and transaction_count > 10
-- MAGIC # limit 20
-- MAGIC # """)
-- MAGIC # df_q07_temp.show(20, truncate=False)
-- MAGIC # ---------------------------------------------
-- MAGIC 
-- MAGIC # ---STEP 3------------------------------------
-- MAGIC # Let's check three blocks '14030300', '14030400', '14030401' (randomly chosen)
-- MAGIC df_q07 = spark.sql("""
-- MAGIC select block_number, transaction_index, gas_price, hash
-- MAGIC from transactions
-- MAGIC where start_block >= 14030000 and block_number in (14030400, 14030401, 14030300)
-- MAGIC """)
-- MAGIC df_q07.show(1000, truncate=True)
-- MAGIC 
-- MAGIC print("The transactions are included in a block in the order of increasing gas price - higher gas price first. This makes sense because, we think the miners can maximize their rewards this ways.")
-- MAGIC print("However, in block '14030400', there are a few transactions with low gas prices that occur before the transaction with the highest gas price. We are not sure why this happened. Probably the miners had a different motivation to include these transactions first.")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Q: What was the highest transaction throughput in transactions per second?
-- MAGIC hint: assume 15 second block time

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC # Note:
-- MAGIC #   -- Given (hint), a block gets executed in 15 seconds. So, the block with maximum number of transactions will give us the maximum throughtput.
-- MAGIC 
-- MAGIC df_q08 = spark.sql("""
-- MAGIC select max(transaction_count)/15 as max_throughput
-- MAGIC from ethereumetl.blocks
-- MAGIC """)
-- MAGIC 
-- MAGIC max_throughput = df_q08.select('max_throughput').first()[0]
-- MAGIC 
-- MAGIC print(f"The highest throughput was {max_throughput} transactions per second.")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Q: What is the total Ether volume?
-- MAGIC Note: 1x10^18 wei to 1 eth and value in the transaction table is in wei

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC # Note:
-- MAGIC #   -- Total ether volume is calculated as the total amount of ether transacted in the blockchain
-- MAGIC 
-- MAGIC df_q09 = spark.sql("""
-- MAGIC select sum(value)/power(10,18) as total_ether_volume
-- MAGIC from ethereumetl.transactions
-- MAGIC """)
-- MAGIC 
-- MAGIC total_ether_volume = df_q09.select('total_ether_volume').first()[0]
-- MAGIC 
-- MAGIC print(f"The total ether volume is {total_ether_volume}.")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Q: What is the total gas used in all transactions?

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC # Note:
-- MAGIC #    -- The 'receipts' table has a lot more transaction entries than the 'transactions' table. For instance, the blocks from 11100000 to 13599999 are not available in the 'transactions' table, but are available in the other tables. We are not sure about the reason for this anomaly.
-- MAGIC #    -- We can calcualte the total gas used in:
-- MAGIC #             1) all transaction entries available in the 'receipts' table
-- MAGIC #             2) only the transactions available in the 'transactions' table 
-- MAGIC #    -- We display the result of appraoch 1.
-- MAGIC #    -- In approach 2, we get a value of 11,221,616,370,530 as the total gas used.
-- MAGIC 
-- MAGIC df_q10 = spark.sql("""
-- MAGIC select 
-- MAGIC sum(gas_used) as total_gas_used
-- MAGIC from ethereumetl.receipts
-- MAGIC """)
-- MAGIC 
-- MAGIC total_gas_used = df_q10.select('total_gas_used').first()[0]
-- MAGIC 
-- MAGIC print(f"The total gas used in all transactions is {total_gas_used}.")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Q: Maximum ERC-20 transfers in a single transaction

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC # Note:
-- MAGIC #   -- The 'g05_db.token_transfers_erc20' table is a subset of 'ethereumetl.token_transfers'
-- MAGIC #   -- The table was created:
-- MAGIC #        1) With only ERC20 tokens (from silver_contracts where 'is_erc20 = True' and 'is_erc721 = False')
-- MAGIC #        2) Without any transfers where from_address or to_address = '0x0000000000000000000000000000000000000000' (null/vanity address on Ethereum blockchain)
-- MAGIC 
-- MAGIC df_q11 = spark.sql("""
-- MAGIC select max(transfer_count) as max_value
-- MAGIC from
-- MAGIC (
-- MAGIC     select transaction_hash, count(*) as transfer_count
-- MAGIC     from g05_db.token_transfers_erc20
-- MAGIC     group by transaction_hash
-- MAGIC )
-- MAGIC """)
-- MAGIC 
-- MAGIC max_erc20_transfer = df_q11.select('max_value').first()[0]
-- MAGIC 
-- MAGIC print(f"There is a maximum of {max_erc20_transfer} ERC-20 transfers in a single transaction.")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Q: Token balance for any address on any date?

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC # Note:
-- MAGIC #   -- We calucate the token balances as the difference between the amount transferred out and in the wallet
-- MAGIC #   -- We consider the wallet_address and start_date values passed in from the widgets for our analysis
-- MAGIC #        1) wallet.address = wallet.address
-- MAGIC #        2) date.of.balance = start.date
-- MAGIC #   -- The 'g05_db.token_transfers_valid' table is a subset of 'ethereumetl.token_transfers'
-- MAGIC #   -- The table was created:
-- MAGIC #        1) Without any transfers where from_address or to_address = '0x0000000000000000000000000000000000000000' (null/vanity address on Ethereum blockchain)
-- MAGIC 
-- MAGIC spark.conf.set('balance.check.date',start_date)
-- MAGIC 
-- MAGIC df_q12_final = spark.sql("""
-- MAGIC select token_address, sum(case when from_address = '${wallet.address}' then -1*value else 1*value end) as balance
-- MAGIC from g05_db.token_transfers_valid t
-- MAGIC inner join ethereumetl.blocks b on b.start_block = t.start_block and b.end_block = t.end_block and b.number = t.block_number
-- MAGIC                                     and to_date(cast(b.`timestamp` as TIMESTAMP)) <= '${balance.check.date}' 
-- MAGIC                                     and (from_address = '${wallet.address}' or to_address = '${wallet.address}')
-- MAGIC group by token_address
-- MAGIC order by balance desc
-- MAGIC """)
-- MAGIC 
-- MAGIC df_q12_final.show(100000,False)
-- MAGIC 
-- MAGIC # NOTE: We use 'g05_db.token_transfers_valid' and not 'ethereumetl.token_transfers'.
-- MAGIC # For the wallet address '0xf02d7ee27ff9b2279e76a60978bf8cca9b18a3ff', the balance of 2022-01-01 is: 
-- MAGIC #
-- MAGIC # 1) in 'ethereumetl.token_transfers'
-- MAGIC # +------------------------------------------+-------------------+
-- MAGIC # |token_address                             |balance            |
-- MAGIC # +------------------------------------------+-------------------+
-- MAGIC # |0x6b175474e89094c44da98b954eedeac495271d0f|3690152290000000000|
-- MAGIC # |0x6d8cc2d52de2ac0b1c6d8bc964ba66c91bb756e1|4802               |
-- MAGIC # +------------------------------------------+-------------------+
-- MAGIC #
-- MAGIC # 2) in 'g05_db.token_transfers_valid'
-- MAGIC # +------------------------------------------+-------------------+
-- MAGIC # |token_address                             |balance            |
-- MAGIC # +------------------------------------------+-------------------+
-- MAGIC # |0x6b175474e89094c44da98b954eedeac495271d0f|3690152290000000000|
-- MAGIC # +------------------------------------------+-------------------+ 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Viz the transaction count over time (network use)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC df_q13 = spark.sql("""
-- MAGIC select
-- MAGIC to_date(CAST(`timestamp` as timestamp)) as `date`,
-- MAGIC sum(transaction_count) as transaction_count_in_a_day
-- MAGIC from blocks
-- MAGIC where year(to_date(CAST(`timestamp` as timestamp))) >= 2015
-- MAGIC group by `date`
-- MAGIC order by `date`
-- MAGIC """)
-- MAGIC 
-- MAGIC display(df_q13)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Viz ERC-20 transfer count over time
-- MAGIC interesting note: https://blog.ins.world/insp-ins-promo-token-mixup-clarified-d67ef20876a3

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC # Note:
-- MAGIC #   -- The 'g05_db.token_transfers_erc20' table is a subset of 'ethereumetl.token_transfers'
-- MAGIC #   -- The table was created:
-- MAGIC #        1) With only ERC20 tokens (from silver_contracts where 'is_erc20 = True' and 'is_erc721 = False')
-- MAGIC #        2) Without any transfers where from_address or to_address = '0x0000000000000000000000000000000000000000' (null/vanity address on Ethereum blockchain)
-- MAGIC 
-- MAGIC df_q14 = spark.sql("""
-- MAGIC select `date`, sum(transfer_count) as transfer_count_in_a_day
-- MAGIC from
-- MAGIC (
-- MAGIC     select start_block, end_block, number, to_date(CAST(`timestamp` AS timestamp)) as `date`
-- MAGIC     from ethereumetl.blocks 
-- MAGIC ) b 
-- MAGIC left join
-- MAGIC (
-- MAGIC     select start_block, end_block, block_number, count(distinct transaction_hash) as transfer_count
-- MAGIC     from g05_db.token_transfers_erc20
-- MAGIC     group by start_block, end_block, block_number
-- MAGIC ) tt
-- MAGIC -- Joining based on 'start_block' and 'end_block' makes the process faster, as the tables are partitioned on those variables, and they have the same partition bounds
-- MAGIC -- Note: One particular partition 'start_block=12400000/end_block=12499999' is not available in token_transfers table
-- MAGIC on b.start_block = tt.start_block and b.end_block = tt.end_block and b.number = tt.block_number
-- MAGIC where year(`date`) >= 2015
-- MAGIC group by `date`
-- MAGIC order by `date`
-- MAGIC """)
-- MAGIC 
-- MAGIC display(df_q14)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Return Success
-- MAGIC dbutils.notebook.exit(json.dumps({"exit_code": "OK"}))
