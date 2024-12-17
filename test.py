from prefect.blocks.system import Secret

secret_block_sf_password = Secret.load("snowflake-password")
secret_block_sf_account = Secret.load("snowflake-account")
secret_block_dbt_api = Secret.load("dbt-api-key")

secret_block_dbt_api.get()
secret_block_sf_password.get()
secret_block_sf_account.get()

print(secret_block_dbt_api)