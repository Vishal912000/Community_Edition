# Databricks notebook source
def mount_adls(storage_account,container_name):
    # Acessing Key vault
    client_id=dbutils.fs.secrets(scope='',key='')
    tenant_id=dbutils.fs.secrets(scope='',key='')
    client_secret=dbutils.fs.secrets(scope='',key='')

    #Providing configs
    configs = {f"fs.azure.account.auth.type": "OAuth",
                f"fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
                f"fs.azure.account.oauth2.client.id": f"{client_id}",
                f"fs.azure.account.oauth2.client.secret":f"{client_secret} "),
                f"fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/<directory-id>/oauth2/token"}
    
    for mount in dbutils.fs.mounts():
        if mount == f"/mnt/{storage_account}/{container_name}"
        print(f"{mount} already exists,please choose a different name")
    
    dbutils.fs.mount(
        source=f"abfss:/{storage_account}@{container_name}.dfs.windows.net"
        mount_point=f"/mnt/{storage_account}/{container_name}"
        extra_configs=configs
    )
    display(dbutils.fs.mounts())
