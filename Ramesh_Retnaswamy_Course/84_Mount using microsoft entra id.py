# Databricks notebook source
MOUNT ADLS USING MICROSOFT ENTRA ID (Service Principal OAuth2)

👉 This is the standard, widely-used, and recommended way (when NOT using Managed Identity).

Below are the complete steps.

✅ STEP 1 — Create an App Registration in Microsoft Entra ID

In Azure Portal:

Go to Microsoft Entra ID

Select App Registrations

Click New Registration

Give a name → e.g., databricks-adls-sp

Click Register

Copy these values:

✔ Application (Client) ID
✔ Directory (Tenant) ID

✅ STEP 2 — Create a Client Secret

Under the App Registration:

Go to Certificates & Secrets

Click New Client Secret

Choose expiry (6 months / 12 months / 24 months)

Copy the Value (not the ID)

This is your client secret.

✅ STEP 3 — Assign Permissions on Storage Account

Go to:

Storage Account → Access Control (IAM) → Add Role Assignment

Select role:

✔ Storage Blob Data Contributor
(needed for read/write)

Assign the role to:

➡️ The App Registration (Service Principal)

✅ STEP 4 — Store the Client Secret in Databricks Secret Scope

This is for security.

Create a secret scope:

databricks secrets create-scope --scope myscope


Store your client secret:

databricks secrets put --scope myscope --key clientsecret

✅ STEP 5 — Mount ADLS Using Entra ID (OAuth2 Token Provider)

Use this exact code:

configs = {
  "fs.azure.account.auth.type": "OAuth",
  "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
  "fs.azure.account.oauth2.client.id": "<client-id>",
  "fs.azure.account.oauth2.client.secret": dbutils.secrets.get("myscope", "clientsecret"),
  "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/<tenant-id>/oauth2/token"
}

dbutils.fs.mount(
  source = "abfss://<container>@<storageaccount>.dfs.core.windows.net/",
  mount_point = "/mnt/<mount-name>",
  extra_configs = configs
)


Replace the values:

<client-id> → App registration Client ID

<tenant-id> → Directory (Tenant) ID

<container>

<storageaccount>

<mount-name>

✅ STEP 6 — Verify the Mount
display(dbutils.fs.ls("/mnt/<mount-name>"))

🧹 STEP 7 — Unmount if Needed
dbutils.fs.unmount("/mnt/<mount-name>")
