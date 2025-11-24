# Oracle - Access Governance Data Feed Analytics (DFA)

Data Feed Analytics (DFA) is an implementation to extract, transform, and load (ETL) data published by the Data Feed feature in the Oracle Access Governance product. This implementation creates several resources (listed below) in your OCI tenancy and transforms the published data into an Oracle Autonomous Database. This makes it simpler to query the data to build powerful reports in your preferred choice of BI tools.

## Getting Started

### Prerequisites

Please verify the following resources can be created in your OCI tenancy (ensuring access and permissions to list, create, and manage the resource as well as availability of resource limits):
    - VCN  
    - Vault  
    - Master Encryption Key  
    - Secrets  
    - Autonomous Database  
    - Functions Application  
    - Concurrency for Functions (70 units required by default)  
    - Event Rules  
    - Connector Hubs  
    - Dynamic Group  
    - Policy  

Install OCI CLI: https://docs.oracle.com/en-us/iaas/Content/API/SDKDocs/cliinstall.htm  
Install Python: https://www.python.org/downloads/. Python version 3.12.x is required.  
Install Docker/Podman in order to build images: https://www.docker.com/get-started https://podman.io/get-started  
Set up a virtual environment for Python: https://docs.python.org/3/library/venv.html  


### Run Unit Tests

To execute DFA's unit tests, ensure the tox package is installed in your virtual environment, and python version is set to 3.11.x. 
Navigate to the project's root, and run: `tox`

### Build and Upload Docker Image

The following article describes a quick tutorial on how to build and upload Docker images -  
https://www.oracle.com/webfolder/technetwork/tutorials/obe/oci/registry/index.html

1. Log in to your OCI tenancy. 
2. Navigate to the Container Registry. 
3. Create a new repo in the target compartment (i.e dfa-images/dfa). This name must be unique across the tenancy. Update the REPOSITORY_NAME variable value in config.ini to match the new repo name if needed. 
4. Build the DFA image. If using podman, replace 'docker' with 'podman' for the following commands. 
```shell
docker build --platform linux/amd64 -t <region-key>.ocir.io/<tenancy-namespace>/<oci-repo-name>:0.1.0 -f Dockerfile .
```
Example:
```shell
docker build --platform linux/amd64 -t iad.ocir.io/iwxyzlmkmn63/dfa-images/dfa:0.1.0 -f Dockerfile .
```
5. Log in to Docker. 

```shell
docker login <region-key>.ocir.io
Username: <docker-username>
Password: <oci-auth-token>
```
Note: The username may be <tenancy-namespace>/<identity-domain>/<username> for service accounts. 

6. Push the image to your OCI tenancy. 
```shell
docker push <region-key>.ocir.io/<tenancy-namespace>/<oci-repo-name>:0.1.0
```
Example:
```shell
docker push iad.ocir.io/iwxyzlmkmn63/dfa-images/dfa:0.1.0
```

### Configure and Run Installer

As part of the installation process, the installer script will create the following OCI resources in your tenancy:
- VCN
- Vault
- Secrets
- Autonomous Database
- Database Schema
- Database Tables
- Functions Application
- OCI Functions
- Event Rules
- Connector Hubs
- Dynamic Group
- Policy

Login to your OCI tenancy to get the following values. These are added to the repository's config.ini file and are utilized by the installer script - 

**1. Tenancy ID**
1. Click on your profile icon in the upper right corner. Then click on Tenancy.
2. Copy and paste this value to the `DFA_TENANCY_ID` variable in the `config.ini` file.

**2. Region, Region Key, and Realm Key**
1. Note your region in the upper right corner of your OCI console.
2. Open this link: https://docs.oracle.com/en-us/iaas/Content/General/Concepts/regions.htm#:~:text=%3A-,Region%20Name,-Region%20Identifier
3. Copy and paste the corresponding Region Identifier to the `DFA_REGION_ID` variable in `config.ini`.
4. Copy and paste the corresponding Region Key to the `DFA_REGION_KEY` variable in `config.ini`.
5. Copy and paste the corresponding Realm Key to the `DFA_REALM_KEY` variable in `config.ini`.

**3. Data Feed Stream ID and Service Endpoint**
1. Click on the OCI hamburger menu in the upper left corner. 
2. Select Storage. Under Object Storage & Archive Storage, select Buckets.
3. From the list of buckets, select the bucket configured with AG Data Feed. (Note: Ensure that the Emit Objects Events feature is Enabled)
4. Copy and paste the bucket name to the DFA_BUCKET_NAME variable in the config.ini file.
5. Copy and paste the Namespace to the DFA_NAMESPACE variable in the config.ini file.
6. Click on the hyperlink next to Compartment.
7. Copy and paste the Compartment OCID to the DFA_COMPARTMENT_ID variable in the config.ini file.

**4. Data Feed Stream ID and Service Endpoint**
1. Click on the OCI hamburger menu in the upper left corner.
2. Select Analytics & AI. Under Messaging, select Streaming. 
3. From the list of streams, select the stream configured with AGCS Data Feed.
4. Copy and paste the Stream OCID to the DFA_STREAM_ID variable in the config.ini file.
5. Copy and paste the Messages endpoint to the DFA_STREAM_SERVICE_ENDPOINT variable in the config.ini file.


### Authentication
The installer uses the OCI CLI config created for an API key or a temporary session token.
- API signing key: https://docs.oracle.com/en-us/iaas/Content/API/Concepts/apisigningkey.htm#apisigningkey_topic_How_to_Generate_an_API_Signing_Key_Console
- Temporary session token: https://docs.oracle.com/en-us/iaas/Content/API/SDKDocs/clitoken.htm

If you are using command line, follow this link to generate a temporary security token: https://docs.oracle.com/en-us/iaas/Content/API/SDKDocs/clitoken.htm 
Keep in mind that this is a temporary token and will expire 1 hour from creation. The token can be re-authenticated or refreshed as seen here: https://docs.oracle.com/en-us/iaas/Content/API/SDKDocs/clitoken.htm#Refreshing_a_Token 

If you are using OCI Cloud Shell, follow this link: https://docs.oracle.com/en-us/iaas/Content/API/Concepts/apisigningkey.htm#apisigningkey_topic_How_to_Generate_an_API_Signing_Key_Console  
Then add the following line to your new config:
`delegation_token_file=/etc/oci/delegation_token`

Once the configuration file is ready:
1. Replace the value of DFA_CONFIG_LOCATION in the config.ini file with the path to your config file. 
2. Replace the value of DFA_CONFIG_PROFILE in the config.ini file with the profile name. 
3. Replace the value of OCI_AUTH_TYPE in the config.ini file with security_token_file if you are using a temporary session token or delegation_token_file if using Cloud Shell. 


### Run the Installer

To run the installer:
1. Make sure you are authenticated. 
2. Ensure Docker images are pushed to OCI.
2. In a terminal or IDE, navigate to the DFA project's root. 
3. Create and activate the virtual environment. 
4. Install the following packages in the virtual environment using `pip install`
    - fdk
    - oci
    - oracledb
    - pandas
    - pypika
5. Run `export PYTHONPATH=/Users/yourName/oci-ag/src`
Paste the full path to DFA's source directory for the PYTHONPATH. 
6. Run `python installer.py installer.log 2>&1`

Note: The creation of the Vault and Master Key can take time, the script will wait a max of 20 minutes for an Active state before timing out. In the case of a timeout, please run the script again once the resource is in an active state in OCI. 

### Final Steps and Validation
1. To enable logging for your transformer functions, navigate to the newly created OCI function application. Click on Monitoring. Under Logs, click on the 3 dots next to Function Invocation Logs. Click Enable log. Choose a log group and provide a name for your logs. 
2. Navigate to your Data Feed bucket in OCI object storage. Ensure that the 'Emit Objects Events' feature is Enabled. Else, the file data will not be processed by DFA. 
3. If `MANUALLY_CREATE_DYNAMIC_GROUP` was set to True, navigate to the Identity Domain and create a new dynamic group called `dfa-functions` with the values given at the end of the script execution. This dynamic group will contain all the function OCIDs. 

The script can take a few minutes to complete. Once it's done, you can navigate to the OCI console and see all the new resources that were created. All the resources created with the script will have the following tag 

{'Feature' : 'Data Feed Analytics(DFA)'}

## Documentation

### OCI Policies to be Created

The script will create the required OCI Policies, but for reference the following policies are required:
- Allow dynamic group &lt;domain&gt;/dfa-functions to manage vaults in compartment id &lt;compartment-ocid&gt;
- Allow dynamic group &lt;domain&gt;/dfa-functions to manage secret-family in compartment id &lt;compartment-ocid&gt;
- Allow dynamic group &lt;domain&gt;/dfa-functions to use keys in compartment id &lt;compartment-ocid&gt;
- Allow dynamic group &lt;domain&gt;/dfa-functions to manage autonomous-databases in compartment id &lt;compartment-ocid&gt;
- Allow dynamic group &lt;domain&gt;/dfa-functions to manage object-family in compartment id &lt;compartment-ocid&gt;
- Allow any-user to {STREAM_READ, STREAM_CONSUME} in compartment id &lt;compartment-ocid&gt; where all {request.principal.type='serviceconnector', target.stream.id='&lt;stream-ocid&gt;, request.principal.compartment.id='&lt;compartment-ocid&gt;'}
- Allow any-user to use fn-function in compartment id &lt;compartment-ocid&gt; where all {request.principal.type='serviceconnector', request.principal.compartment.id='&lt;compartment-ocid&gt;'}
- Allow any-user to use fn-invocation in compartment id &lt;compartment-ocid&gt; where all {request.principal.type='serviceconnector', request.principal.compartment.id='&lt;compartment-ocid&gt;'}

### Config.ini Variables Explained
There are a few variables in the config.ini file that can be replaced if you choose to and some that are required. 
</br>

The installer will create a new vault in OCI by default. If you would prefer to reuse an existing vault, paste the vault OCID to the `DFA_VAULT_ID` variable in the config.ini file. Note that a vault is required. The script will create secrets and the DFA code will reference these secrets. 
</br>

The installer will by default download the DB wallet to the /tmp location in your local file system. To change this, please assign the desired location to the `DFA_LOCAL_SAVE_DIRECTORY` variable in the config.ini file. 
</br>

The installer will create State tables but will not create Time Series tables in the database by default. While State tables are a current snapshot of the AG data, Time Series tables show a history of all the events. To create the required resources for the Time Series, set the `CREATE_TIME_SERIES` variable in the config.ini file to True. 
</br>

The `OCI_AUTH_TYPE` variable accepts the values 'security_token_file' or 'delegation_token_file' based on how you've decided to authenticate. 
</br>

If your OCI tenancy has restrictions on creating dynamic groups at the tenancy level, set the `MANUALLY_CREATE_DYNAMIC_GROUP` variable in config.ini to True. Please manually create a dynamic group in your desired domain using the statement printed out at the end of the script. Replace the `DYNAMIC_GROUP_DOMAIN` variable in config.ini with the domain name where the dynamic group will be created. 
</br>

By default, the installer script will create the resources with prefix 'dfa'. This can be changed by updating the `RESOURCE_NAME_PREFIX` variable in config.ini. 
</br>

Any variable ending in *_FUNCTION_PROVISIONED_CONCURRENCY is related to the provisioned concurrency units used by the OCI Functions. Adjust these values in config.ini as needed based on tenancy limits and amount of AG data. The concurrency units can be changed in OCI once the functions have been created by the script. 
</br>

Any variable ending in *_SECRET_NAME is related to secrets in the vault. The installer script creates secrets in an OCI vault to store the DB's ewallet.pem file contents, wallet.sso file contents, user schema name, and user schema password. The secrets will be named based off the values assigned to the *_SECRET_NAME variables. Once the script has created the secrets in the vault, changing the names in the config.ini file will not update the secret names in the OCI Vault, they must be manually changed. 
</br>

The `REPOSITORY_NAME` and `IMAGE_VERSION` variables should match the name and version tag of the Docker image built in Step 2. If necessary, update them in the config.ini file to ensure consistency.
</br>

The `DFA_RECREATE_DFA_ADW_TABLES` variable should be set to false when running the script for the first time. This variable should be set to true if a table's schema or unique constraints have changed. Setting this variable to true will delete the existing DFA tables and will re-create them. 
</br>


### Delete OCI Resources 

In the case you need to delete the OCI resources created by the installer script, search for the tag `'Data Feed Analytics(DFA)'` in OCI. Then, delete all the resources associated with that tag as all the OCI resources created by the script will contain the tag. 

### Relevant Documentation
- Applications and Functions: https://docs.oracle.com/en-us/iaas/Content/Functions/Tasks/functionscreatingapps.htm#top  
- Connector Hub: https://docs.oracle.com/en-us/iaas/Content/connector-hub/create-service-connector.htm  
- Event Rules: https://docs.oracle.com/en-us/iaas/Content/Events/Concepts/eventsgetstarted.htm#Console  
- Using Cloud Shell Private Networking (for databases on private endpoint): https://docs.oracle.com/en-us/iaas/Content/API/Concepts/cloudshellintro_topic-Cloud_Shell_Networking.htm#Cloud_Shell_Private_Access:~:text=Using%20Cloud%20Shell%20Private%20Networking 

## Examples

- See the detailed Examples and Tutorials section below: [Examples and Tutorials](#examples-and-tutorials)
- Unit test backed examples:
  - File-based: tests/dfa/etl/test_file_transformer.py (uses sample JSONL in tests/dfa/etl/test_data/file/)
  - Stream-based: tests/dfa/etl/test_stream_transformer.py
  - Audit events: tests/dfa/etl/test_audit_transformer.py
  - Run locally with coverage: `tox -e py312`
- Quick local demo snippet:
  ```python
  import os
  from dfa.etl.file_transformer import FileTransformer

  os.environ.setdefault("DFA_BATCH_SIZE", "10000")
  os.environ.setdefault("DFA_LOG_LEVEL", "INFO")

  t = FileTransformer(namespace="your-namespace", bucket_name="your-bucket", object_name="path/to/object.jsonl")
  t.extract_data()
  t.transform_data()
  t.clean_data()
  t.load_data()
  ```
- OCI tutorials:
  - Functions: https://docs.oracle.com/en-us/iaas/Content/Functions/Tasks/functionscreatingapps.htm
  - Service Connector Hub: https://docs.oracle.com/en-us/iaas/Content/connector-hub/create-service-connector.htm
  - Events: https://docs.oracle.com/en-us/iaas/Content/Events/Concepts/eventsgetstarted.htm

## Help

Need help? Please contact Oracle Customer Support: https://support.oracle.com/signin

## Contributing

This project is not accepting external contributions at this time. For bugs or enhancement requests, please file a GitHub issue unless it's security related. When filing a bug remember that the better written the bug is, the more likely it is to be fixed. If you think you've found a security vulnerability, do not raise a GitHub issue and follow the instructions in our security policy.

## Security

Please consult the [security guide](./SECURITY.md) for our responsible security vulnerability disclosure process

## Runtime Configuration and Environment Variables
At runtime (inside OCI Functions), the application expects environment variables for configuration. The installer creates most of the necessary infrastructure and secrets; the function should reference these via environment variables.

Core runtime environment variables:
- DFA_FUNCTION_NAME: Selects which handler to run. Supported values:
  - `audit`, `stream`, `file`, `stream_to_ts`, `file_to_ts`
- DFA_LOG_LEVEL: Optional log level for structured logs. Defaults to `INFO`. Examples: `DEBUG`, `INFO`, `WARNING`.
- DFA_BATCH_SIZE: Optional batch size for load operations. Defaults to `10000`.

ADW connection and wallet:
- DFA_ADW_DFA_SCHEMA: Database username (schema) for DFA.
- DFA_CONN_PROTOCOL: Typically `tcps`.
- DFA_CONN_HOST: Database host.
- DFA_CONN_PORT: Database port (e.g., `1522`).
- DFA_CONN_SERVICE_NAME: Database service name.
- DFA_CONN_RETRY_COUNT: Optional retry count for connection.
- DFA_CONN_RETRY_DELAY: Optional delay between retries.
- DFA_ADW_DFA_USER_PASSWORD_SECRET_NAME: Secret name for DFA user password.
- DFA_ADW_WALLET_SECRET_NAME: Secret name for cwallet.sso.
- DFA_ADW_WALLET_PASSWORD_SECRET_NAME: Secret name for wallet password.
- DFA_ADW_EWALLET_PEM_SECRET_NAME: Secret name for ewallet.pem.

OCI access and vault:
- DFA_SIGNER_TYPE: `resource` (default for Functions) or `user` (when using a user token).
- DFA_VAULT_ID: OCID of the vault holding secrets.
- DFA_COMPARTMENT_ID: OCID of the compartment where secrets reside.
- DFA_CONFIG_LOCATION / DFA_CONFIG_PROFILE: Required if `DFA_SIGNER_TYPE=user` for reading OCI CLI config.

Notes:
- The connection layer enforces secure wallet directory permissions and constructs DSNs robustly.
- Missing or unknown secrets will raise a descriptive error with the secret name and vault/compartment identifiers.

## Version Alignment
- Python: The project targets Python 3.12 (tox env `py312`, requires-python `>=3.12`).
- Ensure the Functions base image and local tooling use Python 3.12 for consistency, or adjust tox/metadata if you decide to use another version and validate dependencies.

## Examples and Tutorials

This repository includes practical examples you can use to understand DFA’s ETL flow and to validate your setup locally.

- Unit-test backed examples (recommended starting point)
  - Explore the test suites under tests/dfa/etl to see input and expected transformations:
    - File-based examples: tests/dfa/etl/test_file_transformer.py
      - Uses sample JSONL in tests/dfa/etl/test_data/file/*.jsonl (e.g., permission.jsonl, identity.jsonl)
    - Stream-based examples: tests/dfa/etl/test_stream_transformer.py
    - Audit events example: tests/dfa/etl/test_audit_transformer.py
  - Run with coverage:
    tox -e py312

- Minimal local snippet (FileTransformer)
  - Illustrative example of how a file-based event is processed (assumes BaseObjectStorage configured and PYTHONPATH=./src):
    ```python
    import os
    from dfa.etl.file_transformer import FileTransformer

    # Example environment for batch sizing and logging
    os.environ.setdefault("DFA_BATCH_SIZE", "10000")
    os.environ.setdefault("DFA_LOG_LEVEL", "INFO")

    # Replace with your namespace/bucket/object
    t = FileTransformer(namespace="your-namespace", bucket_name="your-bucket", object_name="path/to/object.jsonl")
    t.extract_data()
    t.transform_data()
    t.clean_data()
    t.load_data()  # Writes to ADW using configured wallet and connection details
    ```

- Minimal local snippet (dispatcher)
  - The dispatcher selects the handler by DFA_FUNCTION_NAME (audit, stream, file, stream_to_ts, file_to_ts):
    ```python
    import io
    import json
    from types import SimpleNamespace
    from handlers.dispatcher import dispatch

    # Emulate Fn context with Config().get(...)
    ctx = SimpleNamespace(Config=lambda: SimpleNamespace(get=lambda k, d=None: "file"))
    # Example body for file handler (object storage event)
    body = {
      "data": {
        "resourceName": "path/to/object.jsonl",
        "additionalDetails": {"bucketName": "your-bucket", "namespace": "your-namespace"},
      }
    }
    data = io.BytesIO(json.dumps(body).encode("utf-8"))
    result = dispatch(ctx, data)
    print(result)
    ```

- Sample configuration walkthrough
  - Review README “Configure and Run Installer” to create OCI resources (VCN, Vault, ADW, Functions, Event Rules, Service Connectors).
  - Use config.ini to map your tenancy and resource identifiers.
  - The installer creates the required resources and secrets; the application reads ADW wallet/password secrets and connects securely to ADW.

- Docker demo
  - Build the image (see Build and Upload Docker Image section) and run it with your environment variables configured to point to OCI Vault/ADW and Stream/Bucket sources.

Helpful tutorials and docs
- OCI Functions getting started: https://docs.oracle.com/en-us/iaas/Content/Functions/Tasks/functionscreatingapps.htm
- Service Connector Hub: https://docs.oracle.com/en-us/iaas/Content/connector-hub/create-service-connector.htm
- OCI Events: https://docs.oracle.com/en-us/iaas/Content/Events/Concepts/eventsgetstarted.htm

If you’d like a guided demo, start by running tox -e py312 which exercises the ETL logic against included sample datasets and prints out transformation progress in the logs.

## License

Copyright (c) 2025 Oracle and/or its affiliates.
Released under the Universal Permissive License v1.0 as shown at
<https://oss.oracle.com/licenses/upl/>.
