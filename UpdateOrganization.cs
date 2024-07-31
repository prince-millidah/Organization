using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using System.Data;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using Azure.Security.KeyVault.Secrets;
using Azure.Identity;
using Snowflake.Data.Client;
using Microsoft.Azure.Cosmos;
using Microsoft.Extensions.Configuration;


namespace UiPath.CosmosMigration
{
    public static class ConfigManager
    {
        private static IConfiguration _config;
        public static IConfiguration Configuration
        {
            get
            {
                if (_config == null)
                {
                    var assemblyPath = System.Reflection.Assembly.GetExecutingAssembly().Location;
                    var directoryPath = System.IO.Path.GetDirectoryName(assemblyPath);

                    _config = new ConfigurationBuilder()
                    .SetBasePath(directoryPath)
                    .AddJsonFile("local.settings.json", optional: true, reloadOnChange: true)
                    .AddEnvironmentVariables()
                    .Build();
                }
                return _config;
            }
        }
    }

    public class UpdateOrganization
    {
        [Function("UpdateOrganization")]
        public async Task Run([TimerTrigger("0 */5 * * * *")] TimerInfo myTimer, FunctionContext context)
        {
                            
            var log = context.GetLogger("UpdateOrganization");
            log.LogInformation($"C# Timer trigger function executed at: {DateTime.Now}");

            // Establishing connection to the cosmos database
            var cosmosDBUrl = GetSecret(ConfigManager.Configuration["cosmosURL"], log);
            var cosmosDBKey = GetSecret(ConfigManager.Configuration["cosmosKey"], log);
            var cosmosClient = new CosmosClient(cosmosDBUrl, cosmosDBKey);
            var database = cosmosClient.GetDatabase("LocationService");
            
            // Connecting to Snowflake
            using IDbConnection conn = ConnectSnowflake(log);

            if (conn != null)
            {
                // Getting the last update timestamp
                long previousTime = getPreviousTime(log, conn);
                
                // Reading data from cosmos database
                var cosmosQuery = String.Format("SELECT TOP 1000 * FROM c where c._ts >= {0} ORDER BY c._ts ASC", previousTime);
                var queryDefinition = new QueryDefinition(cosmosQuery);
                var requestOptions = new QueryRequestOptions { MaxItemCount = 1000 };
                FeedIterator<dynamic> queryResultSetIterator = database.GetContainer("Organization").GetItemQueryIterator<dynamic>(queryDefinition, requestOptions: requestOptions);
            
                var currentResultSet = await queryResultSetIterator.ReadNextAsync();
                var currentBatch = new List<string>();
                long timestamp = 0;

                // Transforming the read data to get specific fields
                foreach (var item in currentResultSet)
                {
                    string id = item.id?.ToString() ?? "";
                    string name = item.normalizedName?.ToString() ?? "";
                    string scaleUnits = item.scaleUnits?.ToString() ?? "";
                    bool isDeleted = item.isDeleted ?? false;
                    string serviceStatuses = item.serviceStatuses?.ToString() ?? "";
                    bool isInMaintenance = item.isInMaintenance ?? false;
                    timestamp = item._ts;

                    currentBatch.Add($"('{id}', '{name}', '{scaleUnits}', '{isDeleted}', '{serviceStatuses}', '{isInMaintenance}', {timestamp})");
                }
    
                
                // Write read data to snowflake
                if (currentBatch.Count > 1)
                {
                    // Creating temporary table using the Organization table schema
                    string createTableCommandText = "CREATE TEMPORARY TABLE TEMP_TABLE AS SELECT * FROM ORGANIZATION WHERE 1=0;";
                    using (IDbCommand cmd = conn.CreateCommand())
                    {
                        cmd.CommandText = createTableCommandText;
                        cmd.ExecuteNonQuery();
                    }

                    // Insering data in the temporary table
                    using (IDbCommand cmd = conn.CreateCommand())
                    {
                        string query = "INSERT INTO TEMP_TABLE (ID, NAME, SCALEUNITS, ISDELETED, SERVICESTATUSES, ISINMAINTENANCE, TIMESTAMP) VALUES ";
                        query += string.Join(", ", currentBatch);
                        cmd.CommandText = query;
                        cmd.ExecuteNonQuery();
                    }

                    // Updating the Organization table using the temporary table
                    using (IDbCommand cmd = conn.CreateCommand())
                    {
                        cmd.CommandText = @"MERGE INTO ORGANIZATION t 
                                            USING TEMP_TABLE s 
                                            ON t.ID = s.ID 
                                            WHEN MATCHED THEN UPDATE SET 
                                                t.NAME = s.NAME,
                                                t.SCALEUNITS = s.SCALEUNITS,
                                                t.ISDELETED = s.ISDELETED,
                                                t.SERVICESTATUSES = s.SERVICESTATUSES,
                                                t.ISINMAINTENANCE = s.ISINMAINTENANCE,
                                                t.TIMESTAMP = s.TIMESTAMP 
                                            WHEN NOT MATCHED THEN 
                                                INSERT (ID, NAME, SCALEUNITS, ISDELETED, SERVICESTATUSES, ISINMAINTENANCE, TIMESTAMP) 
                                                VALUES (s.ID, s.NAME, s.SCALEUNITS, s.ISDELETED, s.SERVICESTATUSES, s.ISINMAINTENANCE, s.TIMESTAMP)";
                        cmd.ExecuteNonQuery();
                    }

                    using (IDbCommand cmd = conn.CreateCommand())
                    {
                        cmd.CommandText = "DROP TABLE TEMP_TABLE";
                        cmd.ExecuteNonQuery();
                    }

                    // Updating the database with the timestamp of the last read document
                    using (IDbCommand cmd = conn.CreateCommand())
                    {
                        cmd.CommandText = "UPDATE UPDATETIME SET TIME = :2 WHERE TABLENAME = :1";
                        IDbDataParameter tableNameParam = cmd.CreateParameter();
                        tableNameParam.ParameterName = "1";
                        tableNameParam.Value = "Organization";
                        cmd.Parameters.Add(tableNameParam);
                        
                        IDbDataParameter updateTimeParam = cmd.CreateParameter();
                        updateTimeParam.ParameterName = "2";
                        updateTimeParam.Value = timestamp;
                        cmd.Parameters.Add(updateTimeParam);
                        
                        cmd.ExecuteNonQuery();
                    }
                }
            }
        }

        public static string GetSecret(string secretName, ILogger log)
        {
            try
            {
                // Connecting to key vault and get secrets
                var kvUri = ConfigManager.Configuration["keyVault"];
                var client = new SecretClient(new Uri(kvUri), new DefaultAzureCredential());
                KeyVaultSecret secret = client.GetSecret(secretName);
                return secret.Value;
            }
            catch (Azure.RequestFailedException ex)
            {
                log.LogInformation($"Error getting secret: {ex.Message}");
                return null;
            }
            catch (Exception ex)
            {
                log.LogInformation($"An error occurred: {ex.Message}");
                return null;
            }
        }

        public static IDbConnection ConnectSnowflake(ILogger log)
        {
            try
            {
                // Connecting to snowflake database
                string passcode = GetSecret(ConfigManager.Configuration["snowflakeKey"], log);
                IDbConnection conn = new SnowflakeDbConnection();
                conn.ConnectionString = ConfigManager.Configuration["snowflakeURL"].Replace("{passcode}", passcode);
                conn.Open();

                return conn;
            }
            catch (SnowflakeDbException e)
            {
                log.LogInformation("SnowflakeDbException: {0}", e);
                return null;
            }
            catch (Exception e)
            {
                log.LogInformation("Exception: {0}", e);
                return null;
            }
        }

        public static long getPreviousTime(ILogger log, IDbConnection snowflake)
        {   
            try 
            {   
                // Query snowflake database for the most recent update timestamp
                long time = 0;
                using (IDbCommand cmd = snowflake.CreateCommand())
                {
                    cmd.CommandText = "SELECT TIME FROM UPDATETIME WHERE TABLENAME = 'Organization'";

                    using (IDataReader reader = cmd.ExecuteReader())
                    {
                        if (reader.Read())
                        {
                            time = reader.GetInt64(0);
                        }
                    }
                }

                // Initial insertion into snowflake when no timestamp found
                if (time == 0)
                {
                    using (IDbCommand cmd = snowflake.CreateCommand())
                    {
                        cmd.CommandText = cmd.CommandText = "INSERT INTO UPDATETIME (TABLENAME, TIME) VALUES (:1, :2)";

                        IDbDataParameter tableNameParam = cmd.CreateParameter();
                        tableNameParam.ParameterName = "1";
                        tableNameParam.Value = "Organization";
                        cmd.Parameters.Add(tableNameParam);
                        
                        IDbDataParameter updateTimeParam = cmd.CreateParameter();
                        updateTimeParam.ParameterName = "2";
                        updateTimeParam.Value = time;
                        cmd.Parameters.Add(updateTimeParam);
                        
                        cmd.ExecuteNonQuery();
                    }
                }
                return time;
            } 
            catch (Exception except)
            {
                log.LogInformation("Error occured getting previous epoch time: {0}", except);
                return 0;
            }
        }
    }
}
