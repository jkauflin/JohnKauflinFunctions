/*==============================================================================
(C) Copyright 2025 John J Kauflin, All rights reserved.
--------------------------------------------------------------------------------
DESCRIPTION:  Class to run scheduled jobs via Azure Function TimerTrigger for
                the web application
--------------------------------------------------------------------------------
Modification History
2025-04-30 JJK  Initial version
2025-05-03 JJK  Fixed error in dynamic object value get
================================================================================*/
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using Microsoft.Azure.Cosmos;

namespace JohnKauflinWeb.Function
{
    public class ScheduledJobs
    {
        private readonly ILogger _logger;
        private readonly string? apiCosmosDbConnStr;
        private readonly string databaseId;

        public ScheduledJobs (ILoggerFactory loggerFactory)
        {
            _logger = loggerFactory.CreateLogger<ScheduledJobs>();
            apiCosmosDbConnStr = Environment.GetEnvironmentVariable("API_COSMOS_DB_CONN_STR");
            databaseId = "jjkdb1";
        }

        [Function("PurgeDatabase")]
        //[TimerTrigger("0 0 * * * *")] // Runs every hour
        //[TimerTrigger("0 0 0 * * *")] // Runs every day at midnight UT
        public async Task Run([TimerTrigger("0 0 0 * * *")] TimerInfo myTimer)
        {
            _logger.LogInformation($"PurgeDatabase Timer trigger function executed at: {DateTime.Now}");
            
            if (myTimer.ScheduleStatus is not null)
            {
                _logger.LogInformation($"Next timer schedule at: {myTimer.ScheduleStatus.Next}");
            }

            await DeleteItems("GenvMetricPoint",4);
            //wait DeleteItems("GenvImage",3);
            //await DeleteItems("MetricPoint",3);
        }

        private async Task DeleteItems(string containerId, int daysToKeep) {
            DateTime currDateTime = DateTime.Now;
            string maxYearMonthDay = currDateTime.AddDays(-daysToKeep).ToString("yyyyMMdd");
            _logger.LogInformation($"# days to keep = {daysToKeep}, maxYearMonthDay = {maxYearMonthDay}");
            var queryText = $"SELECT c.id, c.PointDay FROM c WHERE c.PointDay < {maxYearMonthDay} ";
            //List<(string id, string partitionKey)> documents = new List<(string, string)>();

            CosmosClient cosmosClient = new CosmosClient(apiCosmosDbConnStr); 
            Database db = cosmosClient.GetDatabase(databaseId);
            Container container = db.GetContainer(containerId);

            int cnt = 0;
            string id = "";
            long partitionKey;
            /*
            using FeedIterator<dynamic> feed = container.GetItemQueryIterator<dynamic>(queryText);
            while (feed.HasMoreResults)
            {
                var response = await feed.ReadNextAsync();
                foreach (var item in response)
                {
                    cnt++;
                    id = item.id.ToString();
                    partitionKey = item.PointDay.ToString();
                    documents.Add((id, partitionKey));
                }
            }
            */
            var feed = container.GetItemQueryIterator<dynamic>(queryText);
            while (feed.HasMoreResults)
            {
                var response = await feed.ReadNextAsync();
                foreach (var item in response)
                {
                    cnt++;
                    id = item.id.ToString();
                    partitionKey = item.PointDay.Value;
                    Console.WriteLine($"{cnt}, id: {item.id}, PointDay: {item.PointDay.Value} ");
                    await container.DeleteItemAsync<object>(id, new PartitionKey(partitionKey));
                    /*                    
                    try
                    {
                    }
                    catch (Exception ex)
                    {
                        string exMessage = ex.Message;
                        if (!exMessage.Contains("Not Found", StringComparison.OrdinalIgnoreCase))
                        {
                            _logger.LogError(ex, ex.Message);
                            throw;
                        }
                        else
                        {
                                // Resource Not Found
                                //Console.WriteLine($"id: {item.id}, partitionKey: {partitionKey} - Not Found");
                                Console.WriteLine($"*** id: {item.id}, partitionKey:  - Not Found");
                            }
                        }
                    */
                }
            }
            /*
            documents.ForEach(async doc => 
            {
                //Console.WriteLine($"ID: {doc.id}, PartitionKey: {doc.partitionKey}");
                try {
                    await container.DeleteItemAsync<object>(doc.id, new PartitionKey(doc.partitionKey));
                } catch (Exception ex) {
                    string exMessage = ex.Message;
                    if (!exMessage.Contains("Not Found", StringComparison.OrdinalIgnoreCase)) {
                        _logger.LogError(ex, ex.Message);
                        throw;
                    } else {
                        // Resource Not Found
                        _logger.LogWarning($"ID: {doc.id}, PartitionKey: {doc.partitionKey} Not Found");
                    }
                }
            });
            */
            _logger.LogInformation($"{containerId}, Purge cnt = {cnt}");
        }

    }
}
