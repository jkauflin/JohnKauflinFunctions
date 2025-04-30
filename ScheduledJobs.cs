/*==============================================================================
(C) Copyright 2025 John J Kauflin, All rights reserved.
--------------------------------------------------------------------------------
DESCRIPTION:  Class to run scheduled jobs via Azure Function TimerTrigger for
                the web application
--------------------------------------------------------------------------------
Modification History
2025-04-30 JJK  Initial version
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
        //[TimerTrigger("0 0 * * * *")] // Runs every day at midnight UT
        public async Task Run([TimerTrigger("0 0 * * * *")] TimerInfo myTimer)
        {
            _logger.LogInformation($"C# Timer trigger function executed at: {DateTime.Now}");
            _logger.LogWarning($"This is the PURGE DATABASE scheduled job");
            
            if (myTimer.ScheduleStatus is not null)
            {
                _logger.LogInformation($"Next timer schedule at: {myTimer.ScheduleStatus.Next}");
            }

            string containerId = "GenvMetricPoint";
            CosmosClient cosmosClient = new CosmosClient(apiCosmosDbConnStr); 
            Database db = cosmosClient.GetDatabase(databaseId);
            Container container = db.GetContainer(containerId);

            DateTime currDateTime = DateTime.Now;

            string maxYearMonthDay = currDateTime.AddDays(-2).ToString("yyyyMMdd");
            int cnt = 0;
            // currDateTime - 3 days
            //int dayVal = int.Parse(metricData.metricDateTime.ToString("yyyyMMdd"));  // 
            /*
            var queryText = $"SELECT * FROM c WHERE c.PointDay < {maxYearMonthDay} ";
            var feed = container.GetItemQueryIterator<string>(queryText);
            while (feed.HasMoreResults)
            {
                var response = await feed.ReadNextAsync();
                foreach (var item in response)
                {
                    cnt++;
                    // execute a delete item on each document
                    //Console.WriteLine($"{cnt} {item.id} {item.PointDateTime} ");
                    //await container.DeleteItemAsync<MetricPoint>(item.id, new PartitionKey(item.PointDay));
                }
            }
            */
            _logger.LogWarning($"Purge cnt = {cnt}");

            /*
            try
            {

            }
            catch (Exception ex)
            {
                _logger.LogError(ex, " Error in Purge DB: "+ex.Message);
            }
            */

            // Get the existing document from Cosmos DB
            //int partitionKey = int.Parse(trusteeId); // Partition key of the item
            //var trustee = await container.ReadItemAsync<Trustee>(trusteeId, new PartitionKey(partitionKey));

            /*
WHERE c.PointDay < 20250428

        public async Task PurgeMetricsAsync()
        {
            Console.WriteLine($"Purging MetricPoint data older than 3 days ");

            var jjkCosmosClient = new CosmosClient(jjkdb1Uri, jjkdb1Key,
                new CosmosClientOptions()
                {
                    ApplicationName = "MediaGalleryConsole"
                }
            );

            var databaseNEW = jjkCosmosClient.GetDatabase("JJKWebDB");
            var containerNEW = jjkCosmosClient.GetContainer("JJKWebDB", "MetricPoint");

            DateTime currDateTime = DateTime.Now;

            try
            {
                //metricPointContainer.CreateItemAsync<MetricPoint>(metricPoint, new PartitionKey(metricPoint.PointDay));

                string maxYearMonthDay = currDateTime.AddDays(-3).ToString("yyyyMMdd");
                int cnt = 0;
                // currDateTime - 3 days
                //int dayVal = int.Parse(metricData.metricDateTime.ToString("yyyyMMdd"));  // 
                var queryText = $"SELECT * FROM c WHERE c.PointDay < {maxYearMonthDay} ";
                var feed = containerNEW.GetItemQueryIterator<MetricPoint>(queryText);
                while (feed.HasMoreResults)
                {
                    var response = await feed.ReadNextAsync();
                    foreach (var item in response)
                    {
                        cnt++;
                        // execute a delete item on each document
                        Console.WriteLine($"{cnt} {item.id} {item.PointDateTime} ");
                        await containerNEW.DeleteItemAsync<MetricPoint>(item.id, new PartitionKey(item.PointDay));
                    }
                }

            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
                throw;
            }

        } // PurgeMetricsAsync


        int pointDataDays = 3;


                containerId = "hoa_assessments";
                Container assessmentsContainer = db.GetContainer(containerId);
                sql = $"SELECT * FROM c WHERE c.Parcel_ID = '{parcelId}' ORDER BY c.FY DESC ";
                var assessmentsFeed = assessmentsContainer.GetItemQueryIterator<hoa_assessments>(sql);
                cnt = 0;
                DateTime dateTime;
                DateTime dateDue;
                while (assessmentsFeed.HasMoreResults)
                {
                    var response = await assessmentsFeed.ReadNextAsync();
                    foreach (var item in response)
                    {
                        cnt++;
                        if (item.DateDue is null) {
                            dateDue = DateTime.Parse((item.FY-1).ToString()+"-10-01");
                        } else {
                            dateDue = DateTime.Parse(item.DateDue);
                        }
                        item.DateDue = dateDue.ToString("yyyy-MM-dd");
                        // If you don't need the DateTime object, you can do it in 1 line
                        //item.DateDue = DateTime.Parse(item.DateDue).ToString("yyyy-MM-dd");

                        if (item.Paid == 1) {
                            if (string.IsNullOrWhiteSpace(item.DatePaid)) {
                                item.DatePaid = item.DateDue;
                            }
                            dateTime = DateTime.Parse(item.DatePaid);
                            item.DatePaid = dateTime.ToString("yyyy-MM-dd");
                        }

                        hoaRec2.assessmentsList.Add(item);

                    } // Assessments loop
                }
            */

        }
    }
}
