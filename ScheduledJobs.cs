using System;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;

namespace JohnKauflinWeb.Function
{
    public class ScheduledJobs
    {
        private readonly ILogger _logger;

        public ScheduledJobs (ILoggerFactory loggerFactory)
        {
            _logger = loggerFactory.CreateLogger<ScheduledJobs>();
        }

        [Function("PurgeDatabase")]
        //[TimerTrigger("0 0 * * * *")] // Runs every day at midnight UTC
        public void Run([TimerTrigger("0 0 * * * *")] TimerInfo myTimer)
        {
            _logger.LogInformation($"C# Timer trigger function executed at: {DateTime.Now}");
            _logger.LogWarning($"This is the PURGE DATABASE scheduled job");
            
            if (myTimer.ScheduleStatus is not null)
            {
                _logger.LogInformation($"Next timer schedule at: {myTimer.ScheduleStatus.Next}");
            }
        }
    }
}
