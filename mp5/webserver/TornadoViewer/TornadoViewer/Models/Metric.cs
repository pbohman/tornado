using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Web;

using DotNet.Highcharts;
using DotNet.Highcharts.Enums;
using DotNet.Highcharts.Helpers;
using DotNet.Highcharts.Options;
using Point = DotNet.Highcharts.Options.Point;

using MongoDB.Bson;
using MongoDB.Driver;
using MongoDB.Driver.Builders;
using MongoDB.Driver.GridFS;
using MongoDB.Driver.Linq;

namespace CassandraViewer.Models
{
    public class Metric
    {
        public ObjectId Id { get; set; }
        public string Hostname { get; set; }
        public long Timestamp { get; set; }
        public int Value { get; set; }

        public int bytes { get; set; }
        public int tcp { get; set; }
        public int udp { get; set; }
        public int icmp { get; set; }


        public static Series[] GetData(long start, long end)
        {
            DateTime unixEpoch = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);

            ConnectionStringSettings settings =  ConfigurationManager.ConnectionStrings["MongoConnection"];

            var connectionString = "mongodb://localhost";

            if (settings != null)
                connectionString = settings.ConnectionString;

            

            var client = new MongoClient(connectionString);
            var server = client.GetServer();
            var database = server.GetDatabase(ConfigurationManager.AppSettings["MongoDatabase"]);
            var collection = database.GetCollection<Metric>(ConfigurationManager.AppSettings["MongoCollection"]);

            var query = collection.AsQueryable<Metric>();
            var categories = query.Select(x => x.Hostname).Distinct();

            var dtStart = (start == DateTime.MinValue.Ticks) ? query.Min(x => x.Timestamp) : start;
            var dtEnd = (end == DateTime.MinValue.Ticks) ? query.Max(x => x.Timestamp) : end;

            List<Series> series = new List<Series>();

            foreach (var c in categories)
            {
                var q = query
                    .Where(x => x.Hostname.Equals(c) && x.Timestamp >= dtStart && x.Timestamp <= dtEnd).ToArray();

                var g = q.GroupBy(a => a.Timestamp)
                    .Select(y =>
                                new object[] 
                                { 
                                    new DateTime(unixEpoch.Ticks).AddSeconds((double) y.Key).ToLocalTime(), 
                                    y.Sum(z => z.Value)
                                }
                            );

                series.Add(new Series { Name = c, Data = new Data(g.ToArray()) });
            }

            return series.ToArray();
        }
    }
}