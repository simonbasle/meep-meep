using Couchbase.Configuration.Client;
using System;

namespace MeepMeep.Extensions
{
    public static class MeepMeepExtensions
    {
        public static ClientConfiguration ToClientConfig(this MeepMeepOptions options)
        {
            var bucketConf = new BucketConfiguration()
            {
                BucketName = options.Bucket,
                Password = options.BucketPassword,
            };
            bucketConf.Servers.Clear();
            foreach (var node in options.Nodes)
            {
                bucketConf.Servers.Add(new Uri(node));
            };

            var config = new ClientConfiguration();
            config.Servers.Clear();
            foreach (var node in options.Nodes)
            {
                config.Servers.Add(new Uri(node));
            }
            config.BucketConfigs.Clear();
            config.BucketConfigs.Add(options.Bucket, bucketConf);
            
            return config;
        }
    }
}