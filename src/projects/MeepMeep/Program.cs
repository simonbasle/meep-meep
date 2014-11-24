using System;
using System.Collections.Generic;
using MeepMeep.Docs;
using MeepMeep.Extensions;
using MeepMeep.Input;
using MeepMeep.Output;
using MeepMeep.Workloads;
using MeepMeep.Workloads.Runners;
using Couchbase.Core;
using Couchbase.Management;
using Couchbase;

namespace MeepMeep
{
    public class Program
    {
        private static readonly IOutputWriter OutputWriter = new ConsoleOutputWriter();

        public static void Main(string[] args)
        {
            try
            {
                var options = ParseCommandLine(args);

                if (options != null)
                    Run(options);
            }
            catch (Exception ex)
            {
                OutputWriter.Write("Unhandled exception.", ex);
            }
        }

        private static MeepMeepOptions ParseCommandLine(string[] args)
        {
            var commandLineParser = new CommandLineParser();
            var options = new MeepMeepOptions();

            if (!commandLineParser.Parse(options, args))
            {
                OutputWriter.Write(options.GetHelp());
                return null;
            }

            return options;
        }

        private static void Run(MeepMeepOptions options)
        {
            ClusterHelper.Initialize(options.ToClientConfig());
            Cluster cluster = ClusterHelper.Get();
            IBucket bucket = cluster.OpenBucket(options.Bucket, options.BucketPassword);
            IBucketManager bucketManager = bucket.CreateManager(options.Bucket, options.BucketPassword);
            bucketManager.Flush();

            OutputWriter.Verbose = options.Verbose;

            OutputWriter.Write("Running with options:");
            OutputWriter.Write(options);

            OutputWriter.Write("Running workloads...");

            using (var client = bucket)
            {
                var runner = CreateRunner(options);

                foreach (var workload in CreateWorkloads(options))
                    runner.Run(workload, client, OnWorkloadCompleted);
            }

            OutputWriter.Write("Completed");
        }

        private static IWorkloadRunner CreateRunner(MeepMeepOptions options)
        {
            return new TplBasedWorkloadRunner(options);
        }

        private static IEnumerable<IWorkload> CreateWorkloads(MeepMeepOptions options)
        {
            yield return new AddJsonDocumentWorkload(
                new DefaultWorkloadDocKeyGenerator(options.DocKeyPrefix, AddJsonDocumentWorkload.DefaultKeyGenerationPart, options.DocKeySeed),
                options.WorkloadSize,
                options.WarmupMs,
                SampleDocuments.ReadJsonSampleDocument(options.DocSamplePath));

            //saakshi
            //yield return new AddAndGetJsonDocumentWorkload(
            //    new DefaultWorkloadDocKeyGenerator(options.DocKeyPrefix, AddAndGetJsonDocumentWorkload.DefaultKeyGenerationPart, options.DocKeySeed),
            //    options.WorkloadSize,
            //    options.WarmupMs);
        }

        private static void OnWorkloadCompleted(WorkloadResult workloadResult)
        {
            OutputWriter.Write(workloadResult);
        }
    }
}
