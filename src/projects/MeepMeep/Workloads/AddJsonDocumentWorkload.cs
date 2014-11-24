using System.Diagnostics;
using MeepMeep.Docs;
using Couchbase.Core;
using Couchbase;

namespace MeepMeep.Workloads
{
    /// <summary>
    /// Workload representing the use-case of adding JSON documents.
    /// </summary>
    public class AddJsonDocumentWorkload : WorkloadBase
    {
        private readonly string _description;

        protected readonly string SampleDocument;

        public const string DefaultKeyGenerationPart = "ajdw";

        public override string Description
        {
            get { return _description; }
        }

        public AddJsonDocumentWorkload(IWorkloadDocKeyGenerator docKeyGenerator, int workloadSize, int warmupMs, string sampleDocument = null)
            : base(docKeyGenerator, workloadSize, warmupMs)
        {
            SampleDocument = sampleDocument ?? SampleDocuments.Default;
            _description = string.Format("ExecuteStore (Add) of {0} JSON doc(s) with doc size: {1}.",
                WorkloadSize,
                SampleDocument.Length);
        }

        protected override WorkloadOperationResult OnExecuteStep(IBucket client, int workloadIndex, int docIndex, Stopwatch sw)
        {
            var key = DocKeyGenerator.Generate(workloadIndex, docIndex);

            sw.Start();
            IOperationResult<string> storeOpResult = client.Upsert(key, SampleDocument);
            sw.Stop();

            string message;
            if (storeOpResult.Success)
            {
                message = storeOpResult.Message;
            } else {
                message = storeOpResult.Exception.Message;
            }

            return new WorkloadOperationResult(storeOpResult.Success, message, sw.Elapsed)
            {
                DocSize = SampleDocument.Length
            };
        }
    }
}