﻿namespace MeepMeep.Docs
{
    /// <summary>
    /// Responsible for generating document keys for a specific workload.
    /// </summary>
    public interface IWorkloadDocKeyGenerator
    {
        string Generate(int workloadIndex, int docIndex);
    }
}