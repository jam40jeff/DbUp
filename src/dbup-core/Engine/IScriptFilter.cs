using System.Collections.Generic;
using DbUp.Support;

namespace DbUp.Engine
{
    public interface IScriptFilter
    {
        IEnumerable<PreparedSqlScript> Filter(IEnumerable<PreparedSqlScript> sorted, Dictionary<string, AppliedSqlScript> executedScriptsByName, ScriptNameComparer comparer);
    }
}
