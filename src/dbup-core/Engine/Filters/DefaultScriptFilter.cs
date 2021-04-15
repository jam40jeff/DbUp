using System.Collections.Generic;
using System.Linq;
using DbUp.Support;

namespace DbUp.Engine.Filters
{
    public class DefaultScriptFilter : IScriptFilter
    {
        public IEnumerable<PreparedSqlScript> Filter(IEnumerable<PreparedSqlScript> sorted, Dictionary<string, AppliedSqlScript> executedScriptsByName, ScriptNameComparer comparer)
            => sorted.Where(s =>
                s.SqlScriptOptions.ScriptType == ScriptType.RunAlways
                || !executedScriptsByName.ContainsKey(s.Name)
                || (s.SqlScriptOptions.ScriptType == ScriptType.RunIfChanged && s.Contents != executedScriptsByName[s.Name].Contents));
    }
}
