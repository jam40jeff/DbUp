using System.Collections.Generic;
using System.Linq;
using DbUp.Support;

namespace DbUp.Engine.Sorters
{
    public class DefaultScriptSorter : IScriptSorter
    {
        public IEnumerable<PreparedSqlScript> Sort(IEnumerable<PreparedSqlScript> scripts, ScriptNameComparer scriptNameComparer) =>
            scripts.OrderBy(s => s.SqlScriptOptions.RunGroupOrder).ThenBy(s => s.Name, scriptNameComparer);
    }
}
