using System.Collections.Generic;
using DbUp.Support;

namespace DbUp.Engine
{
    public interface IScriptSorter
    {
        IEnumerable<PreparedSqlScript> Sort(IEnumerable<PreparedSqlScript> scripts, ScriptNameComparer scriptNameComparer);
    }
}
