using System;
using DbUp.Engine;

namespace DbUp.Support
{
    public class ScriptExecutionException : Exception
    {
        public ScriptExecutionException(int index, PreparedSqlScript script, Exception innerException)
            : base("An execution occurred while running script " + script?.Name + ".", innerException)
        {
            Index = index;
            Script = script;
        }

        public int Index { get; }
        public PreparedSqlScript Script { get; }
    }
}
