using System;
using DbUp.Support;

namespace DbUp.Engine
{
    public class AppliedSqlScript
    {
        public AppliedSqlScript(string name, ScriptType type, string contents, DateTime appliedDate)
        {
            Name = name;
            Type = type;
            Contents = contents;
            AppliedDate = appliedDate;
        }

        public string Name { get; }
        public ScriptType Type { get; }
        public string Contents { get; }
        public DateTime AppliedDate { get; }
    }
}
