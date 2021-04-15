using System;
using System.Collections.Generic;
using DbUp.Engine;
using DbUp.Support;

namespace DbUp.Tests
{
    public static class TestUtility
    {
        public static PreparedSqlScript ToPreparedSqlScript(this SqlScript sqlScript)
        {
            return new PreparedSqlScript(PassThroughScriptExecutor.Instance, null, sqlScript);
        }
        
        public static string GetContents(this SqlScript sqlScript)
        {
            PreparedSqlScript preparedSqlScript = sqlScript.ToPreparedSqlScript();
            return preparedSqlScript.Contents;
        }
        
        public static AppliedSqlScript ToAppliedSqlScript(this string scriptName)
        {
            return new AppliedSqlScript(scriptName, ScriptType.RunOnce, null, DateTime.MinValue);
        }
        
        private class PassThroughScriptExecutor : IScriptExecutor
        {
            public static readonly PassThroughScriptExecutor Instance = new PassThroughScriptExecutor();

            private PassThroughScriptExecutor()
            {
            }

            public string PreprocessScriptContents(string contents, IDictionary<string, string> variables) => contents;

            public void Execute(PreparedSqlScript script) => throw new System.NotImplementedException();

            public void Execute(PreparedSqlScript script, IDictionary<string, string> variables) => throw new System.NotImplementedException();

            public void VerifySchema() => throw new System.NotImplementedException();

            public int? ExecutionTimeoutSeconds
            {
                get
                {
                    throw new System.NotImplementedException();
                }
                set
                {
                    throw new System.NotImplementedException();
                }
            }
        }
    }
}
