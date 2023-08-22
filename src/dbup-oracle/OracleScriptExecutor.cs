using System;
using System.Collections.Generic;
using DbUp.Engine;
using DbUp.Engine.Output;
using DbUp.Engine.Transactions;
using DbUp.Support;
using Oracle.ManagedDataAccess.Client;

namespace DbUp.Oracle
{
    public class OracleScriptExecutor : ScriptExecutor
    {
        /// <summary>
        /// Initializes an instance of the <see cref="OracleScriptExecutor"/> class.
        /// </summary>
        /// <param name="connectionManagerFactory"></param>
        /// <param name="log">The logging mechanism.</param>
        /// <param name="schema">The schema that contains the table.</param>
        /// <param name="variablesEnabled">Function that returns <c>true</c> if variables should be replaced, <c>false</c> otherwise.</param>
        /// <param name="scriptPreprocessors">Script Preprocessors in addition to variable substitution</param>
        /// <param name="journalFactory">Database journal</param>
        public OracleScriptExecutor(Func<IConnectionManager> connectionManagerFactory, Func<IUpgradeLog> log, string schema, Func<bool> variablesEnabled,
            IEnumerable<IScriptPreprocessor> scriptPreprocessors, Func<IJournal> journalFactory)
            : base(connectionManagerFactory, new OracleObjectParser(), log, schema, variablesEnabled, scriptPreprocessors, journalFactory)
        {

        }

        protected override string GetVerifySchemaSql(string schema)
        {
            throw new NotSupportedException();
        }

        protected override void HandleException(int index, PreparedSqlScript script, Exception e)
        {
            OracleException exception = e as OracleException;
            if (exception != null)
            {
#if MY_SQL_DATA_6_9_5
                var code = exception.ErrorCode;
#else
                var code = exception.ErrorCode;
#endif
                Log().WriteInformation("Oracle exception has occured in script: '{0}'", script.Name);
                Log().WriteError("Oracle error code: {0}; Number {1}; Message: {2}", index, code, exception.Number, exception.Message);
                Log().WriteError(exception.ToString());
            }
            else
            {
                base.HandleException(index, script, e);
            }
        }
    }
}
