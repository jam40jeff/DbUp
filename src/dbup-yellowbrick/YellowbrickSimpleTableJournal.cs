using System;
using DbUp.Engine;
using DbUp.Engine.Output;
using DbUp.Engine.Transactions;
using DbUp.Support;

namespace DbUp.Yellowbrick
{
    /// <summary>
    /// An implementation of the <see cref="IJournal"/> interface which tracks version numbers for a 
    /// Yellowbrick database using a journal table.
    /// </summary>
    public class YellowbrickSimpleTableJournal : TableJournal
    {
        /// <summary>
        /// Creates a new Yellowbrick table journal.
        /// </summary>
        /// <param name="connectionManager">The Yellowbrick connection manager.</param>
        /// <param name="logger">The upgrade logger.</param>
        /// <param name="schema">The name of the schema the journal is stored in.</param>
        /// <param name="tableName">The name of the journal table.</param>
        public YellowbrickSimpleTableJournal(Func<IConnectionManager> connectionManager, Func<IUpgradeLog> logger, string schema, string tableName)
            : base(connectionManager, logger, new YellowbrickObjectParser(), schema, tableName)
        {
        }

        protected override string GetInsertJournalEntrySql(string @scriptName, string @applied)
        {
            return $"insert into {FqSchemaTableName} (ScriptName, Applied) values ({@scriptName}, {@applied})";
        }

        protected override string GetJournalEntriesSql()
        {
            return $"select ScriptName from {FqSchemaTableName} order by ScriptName";
        }

        protected override string CreateSchemaTableSql(string quotedPrimaryKeyName)
        {
            return
$@"CREATE TABLE {FqSchemaTableName}
(
    schemaversionsid uuid NOT NULL default (sys.gen_random_uuid()),
    scriptname character varying(255) NOT NULL,
    applied timestamp without time zone NOT NULL,
    CONSTRAINT {quotedPrimaryKeyName} PRIMARY KEY (schemaversionsid)
)";
        }
    }
}
