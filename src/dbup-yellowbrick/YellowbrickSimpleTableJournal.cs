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
        readonly Func<string> username;
        
        /// <summary>
        /// Creates a new Yellowbrick table journal.
        /// </summary>
        /// <param name="connectionManager">The Yellowbrick connection manager.</param>
        /// <param name="username">The username.</param>
        /// <param name="logger">The upgrade logger.</param>
        /// <param name="schema">The name of the schema the journal is stored in.</param>
        /// <param name="tableName">The name of the journal table.</param>
        public YellowbrickSimpleTableJournal(Func<IConnectionManager> connectionManager, Func<string> username, Func<IUpgradeLog> logger, string schema, string tableName)
            : base(connectionManager, username, logger, new YellowbrickObjectParser(), schema, tableName)
        {
            this.username = username;
        }

        protected override string GetInsertJournalEntrySql(string @scriptName, string @applied, string @appliedBy)
        {
            return $"insert into {FqSchemaTableName} (script_name, applied_date, applied_by) values ({@scriptName}, {@applied}, {@appliedBy})";
        }

        protected override string GetJournalEntriesSql()
        {
            return $"select script_name from {FqSchemaTableName} order by script_name";
        }

        protected override string CreateSchemaTableSql(string quotedPrimaryKeyName)
        {
            return
$@"CREATE TABLE {FqSchemaTableName}
(
    script_id uuid NOT NULL default (sys.gen_random_uuid()),
    script_name character varying(255) NOT NULL,
    applied_date timestamp without time zone NOT NULL,
    applied_by character varying(255) NULL,
    CONSTRAINT {quotedPrimaryKeyName} PRIMARY KEY (script_id)
)";
        }
    }
}
