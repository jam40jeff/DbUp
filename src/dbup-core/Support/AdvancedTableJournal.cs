using System;
using System.Collections.Generic;
using System.Data;
using DbUp.Engine;
using DbUp.Engine.Output;
using DbUp.Engine.Transactions;

namespace DbUp.Support
{
    /// <summary>
    /// The base class for Journal implementations that use a table.
    /// </summary>
    public abstract class AdvancedTableJournal : IJournal
    {
        readonly ISqlObjectParser sqlObjectParser;
        bool journalExists;

        /// <summary>
        /// Initializes a new instance of the <see cref="TableJournal"/> class.
        /// </summary>
        /// <param name="connectionManager">The connection manager.</param>
        /// <param name="logger">The log.</param>
        /// <param name="sqlObjectParser"></param>
        /// <param name="schema">The schema that contains the table.</param>
        /// <param name="table">The table name.</param>
        protected AdvancedTableJournal(
            Func<IConnectionManager> connectionManager,
            Func<IUpgradeLog> logger,
            ISqlObjectParser sqlObjectParser,
            string schema, string table)
        {
            this.sqlObjectParser = sqlObjectParser;
            ConnectionManager = connectionManager;
            Log = logger;
            UnquotedSchemaTableName = table;
            SchemaTableSchema = schema;
            FqSchemaTableName = string.IsNullOrEmpty(schema)
                ? sqlObjectParser.QuoteIdentifier(table)
                : sqlObjectParser.QuoteIdentifier(schema) + "." + sqlObjectParser.QuoteIdentifier(table);
        }

        protected string SchemaTableSchema { get; private set; }

        /// <summary>
        /// Schema table name, no schema and unquoted
        /// </summary>
        protected string UnquotedSchemaTableName { get; private set; }

        /// <summary>
        /// Fully qualified schema table name, includes schema and is quoted.
        /// </summary>
        protected string FqSchemaTableName { get; private set; }

        protected Func<IConnectionManager> ConnectionManager { get; private set; }

        protected Func<IUpgradeLog> Log { get; private set; }

        public AppliedSqlScript[] GetExecutedScripts()
        {
            return ConnectionManager().ExecuteCommandsWithManagedConnection(dbCommandFactory =>
            {
                if (journalExists || DoesTableExist(dbCommandFactory))
                {
                    journalExists = true;
                    
                    Log().WriteInformation("Fetching list of already executed scripts.");

                    var scripts = GetJournalEntries(dbCommandFactory);

                    return scripts.ToArray();
                }
                else
                {
                    Log().WriteInformation("Journal table does not exist");
                    return new AppliedSqlScript[0];
                }
            });
        }

        /// <summary>
        /// Records a database upgrade for a database specified in a given connection string.
        /// </summary>
        /// <param name="script">The script.</param>
        /// <param name="dbCommandFactory"></param>
        public virtual void StoreExecutedScript(PreparedSqlScript script, Func<IDbCommand> dbCommandFactory)
        {
            EnsureTableExistsAndIsLatestVersion(dbCommandFactory);
            InsertJournalEntry(dbCommandFactory, script);
        }

        private static string ScriptTypeToString(ScriptType scriptType)
        {
            switch (scriptType)
            {
                case ScriptType.RunOnce:
                    return "RunOnce";
                case ScriptType.RunAlways:
                    return "RunAlways";
                case ScriptType.RunIfChanged:
                    return "RunIfChanged";
                default:
                    throw new Exception("Unsupported script type: " + scriptType);
            }
        }

        private static ScriptType ScriptTypeFromString(string scriptType)
        {
            switch (scriptType)
            {
                case "RunOnce":
                    return ScriptType.RunOnce;
                case "RunAlways":
                    return ScriptType.RunAlways;
                case "RunIfChanged":
                    return ScriptType.RunIfChanged;
                default:
                    throw new Exception("Unsupported script type: " + scriptType);
            }
        }

        protected IDbCommand GetCreateTableCommand(Func<IDbCommand> dbCommandFactory)
        {
            var command = dbCommandFactory();
            var primaryKeyName = sqlObjectParser.QuoteIdentifier("PK_" + UnquotedSchemaTableName + "_Id");
            command.CommandText = CreateSchemaTableSql(primaryKeyName);
            command.CommandType = CommandType.Text;
            return command;
        }

        /// <summary>
        /// Inserts a journal entry.
        /// </summary>
        protected abstract void InsertJournalEntry(Func<IDbCommand> dbCommandFactory, PreparedSqlScript script);

        /// <summary>
        /// Gets the journal entries.
        /// </summary>
        protected abstract List<AppliedSqlScript> GetJournalEntries(Func<IDbCommand> dbCommandFactory);

        /// <summary>
        /// Sql for creating journal table
        /// </summary>
        /// <param name="quotedPrimaryKeyName">Following PK_{TableName}_Id naming</param>
        protected abstract string CreateSchemaTableSql(string quotedPrimaryKeyName);

        /// <summary>
        /// Unquotes a quoted identifier.
        /// </summary>
        /// <param name="quotedIdentifier">identifier to unquote.</param>
        protected string UnquoteSqlObjectName(string quotedIdentifier)
        {
            return sqlObjectParser.UnquoteIdentifier(quotedIdentifier);
        }

        protected virtual void OnTableCreated(Func<IDbCommand> dbCommandFactory)
        {
            // TODO: Now we could run any migration scripts on it using some mechanism to make sure the table is ready for use.
        }

        public virtual void EnsureTableExistsAndIsLatestVersion(Func<IDbCommand> dbCommandFactory)
        {
            if (!journalExists && !DoesTableExist(dbCommandFactory))
            {
                Log().WriteInformation(string.Format("Creating the {0} table", FqSchemaTableName));
                // We will never change the schema of the initial table create.
                using (var command = GetCreateTableCommand(dbCommandFactory))
                {
                    command.ExecuteNonQuery();
                }

                Log().WriteInformation(string.Format("The {0} table has been created", FqSchemaTableName));

                OnTableCreated(dbCommandFactory);
            }

            journalExists = true;
        }

        protected bool DoesTableExist(Func<IDbCommand> dbCommandFactory)
        {
            Log().WriteInformation("Checking whether journal table exists..");
            using (var command = dbCommandFactory())
            {
                command.CommandText = DoesTableExistSql();
                command.CommandType = CommandType.Text;
                var executeScalar = command.ExecuteScalar();
                if (executeScalar == null)
                    return false;
                if (executeScalar is long)
                    return (long)executeScalar == 1;
                if (executeScalar is decimal)
                    return (decimal)executeScalar == 1;
                return (int)executeScalar == 1;
            }
        }

        /// <summary>Verify, using database-specific queries, if the table exists in the database.</summary>
        /// <returns>1 if table exists, 0 otherwise</returns>
        protected virtual string DoesTableExistSql()
        {
            return string.IsNullOrEmpty(SchemaTableSchema)
                ? string.Format("select 1 from INFORMATION_SCHEMA.TABLES where TABLE_NAME = '{0}'", UnquotedSchemaTableName)
                : string.Format("select 1 from INFORMATION_SCHEMA.TABLES where TABLE_NAME = '{0}' and TABLE_SCHEMA = '{1}'", UnquotedSchemaTableName, SchemaTableSchema);
        }
    }
}
