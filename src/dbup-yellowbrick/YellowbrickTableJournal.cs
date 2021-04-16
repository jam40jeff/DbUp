using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using DbUp.Engine;
using DbUp.Engine.Output;
using DbUp.Engine.Transactions;
using DbUp.Support;

namespace DbUp.Yellowbrick
{
    /// <summary>
    /// An implementation of the <see cref="IJournal"/> interface which tracks version numbers for a 
    /// Yellowbrick database using a journal table including script content.
    /// </summary>
    public class YellowbrickTableJournal : AdvancedTableJournal
    {
        private readonly ISqlObjectParser sqlObjectParser;

        /// <summary>
        /// Creates a new Yellowbrick table journal.
        /// </summary>
        /// <param name="connectionManager">The Yellowbrick connection manager.</param>
        /// <param name="logger">The upgrade logger.</param>
        /// <param name="schema">The name of the schema the journal is stored in.</param>
        /// <param name="tableName">The name of the journal table.</param>
        public YellowbrickTableJournal(Func<IConnectionManager> connectionManager, Func<IUpgradeLog> logger, string schema, string tableName)
            : this(connectionManager, logger, new YellowbrickObjectParser(), schema, tableName)
        {
        }

        private YellowbrickTableJournal(Func<IConnectionManager> connectionManager, Func<IUpgradeLog> logger, ISqlObjectParser sqlObjectParser, string schema, string tableName)
            : base(connectionManager, logger, sqlObjectParser, schema, tableName)
        {
            this.sqlObjectParser = sqlObjectParser;

            this.FqScriptsTableName =
                string.IsNullOrEmpty(schema)
                    ? sqlObjectParser.QuoteIdentifier(tableName)
                    : sqlObjectParser.QuoteIdentifier(schema) + "." + sqlObjectParser.QuoteIdentifier(tableName + "_scripts");
        }

        protected string FqScriptsTableName { get; }

        protected override List<AppliedSqlScript> GetJournalEntries(Func<IDbCommand> dbCommandFactory)
        {
            var results = new List<StoredScript>();

            using (var command = dbCommandFactory())
            {
                command.CommandText = $@"select q.schemaversionsid, q.scriptname, q.scripttype, q.applied, s.script, s.sortorder from
(select schemaversionsid, scriptname, scripttype, applied, row_number() over (partition by scriptname order by applied desc) as rownum from {FqSchemaTableName}) as q
join {FqScriptsTableName} as s on q.schemaversionsid = s.schemaversionsid
where q.rownum = 1
order by q.scriptname,s.sortorder";
                command.CommandType = CommandType.Text;
                using (var dataReader = command.ExecuteReader())
                {
                    var schemaVersionsIdIndex = dataReader.GetOrdinal("schemaversionsid");
                    var scriptNameIndex = dataReader.GetOrdinal("scriptname");
                    var scriptTypeIndex = dataReader.GetOrdinal("scripttype");
                    var appliedIndex = dataReader.GetOrdinal("applied");
                    var scriptIndex = dataReader.GetOrdinal("script");
                    var sortOrderIndex = dataReader.GetOrdinal("sortorder");

                    while (dataReader.Read())
                    {
                        results.Add(
                            new StoredScript(
                                dataReader.GetGuid(schemaVersionsIdIndex),
                                dataReader.GetString(scriptNameIndex),
                                ScriptTypeFromString(dataReader.GetString(scriptTypeIndex)),
                                dataReader.GetDateTime(appliedIndex),
                                dataReader.GetString(scriptIndex),
                                dataReader.GetInt32(sortOrderIndex)));
                    }
                }

                return results.GroupBy(r => new {r.Id, r.ScriptName, r.ScriptType, r.Applied})
                    .Select(g => new AppliedSqlScript(g.Key.ScriptName, g.Key.ScriptType, ReassembleLongString(g.OrderBy(r => r.SortOrder).Select(r => r.Script)), g.Key.Applied))
                    .ToList();
            }
        }

        protected override void InsertJournalEntry(Func<IDbCommand> dbCommandFactory, PreparedSqlScript script)
        {
            Guid id = Guid.NewGuid();

            using (var command = dbCommandFactory())
            {
                var idParam = command.CreateParameter();
                idParam.ParameterName = "schemaversionsid";
                idParam.Value = id;
                command.Parameters.Add(idParam);

                var scriptNameParam = command.CreateParameter();
                scriptNameParam.ParameterName = "scriptname";
                scriptNameParam.Value = script.Name;
                command.Parameters.Add(scriptNameParam);

                var scriptTypeParam = command.CreateParameter();
                scriptTypeParam.ParameterName = "scripttype";
                scriptTypeParam.Value = ScriptTypeToString(script.SqlScriptOptions.ScriptType);
                command.Parameters.Add(scriptTypeParam);

                var appliedParam = command.CreateParameter();
                appliedParam.ParameterName = "applied";
                appliedParam.Value = DateTime.Now;
                command.Parameters.Add(appliedParam);

                command.CommandText = $"insert into {FqSchemaTableName} (schemaversionsid, scriptname, scripttype, applied) values (@schemaversionsid, @scriptname, @scripttype, @applied)";
                command.CommandType = CommandType.Text;
                command.ExecuteNonQuery();
            }

            IEnumerable<string> contentsChunks = SplitLongString(script.Contents);
            int counter = 0;
            foreach (string contents in contentsChunks)
            {
                counter++;
                
                using (var command = dbCommandFactory())
                {
                    var idParam = command.CreateParameter();
                    idParam.ParameterName = "schemaversionsid";
                    idParam.Value = id;
                    command.Parameters.Add(idParam);

                    var scriptParam = command.CreateParameter();
                    scriptParam.ParameterName = "script";
                    scriptParam.Value = contents;
                    command.Parameters.Add(scriptParam);

                    var orderParam = command.CreateParameter();
                    orderParam.ParameterName = "sortorder";
                    orderParam.Value = counter;
                    command.Parameters.Add(orderParam);

                    command.CommandText = $"insert into {FqSchemaTableName} (schemaversionsid, script, sortorder) values (@schemaversionsid, @script, @sortorder)";
                    command.CommandType = CommandType.Text;
                    command.ExecuteNonQuery();
                }
            }
        }

        protected override void CreateJournalTable(Func<IDbCommand> dbCommandFactory)
        {
            using (var command = dbCommandFactory())
            {
                var primaryKeyName = sqlObjectParser.QuoteIdentifier("PK_" + UnquotedSchemaTableName + "_Id");
                command.CommandText = $@"CREATE TABLE {FqSchemaTableName}
(
    schemaversionsid uuid NOT NULL default (sys.gen_random_uuid()),
    scriptname character varying(255) NOT NULL,
    scripttype character varying(50) NOT NULL,
    applied timestamp without time zone NOT NULL,
    CONSTRAINT {primaryKeyName} PRIMARY KEY (schemaversionsid)
)";
                command.CommandType = CommandType.Text;
                command.ExecuteNonQuery();
            }

            using (var command = dbCommandFactory())
            {
                var primaryKeyName = sqlObjectParser.QuoteIdentifier("PK_" + UnquotedSchemaTableName + "_scripts_Id");
                command.CommandText = $@"CREATE TABLE {FqScriptsTableName}
(
    scriptsid uuid NOT NULL default (sys.gen_random_uuid()),
    schemaversionsid uuid NOT NULL REFERENCES {FqSchemaTableName} (schemaversionsid),
    script character varying(60000) NOT NULL,
    sortorder int NOT NULL,
    CONSTRAINT {primaryKeyName} PRIMARY KEY (scriptsid)
)";
                command.CommandType = CommandType.Text;
                command.ExecuteNonQuery();
            }
        }

        private static IEnumerable<string> SplitLongString(string s, int chunkSize = 60000)
        {
            if (chunkSize < 1)
            {
                throw new ArgumentException("Chunk size must be greater than 0.", nameof(chunkSize));
            }

            if (!string.IsNullOrEmpty(s))
            {
                for (int i = 0; i < s.Length; i += chunkSize)
                {
                    if (chunkSize + i > s.Length)
                    {
                        chunkSize = s.Length - i;
                    }

                    yield return s.Substring(i, chunkSize);
                }
            }
        }

        private static string ReassembleLongString(IEnumerable<string> splitStrings) =>
            splitStrings == null ? null : string.Join(string.Empty, splitStrings.ToArray());

        private class StoredScript
        {
            public StoredScript(Guid id, string scriptName, ScriptType scriptType, DateTime applied, string script, int sortOrder)
            {
                Id = id;
                ScriptName = scriptName;
                ScriptType = scriptType;
                Applied = applied;
                Script = script;
                SortOrder = sortOrder;
            }

            public Guid Id { get; }
            public string ScriptName { get; }
            public ScriptType ScriptType { get; }
            public DateTime Applied { get; }
            public string Script { get; }
            public int SortOrder { get; }
        }
    }
}
