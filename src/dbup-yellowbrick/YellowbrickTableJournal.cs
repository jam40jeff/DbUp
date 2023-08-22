using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
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
        readonly Func<string> username;
        readonly ISqlObjectParser sqlObjectParser;
        
        List<StoredScript> executedScripts;
        Dictionary<string, Guid> scriptContentIdByContent;

        /// <summary>
        /// Creates a new Yellowbrick table journal.
        /// </summary>
        /// <param name="connectionManager">The Yellowbrick connection manager.</param>
        /// <param name="username">The username.</param>
        /// <param name="logger">The upgrade logger.</param>
        /// <param name="schema">The name of the schema the journal is stored in.</param>
        /// <param name="tableName">The name of the journal table.</param>
        public YellowbrickTableJournal(Func<IConnectionManager> connectionManager, Func<string> username, Func<IUpgradeLog> logger, string schema, string tableName)
            : this(connectionManager, username, logger, new YellowbrickObjectParser(), schema, tableName)
        {
        }

        private YellowbrickTableJournal(Func<IConnectionManager> connectionManager, Func<string> username, Func<IUpgradeLog> logger, ISqlObjectParser sqlObjectParser, string schema, string tableName)
            : base(connectionManager, logger, sqlObjectParser, schema, tableName)
        {
            this.username = username;
            this.sqlObjectParser = sqlObjectParser;

            string fqScriptContentTableName = tableName + "_content";
            this.FqScriptContentTableName =
                string.IsNullOrEmpty(schema)
                    ? sqlObjectParser.QuoteIdentifier(fqScriptContentTableName)
                    : sqlObjectParser.QuoteIdentifier(schema) + "." + sqlObjectParser.QuoteIdentifier(fqScriptContentTableName);

            string fqScriptContentDataTableName = tableName + "_content_data";
            this.FqScriptContentDataTableName =
                string.IsNullOrEmpty(schema)
                    ? sqlObjectParser.QuoteIdentifier(fqScriptContentDataTableName)
                    : sqlObjectParser.QuoteIdentifier(schema) + "." + sqlObjectParser.QuoteIdentifier(fqScriptContentDataTableName);

            string fqScriptXContentTableName = tableName + "_x_content";
            this.FqScriptXContentTableName =
                string.IsNullOrEmpty(schema)
                    ? sqlObjectParser.QuoteIdentifier(fqScriptXContentTableName)
                    : sqlObjectParser.QuoteIdentifier(schema) + "." + sqlObjectParser.QuoteIdentifier(fqScriptXContentTableName);
        }

        protected string FqScriptContentTableName { get; }
        protected string FqScriptContentDataTableName { get; }
        protected string FqScriptXContentTableName { get; }

        protected List<StoredScript> GetExecutedScripts(Func<IDbCommand> dbCommandFactory)
        {
            if (executedScripts == null)
            {
                var results = new List<StoredScriptPart>();

                using (var command = dbCommandFactory())
                {
                    command.CommandText = $@"select q.script_id, q.script_name, q.script_type, q.applied_date, q.script_content_id, d.content, d.sort_order from
(select s.script_id, s.script_name, s.script_type, s.applied_date, x.script_content_id, row_number() over (partition by s.script_name order by applied_date desc) as rownum from {FqSchemaTableName} as s left join {FqScriptXContentTableName} as x on s.script_id = x.script_id) as q
left join {FqScriptContentTableName} as c on q.script_content_id = c.script_content_id
left join {FqScriptContentDataTableName} as d on c.script_content_id = d.script_content_id
where q.rownum = 1
order by q.script_name,d.sort_order";
                    command.CommandType = CommandType.Text;
                    using (var dataReader = command.ExecuteReader())
                    {
                        var scriptIdIndex = dataReader.GetOrdinal("script_id");
                        var scriptNameIndex = dataReader.GetOrdinal("script_name");
                        var scriptTypeIndex = dataReader.GetOrdinal("script_type");
                        var appliedIndex = dataReader.GetOrdinal("applied_date");
                        var scriptContentIdIndex = dataReader.GetOrdinal("script_content_id");
                        var contentIndex = dataReader.GetOrdinal("content");
                        var sortOrderIndex = dataReader.GetOrdinal("sort_order");

                        while (dataReader.Read())
                        {
                            results.Add(
                                new StoredScriptPart(
                                    dataReader.GetGuid(scriptIdIndex),
                                    dataReader.GetString(scriptNameIndex),
                                    ScriptTypeFromString(dataReader.GetString(scriptTypeIndex)),
                                    dataReader.GetDateTime(appliedIndex),
                                    dataReader.IsDBNull(scriptContentIdIndex) ? null : (Guid?)dataReader.GetGuid(scriptContentIdIndex),
                                    dataReader.IsDBNull(contentIndex) ? null : dataReader.GetString(contentIndex),
                                    dataReader.IsDBNull(sortOrderIndex) ? null : (int?)dataReader.GetInt32(sortOrderIndex)));
                        }
                    }

                    executedScripts =
                        results.GroupBy(r => new
                            {
                                r.Id,
                                r.ScriptName,
                                r.ScriptType,
                                r.Applied,
                                r.ScriptContentId
                            })
                            .Select(g => new StoredScript(new AppliedSqlScript(g.Key.ScriptName, g.Key.ScriptType, g.Any(r => r.SortOrder == null) ? null : ReassembleLongString(g.OrderBy(r => r.SortOrder).Select(r => r.Content)), g.Key.Applied), g.Key.ScriptContentId))
                            .ToList();
                }
            }

            return executedScripts;
        }

        protected Dictionary<string, Guid> GetScriptContentIdByContent(Func<IDbCommand> dbCommandFactory)
        {
            if (scriptContentIdByContent == null)
            {
                scriptContentIdByContent =
                    GetExecutedScripts(dbCommandFactory)
                        .GroupBy(s => s.AppliedSqlScript.Contents)
                        .Select(g =>
                        {
                            var contentId = g.Select(s => s.ContentId).FirstOrDefault(v => v != null);
                            if (contentId == null)
                            {
                                return null;
                            }

                            return new {Contents = g.Key, ContentId = contentId.Value};
                        })
                        .Where(o => o != null && o.Contents != null)
                        .ToDictionary(o => o.Contents, o => o.ContentId);
            }

            return scriptContentIdByContent;
        }

        protected override List<AppliedSqlScript> GetJournalEntries(Func<IDbCommand> dbCommandFactory)
        {
            return GetExecutedScripts(dbCommandFactory).Select(s => s.AppliedSqlScript).ToList();
        }

        protected override void InsertJournalEntry(Func<IDbCommand> dbCommandFactory, PreparedSqlScript script)
        {
            Guid scriptId = Guid.NewGuid();

            using (var command = dbCommandFactory())
            {
                var idParam = command.CreateParameter();
                idParam.ParameterName = "script_id";
                idParam.Value = scriptId;
                command.Parameters.Add(idParam);

                var scriptNameParam = command.CreateParameter();
                scriptNameParam.ParameterName = "script_name";
                scriptNameParam.Value = script.Name;
                command.Parameters.Add(scriptNameParam);

                var scriptTypeParam = command.CreateParameter();
                scriptTypeParam.ParameterName = "script_type";
                scriptTypeParam.Value = ScriptTypeToString(script.SqlScriptOptions.ScriptType);
                command.Parameters.Add(scriptTypeParam);

                var appliedParam = command.CreateParameter();
                appliedParam.ParameterName = "applied_date";
                appliedParam.Value = DateTime.Now;
                command.Parameters.Add(appliedParam);

                var appliedByParam = command.CreateParameter();
                appliedByParam.ParameterName = "applied_by";
                appliedByParam.Value = username();
                command.Parameters.Add(appliedByParam);

                command.CommandText = $"insert into {FqSchemaTableName} (script_id, script_name, script_type, applied_date, applied_by) values (@script_id, @script_name, @script_type, @applied_date, @applied_by)";
                command.CommandType = CommandType.Text;
                command.ExecuteNonQuery();
            }

            Guid scriptContentId;
            if (string.IsNullOrEmpty(script.Contents))
            {
                scriptContentId = Guid.NewGuid();
            }
            else
            {
                var scriptContentIdByContentLocal = GetScriptContentIdByContent(dbCommandFactory);
                if (!scriptContentIdByContentLocal.TryGetValue(script.Contents, out scriptContentId))
                {
                    scriptContentId = Guid.NewGuid();

                    using (var command = dbCommandFactory())
                    {
                        var idParam = command.CreateParameter();
                        idParam.ParameterName = "script_content_id";
                        idParam.Value = scriptContentId;
                        command.Parameters.Add(idParam);

                        command.CommandText = $"insert into {FqScriptContentTableName} (script_content_id) values (@script_content_id)";
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
                            idParam.ParameterName = "script_content_id";
                            idParam.Value = scriptContentId;
                            command.Parameters.Add(idParam);

                            var scriptParam = command.CreateParameter();
                            scriptParam.ParameterName = "content";
                            scriptParam.Value = contents;
                            command.Parameters.Add(scriptParam);

                            var orderParam = command.CreateParameter();
                            orderParam.ParameterName = "sort_order";
                            orderParam.Value = counter;
                            command.Parameters.Add(orderParam);

                            command.CommandText = $"insert into {FqScriptContentDataTableName} (script_content_id, content, sort_order) values (@script_content_id, @content, @sort_order)";
                            command.CommandType = CommandType.Text;
                            command.ExecuteNonQuery();
                        }
                    }
                }
            }

            using (var command = dbCommandFactory())
            {
                var idParam = command.CreateParameter();
                idParam.ParameterName = "script_id";
                idParam.Value = scriptId;
                command.Parameters.Add(idParam);

                var contentIdParam = command.CreateParameter();
                contentIdParam.ParameterName = "script_content_id";
                contentIdParam.Value = scriptContentId;
                command.Parameters.Add(contentIdParam);

                command.CommandText = $"insert into {FqScriptXContentTableName} (script_id, script_content_id) values (@script_id, @script_content_id)";
                command.CommandType = CommandType.Text;
                command.ExecuteNonQuery();
            }
        }

        protected override void CreateJournalTable(Func<IDbCommand> dbCommandFactory)
        {
            using (var command = dbCommandFactory())
            {
                command.CommandText = $"CREATE SCHEMA IF NOT EXISTS {SchemaTableSchema}";
                command.CommandType = CommandType.Text;
                command.ExecuteNonQuery();
            }

            using (var command = dbCommandFactory())
            {
                var primaryKeyName = sqlObjectParser.QuoteIdentifier("PK_" + UnquotedSchemaTableName + "_Id");
                command.CommandText = $@"CREATE TABLE {FqSchemaTableName}
(
    script_id uuid NOT NULL default (sys.gen_random_uuid()),
    script_name character varying(255) NOT NULL,
    script_type character varying(50) NOT NULL,
    applied_date timestamp without time zone NOT NULL,
    applied_by character varying(255) NULL,
    CONSTRAINT {primaryKeyName} PRIMARY KEY (script_id)
)";
                command.CommandType = CommandType.Text;
                command.ExecuteNonQuery();
            }

            using (var command = dbCommandFactory())
            {
                var primaryKeyName = sqlObjectParser.QuoteIdentifier("PK_" + UnquotedSchemaTableName + "_content_Id");
                command.CommandText = $@"CREATE TABLE {FqScriptContentTableName}
(
    script_content_id uuid NOT NULL default (sys.gen_random_uuid()),
    CONSTRAINT {primaryKeyName} PRIMARY KEY (script_content_id)
)";
                command.CommandType = CommandType.Text;
                command.ExecuteNonQuery();
            }

            using (var command = dbCommandFactory())
            {
                var primaryKeyName = sqlObjectParser.QuoteIdentifier("PK_" + UnquotedSchemaTableName + "_content_data_Id");
                command.CommandText = $@"CREATE TABLE {FqScriptContentDataTableName}
(
    script_content_data_id uuid NOT NULL default (sys.gen_random_uuid()),
    script_content_id uuid NOT NULL REFERENCES {FqScriptContentTableName} (script_content_id),
    content character varying(63000) NOT NULL,
    sort_order int NOT NULL,
    CONSTRAINT {primaryKeyName} PRIMARY KEY (script_content_data_id)
)";
                command.CommandType = CommandType.Text;
                command.ExecuteNonQuery();
            }

            using (var command = dbCommandFactory())
            {
                var primaryKeyName = sqlObjectParser.QuoteIdentifier("PK_" + UnquotedSchemaTableName + "_x_content_Id");
                command.CommandText = $@"CREATE TABLE {FqScriptXContentTableName}
(
    script_content_id uuid NOT NULL REFERENCES {FqScriptContentTableName} (script_content_id),
    script_id uuid NOT NULL REFERENCES {FqSchemaTableName} (script_id),
    CONSTRAINT {primaryKeyName} PRIMARY KEY (script_content_id, script_id)
)";
                command.CommandType = CommandType.Text;
                command.ExecuteNonQuery();
            }
        }

        private static IEnumerable<string> SplitLongString(string s, int chunkSize = 50000)
        {
            if (chunkSize < 1)
            {
                throw new ArgumentException("Chunk size must be greater than 0.", nameof(chunkSize));
            }

            if (!string.IsNullOrEmpty(s))
            {
                StringInfo si = new StringInfo(s);
                
                for (int i = 0; i < si.LengthInTextElements;)
                {
                    int chunkSizeLocal = chunkSize;
                    if (chunkSizeLocal + i > si.LengthInTextElements)
                    {
                        chunkSizeLocal = si.LengthInTextElements - i;
                    }

                    string returnString = si.SubstringByTextElements(i, chunkSizeLocal);
                    while (returnString.Length > chunkSize)
                    {
                        chunkSizeLocal -= returnString.Length - chunkSize;
                        returnString = si.SubstringByTextElements(i, chunkSizeLocal);
                    }

                    i += chunkSizeLocal;

                    yield return returnString;
                }
            }
        }

        private static string ReassembleLongString(IEnumerable<string> splitStrings) =>
            splitStrings == null ? null : string.Join(string.Empty, splitStrings.ToArray());

        private class StoredScriptPart
        {
            public StoredScriptPart(Guid id, string scriptName, ScriptType scriptType, DateTime applied, Guid? scriptContentId, string content, int? sortOrder)
            {
                Id = id;
                ScriptName = scriptName;
                ScriptType = scriptType;
                Applied = applied;
                ScriptContentId = scriptContentId;
                Content = content;
                SortOrder = sortOrder;
            }

            public Guid Id { get; }
            public string ScriptName { get; }
            public ScriptType ScriptType { get; }
            public DateTime Applied { get; }
            public Guid? ScriptContentId { get; }
            public string Content { get; }
            public int? SortOrder { get; }
        }

        protected class StoredScript
        {
            public StoredScript(AppliedSqlScript appliedSqlScript, Guid? contentId)
            {
                AppliedSqlScript = appliedSqlScript;
                ContentId = contentId;
            }

            public AppliedSqlScript AppliedSqlScript { get; }
            public Guid? ContentId { get; }
        }
    }
}
