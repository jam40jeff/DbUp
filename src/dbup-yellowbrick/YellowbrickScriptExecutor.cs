using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using DbUp.Engine;
using DbUp.Engine.Output;
using DbUp.Engine.Preprocessors;
using DbUp.Engine.Transactions;
using DbUp.Support;
using Npgsql;

namespace DbUp.Yellowbrick
{
    /// <summary>
    /// An implementation of <see cref="ScriptExecutor"/> that executes against a Yellowbrick database.
    /// </summary>
    public class YellowbrickScriptExecutor : ScriptExecutor
    {
        readonly Func<bool> variablesEnabled;
        readonly List<IScriptPreprocessor> scriptPreprocessors;

        /// <summary>
        /// Initializes an instance of the <see cref="YellowbrickScriptExecutor"/> class.
        /// </summary>
        /// <param name="connectionManagerFactory"></param>
        /// <param name="log">The logging mechanism.</param>
        /// <param name="schema">The schema that contains the table.</param>
        /// <param name="variablesEnabled">Function that returns <c>true</c> if variables should be replaced, <c>false</c> otherwise.</param>
        /// <param name="scriptPreprocessors">Script Preprocessors in addition to variable substitution</param>
        /// <param name="journalFactory">Database journal</param>
        public YellowbrickScriptExecutor(Func<IConnectionManager> connectionManagerFactory, Func<IUpgradeLog> log, string schema, Func<bool> variablesEnabled,
            IEnumerable<IScriptPreprocessor> scriptPreprocessors, Func<IJournal> journalFactory)
            : this(connectionManagerFactory, new YellowbrickObjectParser(), log, schema, variablesEnabled, (scriptPreprocessors ?? new IScriptPreprocessor[0]).ToList(), journalFactory)
        {
        }

        private YellowbrickScriptExecutor(
            Func<IConnectionManager> connectionManagerFactory, ISqlObjectParser sqlObjectParser,
            Func<IUpgradeLog> log, string schema, Func<bool> variablesEnabled,
            List<IScriptPreprocessor> scriptPreprocessors,
            Func<IJournal> journalFactory)
            : base(connectionManagerFactory, sqlObjectParser, log, schema, variablesEnabled, scriptPreprocessors, journalFactory)
        {
            this.variablesEnabled = variablesEnabled;
            this.scriptPreprocessors = scriptPreprocessors;
        }

        public override string PreprocessScriptContents(string contents, IDictionary<string, string> variables)
        {
            if (variables == null)
                variables = new Dictionary<string, string>();
            if (Schema != null && !variables.ContainsKey("schema"))
                variables.Add("schema", QuoteSqlObjectName(Schema));

            if (string.IsNullOrEmpty(Schema))
                contents = new StripSchemaPreprocessor().Process(contents);
            if (variablesEnabled())
                contents = new YellowbrickVariableSubstitutionPreprocessor(variables).Process(contents);
            contents = (scriptPreprocessors ?? new List<IScriptPreprocessor>())
                .Aggregate(contents, (current, additionalScriptPreprocessor) => additionalScriptPreprocessor.Process(current));

            return contents;
        }
        
        protected override string GetVerifySchemaSql(string schema) => $"CREATE SCHEMA IF NOT EXISTS {schema}";

        protected override void HandleException(int index, PreparedSqlScript script, Exception e)
        {
#if NPGSQLv2
            NpgsqlException exception = e as NpgsqlException;
#else
            PostgresException exception = e as PostgresException;
#endif
            if (exception != null)
            {
                Log().WriteInformation("Npgsql exception has occured in script: '{0}'", script.Name);
                Log().WriteError("Script block number: {0}; Block line {1}; Position: {2}; Message: {3}", index, exception.Line, exception.Position, exception.Message);
                Log().WriteError(exception.ToString());
            }
            else
            {
                base.HandleException(index, script, e);
            }
        }

        /// <summary>
        /// Substitutes variables for values in SqlScripts
        /// </summary>
        private class YellowbrickVariableSubstitutionPreprocessor : IScriptPreprocessor
        {
            readonly IDictionary<string, string> variables;

            /// <summary>
            /// Initializes a new instance of the <see cref="YellowbrickVariableSubstitutionPreprocessor"/> class.
            /// </summary>
            /// <param name="variables">The variables.</param>
            public YellowbrickVariableSubstitutionPreprocessor(IDictionary<string, string> variables)
            {
                this.variables = variables ?? throw new ArgumentNullException(nameof(variables));
            }

            /// <summary>
            /// Substitutes variables 
            /// </summary>
            /// <param name="contents"></param>
            public string Process(string contents)
            {
                using (var parser = new YellowbrickVariableSubstitutionSqlParser(contents))
                {
                    return parser.ReplaceVariables(variables);
                }
            }

            /// <summary>
            /// Parses the Sql and substitutes variables, used by the <see cref="VariableSubstitutionPreprocessor"/>
            /// </summary>
            private class YellowbrickVariableSubstitutionSqlParser : SqlParser
            {
                /// <summary>
                /// Initializes a new instance of the <see cref="YellowbrickVariableSubstitutionSqlParser"/> class.
                /// </summary>
                /// <param name="sqlText">The sql to be parsed</param>
                /// <param name="delimiter">The command delimiter (default = "GO")</param>
                /// <param name="delimiterRequiresWhitespace">Whether the delimiter requires whitespace after it (default = true)</param>
                public YellowbrickVariableSubstitutionSqlParser(string sqlText, string delimiter = "GO", bool delimiterRequiresWhitespace = true)
                    : base(sqlText, delimiter, delimiterRequiresWhitespace)
                {
                }

                /// <summary>
                /// Delimiter character for variables.
                /// Defaults to "$__var__$" but can be overriden in derived classes.
                /// </summary>
                protected virtual string VariableDelimiter => "$__var__$";

                /// <summary>
                /// Replaces variables in the parsed SQL
                /// </summary>
                /// <param name="variables">Variable map</param>
                /// <returns>The sql with all variables replaced</returns>
                /// <exception cref="ArgumentNullException">Throws if <paramref name="variables"/> is null</exception>
                /// <exception cref="InvalidOperationException">Throws if a variable is present in the SQL but not in the `variables` map</exception>
                public string ReplaceVariables(IDictionary<string, string> variables)
                {
                    if (variables == null)
                        throw new ArgumentNullException(nameof(variables));

                    var sb = new StringBuilder();

                    ReadCharacter += (type, c) => sb.Append(c);

                    ReadVariableName += (name) =>
                    {
                        if (!variables.ContainsKey(name))
                        {
                            throw new InvalidOperationException(string.Format(CultureInfo.CurrentCulture, "Variable {0} has no value defined", name));
                        }

                        sb.Append(variables[name]);
                    };

                    Parse();

                    return sb.ToString();
                }

                /// <summary>
                /// Checks if it's the beginning of a variable
                /// </summary>
                protected override bool IsCustomStatement
                {
                    get
                    {
                        return IsDelimiter(false);
                    }
                }

                private bool IsDelimiter(bool end)
                {
                    var isCurrentCharacterStartOfDelimiter = CurrentChar == VariableDelimiter[0];
                    return isCurrentCharacterStartOfDelimiter
                           && TryPeek(VariableDelimiter.Length, out var result)
                           && (VariableDelimiter.Length < 2 || string.Equals(result.Substring(0, VariableDelimiter.Length - 1), VariableDelimiter.Substring(1)))
                           && (end || ValidVariableNameCharacter(result[VariableDelimiter.Length - 1]));
                }

                /// <summary>
                /// Verifies a character satisfies variable naming rules
                /// </summary>
                /// <param name="c">The character</param>
                /// <returns>True if it's a valid variable name character</returns>
                protected virtual bool ValidVariableNameCharacter(char c)
                {
                    return char.IsLetterOrDigit(c) || c == '_' || c == '-';
                }

                /// <summary>
                /// Read a variable for substitution
                /// </summary>
                protected override void ReadCustomStatement()
                {
                    for (int i = 0; i < VariableDelimiter.Length - 1; i++)
                    {
                        Read();
                    }
                    
                    var sb = new StringBuilder();
                    while (Read() > 0 && !IsDelimiter(true) && ValidVariableNameCharacter(CurrentChar))
                    {
                        sb.Append(CurrentChar);
                    }

                    var buffer = sb.ToString();

                    if (IsDelimiter(true) && ReadVariableName != null)
                    {
                        ReadVariableName(buffer);
                        
                        for (int i = 0; i < VariableDelimiter.Length - 1; i++)
                        {
                            Read();
                        }
                    }
                    else
                    {
                        OnReadCharacter(CharacterType.Command, VariableDelimiter[0]);
                        foreach (var c in buffer)
                        {
                            OnReadCharacter(CharacterType.Command, c);
                        }

                        OnReadCharacter(CharacterType.Command, CurrentChar);
                    }
                }

                event Action<string> ReadVariableName;
            }
        }
    }
}
