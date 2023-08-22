using System;
using System.Collections.Generic;
using System.IO;

namespace DbUp.Engine
{
    /// <summary>
    /// Represents a script that comes from some source, e.g. an embedded resource in an assembly, and whose contents have been preprocessed. 
    /// </summary>
    [System.Diagnostics.DebuggerDisplay("{Name}")]
    public class PreparedSqlScript
    {
        private readonly IScriptExecutor scriptExecutor;
        private readonly IDictionary<string, string> variables;
        readonly SqlScript sqlScript;
        string content;

        /// <summary>
        /// Initializes a new instance of the <see cref="PreparedSqlScript"/> class with a specific options - script type and a script order
        /// </summary>
        /// <param name="scriptExecutor">The script executor.</param>
        /// <param name="variables">The variables.</param>
        /// <param name="sqlScript">The SQL script.</param>        
        public PreparedSqlScript(IScriptExecutor scriptExecutor, IDictionary<string, string> variables, SqlScript sqlScript)
        {
            this.scriptExecutor = scriptExecutor;
            this.variables = variables;
            this.sqlScript = sqlScript;
        }

        /// <summary>
        /// Gets the contents of the script.
        /// </summary>
        public string Contents => content ?? (content = sqlScript.ContentProvider(scriptExecutor, variables));

        /// <summary>
        /// Gets the contents of the script as a stream.
        /// </summary>
        public Stream GetContentStream() => sqlScript.ContentStreamProvider(scriptExecutor, variables);

        /// <summary>
        /// Gets the SQL Script Options
        /// </summary>
        public SqlScriptOptions SqlScriptOptions => sqlScript.SqlScriptOptions;

        /// <summary>
        /// Gets the name of the script.
        /// </summary>
        public string Name => sqlScript.Name;
    }
}
