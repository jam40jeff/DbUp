using System;

namespace DbUp.Engine
{
    /// <summary>
    /// Represents a SQL script that is fetched at execution time, rather than discovery time
    /// </summary>
    public class LazySqlScript : SqlScript
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="LazySqlScript"/> class.
        /// </summary>
        /// <param name="name">The name.</param>
        /// <param name="contentProvider">The delegate which creates the content at execution time.</param>
        public LazySqlScript(string name, Func<string> contentProvider)
            : this(name, new SqlScriptOptions(), contentProvider)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="LazySqlScript"/> class.
        /// </summary>
        /// <param name="name">The name.</param>
        /// <param name="sqlScriptOptions">The sql script options.</param>        
        /// <param name="contentProvider">The delegate which creates the content at execution time.</param>
        public LazySqlScript(string name, SqlScriptOptions sqlScriptOptions, Func<string> contentProvider)
            : base(name, contentProvider, sqlScriptOptions)
        {
        }
    }
}
