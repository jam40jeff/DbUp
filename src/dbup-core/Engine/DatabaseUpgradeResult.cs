using System;
using System.Collections.Generic;

namespace DbUp.Engine
{
    /// <summary>
    /// Represents the results of a database upgrade.
    /// </summary>
    public sealed class DatabaseUpgradeResult
    {
        readonly List<PreparedSqlScript> scripts;
        readonly bool successful;
        readonly Exception error;
        readonly PreparedSqlScript errorScript;

        [Obsolete]
        public DatabaseUpgradeResult(IEnumerable<PreparedSqlScript> scripts, bool successful, Exception error)
            : this(scripts, successful, error, null)
        {
        }
        
        /// <summary>
        /// Initializes a new instance of the <see cref="DatabaseUpgradeResult"/> class.
        /// </summary>
        /// <param name="scripts">The scripts that were executed.</param>
        /// <param name="successful">if set to <c>true</c> [successful].</param>
        /// <param name="error">The error.</param>
        /// <param name="errorScript">The script that was executing when the error occured</param>
        public DatabaseUpgradeResult(IEnumerable<PreparedSqlScript> scripts, bool successful, Exception error, PreparedSqlScript errorScript)
        {
            this.scripts = new List<PreparedSqlScript>();
            this.scripts.AddRange(scripts);
            this.successful = successful;
            this.error = error;
            this.errorScript = errorScript;
        }

        /// <summary>
        /// Gets the scripts that were executed.
        /// </summary>
        public IEnumerable<PreparedSqlScript> Scripts => scripts;

        /// <summary>
        /// Gets a value indicating whether this <see cref="DatabaseUpgradeResult"/> is successful.
        /// </summary>
        public bool Successful => successful;

        /// <summary>
        /// Gets the error.
        /// </summary>
        public Exception Error => error;

        /// <summary>
        /// Gets the script that was executing when an error occured.
        /// </summary>
        public PreparedSqlScript ErrorScript => errorScript;
    }
}
