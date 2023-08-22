using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography.X509Certificates;
using System.Text.RegularExpressions; 
using DbUp.Engine.Transactions;
using Npgsql;

namespace DbUp.Yellowbrick
{
    /// <summary>
    /// Manages Yellowbrick database connections.
    /// </summary>
    public class YellowbrickConnectionManager : DatabaseConnectionManager
    {
        readonly string role;

        /// <summary>
        /// Creates a new Yellowbrick database connection.
        /// </summary>
        /// <param name="connectionString">The Yellowbrick connection string.</param>
        /// <param name="role">Role to execute as.</param>
        public YellowbrickConnectionManager(string connectionString, string role)
            : base(new DelegateConnectionFactory(l => new NpgsqlConnection(connectionString)))
        {
            this.role = role;
        }

        /// <summary>
        /// Creates a new Yellowbrick database connection with a certificate.
        /// </summary>
        /// <param name="connectionString">The Yellowbrick connection string.</param>
        /// <param name="role">Role to execute as.</param>
        /// <param name="certificate">Certificate for securing connection.</param>
        public YellowbrickConnectionManager(string connectionString, string role, X509Certificate2 certificate)
            : base(new DelegateConnectionFactory(l =>
                {
                    NpgsqlConnection databaseConnection = new NpgsqlConnection(connectionString);
                    databaseConnection.ProvideClientCertificatesCallback +=
                        certs => certs.Add(certificate);

                    return databaseConnection;
                }
                ))
        {
            this.role = role;
        }

        protected override void SetRole(ITransactionStrategy strategy)
        {
            if (role != null)
            {
                strategy.Execute(dbCommandFactory =>
                {
                    using (var command = dbCommandFactory())
                    {
                        command.CommandText = "SET ROLE \"" + role + "\";";
                        command.ExecuteScalar();
                    }
                });
            }
        }

        /// <summary>
        /// Splits the statements in the script using the string ";;;;" character.
        /// </summary>
        /// <param name="scriptContents">The contents of the script to split.</param>
        public override IEnumerable<string> SplitScriptIntoCommands(string scriptContents)
        {
            var scriptStatements =
                Regex.Split(scriptContents, "^\\s*;;;;\\s*$", RegexOptions.IgnoreCase | RegexOptions.Multiline)
                    .Select(x => x.Trim())
                    .Where(x => x.Length > 0)
                    .ToArray();

            return scriptStatements;
        }
    }
}
