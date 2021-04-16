﻿using System;
using System.Data;
using System.Security.Cryptography.X509Certificates;
using DbUp;
using DbUp.Builder;
using DbUp.Engine.Output;
using DbUp.Engine.Transactions;
using DbUp.Yellowbrick;
using Npgsql;

// ReSharper disable once CheckNamespace

/// <summary>
/// Configuration extension methods for Yellowbrick.
/// </summary>
public static class YellowbrickExtensions
{
    /// <summary>
    /// Creates an upgrader for Yellowbrick databases.
    /// </summary>
    /// <param name="supported">Fluent helper type.</param>
    /// <param name="connectionString">Yellowbrick database connection string.</param>
    /// <returns>
    /// A builder for a database upgrader designed for Yellowbrick databases.
    /// </returns>
    public static UpgradeEngineBuilder YellowbrickDatabase(this SupportedDatabases supported, string connectionString)
        => YellowbrickDatabase(supported, connectionString, null);

    /// <summary>
    /// Creates an upgrader for Yellowbrick databases.
    /// </summary>
    /// <param name="supported">Fluent helper type.</param>
    /// <param name="connectionString">Yellowbrick database connection string.</param>
    /// <param name="schema">The schema in which to check for changes</param>
    /// <returns>
    /// A builder for a database upgrader designed for Yellowbrick databases.
    /// </returns>
    public static UpgradeEngineBuilder YellowbrickDatabase(this SupportedDatabases supported, string connectionString, string schema)
        => YellowbrickDatabase(new YellowbrickConnectionManager(connectionString), schema);

    /// <summary>
    /// Creates an upgrader for Yellowbrick databases that use SSL.
    /// </summary>
    /// <param name="supported">Fluent helper type.</param>
    /// <param name="connectionString">Yellowbrick database connection string.</param>
    /// <param name="schema">The schema in which to check for changes</param>
    /// <param name="certificate">Certificate for securing connection.</param>
    /// <returns>
    /// A builder for a database upgrader designed for Yellowbrick databases.
    /// </returns>
    public static UpgradeEngineBuilder YellowbrickDatabase(this SupportedDatabases supported, string connectionString, string schema, X509Certificate2 certificate)
        => YellowbrickDatabase(new YellowbrickConnectionManager(connectionString, certificate), schema);

    /// <summary>
    /// Creates an upgrader for Yellowbrick databases.
    /// </summary>
    /// <param name="supported">Fluent helper type.</param>
    /// <param name="connectionManager">The <see cref="YellowbrickConnectionManager"/> to be used during a database upgrade.</param>
    /// <returns>
    /// A builder for a database upgrader designed for Yellowbrick databases.
    /// </returns>
    public static UpgradeEngineBuilder YellowbrickDatabase(this SupportedDatabases supported, IConnectionManager connectionManager)
        => YellowbrickDatabase(connectionManager);

    /// <summary>
    /// Creates an upgrader for Yellowbrick databases.
    /// </summary>
    /// <param name="connectionManager">The <see cref="YellowbrickConnectionManager"/> to be used during a database upgrade.</param>
    /// <returns>
    /// A builder for a database upgrader designed for Yellowbrick databases.
    /// </returns>
    public static UpgradeEngineBuilder YellowbrickDatabase(IConnectionManager connectionManager)
        => YellowbrickDatabase(connectionManager, null);

    /// <summary>
    /// Creates an upgrader for Yellowbrick databases.
    /// </summary>
    /// <param name="connectionManager">The <see cref="YellowbrickConnectionManager"/> to be used during a database upgrade.</param>
    /// <param name="schema">The schema in which to check for changes</param>
    /// <returns>
    /// A builder for a database upgrader designed for Yellowbrick databases.
    /// </returns>
    public static UpgradeEngineBuilder YellowbrickDatabase(IConnectionManager connectionManager, string schema)
    {
        var builder = new UpgradeEngineBuilder();
        builder.Configure(c => c.ConnectionManager = connectionManager);
        builder.Configure(c => c.ScriptExecutor = new YellowbrickScriptExecutor(() => c.ConnectionManager, () => c.Log, schema, () => c.VariablesEnabled, c.ScriptPreprocessors, () => c.Journal));
        builder.Configure(c => c.Journal = new YellowbrickTableJournal(() => c.ConnectionManager, () => c.Log, schema, "schemaversions"));
        builder.WithPreprocessor(new YellowbrickPreprocessor());
        return builder;
    }

    /// <summary>
    /// Ensures that the database specified in the connection string exists.
    /// </summary>
    /// <param name="supported">Fluent helper type.</param>
    /// <param name="connectionString">The connection string.</param>
    /// <returns></returns>
    public static void YellowbrickDatabase(this SupportedDatabasesForEnsureDatabase supported, string connectionString)
    {
        YellowbrickDatabase(supported, connectionString, new ConsoleUpgradeLog());
    }

    /// <summary>
    /// Ensures that the database specified in the connection string exists using SSL for the connection.
    /// </summary>
    /// <param name="supported">Fluent helper type.</param>
    /// <param name="connectionString">The connection string.</param>
    /// <param name="certificate">Certificate for securing connection.</param>
    /// <returns></returns>
    public static void YellowbrickDatabase(this SupportedDatabasesForEnsureDatabase supported, string connectionString, X509Certificate2 certificate)
    {
        YellowbrickDatabase(supported, connectionString, new ConsoleUpgradeLog(), certificate);
    }

    /// <summary>
    /// Ensures that the database specified in the connection string exists.
    /// </summary>
    /// <param name="supported">Fluent helper type.</param>
    /// <param name="connectionString">The connection string.</param>
    /// <param name="logger">The <see cref="DbUp.Engine.Output.IUpgradeLog"/> used to record actions.</param>
    /// <returns></returns>
    public static void YellowbrickDatabase(this SupportedDatabasesForEnsureDatabase supported, string connectionString, IUpgradeLog logger)
    {
        YellowbrickDatabase(supported, connectionString, logger, null);
    }

    private static void YellowbrickDatabase(this SupportedDatabasesForEnsureDatabase supported, string connectionString, IUpgradeLog logger, X509Certificate2 certificate)
    {
        if (supported == null) throw new ArgumentNullException("supported");

        if (string.IsNullOrEmpty(connectionString) || connectionString.Trim() == string.Empty)
        {
            throw new ArgumentNullException("connectionString");
        }

        if (logger == null) throw new ArgumentNullException("logger");

        var masterConnectionStringBuilder = new NpgsqlConnectionStringBuilder(connectionString);

        var databaseName = masterConnectionStringBuilder.Database;

        if (string.IsNullOrEmpty(databaseName) || databaseName.Trim() == string.Empty)
        {
            throw new InvalidOperationException("The connection string does not specify a database name.");
        }

        masterConnectionStringBuilder.Database = "yellowbrick";

        var logMasterConnectionStringBuilder = new NpgsqlConnectionStringBuilder(masterConnectionStringBuilder.ConnectionString);
        if (!string.IsNullOrEmpty(logMasterConnectionStringBuilder.Password))
        {
            logMasterConnectionStringBuilder.Password = string.Empty.PadRight(masterConnectionStringBuilder.Password.Length, '*');
        }

        logger.WriteInformation("Master ConnectionString => {0}", logMasterConnectionStringBuilder.ConnectionString);

        using (var connection = new NpgsqlConnection(masterConnectionStringBuilder.ConnectionString))
        {
            if (certificate != null)
            {
                connection.ProvideClientCertificatesCallback +=
                    certs => certs.Add(certificate);
            }
            connection.Open();

            var sqlCommandText = string.Format
                (
                    @"SELECT case WHEN oid IS NOT NULL THEN 1 ELSE 0 end FROM pg_database WHERE datname = '{0}' limit 1;",
                    databaseName
                );


            // check to see if the database already exists..
            using (var command = new NpgsqlCommand(sqlCommandText, connection)
            {
                CommandType = CommandType.Text
            })
            {
                var results = (int?)command.ExecuteScalar();

                // if the database exists, we're done here...
                if (results.HasValue && results.Value == 1)
                {
                    return;
                }
            }

            sqlCommandText = string.Format
                (
                    "create database \"{0}\";",
                    databaseName
                );

            // Create the database...
            using (var command = new NpgsqlCommand(sqlCommandText, connection)
            {
                CommandType = CommandType.Text
            })
            {
                command.ExecuteNonQuery();

            }

            logger.WriteInformation(@"Created database {0}", databaseName);
        }
    }

    /// <summary>
    /// Tracks the list of executed scripts in a SQL Server table.
    /// </summary>
    /// <param name="builder">The builder.</param>
    /// <param name="schema">The schema.</param>
    /// <param name="table">The table.</param>
    /// <returns></returns>
    public static UpgradeEngineBuilder JournalToYellowbrickSimpleTable(this UpgradeEngineBuilder builder, string schema, string table)
    {
        builder.Configure(c => c.Journal = new YellowbrickSimpleTableJournal(() => c.ConnectionManager, () => c.Log, schema, table));
        return builder;
    }

    /// <summary>
    /// Tracks the list of executed scripts in a SQL Server table supporting change tracking.
    /// </summary>
    /// <param name="builder">The builder.</param>
    /// <param name="schema">The schema.</param>
    /// <param name="table">The table.</param>
    /// <returns></returns>
    public static UpgradeEngineBuilder JournalToYellowbrickTable(this UpgradeEngineBuilder builder, string schema, string table)
    {
        builder.Configure(c => c.Journal = new YellowbrickTableJournal(() => c.ConnectionManager, () => c.Log, schema, table));
        return builder;
    }
}
