using System;
using System.Collections.Generic;
using System.Data;
using DbUp.Engine.Output;

namespace DbUp.Engine.Transactions
{
    class TransactionPerScriptStrategy : ITransactionStrategy
    {
        IDbConnection connection;

        public void ExecuteWithConnection(Action<Func<IDbConnection>, Func<IDbTransaction>> action)
        {
            using (var transaction = connection.BeginTransaction())
            {
                action(() => connection, () => transaction);
                transaction.Commit();
            }
        }

        public T ExecuteWithConnection<T>(Func<Func<IDbConnection>, Func<IDbTransaction>, T> actionWithResult)
        {
            using (var transaction = connection.BeginTransaction())
            {
                var result = actionWithResult(() => connection, () => transaction);
                transaction.Commit();
                return result;
            }
        }

        public void Execute(Action<Func<IDbCommand>> action)
        {
            using (var transaction = connection.BeginTransaction())
            {
                action(() =>
                {
                    var command = connection.CreateCommand();
                    command.Transaction = transaction;
                    return command;
                });
                transaction.Commit();
            }
        }

        public T Execute<T>(Func<Func<IDbCommand>, T> actionWithResult)
        {
            using (var transaction = connection.BeginTransaction())
            {
                var result = actionWithResult(() =>
                {
                    var command = connection.CreateCommand();
                    command.Transaction = transaction;
                    return command;
                });
                transaction.Commit();
                return result;
            }
        }

        public void Initialise(IDbConnection dbConnection, IUpgradeLog upgradeLog, List<PreparedSqlScript> executedScripts)
        {
            connection = dbConnection ?? throw new ArgumentNullException(nameof(dbConnection));
        }

        public void Dispose() { }
    }
}
