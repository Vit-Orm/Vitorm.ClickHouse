using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;

using Vit.Extensions.Linq_Extensions;

using Vitorm.Sql;
using Vitorm.Sql.SqlTranslate;

namespace Vit.Extensions
{
    public static class DbContext_Extensions
    {
        public static SqlDbContext UseClickHouse(this SqlDbContext dbContext, string connectionString, int? commandTimeout = null)
        {
            ISqlTranslateService sqlTranslateService = Vitorm.ClickHouse.SqlTranslateService.Instance;

            //Func<IDbConnection> createDbConnection = () => new ClickHouse.Ado.ClickHouseConnection(ConnectionString);
            Func<IDbConnection> createDbConnection = () => new ClickHouse.Client.ADO.ClickHouseConnection(connectionString);


            dbContext.Init(sqlTranslateService: sqlTranslateService, createDbConnection: createDbConnection, sqlExecutor: SqlExecutorWithoutNull.Instance, dbHashCode: connectionString.GetHashCode().ToString());

            //dbContext.createTransactionScope = (dbContext) => new Vitorm.Sql.Transaction.SqlTransactionScope(dbContext);

            if (commandTimeout.HasValue) dbContext.commandTimeout = commandTimeout.Value;

            return dbContext;
        }



        class SqlExecutorWithoutNull : SqlExecutor
        {
            public readonly static SqlExecutorWithoutNull Instance = new();

            public override int Execute(IDbConnection conn, string sql, IDictionary<string, object> param = null, IDbTransaction transaction = null, int? commandTimeout = null)
            {
                sql = ReplaceNullParameters(sql, param);
                return base.Execute(conn, sql, param, transaction, commandTimeout);
            }
            public override object ExecuteScalar(IDbConnection conn, string sql, IDictionary<string, object> param = null, IDbTransaction transaction = null, int? commandTimeout = null)
            {
                sql = ReplaceNullParameters(sql, param);
                return base.ExecuteScalar(conn, sql, param, transaction, commandTimeout);
            }

            public string ReplaceNullParameters(string sql, IDictionary<string, object> param = null)
            {
                //find null value and replace parameters to "null" in sql
                param?.Where(kv => kv.Value == null).Select(kv => kv.Key).ToList().ForEach(key =>
                {
                    param.Remove(key);
                    sql = sql.Replace("@" + key, "null");
                });

                return sql;
            }

        }

    }
}
