using System.Collections.Generic;
using System.Data;
using System.Linq;

using Vit.Linq;

using Vitorm.ClickHouse;
using Vitorm.Sql;

namespace Vitorm
{
    public static class DbContext_Extensions_UseClickHouse
    {
        public static SqlDbContext UseClickHouse(this SqlDbContext dbContext, string connectionString, int? commandTimeout = null)
                => UseClickHouse(dbContext, new DbConfig(connectionString: connectionString, commandTimeout: commandTimeout));

        public static SqlDbContext UseClickHouse(this SqlDbContext dbContext, DbConfig config)
        {
            dbContext.Init(
                sqlTranslateService: Vitorm.ClickHouse.SqlTranslateService.Instance,
                dbConnectionProvider: config.ToDbConnectionProvider(),
                sqlExecutor: SqlExecutorWithoutNull.Instance
                );

            if (config.commandTimeout.HasValue) dbContext.commandTimeout = config.commandTimeout.Value;

            return dbContext;
        }




        class SqlExecutorWithoutNull : SqlExecutor
        {
            public readonly static new SqlExecutorWithoutNull Instance = new();

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
