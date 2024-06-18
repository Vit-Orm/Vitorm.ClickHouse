using Vitorm.Sql.SqlTranslate;
using Vitorm.StreamQuery;

namespace Vitorm.ClickHouse.TranslateService
{
    public class ExecuteDeleteTranslateService : BaseQueryTranslateService
    {
        /*
ALTER TABLE `User` DELETE 
where id in (
    select u.id 
    from `User` u
    left join `User` father on u.fatherId = father.id 
    where u.id > 10
);
         */
        public override string BuildQuery(QueryTranslateArgument arg, CombinedStream stream)
        {
            var entityDescriptor = arg.dbContext.GetEntityDescriptor(arg.resultEntityType);

            var sqlInner = base.BuildQuery(arg, stream);

            var NewLine = "\r\n";
            var keyName = entityDescriptor.keyName;

            var sql = $"ALTER TABLE {sqlTranslator.DelimitTableName(entityDescriptor)} DELETE";
            sql += $"{NewLine}where {sqlTranslator.DelimitIdentifier(keyName)} in ({sqlInner})";

            return sql;
        }


        public ExecuteDeleteTranslateService(SqlTranslateService sqlTranslator) : base(sqlTranslator)
        {
        }

        protected override string ReadSelect(QueryTranslateArgument arg, CombinedStream stream, string prefix = "select")
        {
            var entityDescriptor = arg.dbContext.GetEntityDescriptor(arg.resultEntityType);

            // primary key
            return $"{prefix} {sqlTranslator.GetSqlField(stream.source.alias, entityDescriptor.keyName)} as {sqlTranslator.DelimitIdentifier(entityDescriptor.keyName)}";
        }



    }
}
