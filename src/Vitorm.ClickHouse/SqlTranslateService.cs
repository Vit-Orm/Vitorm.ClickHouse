using System;
using System.Collections.Generic;
using System.Linq.Expressions;

using Vit.Extensions.Linq_Extensions;
using Vit.Linq.ExpressionTree.ComponentModel;
using Vitorm.Entity;
using Vitorm.Sql;
using Vitorm.Sql.SqlTranslate;
using Vitorm.ClickHouse.TranslateService;
using Vitorm.StreamQuery;
using System.Text;

namespace Vitorm.ClickHouse
{
    public class SqlTranslateService : Vitorm.Sql.SqlTranslate.SqlTranslateService
    {
        public static readonly SqlTranslateService Instance = new SqlTranslateService();

        protected QueryTranslateService queryTranslateService;

        protected ExecuteDeleteTranslateService executeDeleteTranslateService;

        public SqlTranslateService()
        {
            queryTranslateService = new QueryTranslateService(this);

            executeDeleteTranslateService = new ExecuteDeleteTranslateService(this);
        }
        /// <summary>
        ///     Generates the delimited SQL representation of an identifier (column name, table name, etc.).
        /// </summary>
        /// <param name="identifier">The identifier to delimit.</param>
        /// <returns>
        ///     The generated string.
        /// </returns>
        public override string DelimitIdentifier(string identifier) => $"`{EscapeIdentifier(identifier)}`"; // Interpolation okay; strings

        /// <summary>
        ///     Generates the escaped SQL representation of an identifier (column name, table name, etc.).
        /// </summary>
        /// <param name="identifier">The identifier to be escaped.</param>
        /// <returns>
        ///     The generated string.
        /// </returns>
        public override string EscapeIdentifier(string identifier) => identifier.Replace("`", "\\`");

        public override string DelimitTableName(IEntityDescriptor entityDescriptor)
        {
            if (entityDescriptor.schema == null) return DelimitIdentifier(entityDescriptor.tableName);

            return $"{DelimitIdentifier(entityDescriptor.schema)}.{DelimitIdentifier(entityDescriptor.tableName)}";
        }



        #region EvalExpression
        /// <summary>
        /// read where or value or on
        /// </summary>
        /// <param name="arg"></param>
        /// <returns></returns>
        /// <exception cref="NotSupportedException"></exception>
        /// <param name="data"></param>
        public override string EvalExpression(QueryTranslateArgument arg, ExpressionNode data)
        {
            switch (data.nodeType)
            {
                case NodeType.MethodCall:
                    {
                        ExpressionNode_MethodCall methodCall = data;
                        switch (methodCall.methodName)
                        {
                            // ##1 ToString
                            case nameof(object.ToString):
                                {
                                    return $"cast({EvalExpression(arg, methodCall.@object)} as char)";
                                }

                            #region ##2 String method:  StartsWith EndsWith Contains
                            case nameof(string.StartsWith): // String.StartsWith
                                {
                                    var str = methodCall.@object;
                                    var value = methodCall.arguments[0];
                                    return $"{EvalExpression(arg, str)} like concat({EvalExpression(arg, value)},'%')";
                                }
                            case nameof(string.EndsWith): // String.EndsWith
                                {
                                    var str = methodCall.@object;
                                    var value = methodCall.arguments[0];
                                    return $"{EvalExpression(arg, str)} like concat('%',{EvalExpression(arg, value)})";
                                }
                            case nameof(string.Contains) when methodCall.methodCall_typeName == "String": // String.Contains
                                {
                                    var str = methodCall.@object;
                                    var value = methodCall.arguments[0];
                                    return $"{EvalExpression(arg, str)} like concat('%',{EvalExpression(arg, value)},'%')";
                                }
                                #endregion
                        }
                        break;
                    }

                #region Read Value
                case NodeType.Convert:
                    {
                        // cast( 4.1 as signed)

                        ExpressionNode_Convert convert = data;

                        Type targetType = convert.valueType?.ToType();

                        if (targetType == typeof(object)) return EvalExpression(arg, convert.body);

                        // Nullable
                        if (targetType.IsGenericType) targetType = targetType.GetGenericArguments()[0];

                        string targetDbType = GetColumnDbType(targetType);

                        var sourceType = convert.body.Member_GetType();
                        if (sourceType != null)
                        {
                            if (sourceType.IsGenericType) sourceType = sourceType.GetGenericArguments()[0];

                            if (targetDbType == GetColumnDbType(sourceType)) return EvalExpression(arg, convert.body);
                        }

                        if (targetType == typeof(string))
                        {
                            return $"cast({EvalExpression(arg, convert.body)} as String)";
                        }

                        return $"cast({EvalExpression(arg, convert.body)} as {targetDbType})";
                    }
                case nameof(ExpressionType.Add):
                    {
                        ExpressionNode_Binary binary = data;

                        // ##1 String Add
                        if (data.valueType?.ToType() == typeof(string))
                        {
                            // cast(ifNull('null','') as String)
                            //return $"CONCAT({EvalExpression(arg, binary.left)} ,{EvalExpression(arg, binary.right)})";
                            return $"CONCAT(cast(ifNull({EvalExpression(arg, binary.left)},'') as String) ,cast(ifNull({EvalExpression(arg, binary.right)},'') as String) )";
                        }

                        // ##2 Numberic Add
                        return $"{EvalExpression(arg, binary.left)} + {EvalExpression(arg, binary.right)}";
                    }
                case nameof(ExpressionType.Coalesce):
                    {
                        ExpressionNode_Binary binary = data;
                        return $"COALESCE({EvalExpression(arg, binary.left)},{EvalExpression(arg, binary.right)})";
                    }
                case nameof(ExpressionType.Conditional):
                    {
                        // IF(`t0`.`fatherId` is not null,true, false)
                        ExpressionNode_Conditional conditional = data;
                        return $"IF({EvalExpression(arg, conditional.Conditional_GetTest())},{EvalExpression(arg, conditional.Conditional_GetIfTrue())},{EvalExpression(arg, conditional.Conditional_GetIfFalse())})";
                    }
                    #endregion

            }

            return base.EvalExpression(arg, data);
        }
        #endregion



        #region PrepareCreate
        public override string PrepareCreate(IEntityDescriptor entityDescriptor)
        {
            /* //sql
CREATE TABLE IF NOT EXISTS `User`
(
    `id` Int32,
    `name` Nullable(String),
    `birth` Nullable(DateTime),
    `fatherId` Nullable(Int32),
    `motherId` Nullable(Int32)
)
ENGINE = MergeTree
ORDER BY `id`;
              */
            List<string> sqlFields = new();

            // #1 columns
            entityDescriptor.allColumns?.ForEach(column => sqlFields.Add(GetColumnSql(column)));

            return $@"
CREATE TABLE IF NOT EXISTS {DelimitTableName(entityDescriptor)} (
{string.Join(",\r\n  ", sqlFields)}
)
ENGINE = MergeTree
ORDER BY  {DelimitIdentifier(entityDescriptor.key.columnName)};";

            string GetColumnSql(IColumnDescriptor column)
            {
                var columnDbType = column.databaseType ?? GetColumnDbType(column.type);
                return $"  {DelimitIdentifier(column.columnName)} {(column.isNullable ? $"Nullable({columnDbType})" : columnDbType)}";
            }
        }

        // https://clickhouse.com/docs/en/sql-reference/data-types
        protected readonly static Dictionary<Type, string> columnDbTypeMap = new()
        {
            [typeof(DateTime)] = "DateTime",
            [typeof(string)] = "String",

            [typeof(float)] = "Float32",
            [typeof(double)] = "Float64",
            [typeof(decimal)] = "Decimal32",

            [typeof(Int64)] = "Int64",
            [typeof(Int32)] = "Int32",
            [typeof(Int16)] = "Int16",

            [typeof(UInt64)] = "UInt64",
            [typeof(UInt32)] = "UInt32",
            [typeof(UInt16)] = "UInt16",
            [typeof(byte)] = "UInt8",

            [typeof(bool)] = "UInt8",
        };
        protected override string GetColumnDbType(Type type)
        {
            type = TypeUtil.GetUnderlyingType(type);

            if (columnDbTypeMap.TryGetValue(type, out var dbType)) return dbType;

            //if (type.Name.ToLower().Contains("int")) return "INTEGER";

            throw new NotSupportedException("unsupported column type:" + type.Name);
        }

        #endregion


        public override (string sql, Dictionary<string, object> sqlParam, IDbDataReader dataReader) PrepareQuery(QueryTranslateArgument arg, CombinedStream combinedStream)
        {
            string sql = queryTranslateService.BuildQuery(arg, combinedStream);
            return (sql, arg.sqlParam, arg.dataReader);
        }

        public override (string sql, Dictionary<string, object> sqlParam) PrepareExecuteUpdate(QueryTranslateArgument arg, CombinedStream combinedStream) => throw new NotImplementedException();


        public override string PrepareDelete(SqlTranslateArgument arg)
        {
            // ALTER TABLE `User` DELETE WHERE `id`=2;
            var entityDescriptor = arg.entityDescriptor;

            // #2 build sql
            string sql = $@"ALTER TABLE {DelimitTableName(entityDescriptor)} DELETE where {DelimitIdentifier(entityDescriptor.keyName)}={GenerateParameterName(entityDescriptor.keyName)} ; ";

            return sql;
        }

        public override (string sql, Dictionary<string, object> sqlParam) PrepareDeleteByKeys<Key>(SqlTranslateArgument arg, IEnumerable<Key> keys)
        {
            //  ALTER TABLE `User` DELETE WHERE  id in ( 7 ) ;

            var entityDescriptor = arg.entityDescriptor;

            StringBuilder sql = new StringBuilder();
            Dictionary<string, object> sqlParam = new();

            sql.Append("ALTER TABLE ").Append(DelimitTableName(entityDescriptor)).Append(" DELETE where ").Append(DelimitIdentifier(entityDescriptor.keyName)).Append(" in (");

            int keyIndex = 0;
            foreach (var key in keys)
            {
                var paramName = "p" + (keyIndex++);
                sql.Append(GenerateParameterName(paramName)).Append(",");
                sqlParam[paramName] = key;
            }
            if (keyIndex == 0) sql.Append("null);");
            else
            {
                sql.Length--;
                sql.Append(");");
            }
            return (sql.ToString(), sqlParam);
        }

        public override (string sql, Dictionary<string, object> sqlParam) PrepareExecuteDelete(QueryTranslateArgument arg, CombinedStream combinedStream)
        {
            string sql = executeDeleteTranslateService.BuildQuery(arg, combinedStream);
            return (sql, arg.sqlParam);
        }



    }
}
