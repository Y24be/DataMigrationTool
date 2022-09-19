using Dapper;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DataMigrationTool
{
    internal class DataBase : IDataBase
    {
        /// <summary>
        /// コネクション生成メソッド
        /// </summary>
        /// <param name="source"></param>
        /// <returns></returns>
        public SqlConnection OpenSourceConnection(string source)
        {
            var connectionString = ConfigurationManager.ConnectionStrings[source].ConnectionString;
            var connection = new SqlConnection(connectionString);

            try
            {
                connection.Open();
                return connection;
            }
            catch (Exception)
            {
                Console.WriteLine("DB接続に失敗しました");
                throw;
            }
        }

        /// <summary>
        /// テーブルデータ取得メソッド
        /// </summary>
        /// <param name="schemaName"></param>
        /// <param name="tableName"></param>
        /// <returns></returns>
        public Table GetTable(IDbConnection connection, string schemaName, string tableName)
        {
            string primaryKeyName = GetPrimaryKeyName(connection, tableName);
            return new Table(schemaName, tableName, primaryKeyName);
        }

        /// <summary>
        /// PK取得メソッド
        /// </summary>
        /// <param name="connection"></param>
        /// <param name="tableName"></param>
        /// <returns></returns>
        public string GetPrimaryKeyName(IDbConnection connection, string tableName)
        {
            var query = $@"
select top 1
    cols.name as ColumnName
from
    sys.tables as tbls
    inner join sys.key_constraints as key_const 
		on tbls.object_id = key_const.parent_object_id 
		and key_const.type = 'PK'
        and tbls.name = '{tableName}'
    inner join sys.index_columns as idx_cols 
		on key_const.parent_object_id = idx_cols.object_id
        and key_const.unique_index_id  = idx_cols.index_id
    inner join sys.columns as cols 
		on idx_cols.object_id = cols.object_id
        and idx_cols.column_id = cols.column_id
order by
	cols.column_id
";
            return connection.ExecuteScalar<string>(query);
        }

        /// <summary>
        /// PKの最大値取得メソッド
        /// </summary>
        /// <param name="connection"></param>
        /// <param name="table"></param>
        /// <returns></returns>
        public int GetMaxPrimaryKey(IDbConnection connection, Table table)
        {
            var query = $@"
select
    max({table.PrimaryKeyName})
from
    {table.SchemaName}.{table.TableName}
";
            return connection.ExecuteScalar<int>(query);
        }

        /// <summary>
        /// 転送対象件数取得メソッド
        /// </summary>
        /// <param name="connection"></param>
        /// <param name="maxPrimaryKey"></param>
        /// <returns></returns>
        public int GetTransferCount(IDbConnection connection, Table table, int maxPrimaryKey)
        {
            var query = $@"
select
    count(1)
from
    {table.SchemaName}.{table.TableName}
where
    {maxPrimaryKey} < {table.PrimaryKeyName}
";
            return connection.ExecuteScalar<int>(query);
        }

        /// <summary>
        /// 転送対象取得
        /// </summary>
        /// <param name="connection"></param>
        /// <param name="table"></param>
        /// <param name="maxPrimnaryKey"></param>
        /// <param name="rowCount"></param>
        /// <returns></returns>
        public SqlDataReader OpenSourceReader(SqlConnection connection, Table table, int maxPrimnaryKey, int rowCount)
        {
            using var sourceCommand = new SqlCommand();
            sourceCommand.Connection = connection;
            sourceCommand.CommandText = $@"
select top ({rowCount})
    *
from
    {table.SchemaName}.{table.TableName} with(nolock)
where
    {maxPrimnaryKey} < {table.PrimaryKeyName}
order by
    {table.PrimaryKeyName}
";
            sourceCommand.CommandTimeout = 0;

            return sourceCommand.ExecuteReader();
        }
    }
}
