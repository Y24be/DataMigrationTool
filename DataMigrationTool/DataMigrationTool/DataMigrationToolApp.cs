using Dapper;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DataMigrationTool
{
    public class DataMigrationToolApp : ConsoleAppBase
    {
        public static bool IsCancelled { get; set; }

        public void Copy(
            [Option("t")] string tableName,
            [Option("r")] int rowCount)
        {
            // DB接続
            using var sourceConnection = OpenConnection.OpenSourceConnection("source");
            using var destinationConnection = OpenConnection.OpenSourceConnection("destination");

            // PK名取得
            var primaryKeyName = GetPrimaryKeyName(sourceConnection, tableName);

            // 最大PK取得
            var maxPrimaryKey = GetMaxPrimaryKey(sourceConnection, tableName, primaryKeyName);

            // 転送対象件数取得
            var transferCount = GetTransferCount(sourceConnection, tableName, primaryKeyName, GetMaxPrimaryKey(destinationConnection, tableName, primaryKeyName));

            // 転送済件数
            var transferredCount = 0;

            Console.WriteLine($"テーブル名「{tableName}」の移行を開始します...");
            Console.WriteLine($"転送件数：{transferCount:###,###,###}");

            Stopwatch stopwatch = new();
            stopwatch.Start();
            var beforeElapsed = TimeSpan.Zero;

            // Ctrl+Cが押されたら中断する
            while (!IsCancelled)
            {
                var destinationMaxPrimaryKey = GetMaxPrimaryKey(destinationConnection, tableName, primaryKeyName);
                // 送信元と送信先の最大PKが一致したら転送完了
                if (maxPrimaryKey == destinationMaxPrimaryKey) break;

                using var reader = OpenSourceReader(rowCount, sourceConnection, tableName, primaryKeyName, destinationMaxPrimaryKey);
                using var bulkCopy = new SqlBulkCopy(ConfigurationManager.ConnectionStrings["destination"].ConnectionString, SqlBulkCopyOptions.KeepIdentity);
                bulkCopy.DestinationTableName = tableName;
                bulkCopy.BulkCopyTimeout = 0;
                bulkCopy.WriteToServer(reader);

                transferredCount += rowCount;
                if (transferCount < transferredCount)
                {
                    transferredCount = transferCount;
                }

                var totalElapsed = stopwatch.Elapsed;
                var currentElapsed = totalElapsed - beforeElapsed;
                var remainingTime = (double)(transferCount - transferredCount) / transferredCount * totalElapsed;
                Console.WriteLine($"{transferredCount}/{transferCount} 経過時間：{totalElapsed:hh\\:mm\\:ss\\.ff} カレント：{currentElapsed:hh\\:mm\\:ss\\.ff} 残予測：{remainingTime:hh\\:mm\\:ss\\.ff}");

                beforeElapsed = totalElapsed;
            }

            Console.WriteLine(
            IsCancelled
                ? "転送を中断しました。"
                : "転送を完了しました。");
        }

        /// <summary>
        /// PK取得メソッド
        /// </summary>
        /// <param name="connection"></param>
        /// <param name="tableName"></param>
        /// <returns></returns>
        private string GetPrimaryKeyName(IDbConnection connection, string tableName)
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
        /// <param name="tableName"></param>
        /// <param name="primaryKeyName"></param>
        /// <returns></returns>
        private int GetMaxPrimaryKey(IDbConnection connection, string tableName, string primaryKeyName)
        {
            var query = $@"
select
    max({primaryKeyName})
from
    {tableName}
";
            return connection.ExecuteScalar<int>(query);
        }

        /// <summary>
        /// 転送対象件数取得メソッド
        /// </summary>
        /// <param name="connection"></param>
        /// <param name="tableName"></param>
        /// <param name="primaryKeyName"></param>
        /// <param name="maxPrimaryKey"></param>
        /// <returns></returns>
        private int GetTransferCount(IDbConnection connection, string tableName, string primaryKeyName, int maxPrimaryKey)
        {
            var query = $@"
select
    count(1)
from
    {tableName}
where
    {maxPrimaryKey} < {primaryKeyName}
";
            return connection.ExecuteScalar<int>(query);
        }

        /// <summary>
        /// 転送対象取得
        /// </summary>
        /// <param name="rowCount"></param>
        /// <param name="connection"></param>
        /// <param name="tableName"></param>
        /// <param name="primaryKeyName"></param>
        /// <param name="maxPrimnaryKey"></param>
        /// <returns></returns>
        private static SqlDataReader OpenSourceReader(int rowCount, SqlConnection connection, string tableName, string primaryKeyName, int maxPrimnaryKey)
        {
            using var sourceCommand = new SqlCommand();
            sourceCommand.Connection = connection;
            sourceCommand.CommandText = $@"
select top ({rowCount})
    *
from
    {tableName} with(nolock)
where
    {maxPrimnaryKey} < {primaryKeyName}
order by
    {primaryKeyName}
";
            sourceCommand.CommandTimeout = 0;

            return sourceCommand.ExecuteReader();
        }


    }
}
