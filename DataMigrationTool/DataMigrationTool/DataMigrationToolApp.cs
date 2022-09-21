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
            [Option("s")] string schemaName,
            [Option("t")] string tableName,
            [Option("r")] int rowCount)
        {
            var database = new Database();

            // DB接続
            using var sourceConnection = database.OpenSourceConnection("source");
            using var destinationConnection = database.OpenSourceConnection("destination");

            // TableClass作成
            var table = database.GetTable(sourceConnection, schemaName, tableName);

            // 最大PK取得
            var maxPrimaryKey = database.GetMaxPrimaryKey(sourceConnection, table);

            // 転送対象件数取得
            var transferCount = database.GetTransferCount(sourceConnection, table, database.GetMaxPrimaryKey(destinationConnection, table));

            // 転送済件数
            var transferredCount = 0;

            Console.WriteLine($"テーブル名「{table.TableName}」の移行を開始します...");
            Console.WriteLine($"転送件数：{transferCount:###,###,###}");

            Stopwatch stopwatch = new();
            stopwatch.Start();
            var beforeElapsed = TimeSpan.Zero;

            // Ctrl+Cが押されたら中断する
            while (!IsCancelled)
            {
                var destinationMaxPrimaryKey = database.GetMaxPrimaryKey(destinationConnection, table);
                // 送信元と送信先の最大PKが一致したら転送完了
                if (maxPrimaryKey == destinationMaxPrimaryKey) break;

                using var reader = database.OpenSourceReader(sourceConnection, table, destinationMaxPrimaryKey, rowCount);
                using var bulkCopy = new SqlBulkCopy(ConfigurationManager.ConnectionStrings["destination"].ConnectionString, SqlBulkCopyOptions.KeepIdentity);
                bulkCopy.DestinationTableName = $"{table.SchemaName}.{table.TableName}";
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
    }
}
