using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DataMigrationTool
{
    public class Table
    {
        /// <summary>
        /// スキーマ名
        /// </summary>
        public string SchemaName { get; }

        /// <summary>
        /// テーブル名
        /// </summary>
        public string TableName { get; }

        /// <summary>
        /// PK
        /// </summary>
        public string? PrimaryKeyName { get; set; }

        /// <summary>
        /// コンストラクタ
        /// </summary>
        /// <param name="schemaName"></param>
        /// <param name="tableName"></param>
        public Table(string schemaName, string tableName)
        {
            SchemaName = schemaName;
            TableName = tableName;
        }

    }
}
