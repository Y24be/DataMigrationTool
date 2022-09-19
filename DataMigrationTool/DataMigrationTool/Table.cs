using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DataMigrationTool
{
    public record Table
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
        public string PrimaryKeyName { get; }

        /// <summary>
        /// コンストラクタ
        /// </summary>
        /// <param name="schemaName"></param>
        /// <param name="tableName"></param>
        public Table(string schemaName, string tableName, string primaryKeyName)
        {
            SchemaName = schemaName;
            TableName = tableName;
            PrimaryKeyName = primaryKeyName;
        }

    }
}
