using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DataMigrationTool
{
    internal interface IDataBase
    {
        public  SqlConnection OpenSourceConnection(string source);

        public Table GetTable(IDbConnection connection, string schemaName, string tableName);

        public string GetPrimaryKeyName(IDbConnection connection, string tableName);

        public int GetMaxPrimaryKey(IDbConnection connection, Table table);

        public int GetTransferCount(IDbConnection connection, Table table, int maxPrimaryKey);

        public SqlDataReader OpenSourceReader(SqlConnection connection, Table table, int maxPrimnaryKey, int rowCount);
    }
}
