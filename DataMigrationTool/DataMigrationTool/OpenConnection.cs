using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DataMigrationTool
{
    public static class OpenConnection
    {
        public static SqlConnection OpenSourceConnection(string source)
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
    }
}
