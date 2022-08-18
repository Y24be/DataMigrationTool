using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DataMigrationTool
{
    public class DataMigrationToolApp : ConsoleAppBase
    {
        public static bool IsCancelled { get; set; }

        public void Copy()
        {
            using var sourceConnection = OpenConnection.OpenSourceConnection("source");
            using var destinationConnection = OpenConnection.OpenSourceConnection("destination");
        }


    }
}
