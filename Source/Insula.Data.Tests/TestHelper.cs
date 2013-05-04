using Insula.Data.Orm;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Text;

namespace Insula.Data.Tests
{
    static class TestHelper
    {
        internal static Database GetDatabase()
        {
            return new Database(DatabaseEngine.SqlServer, ConfigurationManager.ConnectionStrings["DefaultConnectionString"].ConnectionString);
        }
    }
}
