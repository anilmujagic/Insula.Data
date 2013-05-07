using Insula.Data.Orm;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Text;

namespace Insula.Data.Tests.Orm
{
    static class TestHelper
    {
        internal static DatabaseContext GetDatabase()
        {
            return new DatabaseContext(DatabaseEngine.SqlServer, ConfigurationManager.ConnectionStrings["DefaultConnectionString"].ConnectionString);
        }
    }
}
