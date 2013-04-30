using Insula.Common;
using Insula.Data.Orm;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Text;
using Xunit;

namespace Insula.Data.Tests
{
    public class DatabaseTests
    {
        [Fact]
        public void ExecuteNonQuery_ReturnsNumberOfAffectedRecords()
        {
            using (var db = GetDatabase())
            {
                var name = Guid.NewGuid().ToString();
                var name1 = name + "_1";
                var name2 = name + "_2";
                var name3 = name + "_3";

                var insertedCount1 = db.ExecuteNonQuery(@"INSERT INTO Author (Name) VALUES (@0)", name1);
                var insertedCount2 = db.ExecuteNonQuery(@"INSERT INTO Author (Name) VALUES (@0)", name2);
                var updatedCount = db.ExecuteNonQuery(@"UPDATE Author SET Name = @0 WHERE Name = @1", name3, name1);
                var deletedCount = db.ExecuteNonQuery(@"DELETE Author WHERE Name IN (@0, @1)", name2, name3);

                Assert.True(insertedCount1 == 1);
                Assert.True(insertedCount2 == 1);
                Assert.True(updatedCount == 1);
                Assert.True(deletedCount == 2);
            }
        }

        [Fact]
        public void ExecuteScalar_ReturnsExpectedValue()
        {
            using (var db = GetDatabase())
            {
                var name = Guid.NewGuid().ToString();
                var name1 = name + "_1";
                var name2 = name + "_2";
                var name3 = name + "_3";

                db.ExecuteNonQuery(@"INSERT INTO Author (Name) VALUES (@0)", name1);
                db.ExecuteNonQuery(@"INSERT INTO Author (Name) VALUES (@0)", name2);
                db.ExecuteNonQuery(@"INSERT INTO Author (Name) VALUES (@0)", name3);

                var count = (int)db.ExecuteScalar("SELECT COUNT(1) FROM Author WHERE Name LIKE @0", name + "%");
                var lastInsertedName = (string)db.ExecuteScalar(
                    "SELECT TOP 1 Name FROM Author WHERE Name LIKE @0 ORDER BY AuthorID DESC",
                    name + "%");

                db.ExecuteNonQuery(@"DELETE Author WHERE Name LIKE @0", name + "%");

                Assert.True(count == 3);
                Assert.True(lastInsertedName == name3);
            }
        }

        #region Helpers

        private Database GetDatabase()
        {
            return new Database(ConfigurationManager.ConnectionStrings["MyAppDB"].ConnectionString);
        }

        #endregion
    }
}
