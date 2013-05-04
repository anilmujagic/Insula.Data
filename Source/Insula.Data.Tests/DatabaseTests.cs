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
            using (var db = TestHelper.GetDatabase())
            {
                var id1 = "TEST-1";
                var id2 = "TEST-2";
                var name = Guid.NewGuid().ToString();
                var name1 = name + "_1";
                var name2 = name + "_2";
                var name3 = name + "_3";

                var insertedCount1 = db.ExecuteNonQuery(@"INSERT INTO Customer (CustomerID, Name) VALUES (@0, @1)", id1, name1);
                var insertedCount2 = db.ExecuteNonQuery(@"INSERT INTO Customer (CustomerID, Name) VALUES (@0, @1)", id2, name2);
                var updatedCount = db.ExecuteNonQuery(@"UPDATE Customer SET Name = @0 WHERE Name = @1", name3, name1);
                var deletedCount = db.ExecuteNonQuery(@"DELETE Customer WHERE Name IN (@0, @1)", name2, name3);

                Assert.Equal(1, insertedCount1);
                Assert.Equal(1, insertedCount2);
                Assert.Equal(1, updatedCount);
                Assert.Equal(2, deletedCount);
            }
        }

        [Fact]
        public void ExecuteScalar_ReturnsExpectedValue()
        {
            using (var db = TestHelper.GetDatabase())
            {
                var id1 = "TEST-1";
                var id2 = "TEST-2";
                var id3 = "TEST-3";
                var name = Guid.NewGuid().ToString();
                var name1 = name + "_1";
                var name2 = name + "_2";
                var name3 = name + "_3";

                db.ExecuteNonQuery(@"INSERT INTO Customer (CustomerID, Name) VALUES (@0, @1)", id1, name1);
                db.ExecuteNonQuery(@"INSERT INTO Customer (CustomerID, Name) VALUES (@0, @1)", id2, name2);
                db.ExecuteNonQuery(@"INSERT INTO Customer (CustomerID, Name) VALUES (@0, @1)", id3, name3);

                var count = (int)db.ExecuteScalar("SELECT COUNT(*) FROM Customer WHERE Name LIKE @0", name + "%");
                var lastInsertedName = (string)db.ExecuteScalar(
                    "SELECT TOP 1 Name FROM Customer WHERE Name LIKE @0 ORDER BY CustomerID DESC",
                    name + "%");

                db.ExecuteNonQuery(@"DELETE Customer WHERE Name LIKE @0", name + "%");

                Assert.Equal(3, count);
                Assert.Equal(name3, lastInsertedName);
            }
        }
    }
}
