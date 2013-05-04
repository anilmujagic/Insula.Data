using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using Xunit;

namespace Insula.Data.Tests
{
    public class SqlQueryTests
    {
        public class CustomQueryTests
        {
            public class TestEntity1
            {
                public string CustomerID { get; set; }
                public string Name { get; set; }
            }

            public class TestEntity2
            {
                public int OrderID { get; set; }
                public DateTime PostingDate { get; set; }
                public string CustomerName { get; set; }
            }

            [Fact]
            public void CustomQuery_OnSingleTable_WithoutParameters()
            {
                using (var db = TestHelper.GetDatabase())
                {
                    var count = (int)db.ExecuteScalar("SELECT COUNT(*) FROM Customer WHERE CustomerID LIKE '%0'");
                    var entities = db.Query<TestEntity1>("SELECT * FROM Customer WHERE CustomerID LIKE '%0'")
                        .GetAll();
                    var nameFromFirstRecord = entities.First().Name;

                    Assert.Equal(count, entities.Count());
                    Assert.NotNull(nameFromFirstRecord);
                }
            }

            [Fact]
            public void CustomQuery_OnSingleTable_WithParameters()
            {
                using (var db = TestHelper.GetDatabase())
                {
                    var count = (int)db.ExecuteScalar("SELECT COUNT(*) FROM Customer WHERE Name LIKE @0", "%1");
                    var entities = db.Query<TestEntity1>("SELECT * FROM Customer WHERE Name LIKE @0", "%1")
                        .GetAll();
                    var firstRecord = entities.FirstOrDefault();

                    Assert.Equal(count, entities.Count());
                    Assert.NotNull(firstRecord);
                    Assert.NotNull(firstRecord.Name);
                }
            }

            [Fact]
            public void CustomQuery_OnMultipleJoinedTables_WithParameters()
            {
                using (var db = TestHelper.GetDatabase())
                {
                    var count = (int)db.ExecuteScalar(@"
                        SELECT COUNT(*)
                        FROM [Order] AS o
                        JOIN [Customer] AS c
                            ON c.CustomerID = o.CustomerID
                        WHERE c.CustomerID LIKE @0
                        ", "%2");
                    var entities = db.Query<TestEntity2>(@"
                        SELECT 
                            o.OrderID,
                            o.PostingDate,
                            c.Name AS CustomerName
                        FROM [Order] AS o
                        JOIN [Customer] AS c
                            ON c.CustomerID = o.CustomerID
                        WHERE c.CustomerID LIKE @0
                        ", "%2")
                        .GetAll();
                    var firstResult = entities.FirstOrDefault();

                    Assert.Equal(count, entities.Count());
                    Assert.NotNull(firstResult);
                    Assert.NotEqual(0, firstResult.OrderID);
                    Assert.NotEqual(DateTime.MinValue, firstResult.PostingDate);
                    Assert.NotNull(firstResult.CustomerName);
                }
            }

            [Fact]
            public void CustomQuery_WhenCallingBuilderMethods_ThrowsException()
            {
                using (var db = TestHelper.GetDatabase())
                {
                    var query = db.Query<TestEntity1>("SELECT * FROM Customer");

                    Assert.Throws(typeof(InvalidOperationException), () =>
                    {
                        query.Include("Dummy");
                    });
                    Assert.Throws(typeof(InvalidOperationException), () =>
                    {
                        query.Where("Dummy");
                    });
                    Assert.Throws(typeof(InvalidOperationException), () =>
                    {
                        query.Where(new { AuthorID = 1 });
                    });
                    Assert.Throws(typeof(InvalidOperationException), () =>
                    {
                        query.OrderBy("Dummy");
                    });
                }
            }
        }
    }
}
