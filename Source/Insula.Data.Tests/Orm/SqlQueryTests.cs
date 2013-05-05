using Insula.Common;
using Insula.Data.Tests.Models;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using Xunit;

namespace Insula.Data.Tests.Orm
{
    public class SqlQueryTests
    {
        public class CustomQuery
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

        public class Where
        {
            [Fact]
            public void Where_OnObjectWithoutProperties_DoesNotCreateWhereClause()
            {
                using (var db = TestHelper.GetDatabase())
                {
                    var query = db.Query<Customer>()
                        .Where(new { })
                        .ToString();

                    Assert.DoesNotContain("WHERE", query);
                }
            }

            [Fact]
            public void Where_OnAnonymousObject_CreatesWhereClauseForEachProperty()
            {
                using (var db = TestHelper.GetDatabase())
                {
                    var query = db.Query<Customer>()
                        .Where(new
                        {
                            FirstName = "Anil",
                            LastName = "Mujagic",
                            Email = (string)null
                        })
                        .ToString();

                    Assert.Contains("WHERE", query);
                    Assert.Contains("[FirstName] = @0", query);
                    Assert.Contains("[LastName] = @1", query);
                    Assert.Contains("[Email] IS NULL", query);
                }
            }

            [Fact]
            public void Where_OnTypedObject_CreatesWhereClauseForEachProperty()
            {
                using (var db = TestHelper.GetDatabase())
                {
                    var query = db.Query<Customer>()
                        .Where(new Customer
                        {
                            Name = "Anil Mujagic"
                        })
                        .ToString();

                    Assert.Contains("WHERE", query);
                    Assert.Contains("[CustomerID] IS NULL", query);
                    Assert.Contains("[Name] = @0", query);
                    Assert.Contains("[Address] IS NULL", query);
                }
            }

            [Fact]
            public void Where_ReturnsCorrectResults()
            {
                using (var db = TestHelper.GetDatabase())
                {
                    var id1 = "TEST-1";
                    var id2 = "TEST-2";
                    var id3 = "TEST-3";
                    var name = Guid.NewGuid().ToString();
    
                    db.ExecuteNonQuery(@"INSERT INTO Customer (CustomerID, Name) VALUES (@0, @1)", id1, name);
                    db.ExecuteNonQuery(@"INSERT INTO Customer (CustomerID, Name) VALUES (@0, @1)", id2, name);
                    db.ExecuteNonQuery(@"INSERT INTO Customer (CustomerID, Name) VALUES (@0, @1)", id3, name);
                    var results = db.Query<Customer>()
                        .Where(new { Name = name })
                        .GetAll()
                        .ToList();  //To be able to use index
                    db.ExecuteNonQuery(@"DELETE Customer WHERE Name = @0", name);

                    Assert.False(results.IsNullOrEmpty());
                    Assert.Equal(3, results.Count());
                    Assert.Equal(id1, results[0].CustomerID);
                    Assert.Equal(id2, results[1].CustomerID);
                    Assert.Equal(id3, results[2].CustomerID);
                    for (int i = 0; i < results.Count; i++)
                    {
                        Assert.Equal(name, results[i].Name);
                    }
                }
            }
        }

        public class OrderBy
        {
            [Fact]
            public void OrderBy_IncludesAllPassedColumnsInSqlStatement()
            {
                using (var db = TestHelper.GetDatabase())
                {
                    var query = db.Query<Customer>()
                        .OrderBy("Name", "CustomerID DESC");

                    var expectedOrderByClause = "ORDER BY Name, CustomerID DESC";

                    Assert.Contains(expectedOrderByClause, query.ToString());
                }
            }
        }
    }
}
