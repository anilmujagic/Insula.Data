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
                        .ToList();  //To be able to access by index
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

        public class Include
        {
            [Fact]
            public void Include_ReturnsRelatedEntities()
            {
                using (var db = TestHelper.GetDatabase())
                {
                    var ids = new int[] { 0, 1, 2, 3 };
                    var itemIDs = (ids).Select(id => "TEST-ITEM-" + id.ToString()).ToArray();
                    var itemNames = (ids).Select(id => "TEST ITEM " + id.ToString()).ToArray();
                    var customerIDs = (ids).Select(id => "TEST-CUST-" + id.ToString()).ToArray();
                    var customerNames = (ids).Select(id => "TEST CUSTOMER " + id.ToString()).ToArray();

                    db.ExecuteNonQuery(@"DELETE Discount");
                    db.ExecuteNonQuery(@"DELETE Item WHERE ItemID LIKE @0", "TEST-ITEM-%");
                    db.ExecuteNonQuery(@"DELETE Customer WHERE CustomerID LIKE @0", "TEST-CUST-%");

                    for (int i = 0; i <= 3; i++)
                    {
                        db.ExecuteNonQuery(@"INSERT INTO Item VALUES (@0, @1, @2)", itemIDs[i], itemNames[i], i);
                        db.ExecuteNonQuery(@"INSERT INTO Customer VALUES (@0, @1, @2)", customerIDs[i], customerNames[i], "Some street");
                    }

                    db.ExecuteNonQuery(@"INSERT INTO Discount (ItemID, CustomerID, [Percent]) VALUES (@0, @1, @2)", itemIDs[0], customerIDs[0], 7);
                    db.ExecuteNonQuery(@"INSERT INTO Discount (ItemID, CustomerID, [Percent]) VALUES (@0, @1, @2)", itemIDs[0], customerIDs[1], 7.1);
                    db.ExecuteNonQuery(@"INSERT INTO Discount (ItemID, CustomerID, [Percent]) VALUES (@0, @1, @2)", itemIDs[1], customerIDs[2], 7.2);
                    db.ExecuteNonQuery(@"INSERT INTO Discount (ItemID, CustomerID, [Percent]) VALUES (@0, @1, @2)", itemIDs[2], customerIDs[2], 7.3);
                    db.ExecuteNonQuery(@"INSERT INTO Discount (CustomerID, [Percent]) VALUES (@0, @1)", customerIDs[3], 5);
                    db.ExecuteNonQuery(@"INSERT INTO Discount (CustomerID, [Percent]) VALUES (@0, @1)", customerIDs[1], 5.5);
                    db.ExecuteNonQuery(@"INSERT INTO Discount (ItemID, [Percent]) VALUES (@0, @1)", itemIDs[3], 2);
                    db.ExecuteNonQuery(@"INSERT INTO Discount (ItemID, [Percent]) VALUES (@0, @1)", itemIDs[1], 2.5);
                    db.ExecuteNonQuery(@"INSERT INTO Discount ([Percent]) VALUES (@0)", 9);

                    var query = db.Query<Discount>()
                        .Include("Item", "Customer")
                        .OrderBy("DiscountID");
                    var results = query
                        .GetAll()
                        .ToList();  //To be able to access by index

                    Assert.False(results.IsNullOrEmpty());
                    Assert.Equal(9, results.Count());

                    // Make sure properties are loaded correctly
                    Assert.NotNull(results[0].Item);
                    Assert.Equal(itemIDs[0], results[0].Item.ItemID);
                    Assert.Equal(itemNames[0], results[0].Item.Description);
                    Assert.NotNull(results[0].Customer);
                    Assert.Equal(customerIDs[0], results[0].Customer.CustomerID);
                    Assert.Equal(customerNames[0], results[0].Customer.Name);

                    Assert.NotNull(results[1].Item);
                    Assert.Equal(itemIDs[0], results[1].Item.ItemID);
                    Assert.Equal(itemNames[0], results[1].Item.Description);
                    Assert.NotNull(results[1].Customer);
                    Assert.Equal(customerIDs[1], results[1].Customer.CustomerID);
                    Assert.Equal(customerNames[1], results[1].Customer.Name);

                    Assert.NotNull(results[2].Item);
                    Assert.Equal(itemIDs[1], results[2].Item.ItemID);
                    Assert.Equal(itemNames[1], results[2].Item.Description);
                    Assert.NotNull(results[2].Customer);
                    Assert.Equal(customerIDs[2], results[2].Customer.CustomerID);
                    Assert.Equal(customerNames[2], results[2].Customer.Name);

                    Assert.NotNull(results[3].Item);
                    Assert.Equal(itemIDs[2], results[3].Item.ItemID);
                    Assert.Equal(itemNames[2], results[3].Item.Description);
                    Assert.NotNull(results[3].Customer);
                    Assert.Equal(customerIDs[2], results[3].Customer.CustomerID);
                    Assert.Equal(customerNames[2], results[3].Customer.Name);

                    Assert.Null(results[4].Item);
                    Assert.NotNull(results[4].Customer);
                    Assert.Equal(customerIDs[3], results[4].Customer.CustomerID);
                    Assert.Equal(customerNames[3], results[4].Customer.Name);

                    Assert.Null(results[5].Item);
                    Assert.NotNull(results[5].Customer);
                    Assert.Equal(customerIDs[1], results[5].Customer.CustomerID);
                    Assert.Equal(customerNames[1], results[5].Customer.Name);

                    Assert.NotNull(results[6].Item);
                    Assert.Equal(itemIDs[3], results[6].Item.ItemID);
                    Assert.Equal(itemNames[3], results[6].Item.Description);
                    Assert.Null(results[6].Customer);

                    Assert.NotNull(results[7].Item);
                    Assert.Equal(itemIDs[1], results[7].Item.ItemID);
                    Assert.Equal(itemNames[1], results[7].Item.Description);
                    Assert.Null(results[7].Customer);

                    Assert.Null(results[8].Item);
                    Assert.Null(results[8].Customer);

                    // Make sure object is not created twice
                    Assert.Same(results[0].Item, results[1].Item);
                    Assert.Same(results[2].Item, results[7].Item);
                    Assert.Same(results[2].Customer, results[3].Customer);
                    Assert.Same(results[1].Customer, results[5].Customer);
                }
            }
        }

        public class ResultLimitMethods
        {
            private void PrepareTestData(string testRunID)
            {
                using (var db = TestHelper.GetDatabase())
                {
                    var name = testRunID;
                    var id = name.Substring(0, 15);

                    for (int i = 1; i <= 9; i++)
                    {
                        db.ExecuteNonQuery(@"INSERT INTO Customer (CustomerID, Name) VALUES (@0, @1)",
                            id + "_" + i.ToString(),
                            name + "_" + i.ToString());
                    }
                }
            }

            private void DeleteTestData(string testRunID)
            {
                using (var db = TestHelper.GetDatabase())
                {
                    var name = testRunID.ToString();
                    var id = name.Substring(0, 15);

                    for (int i = 1; i <= 9; i++)
                    {
                        db.ExecuteNonQuery(@"DELETE FROM Customer WHERE CustomerID LIKE @0", id + "%");
                    }
                }
            }

            [Fact]
            public void GetSubset_ReturnsCorrectResult()
            {
                var testRunID = Guid.NewGuid().ToString().Replace("-", "");
                var shortTestRunID = testRunID.Substring(0, 15);

                this.PrepareTestData(testRunID);

                using (var db = TestHelper.GetDatabase())
                {
                    var skip = 3;
                    var take = 4;

                    var results = db.Query<Customer>()
                        .Where("CustomerID LIKE @0", shortTestRunID + "%")
                        .OrderBy("CustomerID")
                        .GetSubset(skip, take)
                        .ToList();

                    for (int i = 0; i < take; i++)
                    {
                        var id = "{0}_{1}".FormatInvariant(shortTestRunID, i + 1 + skip);
                        Assert.Equal(id, results[i].CustomerID);
                    }
                }

                this.DeleteTestData(testRunID);
            }

            [Fact]
            public void GetTop_ReturnsCorrectResult()
            {
                var testRunID = Guid.NewGuid().ToString().Replace("-", "");
                var shortTestRunID = testRunID.Substring(0, 15);

                this.PrepareTestData(testRunID);

                using (var db = TestHelper.GetDatabase())
                {
                    var top = 6;

                    var results = db.Query<Customer>()
                        .Where("CustomerID LIKE @0", shortTestRunID + "%")
                        .OrderBy("CustomerID")
                        .GetTop(top)
                        .ToList();

                    for (int i = 0; i < top; i++)
                    {
                        var id = "{0}_{1}".FormatInvariant(shortTestRunID, i + 1);
                        Assert.Equal(id, results[i].CustomerID);
                    }
                }

                this.DeleteTestData(testRunID);
            }

            [Fact]
            public void GetCount_ReturnsCorrectResult()
            {
                var testRunID = Guid.NewGuid().ToString().Replace("-", "");

                this.PrepareTestData(testRunID);

                using (var db = TestHelper.GetDatabase())
                {
                    var count = db.Query<Customer>()
                        .Where("Name LIKE @0", testRunID + "%")
                        .GetCount();

                    Assert.Equal(9, count);
                }

                this.DeleteTestData(testRunID);
            }

            [Fact]
            public void GetLongCount_ReturnsCorrectResult()
            {
                var testRunID = Guid.NewGuid().ToString().Replace("-", "");

                this.PrepareTestData(testRunID);

                using (var db = TestHelper.GetDatabase())
                {
                    var count = db.Query<Customer>()
                        .Where("Name LIKE @0", testRunID + "%")
                        .GetLongCount();

                    Assert.Equal(9, count);
                }

                this.DeleteTestData(testRunID);
            }
        }
    }
}
