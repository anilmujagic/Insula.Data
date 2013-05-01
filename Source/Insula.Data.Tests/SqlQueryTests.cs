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
            public class TestEntity
            {
                public int AuthorID { get; set; }
                public string Name { get; set; }
            }

            public class TestEntity2
            {
                public string BookTitle { get; set; }
                public string AuthorName { get; set; }
            }

            [Fact]
            public void CustomQuery_OnSingleTable_WithoutParameters()
            {
                using (var db = TestHelper.GetDatabase())
                {
                    var count = (int)db.ExecuteScalar("SELECT COUNT(*) FROM Author WHERE Name LIKE 'A%'");
                    var entities = db.Query<TestEntity>("SELECT * FROM Author WHERE Name LIKE 'A%'")
                        .GetAll();
                    var nameOfFirstAuthor = entities.First().Name;

                    Assert.Equal(count, entities.Count());
                    Assert.NotNull(nameOfFirstAuthor);
                }
            }

            [Fact]
            public void CustomQuery_OnSingleTable_WithParameters()
            {
                using (var db = TestHelper.GetDatabase())
                {
                    var count = (int)db.ExecuteScalar("SELECT COUNT(*) FROM Author WHERE Name LIKE @0", "B%");
                    var entities = db.Query<TestEntity>("SELECT * FROM Author WHERE Name LIKE @0", "B%")
                        .GetAll();
                    var firstAuthor = entities.FirstOrDefault();

                    Assert.Equal(count, entities.Count());
                    Assert.NotNull(firstAuthor);
                    Assert.NotNull(firstAuthor.Name);
                }
            }

            [Fact]
            public void CustomQuery_OnMultipleJoinedTables_WithParameters()
            {
                using (var db = TestHelper.GetDatabase())
                {
                    var count = (int)db.ExecuteScalar(@"
                        SELECT COUNT(*)
                        FROM Book AS b
                        JOIN BookAuthor AS ba
                            ON ba.BookID = b.BookID
                        JOIN Author AS a
                            ON a.AuthorID = ba.AuthorID
                        WHERE a.Name LIKE @0
                        ", "C%");
                    var entities = db.Query<TestEntity2>(@"
                        SELECT 
                            b.Title AS BookTitle,
                            a.Name AS AuthorName
                        FROM Book AS b
                        JOIN BookAuthor AS ba
                            ON ba.BookID = b.BookID
                        JOIN Author AS a
                            ON a.AuthorID = ba.AuthorID
                        WHERE a.Name LIKE @0
                        ", "C%")
                        .GetAll();
                    var firstEntry = entities.FirstOrDefault();

                    Assert.Equal(count, entities.Count());
                    Assert.NotNull(firstEntry);
                    Assert.NotNull(firstEntry.BookTitle);
                    Assert.NotNull(firstEntry.AuthorName);

                    foreach (var e in entities)
                    {
                        Debug.WriteLine(e.BookTitle + " by " + e.AuthorName);
                    }
                }
            }
        }
    }
}
