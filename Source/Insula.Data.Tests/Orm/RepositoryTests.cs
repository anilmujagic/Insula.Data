using Insula.Common;
using Insula.Data.Orm;
using Insula.Data.Tests.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Xunit;

namespace Insula.Data.Tests.Orm
{
    public class RepositoryTests
    {
        [Fact]
        public void CrudMethods_WorkAsExpected()
        {
            using (var db = TestHelper.GetDatabase())
            {
                var repo = db.Repository<Discount>();

                db.ExecuteNonQuery("DELETE FROM Discount");
                var qEmpty = repo.Query().GetAll();
                Assert.Empty(qEmpty);

                var discount = new Discount
                {
                    Percent = 10
                };
                repo.Insert(discount);
                Assert.NotEqual(0, discount.DiscountID);
                var qAfterInsert = repo.Query().GetAll();
                Assert.NotEmpty(qAfterInsert);
                Assert.Equal(1, qAfterInsert.Count());
                var discountFromDB = qAfterInsert.FirstOrDefault();
                Assert.NotNull(discountFromDB);
                Assert.NotSame(discount, discountFromDB);
                Assert.Equal(discount.DiscountID, discountFromDB.DiscountID);
                Assert.Equal(discount.Percent, discountFromDB.Percent);

                discount.Percent = 11;
                repo.Update(discount);
                var qAfterUpdate = repo.Query().GetAll();
                Assert.NotEmpty(qAfterUpdate);
                Assert.Equal(1, qAfterUpdate.Count());
                discountFromDB = qAfterUpdate.FirstOrDefault();
                Assert.NotNull(discountFromDB);
                Assert.NotSame(discount, discountFromDB);
                Assert.Equal(discount.Percent, discountFromDB.Percent);

                repo.Delete(discount);
                var qAfterDelete = repo.Query().GetAll();
                Assert.Empty(qAfterDelete);

                // Get and Delete by key
                discount = new Discount
                {
                    Percent = 5
                };
                repo.Insert(discount);
                discountFromDB = repo.GetByKey(discount.DiscountID);
                Assert.NotNull(discountFromDB);
                Assert.NotSame(discount, discountFromDB);
                Assert.Equal(discount.DiscountID, discountFromDB.DiscountID);
                Assert.Equal(discount.Percent, discountFromDB.Percent);

                repo.DeleteByKey(discount.DiscountID);
                var qAfterDeleteByKey = repo.Query().GetAll();
                Assert.Empty(qAfterDeleteByKey);
            }
        }
    }
}
