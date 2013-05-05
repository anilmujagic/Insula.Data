using Insula.DataAnnotations.Schema;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Insula.Data.Tests.Models
{
    public class Discount
    {
        [Mapped]
        public int DiscountID { get; set; }

        [Mapped]
        public string ItemID { get; set; }

        [Mapped]
        public string CustomerID { get; set; }

        [Mapped]
        public decimal Percent { get; set; }

        public Item Item { get; set; }
        public Customer Customer { get; set; }
    }
}
