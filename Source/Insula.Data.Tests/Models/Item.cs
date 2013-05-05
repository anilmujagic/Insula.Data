using Insula.DataAnnotations.Schema;
using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Text;

namespace Insula.Data.Tests.Models
{
    public class Item
    {
        [Mapped]
        [Key]
        public string ItemID { get; set; }

        [Mapped]
        public string Description { get; set; }

        [Mapped]
        public decimal Price { get; set; }
    }
}
