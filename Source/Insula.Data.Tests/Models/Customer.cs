﻿using Insula.DataAnnotations.Schema;
using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Text;

namespace Insula.Data.Tests.Models
{
    public class Customer
    {
        [Mapped]
        [Key]
        public string CustomerID { get; set; }

        [Mapped]
        public string Name { get; set; }

        [Mapped]
        public string Address { get; set; }
    }
}
