using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Insula.Data.DatabaseServices
{
    public class ColumnInfo
    {
        internal ColumnInfo()
        {
        }

        public string Name { get; internal set; }
        public string DataType { get; internal set; }
        public int? MaxLength { get; internal set; }
        public int? DecimalPlaces { get; internal set; }
        public bool IsNullable { get; internal set; }
        public bool IsPrimaryKey { get; internal set; }
        public bool IsIdentity { get; internal set; }
        public bool IsComputed { get; internal set; }
        public int OrdinalPosition { get; internal set; }
    }
}
