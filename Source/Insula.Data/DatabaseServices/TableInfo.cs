using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Insula.Data.DatabaseServices
{
    public class TableInfo
    {
        internal TableInfo()
        {
            _columns = new List<ColumnInfo>();
        }

        public string Name { get; internal set; }
        public string Schema { get; internal set; }
        public string Database { get; internal set; }

        private List<ColumnInfo> _columns;
        public IEnumerable<ColumnInfo> Columns
        {
            get
            {
                return _columns.AsReadOnly();
            }
        }

        internal void AddColumn(ColumnInfo columnInfo)
        {
            _columns.Add(columnInfo);
        }    
    }
}
