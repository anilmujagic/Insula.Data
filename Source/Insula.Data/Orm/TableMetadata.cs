using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Insula.Data.Orm
{
    internal class TableMetadata
    {
        public TableMetadata(Type type)
        {
            if (type == null)
                throw new ArgumentNullException("type");

            if (!type.IsClass)
                throw new ArgumentException("Parameter must be a class.", "type");

            this.Name = type.Name;
            this.Type = type;

            _columns = new List<ColumnMetadata>();

            var props = type.GetProperties();
            foreach (var p in props)
            {
                _columns.Add(new ColumnMetadata(p));
            }

            this.IdentityColumn = this.Columns
                .SingleOrDefault(c => c.IsMapped && c.IsIdentity);

            this.KeyColumns = this.Columns
                .Where(c => c.IsMapped && c.IsPrimaryKey)
                .ToList();

            this.InsertColumns = this.Columns
                .Where(c => c.IsMapped && !c.IsIdentity)
                .OrderByDescending(c => c.IsPrimaryKey)
                .ToList();

            this.UpdateColumns = this.Columns
                .Where(c => c.IsMapped && !c.IsPrimaryKey && !c.IsIdentity)
                .ToList();

            this.SelectColumns = this.Columns
                .Where(c => c.IsMapped)
                .OrderByDescending(c => c.IsPrimaryKey)
                .ToList();
        }

        public string Name { get; private set; }
        public Type Type { get; private set; }

        public ColumnMetadata IdentityColumn { get; private set; }
        public IEnumerable<ColumnMetadata> KeyColumns { get; private set; }
        public IEnumerable<ColumnMetadata> SelectColumns { get; private set; }
        public IEnumerable<ColumnMetadata> InsertColumns { get; private set; }
        public IEnumerable<ColumnMetadata> UpdateColumns { get; private set; }

        private List<ColumnMetadata> _columns;
        public IEnumerable<ColumnMetadata> Columns
        {
            get
            {
                return _columns.AsReadOnly();
            }
        }
    }
}
