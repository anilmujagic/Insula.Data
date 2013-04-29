using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;

namespace Insula.Data.Orm
{
    internal class ColumnMetadata
    {
        public ColumnMetadata(PropertyInfo propertyInfo)
        {
            if (propertyInfo == null)
                throw new ArgumentNullException("propertyInfo");

            this.PropertyInfo = propertyInfo;
            this.Name = propertyInfo.Name;
            this.Type = propertyInfo.PropertyType;

            this.IsNullable = !Attribute.IsDefined(propertyInfo, typeof(System.ComponentModel.DataAnnotations.RequiredAttribute), true);
            this.IsPrimaryKey = Attribute.IsDefined(propertyInfo, typeof(System.ComponentModel.DataAnnotations.KeyAttribute), true);
            this.IsIdentity = Attribute.IsDefined(propertyInfo, typeof(Insula.DataAnnotations.Schema.IdentityAttribute), true);
            this.IsMapped = Attribute.IsDefined(propertyInfo, typeof(Insula.DataAnnotations.Schema.MappedAttribute), true);
        }

        public PropertyInfo PropertyInfo { get; private set; }
        public string Name { get; private set; }
        public Type Type { get; private set; }
        public bool IsNullable { get; private set; }
        public bool IsPrimaryKey { get; private set; }
        public bool IsIdentity { get; private set; }
        public bool IsMapped { get; private set; }
    }
}
