using System;

namespace Insula.DataAnnotations.Schema
{
    [AttributeUsage(AttributeTargets.Class | AttributeTargets.Property, AllowMultiple = false, Inherited = true)]
    public sealed class MappedAttribute : Attribute
    {
        /// <summary>
        /// Name of the database schema of the table this class is mapped to. This option is used only when attribute is applied on class.
        /// </summary>
        public string SchemaName { get; set; }

        /// <summary>
        /// Name of the database table this class is mapped to. This option is used only when attribute is applied on class.
        /// </summary>
        public string TableName { get; set; }

        /// <summary>
        /// Name of the column in database table this property is mapped to. This option is used only when attribute is applied on property.
        /// </summary>
        public string ColumnName { get; set; }
    }
}
