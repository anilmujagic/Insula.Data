using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data.Common;
using System.Reflection;
using System.Reflection.Emit;
using Insula.Common;

namespace Insula.Data.Orm
{
    class Materializer<T> where T : class, new()
    {
        private delegate void PropertyValueSetterDelegate(T entity, object propertyValue);

        private readonly TableMetadata _tableMetadata;
        private readonly List<ColumnMetadata> _columns;
        private readonly PropertyValueSetterDelegate[] _setters;

        public Materializer(TableMetadata tableMetadata)
        {
            _tableMetadata = tableMetadata;
            _columns = _tableMetadata.SelectColumns.ToList();

            var setters = new List<PropertyValueSetterDelegate>();
            foreach (var c in _columns)
            {
                setters.Add(CreatePropertyValueSetterDelegate(c.PropertyInfo));
            }
            _setters = setters.ToArray();
        }

        public T Materialize(DbDataReader dataReader, bool useAlias = false, string tableAlias = null)
        {
            if (useAlias && tableAlias.IsNullOrWhiteSpace())
                tableAlias = _tableMetadata.Name;

            T entity = new T();

            bool hasNonNullValues = false;  //Used to find out if related record in joined table exists

            for (int i = 0; i < _columns.Count; i++)
            {
                var value = dataReader[useAlias ? tableAlias + "." + _columns[i].Name : _columns[i].Name];
                if (value != DBNull.Value)
                {
                    hasNonNullValues = true;

                    //_columns[i].PropertyInfo.SetValue(entity, value, null);  //Slow
                    _setters[i](entity, value);  //Fast
                }
            }

            if (hasNonNullValues)
                return entity;
            else
                return null;
        }

        private static PropertyValueSetterDelegate CreatePropertyValueSetterDelegate(PropertyInfo propertyInfo)
        {
            //I took the code in this method from following blogpost:
            //http://www.manuelabadia.com/blog/PermaLink,guid,dc72b235-1381-4c91-8706-e36216f49b94.aspx
            //More explanation can be found here:
            //http://www.codeproject.com/KB/cs/HyperPropertyDescriptor.aspx

            //Generates a dynamic method to generate a PropertyValueSetterDelegate
            var dynamicMethod = new DynamicMethod(string.Empty, null, new Type[] { typeof(object), typeof(object) }, propertyInfo.DeclaringType.Module);
            var ilGenerator = dynamicMethod.GetILGenerator();

            //Loads the object into the stack
            ilGenerator.Emit(OpCodes.Ldarg_0);
            //Loads the parameter from the stack
            ilGenerator.Emit(OpCodes.Ldarg_1);

            //Cast to the proper type (unboxing if needed)
            if (propertyInfo.PropertyType.IsValueType)
                ilGenerator.Emit(OpCodes.Unbox_Any, propertyInfo.PropertyType);
            else
                ilGenerator.Emit(OpCodes.Castclass, propertyInfo.PropertyType);

            //Calls the setter
            ilGenerator.EmitCall(OpCodes.Callvirt, propertyInfo.GetSetMethod(), null);
            //Terminates the call
            ilGenerator.Emit(OpCodes.Ret);

            //Converts the DynamicMethod to a PropertyValueSetterDelegate to get the property
            return (PropertyValueSetterDelegate)dynamicMethod.CreateDelegate(typeof(PropertyValueSetterDelegate));
        }
    }
}
