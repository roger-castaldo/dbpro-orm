using System;
using System.Collections.Generic;
using System.Text;
using System.Reflection;
using Org.Reddragonit.Dbpro.Structure.Mapping;

namespace Org.Reddragonit.Dbpro.Virtual.Attributes
{
    [AttributeUsage(AttributeTargets.Class)]
    public class VirtualTableAttribute : Attribute 
    {
        private Type _mainTable;
        public Type MainTable
        {
            get { return _mainTable; }
            set { _mainTable = value; }
        }

        public VirtualTableAttribute() { _mainTable = null; }

        public VirtualTableAttribute(Type MainTable)
        {
            _mainTable = MainTable;
        }

        internal static Type GetMainTableTypeForVirtualTable(object obj)
        {
            return GetMainTableTypeForVirtualTable(obj.GetType());
        }

        internal static Type GetMainTableTypeForVirtualTable(Type type)
        {
            Type ret = null;
            if (type.GetCustomAttributes(typeof(VirtualTableAttribute), false).Length > 0)
                ret = ((VirtualTableAttribute)type.GetCustomAttributes(typeof(VirtualTableAttribute), false)[0]).MainTable;
            else if (type.GetCustomAttributes(typeof(VirtualTableAttribute), true).Length > 0)
                ret = ((VirtualTableAttribute)type.GetCustomAttributes(typeof(VirtualTableAttribute), true)[0]).MainTable;
            if (ret == null)
            {
                Dictionary<Type, int> counts = new Dictionary<Type, int>();
                foreach (PropertyInfo pi in type.GetProperties())
                {
                    if (pi.GetCustomAttributes(typeof(VirtualField), true).Length > 0)
                    {
                        Type t = ((VirtualField)pi.GetCustomAttributes(typeof(VirtualField), true)[0]).ReferencingTable;
                        int cnt = 1;
                        if (counts.ContainsKey(t))
                        {
                            cnt += counts[t];
                            counts.Remove(t);
                        }
                        counts.Add(t, cnt);
                    }
                }
                int maxCount = 0;
                foreach (Type tp in counts.Keys)
                {
                    if (counts[tp] > maxCount)
                    {
                        maxCount = counts[tp];
                        ret = tp;
                    }
                }
            }
            return ret;
        }

        internal static void ValidateLinksInTable(Type type)
        {
            bool isValid = true;
            string connectionName = ClassMapper.GetTableMap(GetMainTableTypeForVirtualTable(type)).ConnectionName;
            foreach (PropertyInfo pi in type.GetProperties())
            {
                if (pi.GetCustomAttributes(typeof(VirtualField), true).Length > 0)
                {
                    TableMap tm = ClassMapper.GetTableMap(((VirtualField)pi.GetCustomAttributes(typeof(VirtualField), true)[0]).ReferencingTable);
                    if (connectionName != tm.ConnectionName)
                    {
                        isValid = false;
                        break;
                    }
                    string fieldName = ((VirtualField)pi.GetCustomAttributes(typeof(VirtualField), true)[0]).FieldName;
                    if (tm.GetTableFieldName(fieldName)==null)
                        throw new Exception("The Virtual table of type "+type.FullName+" is invalid because the referenced table "+((VirtualField)pi.GetCustomAttributes(typeof(VirtualField), true)[0]).ReferencingTable.FullName+" does not contain a field "+fieldName);
                }
            }
            if (!isValid)
                throw new Exception("The Virtual table of type " + type.FullName + " is invalid due to a reference of tables that use different connections.");
        }
    }
}
