using System;
using System.Collections.Generic;
using System.Text;
using Org.Reddragonit.Dbpro.Structure.Attributes;
using System.Reflection;

namespace Org.Reddragonit.Dbpro.Connections.PoolComponents
{
    internal struct sTableField
    {
        private string _name;
        public string Name
        {
            get { return _name; }
        }

        private string _classProperty;
        public string ClassProperty
        {
            get { return _classProperty; }
        }

        private string _externalField;
        public string ExternalField
        {
            get { return _externalField; }
        }

        private FieldType _type;
        public FieldType Type
        {
            get { return _type; }
        }

        private int _length;
        public int Length
        {
            get { return _length; }
        }

        private bool _nullable;
        public bool Nullable
        {
            get { return _nullable; }
        }

        private string _computedCode;
        public string ComputedCode
        {
            get { return _computedCode; }
        }

        public sTableField(string name,string classProperty,string externalField,FieldType type,int length,bool nullable,string computedCode){
            _name = name;
            _classProperty = classProperty;
            _externalField = externalField;
            _type = type;
            _length = length;
            _nullable = nullable;
            _computedCode = computedCode;
        }
    }

    internal struct sTableRelation
    {
        private string _externalTable;
        public string ExternalTable
        {
            get { return _externalTable; }
        }

        private string _classProperty;
        public string ClassProperty
        {
            get { return _classProperty; }
        }

        private ForeignField.UpdateDeleteAction _onUpdate;
        public ForeignField.UpdateDeleteAction OnUpdate
        {
            get { return _onUpdate; }
        }

        private ForeignField.UpdateDeleteAction _onDelete;
        public ForeignField.UpdateDeleteAction OnDelete
        {
            get { return _onDelete; }
        }

        private bool _nullable;
        public bool Nullable
        {
            get { return _nullable; }
        }

        public sTableRelation(string externalTable, string classProperty, ForeignField.UpdateDeleteAction onUpdate, ForeignField.UpdateDeleteAction onDelete,bool nullable)
        {
            _externalTable = externalTable;
            _classProperty = classProperty;
            _onUpdate = onUpdate;
            _onDelete = onDelete;
            _nullable = nullable;
        }
    }

    internal struct sTable
    {
        private string _name;
        public string Name
        {
            get { return _name; }
        }

        private sTableField[] _fields;
        public sTableField[] Fields
        {
            get { return _fields; }
        }

        private sTableRelation[] _relations;
        public sTableRelation[] Relations
        {
            get { return _relations; }
        }

        private string[] _primaryKeyFields;
        public string[] PrimaryKeyFields
        {
            get { return _primaryKeyFields; }
        }

        private string _autoGenField;
        public string AutoGenField
        {
            get { return _autoGenField; }
        }

        private List<string> _arrayProperties;
        public List<string> ArrayProperties
        {
            get{return _arrayProperties;}
        }

        public string AutoGenProperty
        {
            get {
                string ret = null;
                if (_autoGenField != null)
                {
                    foreach (sTableField fld in Fields)
                    {
                        if (fld.Name == _autoGenField)
                        {
                            ret = fld.ClassProperty;
                            break;
                        }
                    }
                }
                return ret;
            }
        }

        public sTable(string name, sTableField[] fields, List<PropertyInfo> arrayProperties, sTableRelation[] relations, string[] primaryKeyFields, string autoGenField)
        {
            _name = name;
            _fields = fields;
            _relations = relations;
            _primaryKeyFields = primaryKeyFields;
            _autoGenField = autoGenField;
            _arrayProperties = new List<string>(new string[(arrayProperties == null ? 0 : arrayProperties.Count)]);
            for (int x = 0; x < _arrayProperties.Count; x++)
            {
                _arrayProperties[x] = arrayProperties[x].Name;
            }
        }

        public sTableField[] this[string property]
        {
            get
            {
                List<sTableField> ret = new List<sTableField>();
                foreach (sTableField f in _fields)
                {
                    if (f.ClassProperty == property)
                        ret.Add(f);
                }
                return ret.ToArray();
            }
        }

        public sTableRelation? GetRelationForProperty(string property)
        {
            foreach (sTableRelation rel in _relations)
            {
                if (Utility.StringsEqual(rel.ClassProperty,property))
                    return rel;
            }
            return null;
        }

        public string[] Properties
        {
            get
            {
                List<string> ret = new List<string>();
                foreach (sTableField f in _fields)
                {
                    if (f.ClassProperty != null)
                    {
                        if (!ret.Contains(f.ClassProperty))
                            ret.Add(f.ClassProperty);
                    }
                }
                foreach (sTableRelation rel in _relations)
                {
                    if (rel.ClassProperty != null)
                    {
                        if (!ret.Contains(rel.ClassProperty))
                            ret.Add(rel.ClassProperty);
                    }
                }
                foreach (string str in _arrayProperties)
                    ret.Add(str);
                return ret.ToArray();
            }
        }

        public string[] PrimaryKeyProperties
        {
            get
            {
                List<string> ret = new List<string>();
                List<string> pkeys = new List<string>(PrimaryKeyFields);
                foreach (sTableField f in _fields)
                {
                    if (pkeys.Contains(f.Name) && !ret.Contains(f.ClassProperty))
                        ret.Add(f.ClassProperty);
                }
                return ret.ToArray();
            }
        }

        public string[] ForeignTableProperties
        {
            get
            {
                List<string> ret = new List<string>();
                foreach (sTableRelation tr in Relations)
                {
                    if (!ret.Contains(tr.ClassProperty))
                        ret.Add(tr.ClassProperty);
                }
                return ret.ToArray();
            }
        }

        internal string GetPropertyNameForField(string str)
        {
            string ret = null;
            foreach (sTableField fld in Fields)
            {
                if (fld.Name == str){
                    ret = fld.ClassProperty;
                    break;
                }
            }
            return ret;
        }

        internal bool IsEnumProperty(string fieldName)
        {
            bool ret = false;
            foreach (sTableField fld in Fields)
            {
                if (fld.Name == fieldName || fld.ClassProperty == fieldName)
                {
                    ret = fld.Type == FieldType.ENUM;
                    break;
                }
            }
            return ret;
        }
    }
}
