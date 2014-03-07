using System;
using System.Collections.Generic;
using System.Text;
using Org.Reddragonit.Dbpro.Connections;
using System.Data;
using Org.Reddragonit.Dbpro.Connections.MsSql;
using Org.Reddragonit.Dbpro.Connections.ClassSQL;
using Org.Reddragonit.Dbpro.Structure;
using Org.Reddragonit.Dbpro.Connections.PoolComponents;
using System.Reflection;

namespace Org.Reddragonit.Dbpro.Virtual
{
    public class ViewResultRow
    {
        private Connection _conn;
        private Dictionary<string, Type> _tableFields;
        private Dictionary<int, string> _fieldNames;
        private Dictionary<int, int> _tableFieldCounts;
        private Dictionary<int, Type> _enumFields;

        internal ViewResultRow(Connection conn,ClassQuery cq)
        {
            _conn = conn;
            cq.GetIndexTranslators(out _tableFields, out _fieldNames, out _tableFieldCounts, out _enumFields);
        }

        private int TranslateFieldIndex(int i)
        {
            if (_tableFields.Count > 0)
            {
                int ret = i;
                int shift = 0;
                foreach (int index in Utility.SortDictionaryKeys(_tableFieldCounts.Keys))
                {
                    if (index < i)
                    {
                        ret += _tableFieldCounts[index];
                        shift++;
                    }
                }
                return (ret == i ? i : ret - shift);
            }
            return i;
        }

        public int FieldCount
        {
            get { return _fieldNames.Count; }
        }

        public bool GetBoolean(int i)
        {
            return _conn.GetBoolean(TranslateFieldIndex(i));
        }

        public byte GetByte(int i)
        {
            return _conn.GetByte(TranslateFieldIndex(i));
        }

        public long GetBytes(int i, long fieldOffset, byte[] buffer, int bufferoffset, int length)
        {
            return _conn.GetBytes(TranslateFieldIndex(i), fieldOffset, buffer, bufferoffset, length);
        }

        public char GetChar(int i)
        {
            return _conn.GetChar(i);
        }

        public long GetChars(int i, long fieldoffset, char[] buffer, int bufferoffset, int length)
        {
            return _conn.GetChars(TranslateFieldIndex(i), fieldoffset, buffer, bufferoffset, length);
        }

        public IDataReader GetData(int i)
        {
            return _conn.GetData(TranslateFieldIndex(i));
        }

        public string GetDataTypeName(int i)
        {
            return _conn.GetDataTypeName(TranslateFieldIndex(i));
        }

        public DateTime GetDateTime(int i)
        {
            return _conn.GetDateTime(TranslateFieldIndex(i));
        }

        public decimal GetDecimal(int i)
        {
            return _conn.GetDecimal(TranslateFieldIndex(i));
        }

        public double GetDouble(int i)
        {
            return _conn.GetDouble(TranslateFieldIndex(i));
        }

        public Type GetFieldType(int i)
        {
            if (_tableFieldCounts.ContainsKey(i))
                return _tableFields[_fieldNames[i]];
            else
                return _conn.GetFieldType(TranslateFieldIndex(i));
        }

        public float GetFloat(int i)
        {
            return _conn.GetFloat(TranslateFieldIndex(i));
        }

        public Guid GetGuid(int i)
        {
            return _conn.GetGuid(TranslateFieldIndex(i));
        }

        public short GetInt16(int i)
        {
            return _conn.GetInt16(TranslateFieldIndex(i));
        }

        public int GetInt32(int i)
        {
            return _conn.GetInt32(TranslateFieldIndex(i));
        }

        public long GetInt64(int i)
        {
            return _conn.GetInt64(TranslateFieldIndex(i));
        }

        public string GetName(int i)
        {
            return _fieldNames[i];
        }

        public int GetOrdinal(string name)
        {
            foreach (int x in _fieldNames.Keys)
            {
                if (_fieldNames[x] == name)
                    return x;
            }
            return -1;
        }

        public string GetString(int i)
        {
            return _conn.GetString(TranslateFieldIndex(i));
        }

        public object GetValue(int i)
        {
            if (IsDBNull(i))
                return null;
            if (_tableFieldCounts.ContainsKey(i))
            {
                Table t = (Table)LazyProxy.Instance(_tableFields[_fieldNames[i]].GetConstructor(System.Type.EmptyTypes).Invoke(new object[0]));
                sTableField[] flds = _conn.Pool.Mapping[_tableFields[_fieldNames[i]]].Fields;
                int index = 0;
                i = TranslateFieldIndex(i);
                foreach (sTableField fld in flds)
                {
                    PropertyInfo pi = t.GetType().GetProperty(fld.ClassProperty, Utility._BINDING_FLAGS_WITH_INHERITANCE);
                    if (!pi.PropertyType.IsArray)
                    {
                        if (_conn.Pool.Mapping.IsMappableType(pi.PropertyType) && !Utility.IsEnum(pi.PropertyType))
                        {
                            if (!_conn.IsDBNull(i + index))
                            {
                                if (t.GetField(pi.Name) == null)
                                {
                                    Table tmp = (Table)LazyProxy.Instance(pi.PropertyType.GetConstructor(Type.EmptyTypes).Invoke(new object[0]));
                                    tmp.LoadStatus = LoadStatus.Partial;
                                    t.SetField(fld.Name, tmp);
                                }
                                Table tbl = (Table)t.GetField(pi.Name);
                                foreach (sTableField f in _conn.Pool.Mapping[tbl.GetType()].Fields)
                                {
                                    if (fld.ExternalField == f.Name)
                                    {
                                        t.RecurSetPropertyValue(f.Name, _conn, _conn.GetName(i + index), tbl);
                                        index++;
                                        break;
                                    }
                                }
                            }
                            else
                                index++;
                        }
                        else
                        {
                            if (!_conn.IsDBNull(i + index))
                            {
                                if (Utility.IsEnum(pi.PropertyType))
                                    t.SetField(fld.ClassProperty, _conn.Pool.GetEnumValue(pi.PropertyType, _conn.GetInt32(i + index)));
                                else
                                    t.SetField(fld.ClassProperty, _conn[i + index]);
                            }
                            index++;
                        }
                    }
                }
                t.LoadStatus = LoadStatus.Complete;
                return t;
            }
            else if (_enumFields.ContainsKey(i))
            {
                return _conn.Pool.GetEnumValue(_enumFields[i], _conn.GetInt32(TranslateFieldIndex(i)));
            }
            else
                return _conn.GetValue(TranslateFieldIndex(i));
        }

        public int GetValues(object[] values)
        {
            values = new object[_fieldNames.Count];
            for (int x = 0; x < values.Length; x++)
            {
                values[x] = GetValue(x);
            }
            return values.Length;
        }

        public bool IsDBNull(string name)
        {
            return IsDBNull(GetOrdinal(name));
        }

        public bool IsDBNull(int i)
        {
            if (!_tableFieldCounts.ContainsKey(i))
                return _conn.IsDBNull(TranslateFieldIndex(i));
            else
            {
                int start = TranslateFieldIndex(i);
                for (int x = 0; x < _tableFieldCounts[i]; x++)
                {
                    if (!_conn.IsDBNull(x + start))
                        return false;
                }
                return true;
            }
        }

        public object this[string name]
        {
            get { return GetValue(GetOrdinal(name)); }
        }

        public object this[int i]
        {
            get { return GetValue(i); }
        }

        public List<string> FieldNames
        {
            get
            {
                List<string> ret = new List<string>();
                foreach (string str in _fieldNames.Values)
                    ret.Add(str);
                return ret;
            }
        }
    }
}
