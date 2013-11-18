using System;
using System.Collections.Generic;
using System.Text;
using Org.Reddragonit.Dbpro.Connections;
using System.Data;
using Org.Reddragonit.Dbpro.Connections.MsSql;

namespace Org.Reddragonit.Dbpro.Virtual
{
    public class ViewResultRow
    {
        private Connection _conn;

        internal ViewResultRow(Connection conn)
        {
            _conn = conn;
        }

        public int Depth
        {
            get
            {
                return _conn.Depth;
            }
        }

        public int FieldCount
        {
            get
            {
                return _conn.FieldCount;
            }
        }

        public DataTable GetSchemaTable()
        {
            return _conn.GetSchemaTable();
        }

        string GetName(int i)
        {
            return _conn.GetName(i);
        }

        public string GetDataTypeName(int i)
        {
            return _conn.GetDataTypeName(i);
        }

        public int GetOrdinal(string name)
        {
            return _conn.GetOrdinal(name);
        }

        public bool ContainsField(string name)
        {
            try
            {
                _conn.GetOrdinal(name);
                return true;
            }
            catch (Exception e)
            {
                return false;
            }
        }

        public byte GetByte(int i)
        {
            return _conn.GetByte(i);
        }

        public long GetBytes(int i, long fieldOffset, byte[] buffer, int bufferoffset, int length)
        {
            return _conn.GetBytes(i, fieldOffset, buffer, bufferoffset, length);
        }

        public char GetChar(int i)
        {
            return _conn.GetChar(i);
        }

        public long GetChars(int i, long fieldoffset, char[] buffer, int bufferoffset, int length)
        {
            return _conn.GetChars(i, fieldoffset, buffer, bufferoffset, length);
        }

        public Guid GetGuid(int i)
        {
            return _conn.GetGuid(i);
        }

        public short GetInt16(int i)
        {
            return _conn.GetInt16(i);
        }

        public long GetInt64(int i)
        {
            return _conn.GetInt64(i);
        }

        public float GetFloat(int i)
        {
            return _conn.GetFloat(i);
        }

        public double GetDouble(int i)
        {
            if (_conn is MsSqlConnection
                && _conn.GetDataTypeName(i).ToUpper() == "DECIMAL")
            {
                double ret = 0;
                if (double.TryParse(_conn[i].ToString(), out ret))
                    return ret;
                return _conn.GetDouble(i);
            }
            return _conn.GetDouble(i);
        }

        public decimal GetDecimal(int i)
        {
            return _conn.GetDecimal(i);
        }

        public DateTime GetDateTime(int i)
        {
            return _conn.GetDateTime(i);
        }

        public IDataReader GetData(int i)
        {
            return _conn.GetData(i);
        }

        public bool IsDBNull(int i)
        {
            return _conn.IsDBNull(i);
        }

        public bool IsDBNull(string name)
        {
            return IsDBNull(GetOrdinal(name));
        }

        public virtual bool GetBoolean(int i)
        {
            return _conn.GetBoolean(i);
        }

        public int GetInt32(int i)
        {
            return _conn.GetInt32(i);
        }

        public string GetString(int i)
        {
            return _conn.GetString(i);
        }

        public object this[int i]
        {
            get
            {
                return this.GetValue(i);
            }
        }

        public object this[string name]
        {
            get
            {
                return this.GetValue(this.GetOrdinal(name));
            }
        }

        public virtual Type GetFieldType(int i)
        {
            return _conn.GetFieldType(i);
        }

        public virtual object GetValue(int i)
        {
            if (_conn is MsSqlConnection
                && _conn.GetDataTypeName(i).ToUpper()=="DECIMAL")
            {
                double ret = 0;
                if (double.TryParse(_conn[i].ToString(), out ret))
                    return ret;
                return _conn[i];
            }
            return _conn.GetValue(i);
        }

        public virtual int GetValues(object[] values)
        {
            object[] ret = new object[_conn.FieldCount];
            for (int x = 0; x < _conn.FieldCount; x++)
            {
                if (_conn.IsDBNull(x))
                    ret[x] = null;
                else
                    ret[x] = this.GetValue(x);
            }
            return ret.Length;
        }

        public object GetEnum(Type type, string name)
        {
            return GetEnum(type, GetOrdinal(name));
        }

        public object GetEnum(Type type, int i)
        {
            return _conn.Pool.Enums.GetEnumValue(type, (int)this[i]);
        }

        public List<string> FieldNames
        {
            get
            {
                List<string> ret = new List<string>();
                for (int x = 0; x < _conn.FieldCount; x++)
                {
                    ret.Add(_conn.GetName(x));
                }
                return ret;
            }
        }
    }
}
