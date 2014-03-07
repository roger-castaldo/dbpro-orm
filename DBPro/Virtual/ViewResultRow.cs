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
        private ClassQuery _cq;

        internal ViewResultRow(Connection conn,ClassQuery cq)
        {
            _conn = conn;
            _cq = cq;
        }

        public int FieldCount
        {
            get { return _cq.FieldCount; }
        }

        public bool GetBoolean(int i)
        {
            return (bool)GetValue(i);
        }

        public byte GetByte(int i)
        {
            return (byte)GetValue(i);
        }

        public long GetBytes(int i, long fieldOffset, byte[] buffer, int bufferoffset, int length)
        {
            return _cq.GetBytes(i, fieldOffset, buffer, bufferoffset, length,_conn);
        }

        public char GetChar(int i)
        {
            return (char)GetValue(i);
        }

        public long GetChars(int i, long fieldoffset, char[] buffer, int bufferoffset, int length)
        {
            return _cq.GetChars(i, fieldoffset, buffer, bufferoffset, length,_conn);
        }

        public IDataReader GetData(int i)
        {
            return (IDataReader)GetValue(i);
        }

        public string GetDataTypeName(int i)
        {
            return _cq.GetDataTypeName(i,_conn);
        }

        public DateTime GetDateTime(int i)
        {
            return (DateTime)GetValue(i);
        }

        public decimal GetDecimal(int i)
        {
            return (decimal)GetValue(i);
        }

        public double GetDouble(int i)
        {
            return (double)GetValue(i);
        }

        public Type GetFieldType(int i)
        {
            return _cq.GetFieldType(i, _conn);
        }

        public float GetFloat(int i)
        {
            return (float)GetValue(i);
        }

        public Guid GetGuid(int i)
        {
            return (Guid)GetValue(i);
        }

        public short GetInt16(int i)
        {
            return (short)GetValue(i);
        }

        public int GetInt32(int i)
        {
            return (int)GetValue(i);
        }

        public long GetInt64(int i)
        {
            return (long)GetValue(i);
        }

        public string GetName(int i)
        {
            return _cq.GetName(i);
        }

        public int GetOrdinal(string name)
        {
            return _cq.GetOrdinal(name);
        }

        public string GetString(int i)
        {
            return (string)GetValue(i);
        }

        public object GetValue(int i)
        {
            return _cq.GetValue(i, _conn);
        }

        public int GetValues(object[] values)
        {
            return _cq.GetValues(values, _conn);
        }

        public bool IsDBNull(string name)
        {
            return _cq.IsDBNull(name, _conn);
        }

        public bool IsDBNull(int i)
        {
            return _cq.IsDBNull(i, _conn);
        }

        public object this[string name]
        {
            get { return _cq[name,_conn]; }
        }

        public object this[int i]
        {
            get { return GetValue(i); }
        }

        public List<string> FieldNames
        {
            get
            {
                return _cq.FieldNames;
            }
        }
    }
}
