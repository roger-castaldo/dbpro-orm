using System;
using System.Collections.Generic;
using System.Text;
using System.Data;

namespace Org.Reddragonit.Dbpro.Connections.PoolComponents
{
    internal class EnumsHandler
    {
        private ConnectionPool _pool;
        internal Dictionary<Type, string> _enumTableMaps = new Dictionary<Type, string>();
        internal Dictionary<Type, Dictionary<string, int>> _enumValuesMap = new Dictionary<Type, Dictionary<string, int>>();
        internal Dictionary<Type, Dictionary<int, string>> _enumReverseValuesMap = new Dictionary<Type, Dictionary<int, string>>();

        public EnumsHandler(ConnectionPool pool)
        {
            _pool = pool;
            _enumTableMaps = new Dictionary<Type, string>();
            _enumValuesMap = new Dictionary<Type, Dictionary<string, int>>();
            _enumReverseValuesMap = new Dictionary<Type, Dictionary<int, string>>();
        }

        private void _initEnum(Type enumType)
        {
            enumType = (enumType.IsGenericType ? enumType.GetGenericArguments()[0] : enumType);
            if (!_enumReverseValuesMap.ContainsKey(enumType))
            {
                Connection conn = _pool.GetConnection();
                _pool.Updater.InitType(enumType, conn);
                conn.CloseConnection();
            }
        }

        public object GetEnumValue(Type enumType, int ID)
        {
            enumType = (enumType.IsGenericType ? enumType.GetGenericArguments()[0] : enumType);
            _initEnum(enumType);
            return Enum.Parse(enumType, _enumReverseValuesMap[enumType][ID]);
        }

        public int GetEnumID(Type enumType, string enumName)
        {
            enumType = (enumType.IsGenericType ? enumType.GetGenericArguments()[0] : enumType);
            _initEnum(enumType);
            return _enumValuesMap[enumType][enumName];
        }

        internal void WipeOutEnums(Connection c)
        {
            foreach (Type t in _enumTableMaps.Keys)
                c.ExecuteNonQuery("DELETE FROM " + _enumTableMaps[t]);
        }

        internal void InsertToDB(Type t, int id, string value, Connection c)
        {
            t = (t.IsGenericType ? t.GetGenericArguments()[0] : t);
            c.ExecuteNonQuery("INSERT INTO " + _enumTableMaps[t] + " VALUES(" + c.CreateParameterName("id") + "," + c.CreateParameterName("value") + ");",
                            new System.Data.IDbDataParameter[]{
                                c.Pool.CreateParameter(c.CreateParameterName("id"),id),
                                c.Pool.CreateParameter(c.CreateParameterName("value"),value)
                            });
        }

        internal void AssignMapValues(Type t, Dictionary<string, int> enumMap, Dictionary<int, string> reverseMap)
        {
            t = (t.IsGenericType ? t.GetGenericArguments()[0] : t);
            _enumReverseValuesMap.Remove(t);
            _enumValuesMap.Remove(t);
            _enumReverseValuesMap.Add(t, reverseMap);
            _enumValuesMap.Add(t, enumMap);
        }

        public string this[Type t]
        {
            get
            {
                t = (t.IsGenericType ? t.GetGenericArguments()[0] : t);
                if (_enumTableMaps.ContainsKey(t))
                    return _enumTableMaps[t];
                return null;
            }
        }

        internal void Add(Type type, string name)
        {
            type = (type.IsGenericType ? type.GetGenericArguments()[0] : type);
            _enumTableMaps.Add(type, name);
        }

        public int Count
        {
            get { return _enumTableMaps.Count; }
        }

        public Dictionary<Type, string>.KeyCollection Keys
        {
            get { return _enumTableMaps.Keys; }
        }

        internal void InsertEnumIntoTable(Type t, Connection conn)
        {
            t = (t.IsGenericType ? t.GetGenericArguments()[0] : t);
            foreach (string str in Enum.GetNames(t))
            {
                conn.ExecuteNonQuery(string.Format("INSERT INTO {0}({1}) VALUES({2});",
                        _enumTableMaps[t],
                        _pool.Translator.GetEnumValueFieldName(t, conn),
                        conn.CreateParameterName("value")),
                    new System.Data.IDbDataParameter[]{conn.CreateParameter(conn.CreateParameterName("value"),str)});
            }
            conn.Commit();
            LoadEnumsFromTable(t, conn);
        }

        internal void LoadEnumsFromTable(Type t, Connection conn)
        {
            t = (t.IsGenericType ? t.GetGenericArguments()[0] : t);
            conn.ExecuteQuery("SELECT * FROM " + _enumTableMaps[t]);
            Dictionary<string, int> vals = new Dictionary<string, int>();
            while (conn.Read())
                vals.Add(conn[1].ToString(), conn.GetInt32(0));
            conn.Close();
            if (_enumValuesMap.ContainsKey(t))
                _enumValuesMap.Remove(t);
            if (_enumReverseValuesMap.ContainsKey(t))
                _enumReverseValuesMap.Remove(t);
            if (vals.Count == 0)
                InsertEnumIntoTable(t, conn);
            else
            {
                foreach (string str in Enum.GetNames(t))
                {
                    if (!vals.ContainsKey(str))
                    {
                        vals = _SyncMissingValues(vals, t, conn);
                        break;
                    }
                }
                _enumValuesMap.Add(t, vals);
                Dictionary<int, string> revs = new Dictionary<int, string>();
                foreach (string str in vals.Keys)
                    revs.Add(vals[str], str);
                _enumReverseValuesMap.Add(t, revs);
            }
        }

        private Dictionary<string, int> _SyncMissingValues(Dictionary<string, int> vals, Type t, Connection conn)
        {
            t = (t.IsGenericType ? t.GetGenericArguments()[0] : t);
            string[] keys = new string[vals.Count];
            vals.Keys.CopyTo(keys, 0);
            foreach (string str in Enum.GetNames(t))
            {
                if (!vals.ContainsKey(str))
                {
                    conn.ExecuteNonQuery("INSERT INTO " + _enumTableMaps[t] + " VALUES(" + conn.CreateParameterName("id") + "," + conn.CreateParameterName("value") + ");",
                    new System.Data.IDbDataParameter[]{
                                conn.Pool.CreateParameter(conn.CreateParameterName("id"),null,Org.Reddragonit.Dbpro.Structure.Attributes.FieldType.INTEGER,4),
                                conn.CreateParameter(conn.CreateParameterName("value"),str)
                            });
                    conn.ExecuteQuery("SELECT ID FROM " + _enumTableMaps[t] + " WHERE " + _pool.Translator.GetEnumValueFieldName(t, conn) + " = " + conn.CreateParameterName("value"),
                        new IDbDataParameter[]{
                            conn.CreateParameter(conn.CreateParameterName("value"),str)
                        });
                    conn.Read();
                    vals.Add(str, conn.GetInt32(0));
                    conn.Close();
                }
            }
            return vals;
        }
    }
}
