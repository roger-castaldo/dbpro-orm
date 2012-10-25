using System;
using System.Collections.Generic;
using System.Text;

namespace Org.Reddragonit.Dbpro.Connections.PoolComponents
{
    internal class EnumsHandler
    {
        internal Dictionary<Type, string> _enumTableMaps = new Dictionary<Type, string>();
        internal Dictionary<Type, Dictionary<string, int>> _enumValuesMap = new Dictionary<Type, Dictionary<string, int>>();
        internal Dictionary<Type, Dictionary<int, string>> _enumReverseValuesMap = new Dictionary<Type, Dictionary<int, string>>();

        public EnumsHandler()
        {
            _enumTableMaps = new Dictionary<Type, string>();
            _enumValuesMap = new Dictionary<Type, Dictionary<string, int>>();
            _enumReverseValuesMap = new Dictionary<Type, Dictionary<int, string>>();
        }

        public object GetEnumValue(Type enumType, int ID)
        {
            return Enum.Parse(enumType, _enumReverseValuesMap[enumType][ID]);
        }

        public int GetEnumID(Type enumType, string enumName)
        {
            return _enumValuesMap[enumType][enumName];
        }

        internal void WipeOutEnums(Connection c)
        {
            foreach (Type t in _enumTableMaps.Keys)
                c.ExecuteNonQuery("DELETE FROM " + _enumTableMaps[t]);
        }

        internal void InsertToDB(Type t, int id, string value, Connection c)
        {
            c.ExecuteNonQuery("INSERT INTO " + _enumTableMaps[t] + " VALUES(" + c.CreateParameterName("id") + "," + c.CreateParameterName("value") + ");",
                            new System.Data.IDbDataParameter[]{
                                c.CreateParameter(c.CreateParameterName("id"),id),
                                c.CreateParameter(c.CreateParameterName("value"),value)
                            });
        }

        internal void AssignMapValues(Type t, Dictionary<string, int> enumMap, Dictionary<int, string> reverseMap)
        {
            _enumReverseValuesMap.Remove(t);
            _enumValuesMap.Remove(t);
            _enumReverseValuesMap.Add(t, reverseMap);
            _enumValuesMap.Add(t, enumMap);
        }

        public string this[Type t]
        {
            get
            {
                if (_enumTableMaps.ContainsKey(t))
                    return _enumTableMaps[t];
                return null;
            }
        }

        internal void Add(Type type, string name)
        {
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
    }
}
