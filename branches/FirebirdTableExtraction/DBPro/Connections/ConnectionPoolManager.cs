using System;
using System.Collections.Generic;
using System.Text;

namespace Org.Reddragonit.Dbpro.Connections
{
    static class ConnectionPoolManager
    {

        private static Dictionary<string, ConnectionPool> _connectionPools = new Dictionary<string, ConnectionPool>();

        public static ConnectionPool GetConnection(string name)
        {
            if (_connectionPools.ContainsKey(name))
            {
                return _connectionPools[name];
            }
            return null;
        }

        public static void AddConnection(string name, ConnectionPool pool)
        {
            if (_connectionPools.ContainsKey(name))
            {
                _connectionPools.Remove(name);
            }
            _connectionPools.Add(name, pool);
        }

    }
}
