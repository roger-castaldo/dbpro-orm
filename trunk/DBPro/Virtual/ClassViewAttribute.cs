using System;
using System.Collections.Generic;
using System.Text;
using Org.Reddragonit.Dbpro.Connections;
using Org.Reddragonit.Dbpro.Connections.ClassSQL;

namespace Org.Reddragonit.Dbpro.Virtual
{
    [AttributeUsage(AttributeTargets.Class)]
    public class ClassViewAttribute : Attribute
    {
        private string _connectionName;
        public string ConnectionName
        {
            get { return _connectionName; }
        }

        private string _namespace;
        public string Namespace
        {
            get { return _namespace; }
        }

        private string _classQuery;
        public string ClassQuery
        {
            get { return _classQuery; }
        }

        private ClassQuery _query;
        public ClassQuery Query
        {
            get {
                _query = (_query == null ? new ClassQuery(_namespace, _classQuery) : _query);
                return _query; 
            }
        }

        public ClassViewAttribute(string nameSpace, string classQuery)
            : this(null,nameSpace,classQuery)
        {}

        public ClassViewAttribute(string connectionName,string nameSpace, string classQuery)
        {
            _connectionName = (connectionName == null ? ConnectionPoolManager.DEFAULT_CONNECTION_NAME : connectionName);
            _namespace = nameSpace;
            _classQuery = classQuery;
        }
    }
}
