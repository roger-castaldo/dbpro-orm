using System;
using System.Collections.Generic;
using System.Text;
using Org.Reddragonit.Dbpro.Connections.PoolComponents;
using Org.Reddragonit.Dbpro.Virtual;

namespace Org.Reddragonit.Dbpro.Connections.Parameters
{
    public class NotParameter : SelectParameter
    {
        private SelectParameter _negatedParameter;

        public NotParameter(SelectParameter negatedParameter)
        {
            _negatedParameter = negatedParameter;
        }

        internal override List<string> Fields
        {
            get { return _negatedParameter.Fields; }
        }

        internal override string ConstructString(Type tableType, ConnectionPool pool, QueryBuilder builder, ref List<System.Data.IDbDataParameter> queryParameters, ref int parCount)
        {
            return "NOT ( " + _negatedParameter.ConstructString(tableType, pool, builder, ref queryParameters, ref parCount) + " ) ";
        }

        internal override string ConstructClassViewString(ClassViewAttribute cva, ConnectionPool pool, QueryBuilder builder, ref List<System.Data.IDbDataParameter> queryParameters, ref int parCount)
        {
            return "NOT ( " + _negatedParameter.ConstructClassViewString(cva, pool, builder, ref queryParameters, ref parCount) + " ) ";
        }
    }
}
