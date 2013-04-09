using System;
using System.Collections.Generic;
using System.Text;
using Org.Reddragonit.Dbpro.Connections.PoolComponents;

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

        internal override string ConstructString(Type tableType, Connection conn, QueryBuilder builder, ref List<System.Data.IDbDataParameter> queryParameters, ref int parCount)
        {
            return "NOT ( " + _negatedParameter.ConstructString(tableType, conn, builder, ref queryParameters, ref parCount) + " ) ";
        }

        internal override string ConstructVirtualTableString(sTable tbl, Connection conn, QueryBuilder builder, ref List<System.Data.IDbDataParameter> queryParameters, ref int parCount)
        {
            return "NOT ( " + _negatedParameter.ConstructVirtualTableString(tbl, conn, builder, ref queryParameters, ref parCount) + " ) ";
        }
    }
}
