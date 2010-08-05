using System;
using System.Collections.Generic;
using System.Text;

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

        internal override string ConstructString(Org.Reddragonit.Dbpro.Structure.Mapping.TableMap map, Connection conn, QueryBuilder builder, ref List<System.Data.IDbDataParameter> queryParameters, ref int parCount)
        {
            return "NOT ( " + _negatedParameter.ConstructString(map, conn, builder, ref queryParameters, ref parCount) + " ) ";
        }
    }
}
