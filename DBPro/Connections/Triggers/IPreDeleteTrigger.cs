using Org.Reddragonit.Dbpro.Connections.Parameters;
using Org.Reddragonit.Dbpro.Structure;
using System;
using System.Collections.Generic;
using System.Text;

namespace Org.Reddragonit.Dbpro.Connections.Triggers
{
    public interface IPreDeleteTrigger : ITrigger
    {
        void PreDelete(Connection conn, Table table, out bool abort);
        void PreDelete(Connection conn, Type tableType, SelectParameter[] parameters, out bool abort);
        void PreDeleteAll(Connection conn, out bool abort);
    }
}
