using Org.Reddragonit.Dbpro.Connections.Parameters;
using Org.Reddragonit.Dbpro.Structure;
using System;
using System.Collections.Generic;
using System.Text;

namespace Org.Reddragonit.Dbpro.Connections.Triggers
{
    public interface IPostDeleteTrigger : ITrigger
    {
        void PostDeleteAll(Connection conn);
        void PostDelete(Connection conn, Table table);
        void PostDelete(Connection conn, Type tableType, SelectParameter[] parameters);
    }
}
