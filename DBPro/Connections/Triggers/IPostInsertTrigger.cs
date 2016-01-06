using Org.Reddragonit.Dbpro.Structure;
using System;
using System.Collections.Generic;
using System.Text;

namespace Org.Reddragonit.Dbpro.Connections.Triggers
{
    public interface IPostInsertTrigger : ITrigger
    {
        void PostInsert(Connection conn, Table table);
    }
}
