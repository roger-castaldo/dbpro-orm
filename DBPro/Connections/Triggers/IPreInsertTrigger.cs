using Org.Reddragonit.Dbpro.Structure;
using System;
using System.Collections.Generic;
using System.Text;

namespace Org.Reddragonit.Dbpro.Connections.Triggers
{
    public interface IPreInsertTrigger : ITrigger
    {
        void PreInsert(Connection conn, Table table, out bool abort);
    }
}
