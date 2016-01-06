using Org.Reddragonit.Dbpro.Connections.Parameters;
using Org.Reddragonit.Dbpro.Structure;
using System;
using System.Collections.Generic;
using System.Text;

namespace Org.Reddragonit.Dbpro.Connections.Triggers
{
    public interface IPreUpdateTrigger : ITrigger
    {
        void PreUpdate(Connection conn, Table originalValue, Table newValue, List<string> changedFields, out bool abort);
        void PreUpdate(Connection conn, Type tableType, Dictionary<string, object> updateFields, SelectParameter[] parameters, out bool abort);
        
    }
}
