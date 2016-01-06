using Org.Reddragonit.Dbpro.Connections.Parameters;
using Org.Reddragonit.Dbpro.Structure;
using System;
using System.Collections.Generic;
using System.Text;

namespace Org.Reddragonit.Dbpro.Connections.Triggers
{
    public interface IPostUpdateTrigger : ITrigger
    {
        void PostUpdate(Connection conn, Table originalValue, Table newValue, List<string> changedFields);
        void PostUpdate(Connection conn, Type tableType, Dictionary<string, object> updateFields, SelectParameter[] parameters);
    }
}
