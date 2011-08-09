using System;
using System.Collections.Generic;
using System.Text;
using Org.Reddragonit.Dbpro.Structure;
using Org.Reddragonit.Dbpro.Connections.Parameters;

namespace Org.Reddragonit.Dbpro.Connections
{
    public interface ITrigger
    {
        void PostUpdate(Table originalValue,Table newValue, List<string> changedFields);
        void PostUpdate(Type tableType, Dictionary<string, object> updateFields, SelectParameter[] parameters);
        void PreUpdate(Table originalValue,Table newValue, List<string> changedFields);
        void PreUpdate(Type tableType, Dictionary<string, object> updateFields, SelectParameter[] parameters);
        void PreDelete(Table table);
        void PostDelete(Table table);
        void PreDeleteAll();
        void PostDeleteAll();
        void PreInsert(Table table);
        void PostInsert(Table table);
    }
}
