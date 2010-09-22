using System;
using System.Collections.Generic;
using System.Text;
using Org.Reddragonit.Dbpro.Structure;

namespace Org.Reddragonit.Dbpro.Connections
{
    public interface ITrigger
    {
        void PostUpdate(Table table, List<string> changedFields);
        void PreUpdate(Table table, List<string> changedFields);
        void PreDelete(Table table);
        void PostDelete(Table table);
        void PreDeleteAll();
        void PostDeleteAll();
        void PreInsert(Table table);
        void PostInsert(Table table);
    }
}
