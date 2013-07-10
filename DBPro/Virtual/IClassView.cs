using System;
using System.Collections.Generic;
using System.Text;
using Org.Reddragonit.Dbpro.Connections;

namespace Org.Reddragonit.Dbpro.Virtual
{
    public interface IClassView
    {
        void LoadFromRow(ViewResultRow row);
    }
}
