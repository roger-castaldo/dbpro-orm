using System;
using System.Collections.Generic;
using System.Text;

namespace Org.Reddragonit.Dbpro
{
    [AttributeUsage(AttributeTargets.Property|AttributeTargets.Method)]
    public class CompleteLazyLoadPriorToCall : Attribute
    {
    }
}
