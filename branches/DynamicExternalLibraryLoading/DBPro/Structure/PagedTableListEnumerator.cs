/*
 * Created by SharpDevelop.
 * User: Roger
 * Date: 31/03/2009
 * Time: 9:12 PM
 * 
 * To change this template use Tools | Options | Coding | Edit Standard Headers.
 */
using System;
using System.Collections.Generic;

namespace Org.Reddragonit.Dbpro.Structure
{
	/// <summary>
	/// Description of PagedTableEnumerator.
	/// </summary>
	public class PagedTableListEnumerator : IEnumerator<Table>
	{
		private PagedTableList _list;
		int pos=-1;
		
		public PagedTableListEnumerator(PagedTableList list)
		{
			_list=list;
		}
		
		public Table Current {
			get {
				if (pos<0)
					throw new Exception("Cannot access list without exeuting movenext first.");
				if (pos==_list.Count)
					throw new Exception("Index out of bounds.");
				return _list[pos];
			}
		}
		
		object System.Collections.IEnumerator.Current {
			get {
				if (pos<0)
					throw new Exception("Cannot access list without exeuting movenext first.");
				if (pos==_list.Count)
					throw new Exception("Index out of bounds.");
				return _list[pos];
			}
		}
		
		void IDisposable.Dispose()
		{}
		
		public bool MoveNext()
		{
			pos++;
			return pos<_list.Count;
		}
		
		public void Reset()
		{
			pos=0;
		}
	}
}
