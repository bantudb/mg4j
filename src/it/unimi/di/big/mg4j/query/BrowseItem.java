package it.unimi.di.big.mg4j.query;
/*		 
 * MG4J: Managing Gigabytes for Java (big)
 *
 * Copyright (C) 2005-2016 Sebastiano Vigna 
 *
 *  This program is free software; you can redistribute it and/or modify it
 *  under the terms of the GNU General Public License as published by the Free
 *  Software Foundation; either version 3 of the License, or (at your option)
 *  any later version.
 *
 *  This program is distributed in the hope that it will be useful, but
 *  WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
 *  or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
 *  for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program; if not, see <http://www.gnu.org/licenses/>.
 *
 */

/** An instance of this class is used to pack the results gathered by {@link it.unimi.di.big.mg4j.query.QueryServlet}
 * in such a way that they are easily accessible from the Velocity Template Language.
 * 
 * @author Sebastiano Vigna
 * @since 0.9.2
 */

public class BrowseItem {
	/** The (possibly marked) title. */
	public CharSequence title;
	/** The result URI. */
	public CharSequence uri;
	/** The marked text. */
	public CharSequence text;
	/** A non-marked version of the title (for IMG ALT attributes). */
	public CharSequence alt;
	
	public BrowseItem() {}
		
	public final CharSequence alt() {
		return alt;
	}

	public CharSequence text() {
		return text;
	}
	
	public CharSequence title() {
		return title;
	}
	
	public CharSequence uri() {
		return uri;
	}
	
	public String toString() {
		return "[title: " + title + " uri:" + uri + "]";
	}
	
}
