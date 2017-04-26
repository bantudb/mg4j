package it.unimi.di.big.mg4j.index;

/*		 
 * MG4J: Managing Gigabytes for Java (big)
 *
 * Copyright (C) 2005-2016 Sebastiano Vigna 
 *
 *  This library is free software; you can redistribute it and/or modify it
 *  under the terms of the GNU Lesser General Public License as published by the Free
 *  Software Foundation; either version 3 of the License, or (at your option)
 *  any later version.
 *
 *  This library is distributed in the hope that it will be useful, but
 *  WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
 *  or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License
 *  for more details.
 *
 *  You should have received a copy of the GNU Lesser General Public License
 *  along with this program; if not, see <http://www.gnu.org/licenses/>.
 *
 */

import it.unimi.dsi.lang.FlyweightPrototype;
import it.unimi.dsi.lang.MutableString;

import java.io.Serializable;

/** A term processor, implementing term/prefix transformation and possibly term/prefix filtering.
 * 
 * <p>Index contruction requires sometimes modifications of
 * the given terms: downcasing, stemming, and so on. The same
 * transformation must be applied to terms in a query. This
 * interface provides a uniform way to perform arbitrary term
 * transformations.
 * 
 * <p>Index construction requires also term filtering: 
 * {@link #processTerm(MutableString)} may
 * return false, indicating that the term should not
 * be processed at all (e.g., because it is a stopword).
 *
 * <p>Additionally, the method {@link #processPrefix(MutableString)} may
 * process analogously a prefix (used for prefix queries).
 * 
 * <p>Implementation are encouraged to expose a singleton, when
 * possible, by means of the static factory method <code>getInstance()</code>.
 * 
 * <strong>Note</strong>: When merging multiple indices, MG4J checks that all
 * components use the same term processor. Please implement correctly {@link #equals(Object)}.
 * 
 * <strong>Warning</strong>: implementations of this class are not required
 * to be thread-safe, but they provide {@link it.unimi.dsi.lang.FlyweightPrototype flyweight copies}.
 * The {@link #copy()} method is strengthened so to return a instance of this class.
 * 
 * <p>This interface was originally suggested by Fabien Campagne.
 */
public interface TermProcessor extends Serializable, FlyweightPrototype<TermProcessor> {
	/** Processes the given term, leaving the result in the same mutable string.
	 * 
	 * @param term a mutable string containing the term to be processed, 
	 * or <code>null</code>.
	 * @return true if the term is not <code>null</code> and should be indexed, false otherwise.
	 */
	public boolean processTerm( MutableString term );

	/** Processes the given prefix, leaving the result in the same mutable string.
	 * 
	 * <p>This method is not used during the indexing phase, but rather at query
	 * time. If the user wants to specify a prefix query, it is sometimes necessary
	 * to transform the prefix 
	 * (e.g., {@linkplain DowncaseTermProcessor#processPrefix(MutableString)} downcasing it).
	 * 
	 * <p>It is of course unlikely that this method returns false, as it is usually not
	 * possible to foresee which are the prefixes of indexable words. In case no natural
	 * transformation applies, this method should leave its argument unchanged.
	 * 
	 * @param prefix a mutable string containing a prefix to be processed, 
	 * or <code>null</code>.
	 * @return true if the prefix is not <code>null</code> and there might be an indexed
	 * word starting with <code>prefix</code>, false otherwise.
	 */
	public boolean processPrefix( MutableString prefix );
	
	public TermProcessor copy();
}
