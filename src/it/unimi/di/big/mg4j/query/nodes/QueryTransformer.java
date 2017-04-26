package it.unimi.di.big.mg4j.query.nodes;

/*		 
 * MG4J: Managing Gigabytes for Java (big)
 *
 * Copyright (C) 2006-2016 Sebastiano Vigna 
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

/** A strategy for transforming queries.
 * 
 * <p>Instances of this class represent query transformation that are applied at the
 * structural level&mdash;that is, when the query has already been parsed and turned
 * into a composite {@link Query} object.
 * 
 * <p>If your only need it to replace terms, for instance, for query expansion, you
 * can subclass from {@link AbstractTermExpander}, which just requires specifying how
 * to expand a term or a prefix.
 */

public interface QueryTransformer {
	public Query transform( Query q );
}
