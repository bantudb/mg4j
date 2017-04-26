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

/** A term expander that replaces every term or prefix with a disjunction of
 *  queries; each query is made by the same term or prefix 
 *  preceded by a selection over a different index.
 */

public class MultiIndexTermExpander extends AbstractTermExpander {
	/** The list of index names that will be used to expand the term. */
	public final String[] index;
	/** A copy of the length of {@link #index}. */
	public final int n;

	protected class ExpanderVisitor extends AbstractTermExpander.ExpanderVisitor {
		int dontExpand;
		
		public ExpanderVisitor copy() {
			return new ExpanderVisitor();
		}

		@Override
		public boolean visitPre( Consecutive node ) {
			dontExpand++;
			return true;
		}
		
		public Query visitPost( Consecutive node, Query[] subNode ) throws QueryBuilderVisitorException {
			dontExpand--;
			return expand( node );
		}

		@Override
		public boolean visitPre( OrderedAnd node ) {
			dontExpand++;
			return true;
		}
		
		public Query visitPost( OrderedAnd node, Query[] subNode ) throws QueryBuilderVisitorException {
			dontExpand--;
			return expand( node );
		}
	}
	
	private final ExpanderVisitor expanderVisitor = new ExpanderVisitor();
	
	@Override
	protected ExpanderVisitor expanderVisitor() {
		return expanderVisitor;
	}

	/** Creates a new multi-index term expander.
	 * 
	 * @param index a list of index names that will be used to expand the term.
	 */
	public MultiIndexTermExpander( String... index ) {
		this.index = index;
		n = index.length;
	}

	protected Query expand( Consecutive consecutive ) {
		Query query[] = new Query[ n ];
		for( int i = 0; i < n; i++ ) query[ i ] = new Select( index[ i ], consecutive );
		return new Or( query );
	}
	
	protected Query expand( OrderedAnd orderedAnd ) {
		Query query[] = new Query[ n ];
		for( int i = 0; i < n; i++ ) query[ i ] = new Select( index[ i ], orderedAnd );
		return new Or( query );
	}
	
	@Override
	public Query expand( Term term ) {
		Query query[] = new Query[ n ];
		for( int i = 0; i < n; i++ ) query[ i ] = new Select( index[ i ], term );
		return new Or( query );
	}

	@Override
	public Query expand( Prefix prefix ) {
		Query query[] = new Query[ n ];
		for( int i = 0; i < n; i++ ) query[ i ] = new Select( index[ i ], prefix );
		return new Or( query );
	}

}
