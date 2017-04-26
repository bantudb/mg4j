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

/** A {@linkplain QueryTransformer query transformer} that just requires
 * implementing a method that {@linkplain #expand(Term) expands terms} (e.g., into
 * disjunctive queries).
 * 
 * <p>To implement more sophisticated behaviours, you can subclass the internal
 * class {@link AbstractTermExpander.ExpanderVisitor}, and override the method
 * {@link #expanderVisitor()} so that it returns a (possibly cached) instance of
 * your own visitor class (see, for an example, {@link MultiIndexTermExpander}).
 */

public abstract class AbstractTermExpander implements QueryTransformer {
	
	protected class ExpanderVisitor extends AbstractQueryBuilderVisitor<Query> {
		
		public ExpanderVisitor copy() {
			return new ExpanderVisitor();
		}

		public Query[] newArray( int len ) {
			return new Query[ len ];
		}

		public QueryBuilderVisitor<Query> prepare() { return this; }

		public Query visit( Term node ) throws QueryBuilderVisitorException {
			return expand( node );
		}

		public Query visit( Prefix node ) throws QueryBuilderVisitorException {
			return expand( node );
		}

		public Query visit( Range node ) throws QueryBuilderVisitorException {
			return node;
		}

		public Query visitPost( And node, Query[] subNode ) throws QueryBuilderVisitorException {
			return new And( subNode );
		}
		
		public Query visitPost( Consecutive node, Query[] subNode ) throws QueryBuilderVisitorException {
			return new Consecutive( subNode );
		}
		
		public Query visitPost( OrderedAnd node, Query[] subNode ) throws QueryBuilderVisitorException {
			return new OrderedAnd( subNode );
		}

		public Query visitPost( Difference node, Query[] subNode ) throws QueryBuilderVisitorException {
			return new Difference( subNode[ 0 ], subNode[ 1 ] );
		}

		public Query visitPost( Inclusion node, Query[] subNode ) throws QueryBuilderVisitorException {
			return new Inclusion( subNode[ 0 ], subNode[ 1 ] );
		}

		public Query visitPost( Containment node, Query[] subNode ) throws QueryBuilderVisitorException {
			return new Containment( subNode[ 0 ], subNode[ 1 ] );
		}

		public Query visitPost( LowPass node, Query subNode ) throws QueryBuilderVisitorException {
			return new LowPass( subNode, node.k );
		}

		public Query visitPost( Annotation node, Query subNode ) throws QueryBuilderVisitorException {
			return new Annotation( subNode );
		}

		public Query visitPost( Not node, Query subNode ) throws QueryBuilderVisitorException {
			return new Not( subNode );
		}

		public Query visitPost( Or node, Query[] subNode ) throws QueryBuilderVisitorException {
			return new Or( subNode );
		}

		public Query visitPost( Align node, Query[] subNode ) throws QueryBuilderVisitorException {
			return new Align( subNode[ 0 ], subNode[ 1 ] );
		}

		public Query visitPost( MultiTerm node, Query[] subNode ) throws QueryBuilderVisitorException {
			// TODO: is this semantics sensible?
			return new Or( subNode );
		}

		public Query visitPost( Select node, Query subNode ) throws QueryBuilderVisitorException {
			return new Select( node.index, subNode );
		}

		public Query visitPost( Remap node, Query subNode ) throws QueryBuilderVisitorException {
			return new Remap( subNode, node.indexRemapping );
		}

		public Query visitPost( Weight node, Query subNode ) throws QueryBuilderVisitorException {
			return new Weight( node.weight, subNode );
		}

		public Query visit( True node ) throws QueryBuilderVisitorException {
			return node;
		}

		public Query visit( False node ) throws QueryBuilderVisitorException {
			return node;
		}
	}
	
	/** Returns a new expander visitor.
	 * 
	 * @return a visitor performing the expansion.
	 */
	
	protected ExpanderVisitor expanderVisitor() {
		return new ExpanderVisitor();
	}
	
	public Query transform( final Query query ) {
		try {
			return query.accept( expanderVisitor() );
		}
		catch ( QueryBuilderVisitorException e ) {
			throw new RuntimeException( e );
		}
	}
	
	/** Expands a single term. 
	 * 
	 * @param term a term to be expanded.
	 * @return the resulting query.
	 */
	public abstract Query expand( Term term );
	
	/** Expands a prefix. 
	 * 
	 * @param prefix the prefix to be expanded.
	 * @return the resulting query.
	 */
	public abstract Query expand( Prefix prefix );
}
