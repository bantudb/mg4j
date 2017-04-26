package it.unimi.di.big.mg4j.mock.search;

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

import it.unimi.di.big.mg4j.index.Index;
import it.unimi.di.big.mg4j.index.IndexIterator;
import it.unimi.di.big.mg4j.index.TooManyTermsException;
import it.unimi.di.big.mg4j.index.payload.Payload;
import it.unimi.di.big.mg4j.query.nodes.AbstractQueryBuilderVisitor;
import it.unimi.di.big.mg4j.query.nodes.Align;
import it.unimi.di.big.mg4j.query.nodes.And;
import it.unimi.di.big.mg4j.query.nodes.Annotation;
import it.unimi.di.big.mg4j.query.nodes.Consecutive;
import it.unimi.di.big.mg4j.query.nodes.Containment;
import it.unimi.di.big.mg4j.query.nodes.Difference;
import it.unimi.di.big.mg4j.query.nodes.False;
import it.unimi.di.big.mg4j.query.nodes.Inclusion;
import it.unimi.di.big.mg4j.query.nodes.LowPass;
import it.unimi.di.big.mg4j.query.nodes.MultiTerm;
import it.unimi.di.big.mg4j.query.nodes.Not;
import it.unimi.di.big.mg4j.query.nodes.Or;
import it.unimi.di.big.mg4j.query.nodes.OrderedAnd;
import it.unimi.di.big.mg4j.query.nodes.Prefix;
import it.unimi.di.big.mg4j.query.nodes.QueryBuilderVisitorException;
import it.unimi.di.big.mg4j.query.nodes.Range;
import it.unimi.di.big.mg4j.query.nodes.Remap;
import it.unimi.di.big.mg4j.query.nodes.Select;
import it.unimi.di.big.mg4j.query.nodes.Term;
import it.unimi.di.big.mg4j.query.nodes.True;
import it.unimi.di.big.mg4j.query.nodes.Weight;
import it.unimi.di.big.mg4j.search.AnnotationDocumentIterator;
import it.unimi.di.big.mg4j.search.DocumentIterator;
import it.unimi.di.big.mg4j.search.FalseDocumentIterator;
import it.unimi.di.big.mg4j.search.RemappingDocumentIterator;
import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import it.unimi.dsi.fastutil.objects.Object2ReferenceMap;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.objects.Reference2ReferenceArrayMap;
import it.unimi.dsi.fastutil.objects.Reference2ReferenceMap;
import it.unimi.dsi.fastutil.objects.Reference2ReferenceMaps;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.NoSuchElementException;

/** A {@link it.unimi.di.big.mg4j.query.nodes.QueryBuilderVisitor}
 * that builds a {@link it.unimi.di.big.mg4j.search.DocumentIterator}
 * resolving the queries using the objects in {@link it.unimi.di.big.mg4j.search}.
 * 
 * <p>This elementary builder visitor invokes {@link it.unimi.di.big.mg4j.index.Index#documents(CharSequence)}
 * to build the leaf {@linkplain it.unimi.di.big.mg4j.index.IndexIterator index iterators}. Thus, the
 * resulting {@link it.unimi.di.big.mg4j.search.DocumentIterator} should be carefully
 * {@linkplain it.unimi.di.big.mg4j.search.DocumentIterator#dispose() disposed} after usage (every
 * index iterator may open a file or a socket).
 * 
 * <p>{@link Prefix} and {@link MultiTerm} nodes cause the creation of a {@link MultiTermIndexIterator},
 * in the first case by calling {@link it.unimi.di.big.mg4j.index.Index#documents(CharSequence,int)} and
 * in the second case by creating a {@link MultiTermIndexIterator} with the name and frequency equal to the
 * maximum frequency over all terms. Other implementations might choose differently.
 * 
 * <p>At construction time, you must provide a map from strings to indices that will be used to resolve
 * {@link it.unimi.di.big.mg4j.query.nodes.Select} nodes. The map may be <code>null</code>, in which case
 * such nodes will cause an {@link java.lang.IllegalArgumentException}. 
 * If a {@link it.unimi.di.big.mg4j.query.nodes.Select}
 * node contains an index name that does not appear in the map a {@link NoSuchElementException}
 * will be thrown instead.
 * 
 * <p>A production site will likely substitute this builder visitor with one that reuses 
 * {@linkplain it.unimi.di.big.mg4j.index.IndexReader index readers} out of a pool.
 * 
 * <p>Instances of this class may be safely reused by calling {@link #prepare()}.
 */

public class DocumentIteratorBuilderVisitor extends AbstractQueryBuilderVisitor<DocumentIterator> {
	
	/** A map associating a textual key to indices. */
	protected final Object2ReferenceMap<String, Index> indexMap;
	/** A map associating an object with a <code>parse(String)</code> method to each payload-based index. */
	protected final Reference2ReferenceMap<Index, Object> index2Parser;
	/** The default index. */
	protected final Index defaultIndex;
	/** The number of documents (fetched from the default index). */
	protected final long numberOfDocuments;
	/** The limit on prefix queries provided in the constructor. */
	protected final int limit;
	/** The stack of selected indices (changed by {@link Select} nodes). */
	protected ObjectArrayList<Index> curr;
	/** The stack of weights. */
	protected DoubleArrayList weights;
	/** The last seen, but still not consumed, weight, or {@link Double#NaN}. */
	protected double weight;
	
	/** Creates a new builder visitor.
	 * 
	 * @param indexMap a map from index names to indices, to be used in {@link Select} nodes, or <code>null</code>
	 * if the only used index is the default index.
	 * @param defaultIndex the default index.
	 * @param limit a limit that will be used with {@link Prefix} nodes.
	 */
	@SuppressWarnings("unchecked")
	public DocumentIteratorBuilderVisitor( final Object2ReferenceMap<String,Index> indexMap, final Index defaultIndex, final int limit ) {
		this( indexMap, Reference2ReferenceMaps.EMPTY_MAP, defaultIndex, limit );
	}
	
	/** Creates a new builder visitor with additional parsers for payload-based indices.
	 * 
	 * @param indexMap a map from index names to indices, to be used in {@link Select} nodes, or <code>null</code>
	 * if the only used index is the default index.
	 * @param defaultIndex the default index.
	 * @param limit a limit that will be used with {@link Prefix} nodes.
	 */
	public DocumentIteratorBuilderVisitor( final Object2ReferenceMap<String,Index> indexMap, final Reference2ReferenceMap<Index,Object> index2Parser, final Index defaultIndex, final int limit ) {
		this.indexMap = indexMap;
		this.defaultIndex = defaultIndex;
		this.index2Parser = index2Parser;
		this.limit = limit;
		weights = new DoubleArrayList();
		weight = Double.NaN;
		curr = new ObjectArrayList<Index>();
		curr.push( defaultIndex );
		this.numberOfDocuments = defaultIndex.numberOfDocuments;
	}
	
	/** Pushes {@link #weight}, if it is not {@link Double#NaN}, or 1, otherwise, on the {@linkplain #weights stack of weights}; in either case, sets {@link #weight} to {@link Double#NaN}.
	 */

	protected void pushWeight() {
		weights.push( Double.isNaN( weight ) ? 1 : weight );
		weight = Double.NaN;
	}

	/** Returns {@link #weight}, if it is not {@link Double#NaN}, or 1, otherwise; in either case, sets {@link #weight} to {@link Double#NaN}.
	 * 
	 * @return {@link #weight}, if it is not {@link Double#NaN}, or 1, otherwise.
	 */

	protected double weight() {
		final double result = Double.isNaN( weight ) ? 1 : weight;
		weight = Double.NaN;
		return result;
	}

	public DocumentIteratorBuilderVisitor copy() {
		return new DocumentIteratorBuilderVisitor( indexMap, defaultIndex, limit );
	}
	
	public DocumentIteratorBuilderVisitor prepare() {
		curr.size( 1 );
		weights.size( 0 );
		weight = Double.NaN;
		return this;
	}

	public DocumentIterator[] newArray( final int len ) { return new DocumentIterator[ len ]; }

	public DocumentIterator visit( final Term node ) throws QueryBuilderVisitorException {
		try {
			if ( node.termNumber != -1 ) return curr.top().documents( node.termNumber ).weight( weight() );
			return curr.top().documents( node.term ).weight( weight() );
		}
		catch ( IOException e ) {
			throw new QueryBuilderVisitorException( e );
		}
	}
	
	public DocumentIterator visit( final Prefix node ) throws QueryBuilderVisitorException {
		try {
			return curr.top().documents( node.prefix, limit ).weight( weight() );
		}
		catch ( IOException e ) {
			throw new QueryBuilderVisitorException( e );
		}
		catch ( TooManyTermsException e ) {
			throw new QueryBuilderVisitorException( e );
		}
	}
		
	public DocumentIterator visit( Range node ) throws QueryBuilderVisitorException {
		final Index index = curr.top();
		if ( ! index.hasPayloads ) throw new IllegalStateException( "Index " + index + " does not have payloads" );
		try {
			final Object parser = index2Parser.containsKey( index ) ? index2Parser.get( index ) : index.payload;
			final Method method = parser.getClass().getMethod( "parse", String.class );
			final Payload left = index.payload.copy(), right = index.payload.copy();
			if ( node.left != null ) left.set( method.invoke( parser, node.left.toString() ) );
			if ( node.right != null ) right.set( method.invoke( parser, node.right.toString() ) );
			return PayloadPredicateDocumentIterator.getInstance( index.documents( 0 ), 
					index.payload.rangeFilter( node.left == null ? null : left, node.right == null ? null : right ) ).weight( weight() );
		}
		catch( InvocationTargetException e ) {
			throw new QueryBuilderVisitorException( e.getCause() );
		}
		catch ( Exception e ) {
			e.printStackTrace();
			throw new QueryBuilderVisitorException( e );
		}
	}
	
	public boolean visitPre( final And node ) throws QueryBuilderVisitorException {
		pushWeight();
		return true;
	}
	
	public DocumentIterator visitPost( final And node, final DocumentIterator[] subNode ) throws QueryBuilderVisitorException {
		try {
			return AndDocumentIterator.getInstance( curr.top(), subNode ).weight( weights.popDouble() );
		}
		catch ( IOException e ) {
			throw new QueryBuilderVisitorException( e );
		}
	}

	public boolean visitPre( final Consecutive node ) throws QueryBuilderVisitorException {
		pushWeight();
		return true;
	}
	
	public DocumentIterator visitPost( final Consecutive node, final DocumentIterator[] subNode ) throws QueryBuilderVisitorException {
		try {
			return ConsecutiveDocumentIterator.getInstance( subNode, node.gap ).weight( weights.popDouble() );
		}
		catch ( IOException e ) {
			throw new QueryBuilderVisitorException( e );
		}
	}

	public boolean visitPre( final LowPass node ) throws QueryBuilderVisitorException {
		pushWeight();
		return true;
	}
	
	public DocumentIterator visitPost( final LowPass node, final DocumentIterator subNode ) {
		return LowPassDocumentIterator.getInstance( subNode, node.k ).weight( weights.popDouble() );
	}

	public boolean visitPre( final Annotation node ) throws QueryBuilderVisitorException {
		pushWeight();
		return true;
	}
	
	// ALERT: this should be mocked
	public DocumentIterator visitPost( final Annotation node, final DocumentIterator subNode ) {
		return AnnotationDocumentIterator.getInstance( (IndexIterator)subNode ).weight( weights.popDouble() );
	}

	public boolean visitPre( final Not node ) throws QueryBuilderVisitorException {
		pushWeight();
		return true;
	}
	

	public DocumentIterator visitPost( final Not node, final DocumentIterator subNode ) throws QueryBuilderVisitorException {
		return NotDocumentIterator.getInstance( subNode, numberOfDocuments ).weight( weights.popDouble() );
	}

	public boolean visitPre( final Or node ) throws QueryBuilderVisitorException {
		pushWeight();
		return true;
	}
	
	public DocumentIterator visitPost( final Or node, final DocumentIterator[] subNode ) throws QueryBuilderVisitorException {
		try {
			return OrDocumentIterator.getInstance( curr.top(), subNode ).weight( weights.popDouble() );
		}
		catch ( IOException e ) {
			throw new QueryBuilderVisitorException( e );
		}
	}
	
	public boolean visitPre( final OrderedAnd node ) throws QueryBuilderVisitorException {
		pushWeight();
		return true;
	}
	
	public DocumentIterator visitPost( final OrderedAnd node, final DocumentIterator[] subNode ) throws QueryBuilderVisitorException {
		try {
			return OrderedAndDocumentIterator.getInstance( curr.top(), subNode ).weight( weights.popDouble() );
		}
		catch ( IOException e ) {
			throw new QueryBuilderVisitorException( e );
		}
	}
	
	public boolean visitPre( final Align node ) throws QueryBuilderVisitorException {
		pushWeight();
		return true;
	}

	public DocumentIterator visitPost( final Align node, final DocumentIterator[] subNode ) throws QueryBuilderVisitorException {
		try {
			return AlignDocumentIterator.getInstance( subNode[ 0 ], subNode[ 1 ] ).weight( weights.popDouble() );
		} catch ( IOException e ) {
			throw new QueryBuilderVisitorException( e );
		}
	}

	public boolean visitPre( final Difference node ) throws QueryBuilderVisitorException {
		pushWeight();
		return true;
	}

	public DocumentIterator visitPost( final Difference node, final DocumentIterator[] subNode ) throws QueryBuilderVisitorException {
		try {
			return DifferenceDocumentIterator.getInstance( subNode[ 0 ], subNode[ 1 ], node.leftMargin, node.rightMargin ).weight( weights.popDouble() );
		} catch ( IOException e ) {
			throw new QueryBuilderVisitorException( e );
		}
	}

	public boolean visitPre( final Inclusion node ) throws QueryBuilderVisitorException {
		pushWeight();
		return true;
	}

	public DocumentIterator visitPost( final Inclusion node, final DocumentIterator[] subNode ) throws QueryBuilderVisitorException {
		try {
			return InclusionDocumentIterator.getInstance( subNode[ 0 ], subNode[ 1 ], node.leftMargin, node.rightMargin ).weight( weights.popDouble() );
		} catch ( IOException e ) {
			throw new QueryBuilderVisitorException( e );
		}
	}

	public boolean visitPre( final Containment node ) throws QueryBuilderVisitorException {
		pushWeight();
		return true;
	}

	public DocumentIterator visitPost( final Containment node, final DocumentIterator[] subNode ) throws QueryBuilderVisitorException {
		try {
			return ContainmentDocumentIterator.getInstance( subNode[ 0 ], subNode[ 1 ], node.leftMargin, node.rightMargin ).weight( weights.popDouble() );
		} catch ( IOException e ) {
			throw new QueryBuilderVisitorException( e );
		}
	}

	public boolean visitPre( final MultiTerm node ) throws QueryBuilderVisitorException {
		pushWeight();
		return true;
	}
	
	public DocumentIterator visitPost( final MultiTerm node, final DocumentIterator subNode[] ) throws QueryBuilderVisitorException {
		final IndexIterator[] indexIterator = new IndexIterator[ subNode.length ];
		System.arraycopy( subNode, 0, indexIterator, 0, indexIterator.length );
		IndexIterator result;
		try {
			result = MultiTermIndexIterator.getInstance( curr.top(), indexIterator ).weight( weights.popDouble() );
		}
		catch ( IOException e ) {
			throw new QueryBuilderVisitorException( e );
		}
		result.term( node.toString() );
		return result;
	}

	public boolean visitPre( final Select node ) throws QueryBuilderVisitorException {
		if ( indexMap == null ) throw new IllegalArgumentException( "You cannot use Select nodes without an index map" );
		final Index index = indexMap.get( node.index.toString() );
		if ( index == null ) throw new NoSuchElementException( "The selected index (" + node.index + ")" + " does not appear in the index map (" + indexMap + ")" ); 
		curr.push( indexMap.get( node.index.toString() ) );
		return true;
	}

	public DocumentIterator visitPost( final Select node, final DocumentIterator subNode ) {
		curr.pop();
		return subNode;
	}

	public boolean visitPre( final Remap node ) throws QueryBuilderVisitorException {
		pushWeight();
		return true;
	}

	public DocumentIterator visitPost( final Remap node, final DocumentIterator subNode ) {
		if ( indexMap == null ) throw new IllegalArgumentException( "You cannot use Remap nodes without an index map" );
		final Reference2ReferenceArrayMap<Index, Index> indexInverseRemapping = new Reference2ReferenceArrayMap<Index, Index>( node.indexInverseRemapping.size() );
		for( Map.Entry<String,String> e: node.indexInverseRemapping.entrySet() ) {
			final Index externalIndex = indexMap.get( e.getKey() );
			final Index internalIndex = indexMap.get( e.getValue() );
			if ( internalIndex == null ) throw new NoSuchElementException( "The internal index \"" + e.getValue() + "\" does not appear in the index map (" + indexMap + ")" ); 
			if ( externalIndex == null ) throw new NoSuchElementException( "The external index \"" + e.getKey() + "\" does not appear in the index map (" + indexMap + ")" ); 
			indexInverseRemapping.put( externalIndex, internalIndex );
		}
		return new RemappingDocumentIterator( subNode, indexInverseRemapping );
	}

	public boolean visitPre( final Weight node ) throws QueryBuilderVisitorException {
		weight = node.weight;
		return true;
	}

	public DocumentIterator visitPost( final Weight node, final DocumentIterator subNode ) {
		return subNode;
	}

	public DocumentIterator visit( True node ) throws QueryBuilderVisitorException {
		return TrueDocumentIterator.getInstance( curr.top() ); 
	}

	public DocumentIterator visit( False node ) throws QueryBuilderVisitorException {
		return FalseDocumentIterator.getInstance( curr.top() ); 
	}
}
