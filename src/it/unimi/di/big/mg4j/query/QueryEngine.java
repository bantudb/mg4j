package it.unimi.di.big.mg4j.query;

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

import static it.unimi.di.big.mg4j.search.DocumentIterator.END_OF_LIST;
import it.unimi.di.big.mg4j.index.Index;
import it.unimi.di.big.mg4j.query.nodes.Query;
import it.unimi.di.big.mg4j.query.nodes.QueryBuilderVisitor;
import it.unimi.di.big.mg4j.query.nodes.QueryBuilderVisitorException;
import it.unimi.di.big.mg4j.query.nodes.QueryTransformer;
import it.unimi.di.big.mg4j.query.parser.QueryParser;
import it.unimi.di.big.mg4j.query.parser.QueryParserException;
import it.unimi.di.big.mg4j.search.DocumentIterator;
import it.unimi.di.big.mg4j.search.score.AbstractAggregator;
import it.unimi.di.big.mg4j.search.score.DocumentScoreInfo;
import it.unimi.di.big.mg4j.search.score.LinearAggregator;
import it.unimi.di.big.mg4j.search.score.ScoredDocumentBoundedSizeQueue;
import it.unimi.di.big.mg4j.search.score.Scorer;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongSet;
import it.unimi.dsi.fastutil.objects.Object2ReferenceMap;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.objects.ObjectArrays;
import it.unimi.dsi.fastutil.objects.Reference2DoubleMap;
import it.unimi.dsi.fastutil.objects.Reference2DoubleOpenHashMap;
import it.unimi.dsi.fastutil.objects.Reference2ObjectArrayMap;
import it.unimi.dsi.fastutil.objects.Reference2ObjectMap;
import it.unimi.dsi.lang.FlyweightPrototype;
import it.unimi.dsi.lang.FlyweightPrototypes;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



/** An engine that takes a query and returns results, using a programmable
 * set of scorers and policies.
 * 
 * <p>This class embodies most of the work that must be done when answering a query.
 * Basically, {@link #process(String, int, int, ObjectArrayList) process(query,offset,length,results)} takes <code>query</code>,
 * parses it, turns it into a document iterator, scans the results, and deposits
 * <code>length</code> results starting at <code>offset</code> into the list <code>results</code>.
 * 
 * <p>There however several additional features available. First of all, either by separating
 * several queries with commas, or using directly {@link #process(Query[], int, int, ObjectArrayList)}
 * it is possible to resolve a series of queries with an &ldquo;and-then&rdquo; semantics: results
 * are added from each query, provided they did not appear before.
 * 
 * <p>It is possible to {@linkplain #score(Scorer[], double[]) score queries} using one or
 * more scorer with different weights (see {@link it.unimi.di.big.mg4j.search.score}), and also
 * set {@linkplain #setWeights(Reference2DoubleMap) different weights for different indices} (they 
 * will be passed to the scorers). The scorers influence the order when processing each query,
 * but results from different &ldquo;and-then&rdquo; queries are simply concatenated.
 * 
 * <p>When using multiple scorers, <em>{@linkplain #equalize(int) equalisation}</em> can be used
 * to avoid the problem associated with the potentially different value ranges of each scorer. Equalisation
 * evaluates a settable number of sample documents and normalize the scorers using the maximum value in
 * the sample. See {@link it.unimi.di.big.mg4j.search.score.AbstractAggregator} for some elaboration.
 * 
 * <p><em>{@linkplain #multiplex Multiplexing}</em> transforms a query <code><var>q</var></code> into <code>index0:<var>q</var> | index1:<var>q</var> &hellip;</code>.
 * In other words, the query is multiplexed on all available indices. Note that if inside <code><var>q</var></code>
 * there are selection operators that specify an index, the inner specification will overwrite
 * the external one, so that the semantics of the query is only amplified, but never contradicted.
 * 
 * <p>The results returned are instances of {@link it.unimi.di.big.mg4j.search.score.DocumentScoreInfo}. If
 * an {@linkplain #intervalSelector interval selector} has been set, 
 * the <code>info</code> field will contain a map from indices to arrays of {@linkplain it.unimi.di.big.mg4j.query.SelectedInterval selected intervals}
 * satisfying the query (see {@link it.unimi.di.big.mg4j.search} for some elaboration on minimal-interval semantics support in MG4J). 
 * 
 * <p>For examples of usage of this class, please look at {@link it.unimi.di.big.mg4j.query.Query}
 * and {@link it.unimi.di.big.mg4j.query.QueryServlet}.
 * 
 * <p><strong>Warning:</strong> This class is <strong>highly experimental</strong>. It has become
 * definitely more decent in MG4J, but still needs some refactoring.
 * 
 * <p><strong>Warning</strong>: This class is not
 * thread safe, but it provides {@linkplain it.unimi.dsi.lang.FlyweightPrototype flyweight copies}.
 * The {@link #copy()} method is strengthened so to return an object implementing this interface.
 * 
 * @author Sebastiano Vigna
 * @author Paolo Boldi
 * @since 1.0
 */

public class QueryEngine implements FlyweightPrototype<QueryEngine> {
	private static final Logger LOGGER = LoggerFactory.getLogger( QueryEngine.class );
	private static final boolean ASSERTS = false;
	
	/** The parser used to parse queries. */
	public final QueryParser queryParser;
	/** A map from names to indices. */
	public final Object2ReferenceMap<String,Index> indexMap;
	/** The number of indices used by {@link #queryParser}. */
	public final int numIndices;

	/** Whether multiplex is active. */
	public volatile boolean multiplex;
	/** The current interval selector, if any. */
	public volatile IntervalSelector intervalSelector;

	/** The current scorer, or {@code null} if no scorer is in use. */
	private Scorer scorer;
	/** The builder visitor used to make queries into document iterators. */
	private final QueryBuilderVisitor<DocumentIterator> builderVisitor;
	/** A map associating a weight with each index. */
	protected final Reference2DoubleOpenHashMap<Index> index2Weight;
	
	/** A transformer that will be applied to queries before resolving them, or {@code null}. */
	private QueryTransformer transformer;

	/** Creates a new query engine.
	 * 
	 * @param queryParser a query parser, or {@code null} if this query engine will {@linkplain #process(Query[], int, int, ObjectArrayList) just process pre-parsed queries}.
	 * @param builderVisitor a builder visitor to transform {@linkplain Query queries} into {@linkplain DocumentIterator document iterators}.
	 * @param indexMap a map from symbolic name to indices (used for multiplexing and default weight initialisation).
	 */
	
	public QueryEngine( final QueryParser queryParser, final QueryBuilderVisitor<DocumentIterator> builderVisitor, final Object2ReferenceMap<String,Index> indexMap ) {
		this.queryParser = queryParser;
		this.builderVisitor = builderVisitor;
		this.indexMap = indexMap;
		
		numIndices = indexMap.size();
		index2Weight = new Reference2DoubleOpenHashMap<Index>( indexMap.size() );
		index2Weight.defaultReturnValue( Double.NaN ); // Safety measure against improper access
		for( Index index : indexMap.values() ) this.index2Weight.put( index, 1.0 / numIndices );
	}

	public synchronized QueryEngine copy() {
		final QueryEngine newEngine = new QueryEngine( FlyweightPrototypes.copy( queryParser ), builderVisitor.copy(), indexMap );
		newEngine.multiplex = multiplex;
		newEngine.intervalSelector = FlyweightPrototypes.copy( intervalSelector );
		newEngine.scorer = FlyweightPrototypes.copy( scorer );
		newEngine.setWeights( index2Weight );
		return newEngine;				
	}

	/** Activate equalisation with the given number of samples-
	 * 
	 * @param samples the number of samples for equalisation, or 0 for no equalisation.
	 */
	
	public synchronized void equalize( final int samples ) {
		if ( scorer == null ) throw new IllegalStateException( "There is no scorer" );
		if ( ! ( scorer instanceof AbstractAggregator ) ) throw new IllegalStateException( "The current scorer is not aggregated" );
		((AbstractAggregator)scorer).equalize( samples );
	}
		
	/** Sets the scorers for this query engine.
	 *
	 * <p>If <code>scorer</code> has length zero, scoring is disabled. If it has length 1,
	 * the only scorer is used for scoring, and the only element of <code>weight</code> is
	 * discarded. Otherwise, a {@link LinearAggregator} is used to combine results from
	 * the given scorers, using the given weights.
	 * 
	 * @param scorer a (possibly empty) array of {@linkplain Scorer scorers}.
	 * @param weight a parallel array of weights (not to be confused with <em>index</em> weights).
	 */
	public synchronized void score( final Scorer[] scorer, final double[] weight ) {
		if ( scorer.length == 0 ) this.scorer = null;
		else {
			if ( scorer.length == 1 ) this.scorer = scorer[ 0 ];
			else this.scorer = new LinearAggregator( scorer, weight );
			this.scorer.setWeights( index2Weight );
		}
	}

	/** Sets a scorer for this query engine.
	 *
	 * @param scorer a scorer.
	 * @see #score(Scorer[], double[])
	 */
	public synchronized void score( final Scorer scorer ) {
		score( new Scorer[] { scorer }, new double[] { 1 } );
	}


	/** Sets the transformer for this engine, or disables query transformation. 
	 * 
	 * @param transformer a {@linkplain QueryTransformer query transformer}, or {@code null} to disable query transformation.
	 * */
	public synchronized void transformer( final QueryTransformer transformer ) {
		this.transformer = transformer;
	}
	

	/** Sets the index weights.
	 * 
	 * <p>This method just delegates to {@link Scorer#setWeights(Reference2DoubleMap)}. 
	 * 
	 * @param index2Weight a map from indices to weights.
	 */
	
	public synchronized void setWeights( final Reference2DoubleMap<Index> index2Weight ) {
		this.index2Weight.clear();
		this.index2Weight.defaultReturnValue( 0 );
		this.index2Weight.putAll( index2Weight );
		if ( scorer != null ) scorer.setWeights( index2Weight );
	}

	/** Turns the given query into a multiplexed query if {@link #multiplex} is on.
	 * 
	 * @param query a query.
	 * @return <code>query</code>, if {@link #multiplex} is off; a multiplexed version of <code>query</code>, otherwise.
	 */
	
	private String multiplex( final String query ) {
		if ( ! multiplex ) return query;
		
		final Iterator<String> it = indexMap.keySet().iterator();
		final StringBuilder builder = new StringBuilder();
		
		while ( it.hasNext() ) {
			builder.append( it.next() + ":(" + query + ")" );
			if ( it.hasNext() ) builder.append( " | " );
		}
		LOGGER.debug( "Multiplex is active: submitting " + builder );
		return builder.toString();
	}

	/** Parses one or more comma-separated queries and deposits in a given array a segment of the
	 * results corresponding to the queries, using the current settings of this query engine.
	 * 
	 * <p>Results are accumulated with an &ldquo;and-then&rdquo; semantics: results
	 * are added from each query in order, provided they did not appear before.
	 * 
	 * @param queries one or more queries separated by commas. 
	 * @param offset the first result to be added to <code>results</code>.
	 * @param length the number of results to be added to <code>results</code>
	 * @param results an array list that will hold all results.
	 * @return the number of relevant documents scanned while filling <code>results</code>.
	 */
	
	public int process( final String queries, int offset, final int length, final ObjectArrayList<DocumentScoreInfo<Reference2ObjectMap<Index,SelectedInterval[]>>> results ) throws QueryParserException, QueryBuilderVisitorException, IOException {
		LOGGER.debug( "Processing query \"" + queries + "\", offset=" + offset + ", length="+ length );
		final String[] part = queries.split( "," );
		final Query[] partQuery = new Query[ part.length ]; 
		for( int i = 0; i < part.length; i++ ) {
			final String q = multiplex( part[ i ] );
			partQuery[ i ] = queryParser.parse( q );
			if ( transformer != null ) partQuery[ i ] = transformer.transform( partQuery[ i ] );
		}
		
		return process( partQuery, offset, length, results );
	}

	/** Processes one pre-parsed query and deposits in a given array a segment of the
	 * results corresponding to the query, using the current settings of this query engine.
	 * 
	 * <p>Results are accumulated with an &ldquo;and-then&rdquo; semantics: results
	 * are added from each query in order, provided they did not appear before.
	 * 
	 * @param query a query;
	 * @param offset the first result to be added to <code>results</code>.
	 * @param length the number of results to be added to <code>results</code>
	 * @param results an array list that will hold all results.
	 * @return the number of documents scanned while filling <code>results</code>.
	 */
	public int process( final Query query, final int offset, final int length, final ObjectArrayList<DocumentScoreInfo<Reference2ObjectMap<Index,SelectedInterval[]>>> results ) throws QueryBuilderVisitorException, IOException {
		return process( new Query[] { query }, offset, length, results );
	}

	/** Processes one or more pre-parsed queries and deposits in a given array a segment of the
	 * results corresponding to the queries, using the current settings of this query engine.
	 * 
	 * <p>Results are accumulated with an &ldquo;and-then&rdquo; semantics: results
	 * are added from each query in order, provided they did not appear before.
	 * 
	 * @param query an array of queries. 
	 * @param offset the first result to be added to <code>results</code>.
	 * @param length the number of results to be added to <code>results</code>
	 * @param results an array list that will hold all results.
	 * @return the number of documents scanned while filling <code>results</code>.
	 */
	@SuppressWarnings("unchecked")
	public int process( final Query query[], final int offset, final int length, final ObjectArrayList<DocumentScoreInfo<Reference2ObjectMap<Index,SelectedInterval[]>>> results ) throws QueryBuilderVisitorException, IOException {
		LOGGER.debug( "Processing Query array \"" + Arrays.toString( query ) + "\", offset=" + offset + ", length="+ length );
		results.clear();
		double lastMinScore = 1;
		int total = 0, count, currOffset = offset, currLength = length;
		final LongSet alreadySeen = query.length > 1 ? new LongOpenHashSet() : null;

		for( int i = 0; i < query.length; i++ ) {
			final int initialResultSize = results.size();
			
			DocumentIterator documentIterator = query[ i ].accept( builderVisitor.prepare() );
			
			count = scorer != null? 
					getScoredResults( documentIterator, currOffset, currLength, lastMinScore, results, alreadySeen ) :
						getResults( documentIterator, currOffset, currLength, results, alreadySeen );
					
			documentIterator.dispose();
			if ( results.size() > 0 ) lastMinScore = results.get( results.size() - 1 ).score;
			
			total += count;
			currOffset -= count;

			if ( currOffset < 0 ) {
				currLength += currOffset;
				currOffset = 0;
			}

			// Check whether we have intervals, we want intervals *and* we added some results.
			boolean someHavePositions = false;
			for( Index index: documentIterator.indices() ) someHavePositions |= index.hasPositions;
			
			if ( someHavePositions && intervalSelector != null && results.size() != initialResultSize ) {
				// We must now enrich the returned result with intervals
				DocumentScoreInfo<Reference2ObjectMap<Index,SelectedInterval[]>> sorted[] = 
					results.subList( initialResultSize, results.size() ).toArray( new DocumentScoreInfo[ results.size() - initialResultSize ] );
				ObjectArrays.quickSort( sorted, DocumentScoreInfo.DOCUMENT_COMPARATOR );

				documentIterator = query[ i ].accept( builderVisitor.prepare() );
			
				for( DocumentScoreInfo<Reference2ObjectMap<Index,SelectedInterval[]>> dsi: sorted ) {
					documentIterator.skipTo( dsi.document );
					dsi.info = intervalSelector.select( documentIterator, new Reference2ObjectArrayMap<Index,SelectedInterval[]>( numIndices ) );
				}
			
				documentIterator.dispose();
			}
			
			if ( ASSERTS ) assert length >= results.size();
			if ( length == results.size() ) break;
		}
		return total;
	}
	
	private int getScoredResults( final DocumentIterator documentIterator, final int offset, final int length, final double lastMinScore, final ObjectArrayList<DocumentScoreInfo<Reference2ObjectMap<Index,SelectedInterval[]>>> results, final LongSet alreadySeen ) throws IOException {
		final ScoredDocumentBoundedSizeQueue<Reference2ObjectMap<Index,SelectedInterval[]>> top = new ScoredDocumentBoundedSizeQueue<Reference2ObjectMap<Index,SelectedInterval[]>>( offset + length );
		long document;
		int count = 0; // Number of not-already-seen documents

		scorer.wrap( documentIterator );
		// TODO: we should avoid enqueueing until we really know we shall use the values
		if ( alreadySeen != null ) 
			while ( ( document = scorer.nextDocument() ) != END_OF_LIST ) {
				if ( ! alreadySeen.add( document ) ) continue;
				count++;
				top.enqueue( document, scorer.score() );
			}
		else 
			while ( ( document = scorer.nextDocument() ) != END_OF_LIST ) {
				count++;
				top.enqueue( document, scorer.score() );
			}
		
		final int n = Math.max( top.size() - offset, 0 ); // Number of actually useful documents, if any
		if ( ASSERTS ) assert n <= length : n;
		if ( n > 0 ) {
			final int s = results.size();
			results.size( s + n );
			final Object[] elements = results.elements();
			// We scale all newly inserted item so that scores are always decreasing
			for ( int i = n; i-- != 0; ) elements[ i + s ] = top.dequeue();
			// The division by the maximum score was missing in previous versions; can be removed to reproduce regressions.
			// TODO: this will change scores if offset leaves out an entire query
			final double adjustment = lastMinScore / ( s != 0 ? ((DocumentScoreInfo<?>)elements[ s ]).score : 1.0 );
			for ( int i = n; i-- != 0; ) ((DocumentScoreInfo<?>)elements[ i + s ]).score *= adjustment;
		}
		
		return count;
	}

	private int getResults( final DocumentIterator documentIterator, final int offset, final int length, final ObjectArrayList<DocumentScoreInfo<Reference2ObjectMap<Index,SelectedInterval[]>>> results, final LongSet alreadySeen ) throws IOException {
		long document;
		int count = 0; // Number of not-already-seen documents

		// Unfortunately, to provide the exact count of results we have to scan the whole iterator.
		if ( alreadySeen != null ) 
			while ( ( document = documentIterator.nextDocument() ) != END_OF_LIST ) {
				if ( ! alreadySeen.add( document ) ) continue;
				if ( count >= offset && count < offset + length ) results.add( new DocumentScoreInfo<Reference2ObjectMap<Index,SelectedInterval[]>>( document, -1 ) );
				count++;
			}
		else if ( length != 0 ) 
			while ( ( document = documentIterator.nextDocument() ) != END_OF_LIST ) {
				if ( count < offset + length && count >= offset ) results.add( new DocumentScoreInfo<Reference2ObjectMap<Index,SelectedInterval[]>>( document, -1 ) );
				count++;
			}
		else while ( ( document = documentIterator.nextDocument() ) != END_OF_LIST ) count++;
		
		return count;
	}

	public String toString() {
		return this.getClass().getName() + indexMap;
	}
}
