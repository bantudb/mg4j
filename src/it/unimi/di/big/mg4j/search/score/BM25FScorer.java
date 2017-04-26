package it.unimi.di.big.mg4j.search.score;

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
import it.unimi.di.big.mg4j.query.Query;
import it.unimi.di.big.mg4j.query.nodes.MultiIndexTermExpander;
import it.unimi.di.big.mg4j.search.DocumentIterator;
import it.unimi.di.big.mg4j.search.visitor.AbstractDocumentIteratorVisitor;
import it.unimi.di.big.mg4j.search.visitor.CounterSetupVisitor;
import it.unimi.di.big.mg4j.search.visitor.TermCollectionVisitor;
import it.unimi.di.big.mg4j.tool.Paste;
import it.unimi.dsi.big.util.ImmutableExternalPrefixMap;
import it.unimi.dsi.big.util.SemiExternalGammaBigList;
import it.unimi.dsi.big.util.StringMap;
import it.unimi.dsi.fastutil.ints.IntBigList;
import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.fastutil.longs.LongBigList;
import it.unimi.dsi.fastutil.longs.LongList;
import it.unimi.dsi.fastutil.objects.Object2DoubleMap;
import it.unimi.dsi.fastutil.objects.Object2DoubleOpenHashMap;
import it.unimi.dsi.fastutil.objects.Reference2DoubleMap;
import it.unimi.dsi.io.InputBitStream;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/** A scorer that implements the BM25F ranking scheme.
 * 
 * <p>BM25F is an evolution of {@linkplain BM25Scorer BM25} described by
 * Stephen Robertson, Hugo Zaragoza and Michael Taylor in &ldquo;Simple BM25 extension to multiple weighted fields&rdquo;,
 * <i>CIKM '04: Proceedings of the thirteenth ACM international Conference on Information and Knowledge Management</i>, 
 * pages 42&minus;49, ACM Press, 2004.
 * 
 * <p>The idea behind BM25F is that adding up (albeit with weights) BM25 scores from different fields breaks down the nonlinearity
 * of BM25. Instead, we should work on a <em>virtual document collection</em>: more precisely,
 * we should behave as if all fields were concatenated in a single stream of text.
 * For instance, if weights are integers, the formula behaves as if the text of each field is concatenated as many times as its weight to form a global
 * text, which is then scored using BM25.
 *  
 * <p>Note that, for this to happen, we would need to know the corresponding frequency&mdash;that is, for each term, the number of documents in which
 * the term appears <em>in at least one of the fields</em>. This number must be provided at construction time: more precisely, you must specify
 * a {@link StringMap} that maps each term appearing in some field to an index into a {@link LongList} containing
 * the correct frequencies. These data is
 * accessed only in the preparatory phase, so access can be reasonably slow.
 * 
 * <p><strong>Important</strong>: the only source of knowledge about the overall set of indices involved in query resolution is given
 * by calls to {@link #setWeights(it.unimi.dsi.fastutil.objects.Reference2DoubleMap)}. That is, this scorer will assume that all indices
 * appearing in a query are also keys of the weight function passed to {@link #setWeights(it.unimi.dsi.fastutil.objects.Reference2DoubleMap)}. An
 * exception will be raised if these guidelines are not followed.
 * 
 * <h2>Computing frequency data</h2>
 * 
 * <p>The tool {@link Paste} can be used to create the metadata of the virtual collection. To do so, simply run {@link Paste} on
 * the indices of <em>all</em> fields over which you want to compute BM25F with the <samp>--metadata-only</samp> option.
 * The resulting frequency file is what you need to pass 
 * to the constructor, and from the term file you can build a {@link StringMap} (e.g., using an {@link ImmutableExternalPrefixMap})
 * that will be used to index the frequencies.
 * 
 * <h2>Boldi's variant</h2>
 * 
 * <p>Providing global frequency data makes it possible to compute the classical BM25F formula. 
 * If no frequency data is provided, this class implements Paolo Boldi's variant of BM25. In this case, we multiply the
 * IDF score by the weighted count of each term to form the virtual count that will be passed through BM25's nonlinear function.
 * 
 * <h2>Using this scorer</h2>
 * 
 * <p>This scorer assigns to each pair index/term {@linkplain DocumentIterator#acceptOnTruePaths(it.unimi.di.big.mg4j.search.visitor.DocumentIteratorVisitor) reachable by true paths}
 * a score that depends on the <em>virtual count</em> of the term, which is the count of the term for the given index multiplied by the weight of the index.
 * To obtain the &ldquo;classical&rdquo; BM25F score you must write a query <var>q</var> that contains no index selector and multiplexes it on all indices, 
 * e.g., <samp>a:<var>q</var> | b:<var>q</var> | c:<var>q</var></samp>. If a term appears only in some specific index/query pair, its score will be computed
 * using a smaller virtual count, obtained just by adding up the values associated with the actually present index/query pairs. Usually, the
 * simplest way to obtain this result is to use a {@link MultiIndexTermExpander}, which can be even set from the command-line interface
 * provided by {@link Query}.
 *
 * <h2>Correctness</h2>
 * 
 * <p>The code in this scorer is verified by unit tests developed jointly with Hugo Zaragoza. 
 * This is an important point, as the definition of BM25F contains many subtleties.
 * 
 * @see BM25Scorer
 * @author Sebastiano Vigna
 */

public class BM25FScorer extends AbstractWeightedScorer implements DelegatingScorer {
	private static final Logger LOGGER = LoggerFactory.getLogger( BM25FScorer.class );
	private static final boolean DEBUG = false;

	protected static final class Visitor extends AbstractDocumentIteratorVisitor {
		/** Cached from {@link BM25Scorer}. */
		protected final double[] sizeWeightComponent;
		/** The length of {@link TermCollectionVisitor#indices()} cached. */
		protected final int numberOfIndices;
		/** An array accumulating the indices in {@link #seen} that have been set to true, so to accelerate {@link #reset(long)}. */
		protected final int[] seenOffsetsList;
		/** The number of valid entries in {@link #seenOffsetsList}. */
		protected int numberOfOffsetsSeen;
		/** Whether we have already seen a specific term/index pair. */
		protected final boolean[] seen;
		/** The number of terms (irrespective of the index) seen up to now and accumulated in {@link #seenTermIdsList}. */
		protected int numberOfTermsSeen;
		/** The list of {@link #numberOfTermsSeen} term ids that we have seen so far. */
		protected final int[] seenTermIdsList;
		/** For each term id, its virtual-counter index (to be used to access {@link #virtualCount} and {@link #virtualIdfCount}). */
		protected final int termId2VirtualCounter[];
		/** For each index, its list of document sizes. */
		protected final IntBigList[] sizes;
		/** For each index, its weight. */
		protected final double[] weight;
		/** For each index, the associated parameter <var>b</bar>. */
		protected final double[] index2B;
		/** Whether we are running Boldi's variant. */
		protected final boolean expectedIDF;
		/** For each offset, the corresponding term id in the query. */
		protected final int[] offset2TermId;
		/** An array indexed by offsets mapping each offset to the corresponding index number. */
		protected final int[] offset2Index;
		/** An array indexed by term ids used by {@link #score()} to compute virtual counts. */
		protected final double[] virtualCount;
		/** For expected IDF runs, an array indexed by term ids used by {@link #score()} to compute virtual counts combined with IDF scoring. */
		protected final double[] virtualIdfCount;
		/** Cached value. */
		protected final double[] index2BDividedByAvgDocumentSize;
		/** Precomputed IDF part. */
		protected final double[] idfPart;
		
		public Visitor( boolean expectedIDF, double[] idfPart, final int[] offset2TermId, final int[] offset2Index, final double[] weight, final double[] index2B, final IntBigList[] sizes, double[] index2BDividedByAvgDocumentSize ) {
			this.expectedIDF = expectedIDF;
			this.idfPart = idfPart;
			this.offset2TermId = offset2TermId;
			this.offset2Index = offset2Index;
			this.index2BDividedByAvgDocumentSize = index2BDividedByAvgDocumentSize;
			this.weight = weight;
			this.index2B = index2B;
			this.sizes = sizes;
			
			numberOfIndices = weight.length;
			sizeWeightComponent = new double[ numberOfIndices ];

			final int numberOfPairs = idfPart.length;
			virtualCount = new double[ numberOfPairs ];
			virtualIdfCount = new double[ numberOfPairs ];

			seenOffsetsList = new int[ numberOfPairs ];
			seenTermIdsList = new int[ numberOfPairs ];
			termId2VirtualCounter = new int[ numberOfPairs ];
			Arrays.fill( termId2VirtualCounter, -1 );
			seen = new boolean[ numberOfPairs ];
			
		}

		public Boolean visit( final IndexIterator indexIterator ) throws IOException {
			final int offset = indexIterator.id();

			if ( ! seen[ offset ] ) {
				// If we have never seen this particular term/offset pair, we record it.
				seenOffsetsList[ numberOfOffsetsSeen++ ] = offset;
				seen[ offset ] = true;
				
				final int termId = offset2TermId[ offset ];
				// If we have never seen this term, we set up a virtual counter for it.
				if ( termId2VirtualCounter[ termId ] == -1 ) {
					termId2VirtualCounter[ termId ] = numberOfTermsSeen;
					virtualCount[ numberOfTermsSeen ] = 0;
					if ( virtualIdfCount != null ) virtualIdfCount[ numberOfTermsSeen ] = 0;
					seenTermIdsList[ numberOfTermsSeen++ ] = termId;
				}
				
				final double v = indexIterator.count() * sizeWeightComponent[ offset2Index[ offset ] ];
				virtualCount[ termId2VirtualCounter[ termId ] ] += v;
				if ( expectedIDF ) virtualIdfCount[ termId2VirtualCounter[ termId ] ] += idfPart[ offset ] * v;
			}
			
			return Boolean.TRUE;
		}
		
		public void reset( final long document ) {
			// Clear seen information (on the first invocation does nothing as numberOfSeen == 0 ).
			while( numberOfOffsetsSeen-- != 0 ) {
				final int offset = seenOffsetsList[ numberOfOffsetsSeen ];
				seen[ offset ] = false;
				termId2VirtualCounter[ offset2TermId[ offset ] ] = -1;
			}
			numberOfOffsetsSeen = 0;
			numberOfTermsSeen = 0;
			for( int i = numberOfIndices; i-- != 0; ) {
				sizeWeightComponent[ i ] = weight[ i ] / ( 1 - index2B[ i ] + sizes[ i ].getInt( document ) * index2BDividedByAvgDocumentSize[ i ] );
			}
		}
	}

	/** The default value used for the parameter <var>k</var><sub>1</sub>. */
	public final static double DEFAULT_K1 = 1.2;
	/** The default value used for the parameter <var>b</var>. */
	public final static double DEFAULT_B = 0.5;
	/** The value of the document-frequency part for terms appearing in more than half of the documents. */
	public final static double EPSILON_SCORE = 1E-6;
	
	/** The counter setup visitor used to estimate counts. */
	private final CounterSetupVisitor setupVisitor;
	/** The term collection visitor used to estimate counts. */
	private final TermCollectionVisitor termVisitor;

	/** The parameter <var>k</var><sub>1</sub>. */
	public final double k1;
	/** The parameter {@link #k1} plus one, precomputed. */
	private final double k1Plus1;
	/** Whether {@link #termMap} is <code>null</code>, in which case we apply Boldi's variant and compute the expected frequency. */
	private final boolean expectedIDF;
	/** A term map to index {@link #frequencies}. */
	private final StringMap<? extends CharSequence> termMap;
	/** The list of virtual frequencies (possibly approximated using just the frequencies of the main field). */
	private final LongBigList frequencies;
	/** A map from field name to values of the parameter <var>b</var>. */
	private Object2DoubleMap<String> bByName;
	/** The parameter <var>b</var>; you must provide one value for each index. */
	public final Reference2DoubleMap<Index> bByIndex;
	/** A visitor used by the score evaluator. */
	private Visitor visitor;
	
	/** Creates a BM25F scorer.
	 * @param k1 the <var>k</var><sub>1</sub> parameter.
	 * @param b the <var>b</var> parameter, specified as a map from indices to values.
	 * @param termMap a map from terms to positions in <code>frequencies</code>, or <code>null</code> if <code>frequencies</code> is <code>null</code>.
	 * @param frequencies the frequencies, or <code>null</code> for Boldi's variant.
	 */
	public BM25FScorer( final double k1, final Reference2DoubleMap<Index> b, final StringMap<? extends CharSequence> termMap, final LongBigList frequencies ) {
		this.termMap = termMap;
		this.k1 = k1;
		this.bByIndex = b;
		this.frequencies = frequencies;
		termVisitor = new TermCollectionVisitor();
		setupVisitor = new CounterSetupVisitor( termVisitor );
		k1Plus1 = k1 + 1;
		bByName = null;
		expectedIDF = termMap == null;
	}

	/** Creates a BM25F scorer.
	 * 
	 * <p>This constructor exists to provide a typed counterpart to the {@linkplain #BM25FScorer(String...) string-based constructor} (mainly
	 * for documentation purposes).
	 * 
	 * @param k1 the <var>k</var><sub>1</sub> parameter.
	 * @param termMap a map from terms to positions in <code>frequencies</code>, or <code>null</code> if <code>frequencies</code> is <code>null</code>.
	 * @param frequencies the frequencies, or <code>null</code> for Boldi's variant.
	 * @param b the <var>b</var> parameter, specified as a map from indices to values.
	 */
	public BM25FScorer( final double k1, final StringMap<? extends CharSequence> termMap, final LongBigList frequencies, final Object2DoubleMap<String> b ) {
		this.termMap = termMap;
		this.k1 = k1;
		this.bByName = b;
		this.frequencies = frequencies;
		termVisitor = new TermCollectionVisitor();
		setupVisitor = new CounterSetupVisitor( termVisitor );
		k1Plus1 = k1 + 1;
		bByIndex = null;
		expectedIDF = termMap == null;
	}

	/** Creates a BM25F scorer using Boldi's variant (frequencies are not needed).
	 * @param k1 the <var>k</var><sub>1</sub> parameter.
	 * @param b the <var>b</var> parameter, specified as a map from indices to values.
	 */
	public BM25FScorer( final double k1, final Reference2DoubleMap<Index> b ) {
		this( k1, b, null, null );
	}

	private static Object2DoubleMap<String> parseBArray( final String[] b ) {
		final Object2DoubleOpenHashMap<String> result = new Object2DoubleOpenHashMap<String>();
		for( int i = 3; i < b.length; i++ ) {
			final String[] part = b[ i ].split( "=" );
			result.put( part[ 0 ], Double.parseDouble( part[ 1 ] ) ); 
		}
		return result;
	}
	
	/** Creates a BM25F scorer using parameters specified by strings.
	 * 
	 * <p>This constructor has string parameters that correspond to the arguments of {@link #BM25FScorer(double, StringMap, LongBigList, Object2DoubleMap)}.
	 * The two middle arguments can be omitted by specifying them as empty. The last argument is represented by a number of
	 * assignments <samp><var>index</var>=<var>b</var></samp>, separated by commas (as if they were multiple arguments), which
	 * will be compacted into a function representing the values of <var>b</var>. 
	 */
	@SuppressWarnings("unchecked")
	public BM25FScorer( String... arg ) throws NumberFormatException, FileNotFoundException, IOException, ClassNotFoundException {
		this( 
				Double.parseDouble( arg[ 0 ] ), // k1
				arg[ 1 ].length() == 0 ? null : (StringMap<? extends CharSequence>)BinIO.loadObject( arg[ 1 ] ), // termMap
				arg[ 2 ].length() == 0 ? null : new SemiExternalGammaBigList( new InputBitStream( arg[ 2 ] ) ), // frequencies 
				parseBArray( arg )
		);
	}

	
	public synchronized BM25FScorer copy() {
		final BM25FScorer scorer = new BM25FScorer( k1, bByIndex, termMap, frequencies );
		scorer.setWeights( index2Weight );
		return scorer;
	}

	public double score() throws IOException {
		final Visitor visitor = this.visitor;
		visitor.reset( documentIterator.document() );
		documentIterator.acceptOnTruePaths( visitor );

		final double[] virtualCount = visitor.virtualCount;
		double score = 0;

		if ( expectedIDF ) { 
			final double[] virtualIdfCount = visitor.virtualIdfCount;
			for ( int i = visitor.numberOfTermsSeen; i-- != 0; )
				score += ( k1Plus1 * virtualIdfCount[ i ] ) / ( virtualCount[ i ] + k1 );
		}
		else { 
			final double[] idfPart = visitor.idfPart;
			final int[] seenTermIdsList = visitor.seenOffsetsList;

			for ( int i = visitor.numberOfTermsSeen; i-- != 0; ) {
				final double v = virtualCount[ i ];
				score += ( k1Plus1 * v ) / ( v + k1 ) * idfPart[ seenTermIdsList[ i ] ];
			}
		}
		
		return score;
		
		/* Previous code--here as a safety measure.
		 
		double[] virtualCount = this.virtualCount.clone();
		DoubleArrays.fill( virtualCount, 0 );
		
		if ( termMap != null ) {
			for ( int i = offset2TermId.length; i-- != 0; ) {
				term2Index = offset2Index[ i ];
				virtualCount[ offset2TermId[ i ] ] += count[ i ] * offset2Weight[ i ] / ( ( 1 - index2B[ term2Index ] ) + index2B[ term2Index ] * size[ term2Index ] / avgDocumentSize[ term2Index ] );
			}
			
			for ( int i = virtualCount.length; i-- != 0; ) {
				v = virtualCount[ i ];
				score += ( k1Plus1 * v ) / ( v + k1 ) * idfPart[ i ];
			}
		}
		else {
			DoubleArrays.fill( virtualIdfCount, 0 );
			for ( int i = offset2TermId.length; i-- != 0; ) {
				term2Index = offset2Index[ i ];
				termId = offset2TermId[ i ];
				v = count[ i ] * offset2Weight[ i ] / ( ( 1 - index2B[ term2Index ] ) + index2B[ term2Index ] * size[ term2Index ] / avgDocumentSize[ term2Index ] );
				virtualCount[ termId ] += v; 
				virtualIdfCount[ termId ] += idfPart[ i ] * v;
			}

			for ( int i = virtualCount.length; i-- != 0; )
				score += ( k1Plus1 * virtualIdfCount[ i ] ) / ( virtualCount[ i ] + k1 );
		}
		return score;*/
}

	public double score( final Index index ) {
		throw new UnsupportedOperationException();
	}


	public void wrap( DocumentIterator d ) throws IOException {
		super.wrap( d );
		documentIterator = d;
		
		/* Note that we use the index array provided by the weight function, *not* by the visitor or by the iterator.
		 * If the function has an empty domain, this call is equivalent to prepare(). */
		termVisitor.prepare( index2Weight.keySet() );
		if ( DEBUG ) LOGGER.debug( "Weight map: " + index2Weight );
		
		d.accept( termVisitor );
		if ( DEBUG ) LOGGER.debug( "Term Visitor found " + termVisitor.numberOfPairs() + " leaves" );

		final Index[] index = termVisitor.indices();
		if ( DEBUG ) LOGGER.debug( "Indices: " + Arrays.toString( index ) );
		if ( ! index2Weight.keySet().containsAll( Arrays.asList( index ) ) ) throw new IllegalArgumentException( "A BM25F scorer must have a weight for all indices involved in a query" );
		
		for( Index i: index )
			if ( bByIndex != null && ! bByIndex.containsKey( i ) || bByName != null && ! bByName.containsKey( i.field ) )
				throw new IllegalArgumentException( "A BM25F scorer must have a b parameter for all indices involved in a query" );

		// Some caching of frequently-used values
		final IntBigList[] sizes = new IntBigList[ index.length ];
		for( int i = index.length; i-- != 0; )
			if ( ( sizes[ i ] = index[ i ].sizes ) == null ) throw new IllegalStateException( "A BM25F scorer requires document sizes" );
		
		setupVisitor.prepare();
		d.accept( setupVisitor );

		final double[] index2BDividedByAvgDocumentSize = new double[ index.length ];

		final double[] weight = new double[ index.length ];
		final double[] index2B = new double[ index.length ];
		for( int i = weight.length; i-- != 0; ) {
			weight[ i ] = index2Weight.getDouble( index[ i ] );
			index2B[ i ] = bByIndex != null ? bByIndex.getDouble( index[ i ] ) : bByName.getDouble( index[ i ].field );
			index2BDividedByAvgDocumentSize[ i ] = index2B[ i ] * index[ i ].numberOfDocuments / index[ i ].numberOfOccurrences;
		}
		
		// We do all logs here
		final double[] idfPart = new double[ termVisitor.numberOfPairs() ];

		if ( expectedIDF ) {
			// Modified BM25F, using expected IDF
			final int[] indexNumber = setupVisitor.indexNumber;
			final long[] frequency = setupVisitor.frequency;
			for( int i = idfPart.length; i-- != 0; ) {
				idfPart[ i ] = Math.max( EPSILON_SCORE,  
						Math.log( ( index[ indexNumber[ i ] ].numberOfDocuments - frequency[ i ] + 0.5 ) / ( frequency[ i ] + 0.5 ) ) );
			}
		}
		else {
			// Classical BM25F, based on global frequency data
			for( int i = idfPart.length; i-- != 0; ) {
				final String term = setupVisitor.termId2Term[ setupVisitor.offset2TermId[ i ] ];
				final long id = termMap.getLong( term );
				if ( id == -1 ) throw new IllegalStateException( "The term map passed to a BM25F scorer must contain all terms appearing in all indices (\"" + term + "\" unknown)" );
				final long f = frequencies.getLong( id );
				idfPart[ i ] = Math.max( EPSILON_SCORE, Math.log( ( index[ 0 ].numberOfDocuments - f + 0.5 ) / ( f + 0.5 ) ) );
			}
		}

		visitor = new Visitor( expectedIDF, idfPart, setupVisitor.offset2TermId, setupVisitor.indexNumber, weight, index2B, sizes, index2BDividedByAvgDocumentSize );
	}
	
	public boolean usesIntervals() {
		return false;
	}
}
