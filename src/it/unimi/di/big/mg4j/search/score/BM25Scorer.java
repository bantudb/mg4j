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
import it.unimi.di.big.mg4j.search.AbstractCompositeDocumentIterator;
import it.unimi.di.big.mg4j.search.AbstractIntersectionDocumentIterator;
import it.unimi.di.big.mg4j.search.AbstractUnionDocumentIterator;
import it.unimi.di.big.mg4j.search.DocumentIterator;
import it.unimi.di.big.mg4j.search.visitor.AbstractDocumentIteratorVisitor;
import it.unimi.di.big.mg4j.search.visitor.CounterSetupVisitor;
import it.unimi.di.big.mg4j.search.visitor.TermCollectionVisitor;
import it.unimi.dsi.fastutil.ints.IntBigList;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;

import java.io.IOException;
import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/** A scorer that implements the BM25 ranking scheme.
 * 
 * <p>BM25 is the name of a ranking scheme for text derived from the probabilistic model. The essential feature
 * of the scheme is that of assigning to each term appearing in a given document a weight depending
 * both on the count (the number of occurrences of the term in the document), on the frequency (the
 * number of the documents in which the term appears) and on the document length (in words). It was
 * devised in the early nineties, and it provides a significant improvement over the classical {@linkplain TfIdfScorer TF/IDF scheme}.
 * Karen Sp&auml;rck Jones, Steve Walker and Stephen E. Robertson give a full account of BM25 and of the
 * probabilistic model in &ldquo;A probabilistic model of information retrieval:
 * development and comparative experiments&rdquo;, <i>Inf. Process. Management</i> 36(6):779&minus;840, 2000.
 * 
 * <p>There are a number of incarnations with small variations of the formula itself. Here, the weight
 * assigned to a term which appears in <var>f</var> documents out of a collection of <var>N</var> documents
 * w.r.t. to a document of length <var>l</var> in which the term appears <var>c</var> times is
 * <div style="text-align: center">
 * log<big>(</big> (<var>N</var> &minus; <var>f</var> + 1/2) / (<var>f</var> + 1/2) <big>)</big> ( <var>k</var><sub>1</sub> + 1 ) <var>c</var> &nbsp;<big>&frasl;</big>&nbsp; <big>(</big> <var>c</var> + <var>k</var><sub>1</sub> ((1 &minus; <var>b</var>) + <var>b</var><var>l</var> / <var>L</var>) <big>)</big>,
 * </div>
 * where <var>L</var> is the average document length, and <var>k</var><sub>1</sub> and <var>b</var> are
 * parameters that default to {@link #DEFAULT_K1} and {@link #DEFAULT_B}: these values were chosen
 * following the suggestions given in 
 * &ldquo;Efficiency vs. effectiveness in Terabyte-scale information retrieval&rdquo;, by Stefan B&#252;ttcher and Charles L. A. Clarke, 
 * in <i>Proceedings of the 14th Text REtrieval 
 * Conference (TREC 2005)</i>. Gaithersburg, USA, November 2005. The logarithmic part (a.k.a.
 * <em>idf (inverse document-frequency)</em> part) is actually
 * maximised with {@link #EPSILON_SCORE}, so it is never negative (the net effect being that terms appearing 
 * in more than half of the documents have almost no weight).
 *  
 * <h2>Evaluation</h2>
 *  
 * <p>This class has two modes of evaluation, <em>generic</em> and <em>flat</em>. The generic evaluator uses an internal 
 * visitor building on {@link it.unimi.di.big.mg4j.search.visitor.CounterSetupVisitor}
 * and related classes (by means of {@link DocumentIterator#acceptOnTruePaths(it.unimi.di.big.mg4j.search.visitor.DocumentIteratorVisitor)})
 * to take into consideration only terms that are actually involved in query semantics for the current document.
 * The flat evaluator simulates the behaviour of the generic evaluator on a special subset of queries, that is, queries that
 * are formed by an {@linkplain IndexIterator index iterator} or a {@linkplain AbstractCompositeDocumentIterator composite document
 * iterator} whose underlying queries are all index iterators, by means of a simple loop. This is significantly faster
 * than the generic evaluator (as there is no recursive visit) either if document iterator is a subclass of {@link AbstractIntersectionDocumentIterator},
 * or if it is a subclass of {@link AbstractUnionDocumentIterator} and the disjuncts are not too many (less than {@link #MAX_FLAT_DISJUNCTS}).
 * 
 * @author Mauro Mereu
 * @author Sebastiano Vigna
 */
public class BM25Scorer extends AbstractWeightedScorer implements DelegatingScorer {
	public static final Logger LOGGER = LoggerFactory.getLogger( BM25Scorer.class );
	public static final boolean DEBUG = true;

	private static final class Visitor extends AbstractDocumentIteratorVisitor {
		/** Offset-indexed precomputed values. */
		private final double[] k1Plus1TimesWeightedIdfPart;
		/** Offset-indexed precomputed values. */
		private final double k1Times1MinusB;
		/** An array (parallel to {@link TermCollectionVisitor#indices()} that caches average document sizes. */
		private final double k1TimesBDividedByAverageDocumentSize[];
		/** An array (parallel to {@link TermCollectionVisitor#indices()} that caches size lists. */
		private final IntBigList sizes[];
		/** Cached from {@link BM25Scorer}. */
		private final double[] sizeComponent;
		/** Cached from {@link CounterSetupVisitor}. */
		private final int[] indexNumber;
		/** The length of {@link TermCollectionVisitor#indices()} cached. */
		private final int numberOfIndices;
		/** An array parallel to {@link #indexNumber} keeping track of whether we already accumulated the score for a specific term/index pair. */
		private final boolean[] seen;
		/** An array accumulating the indices in {@link #seen} that have been set to true, so to accelerate {@link #reset(int)}. */
		private final int[] seenList;
		/** The accumulated score. */
		public double score;
		/** The number of valid entries in {@link #seenList}. */
		private int numberOfSeen;
		
		public Visitor( final double k1Times1Minusb, final double[] k1Plus1TimesWeightedIdfPart, final double[] k1TimesBDividedByAverageDocumentSize, final int numberOfIndices, final int[] indexNumber, final IntBigList[] sizes ) {
			this.k1Times1MinusB = k1Times1Minusb;
			this.k1Plus1TimesWeightedIdfPart = k1Plus1TimesWeightedIdfPart;
			this.k1TimesBDividedByAverageDocumentSize = k1TimesBDividedByAverageDocumentSize;
			this.sizeComponent = new double[ numberOfIndices ];
			this.numberOfIndices = numberOfIndices;
			this.indexNumber = indexNumber;
			this.seen = new boolean[ indexNumber.length ];
			this.seenList = new int[ indexNumber.length ];
			this.sizes = sizes;
		}

		public Boolean visit( final IndexIterator indexIterator ) throws IOException {
			final int offset = indexIterator.id();
			if ( ! seen[ offset ] ) {
				seen[ seenList[ numberOfSeen++ ] = offset ] = true;
				final int count = indexIterator.count();
				score += ( count * k1Plus1TimesWeightedIdfPart[ offset ] ) / ( count + sizeComponent[ indexNumber[ offset ] ] );
			}
			return Boolean.TRUE;
		}
		
		public void reset( final long document ) {
			score = 0;
			// Clear seen information (on the first invocation does nothing as numberOfSeen == 0 ).
			while( numberOfSeen-- != 0 ) seen[ seenList[ numberOfSeen ] ] = false;
			numberOfSeen = 0;

			for( int i = numberOfIndices; i-- != 0; ) sizeComponent[ i ] = k1Times1MinusB + k1TimesBDividedByAverageDocumentSize[ i ] * sizes[ i ].getInt( document );
		}
	}

	
	/** The default value used for the parameter <var>k</var><sub>1</sub>. */
	public final static double DEFAULT_K1 = 1.2;
	/** The default value used for the parameter <var>b</var>. */
	public final static double DEFAULT_B = 0.5;
	/** The value of the document-frequency part for terms appearing in more than half of the documents. */
	public final static double EPSILON_SCORE = 1.0E-6;
	/** Disjunctive queries on {@linkplain IndexIterator index iterators} are handled using the flat evaluator only if they contain less than 
	 * this number of disjuncts. The generic evaluator is more efficient if there are several disjuncts, as it
	 * invokes {@link IndexIterator#count()} only on the terms that are part of the front. This value is largely architecture, query,
	 * term-distribution, and whatever else dependent. */
	public static final int MAX_FLAT_DISJUNCTS = 16;
	
	/** The counter setup visitor used to estimate counts. */
	private final CounterSetupVisitor setupVisitor;
	/** The term collection visitor used to estimate counts. */
	private final TermCollectionVisitor termVisitor;

	/** The parameter <var>k</var><sub>1</sub>. */
	private final double k1;
	/** The parameter <var>b</var>. */
	private final double b;

	/** The parameter {@link #k1} multiplied by one minus {@link #b}, precomputed. */
	private final double k1Times1MinusB;
	/** A value precomputed for flat evaluation. */
	private double k1TimesBDividedByAverageDocumentSize;
	/** The list of sizes, cached for flat evaluation. */
	private IntBigList sizes;
	/** An array indexed by offsets that caches the inverse document-frequency part of the formula, multiplied by the index weight, cached for flat evaluation. */
	private double[] k1Plus1TimesWeightedIdfPart;
	/** The value of {@link TermCollectionVisitor#numberOfPairs()} cached, if {@link #indexIterator} is <code>null</code>. */
	private int numberOfPairs;
	/** An array of nonzero-frequency index iterators, all on the same index, used by the flat evaluator, or <code>null</code> for generic evaluation. */
	private IndexIterator[] flatIndexIterator;
	/** A visitor used by the generic evaluator. */
	private Visitor visitor;

	/** Creates a BM25 scorer using {@link #DEFAULT_K1} and {@link #DEFAULT_B} as parameters.
	 */
	public BM25Scorer() {
		this( DEFAULT_K1, DEFAULT_B );
	}

	/** Creates a BM25 scorer using specified <var>k</var><sub>1</sub> and <var>b</var> parameters.
	 * @param k1 the <var>k</var><sub>1</sub> parameter.
	 * @param b the <var>b</var> parameter.
	 */
	public BM25Scorer( final double k1, final double b ) {
		termVisitor = new TermCollectionVisitor();
		setupVisitor = new CounterSetupVisitor( termVisitor );
		this.k1 = k1;
		this.b = b;
		k1Times1MinusB = k1 * ( 1 - b );
	}

	/** Creates a BM25 scorer using specified <var>k</var><sub>1</sub> and <var>b</var> parameters specified by strings.
	 * 
	 * @param k1 the <var>k</var><sub>1</sub> parameter.
	 * @param b the <var>b</var> parameter.
	 */
	public BM25Scorer( final String k1, final String b ) {
		this( Double.parseDouble( k1 ), Double.parseDouble( b ) );
	}

	public synchronized BM25Scorer copy() {
		final BM25Scorer scorer = new BM25Scorer( k1, b );
		scorer.setWeights( index2Weight );
		return scorer;
	}

	public double score() throws IOException {
		
		final long document = documentIterator.document();

		if ( flatIndexIterator == null ) {
			visitor.reset( document );
			documentIterator.acceptOnTruePaths( visitor );
			return visitor.score;
		}
		else {
			final double sizeComponent = k1Times1MinusB + k1TimesBDividedByAverageDocumentSize * sizes.getInt( document );
			double score = 0;
			final double[] k1Plus1TimesWeightedIdfPart = this.k1Plus1TimesWeightedIdfPart;
			final IndexIterator[] actualIndexIterator = this.flatIndexIterator;

			for ( int i = numberOfPairs; i-- != 0; ) 
				if ( actualIndexIterator[ i ].document() == document ) {
					final int c = actualIndexIterator[ i ].count();
					score += ( c * k1Plus1TimesWeightedIdfPart[ i ] ) / ( c + sizeComponent );
				}
			return score;
		}
	}

	public double score( final Index index ) {
		throw new UnsupportedOperationException();
	}


	public void wrap( DocumentIterator d ) throws IOException {
		super.wrap( d );

		/* Note that we use the index array provided by the weight function, *not* by the visitor or by the iterator.
		 * If the function has an empty domain, this call is equivalent to prepare(). */
		termVisitor.prepare( index2Weight.keySet() );
		
		d.accept( termVisitor );

		if ( DEBUG ) LOGGER.debug( "Term Visitor found " + termVisitor.numberOfPairs() + " leaves" );

		// Note that we use the index array provided by the visitor, *not* by the iterator.
		final Index[] index = termVisitor.indices();

		if ( DEBUG ) LOGGER.debug( "Indices: " + Arrays.toString( index ) );

		flatIndexIterator = null;
		
		/* We use the flat evaluator only for single-index, term-only queries that are either quite small, and
		 * then either conjunctive, or disjunctive with a reasonable number of terms. */

		if ( indexIterator != null && index.length == 1 && ( documentIterator instanceof AbstractIntersectionDocumentIterator || indexIterator.length < MAX_FLAT_DISJUNCTS ) ) {
			/* This code is a flat, simplified duplication of what a CounterSetupVisitor would do. It is here just for efficiency. */
			numberOfPairs = 0;
			/* Find duplicate terms. We score unique pairs term/index with nonzero frequency, as the standard method would do. */
			final LongOpenHashSet alreadySeen = new LongOpenHashSet();

			for( int i = indexIterator.length; i-- != 0; )
				if ( indexIterator[ i ].frequency() != 0 && alreadySeen.add( indexIterator[ i ].termNumber() ) ) numberOfPairs++;

			if ( numberOfPairs == indexIterator.length ) flatIndexIterator = indexIterator;
			else {
				/* We must compact the array, eliminating zero-frequency iterators. */
				flatIndexIterator = new IndexIterator[ numberOfPairs ];
				alreadySeen.clear();
				for( int i = 0, p = 0; i != indexIterator.length; i++ ) 
					if ( indexIterator[ i ].frequency() != 0 &&  alreadySeen.add( indexIterator[ i ].termNumber() ) ) flatIndexIterator[ p++ ] = indexIterator[ i ]; 
			}

			if ( flatIndexIterator.length != 0 ) {
				// Some caching of frequently-used values
				k1TimesBDividedByAverageDocumentSize = k1 * b * flatIndexIterator[ 0 ].index().numberOfDocuments / flatIndexIterator[ 0 ].index().numberOfOccurrences;
				if ( ( this.sizes = flatIndexIterator[ 0 ].index().sizes ) == null ) throw new IllegalStateException( "A BM25 scorer requires document sizes" );

				// We do all logs here, and multiply by the weight
				k1Plus1TimesWeightedIdfPart = new double[ numberOfPairs ];
				for( int i = 0; i < numberOfPairs; i++ ) {
					final long frequency = flatIndexIterator[ i ].frequency();
					k1Plus1TimesWeightedIdfPart[ i ] = ( k1 + 1 ) * Math.max( EPSILON_SCORE,  
							Math.log( ( flatIndexIterator[ i ].index().numberOfDocuments - frequency + 0.5 ) / ( frequency + 0.5 ) ) ) * index2Weight.getDouble( flatIndexIterator[ i ].index() );
				}
			}
		}
		else {
			// Some caching of frequently-used values
			final double[] k1TimesBDividedByAverageDocumentSize = new double[ index.length ];
			for ( int i = index.length; i-- != 0; )
				k1TimesBDividedByAverageDocumentSize[ i ] = k1 * b * index[ i ].numberOfDocuments / index[ i ].numberOfOccurrences;

			if ( DEBUG ) LOGGER.debug( "Average document sizes: " + Arrays.toString( k1TimesBDividedByAverageDocumentSize ) );
			final IntBigList[] sizes = new IntBigList[ index.length ];
			for( int i = index.length; i-- != 0; )
				if ( ( sizes[ i ] = index[ i ].sizes ) == null ) throw new IllegalStateException( "A BM25 scorer requires document sizes" );
			
			setupVisitor.prepare();
			d.accept( setupVisitor );
			numberOfPairs = termVisitor.numberOfPairs();
			final long[] frequency = setupVisitor.frequency;
			final int[] indexNumber = setupVisitor.indexNumber;

			// We do all logs here, and multiply by the weight
			k1Plus1TimesWeightedIdfPart = new double[ frequency.length ];
			for( int i = k1Plus1TimesWeightedIdfPart.length; i-- != 0; )
				k1Plus1TimesWeightedIdfPart[ i ] = ( k1 + 1 ) * Math.max( EPSILON_SCORE,  
						Math.log( ( index[ indexNumber[ i ] ].numberOfDocuments - frequency[ i ] + 0.5 ) / ( frequency[ i ] + 0.5 ) ) ) * index2Weight.getDouble( index[ indexNumber[ i ] ] );

			visitor = new Visitor( k1Times1MinusB, k1Plus1TimesWeightedIdfPart, k1TimesBDividedByAverageDocumentSize, termVisitor.indices().length, indexNumber, sizes );
		}

	}
	
	public boolean usesIntervals() {
		return false;
	}

}
