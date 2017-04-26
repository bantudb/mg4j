package it.unimi.di.big.mg4j.search;

import static it.unimi.di.big.mg4j.search.DocumentIterator.END_OF_LIST;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import it.unimi.di.big.mg4j.index.Index;
import it.unimi.di.big.mg4j.search.score.DocumentRankScorer;
import it.unimi.dsi.fastutil.doubles.DoubleBigArrays;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.objects.Reference2DoubleArrayMap;
import it.unimi.dsi.fastutil.objects.Reference2DoubleMap;
import it.unimi.dsi.util.Interval;

import java.io.IOException;
import java.util.Random;

import org.junit.Test;

public class DifferenceDocumentIteratorTest {

	@Test
	public void testDifference() throws IOException {
		DocumentIterator minuendIterator = new IntArrayDocumentIterator( new long[] { 0, 1, 2, 3 }, 
				new int[][][] { 
				{ { 0, 1 } }, 
				{ { 0, 1 }, { 1, 2 }, { 2, 3 } },
				{ { 0, 1 }, { 1, 2 }, { 2, 3 } },
				{ { 0 } }
				} );
		DocumentIterator subtrahendIterator = new IntArrayDocumentIterator( new long[] { 1, 2 }, 
				new int[][][] { 
				{ { 1 } },
				{ { 1, 3 } } 
				} );
		DocumentIterator differenceIterator = DifferenceDocumentIterator.getInstance( minuendIterator, subtrahendIterator );
		assertTrue( differenceIterator.mayHaveNext() );
		assertTrue( differenceIterator.mayHaveNext() ); // To increase coverage
		assertEquals( 0, differenceIterator.nextDocument() );
		assertEquals( 0, differenceIterator.document() );
		assertEquals( Interval.valueOf( 0, 1 ), differenceIterator.intervalIterator().nextInterval() );
		assertNull( differenceIterator.intervalIterator().nextInterval() );
		assertEquals( 1, differenceIterator.nextDocument() );
		assertEquals( 1, differenceIterator.document() );
		assertEquals( Interval.valueOf( 2, 3 ), differenceIterator.intervalIterator().nextInterval() );
		assertNull( differenceIterator.intervalIterator().nextInterval() );
		assertEquals( 2, differenceIterator.nextDocument() );
		assertEquals( 2, differenceIterator.document() );
		assertEquals( Interval.valueOf( 0, 1 ), differenceIterator.intervalIterator().nextInterval() );
		assertEquals( Interval.valueOf( 1, 2 ), differenceIterator.intervalIterator().nextInterval() );
		assertEquals( Interval.valueOf( 2, 3 ), differenceIterator.intervalIterator().nextInterval() );
		assertNull( differenceIterator.intervalIterator().nextInterval() );
		assertEquals( 3, differenceIterator.nextDocument() );
		assertEquals( 3, differenceIterator.document() );
		assertEquals( Interval.valueOf( 0 ), differenceIterator.intervalIterator().nextInterval() );
		assertNull( differenceIterator.intervalIterator().nextInterval() );
		assertFalse( differenceIterator.mayHaveNext() ); // To increase coverage
		assertEquals( END_OF_LIST, differenceIterator.nextDocument() );
		assertEquals( END_OF_LIST, differenceIterator.nextDocument() );
	} 

	@Test
	public void testSubtrahendExhaustion() throws IOException {
		DocumentIterator minuendIterator = new IntArrayDocumentIterator( new long[] { 0 }, 
				new int[][][] { 
				{ { 0, 1 }, { 1, 2 }, { 2, 3 }, { 3, 4 } }
				} );
		DocumentIterator subtrahendIterator = new IntArrayDocumentIterator( new long[] { 0 }, 
				new int[][][] { 
				{ { 1 } },
				} );
		DocumentIterator differenceIterator = DifferenceDocumentIterator.getInstance( minuendIterator, subtrahendIterator );
		assertTrue( differenceIterator.mayHaveNext() );
		assertEquals( 0, differenceIterator.nextDocument() );
		assertEquals( 0, differenceIterator.document() );
		assertEquals( Interval.valueOf( 2, 3 ), differenceIterator.intervalIterator().nextInterval() );
		assertEquals( Interval.valueOf( 3, 4 ), differenceIterator.intervalIterator().nextInterval() );
		assertNull( differenceIterator.intervalIterator().nextInterval() );
		assertFalse( differenceIterator.mayHaveNext() ); // To increase coverage
	} 


	@Test
	public void testskipTo() throws IOException {
		IntArrayDocumentIterator minuendIterator = new IntArrayDocumentIterator( new long[] { 0, 1, 2, 3, 4 }, 
				new int[][][] { 
				{ { 0, 1 } }, 
				{ { 0, 1 }, { 1, 2 } },
				{ { 0, 1 }, { 1, 2 }, { 2, 3 } },
				{ { 0 } },
				{ { 0 } }
				} );
		IntArrayDocumentIterator subtrahendIterator = new IntArrayDocumentIterator( new long[] { 1, 2, 3, 4 }, 
				new int[][][] { 
				{ { 1 } },
				{ { 1, 3 } },
				{ {} },
				{ {} },
				} );
		DocumentIterator differenceIterator = DifferenceDocumentIterator.getInstance( minuendIterator, subtrahendIterator );
		assertEquals( 2, differenceIterator.skipTo( 1 ) );
		assertEquals( 2, differenceIterator.skipTo( 1 ) ); // To increase coverage
		assertEquals( DocumentIterator.END_OF_LIST, differenceIterator.skipTo( 4 ) );
		assertEquals( END_OF_LIST, differenceIterator.nextDocument() );
		assertEquals( END_OF_LIST, differenceIterator.nextDocument() );

		minuendIterator.reset();
		subtrahendIterator.reset();
		differenceIterator = DifferenceDocumentIterator.getInstance( minuendIterator, subtrahendIterator );
		assertEquals( 2, differenceIterator.skipTo( 2 ) );
		
		
		minuendIterator.reset();
		subtrahendIterator.reset();
		differenceIterator = DifferenceDocumentIterator.getInstance( minuendIterator, subtrahendIterator );
		assertEquals( DocumentIterator.END_OF_LIST, differenceIterator.skipTo( 10 ) );
		assertEquals( DocumentIterator.END_OF_LIST, differenceIterator.skipTo( 10 ) );
		assertEquals( END_OF_LIST, differenceIterator.nextDocument() );
		assertEquals( END_OF_LIST, differenceIterator.nextDocument() );
	}

	@Test
	public void testtrueFalseDifference() throws IOException {
		DocumentIterator minuendIterator = new IntArrayDocumentIterator( new long[] { 0, 1, 2, 3 }, 
				new int[][][] { 
				{ {} }, 
				{},
				{ {} }, 
				{},
				} );
		DocumentIterator subtrahendIterator = new IntArrayDocumentIterator( new long[] { 0, 1, 2, 3 }, 
				new int[][][] { 
				{ {} }, 
				{ {} }, 
				{},
				{},
				} );
		DocumentIterator differenceIterator = DifferenceDocumentIterator.getInstance( minuendIterator, subtrahendIterator );
		assertTrue( differenceIterator.mayHaveNext() );
		assertEquals( 2, differenceIterator.nextDocument() );
		assertEquals( IntervalIterators.TRUE, differenceIterator.intervalIterator() );
		assertEquals( END_OF_LIST, differenceIterator.nextDocument() );
		assertEquals( END_OF_LIST, differenceIterator.nextDocument() );
	}

	@Test
	public void testTrueFalseOtherDifference() throws IOException {
		DocumentIterator minuendIterator = new IntArrayDocumentIterator( new long[] { 0, 1, 2, 3 }, 
				new int[][][] { 
				{ { 0, 1 }, { 1, 2 } }, 
				{ { 0, 1 }, { 1, 2 } },
				{ {} }, 
				{},
				} );
		DocumentIterator subtrahendIterator = new IntArrayDocumentIterator( new long[] { 0, 1, 2, 3 }, 
				new int[][][] { 
				{ {} }, 
				{},
				{ { 0, 1 }, { 1, 2 } }, 
				{ { 0, 1 }, { 1, 2 } },
				} );
		DocumentIterator differenceIterator = DifferenceDocumentIterator.getInstance( minuendIterator, subtrahendIterator );
		assertTrue( differenceIterator.mayHaveNext() );
		assertEquals( 1, differenceIterator.nextDocument() );
		assertEquals( Interval.valueOf( 0, 1 ), differenceIterator.intervalIterator().nextInterval() );
		assertEquals( Interval.valueOf( 1, 2 ), differenceIterator.intervalIterator().nextInterval() );
		assertNull( differenceIterator.intervalIterator().nextInterval() );
		assertEquals( 2, differenceIterator.nextDocument() );
		assertEquals( IntervalIterators.TRUE, differenceIterator.intervalIterator() );
		assertEquals( END_OF_LIST, differenceIterator.nextDocument() );
		assertEquals( END_OF_LIST, differenceIterator.nextDocument() );
	}

	@Test
	public void testDifferenceScorer() throws IOException {
		DocumentIterator minuendIterator = new IntArrayDocumentIterator( new long[] { 0, 1, 2, 3 }, 
				new int[][][] { 
				{ { 0, 1 }, { 1, 2 } }, 
				{ { 0, 1 }, { 1, 2 } },
				{ {} }, 
				{},
				} );
		DocumentIterator subtrahendIterator = new IntArrayDocumentIterator( new long[] { 0, 1, 2, 3 }, 
				new int[][][] { 
				{ {} }, 
				{},
				{ { 0, 1 }, { 1, 2 } }, 
				{ { 0, 1 }, { 1, 2 } },
				} );
		DocumentRankScorer drs = new DocumentRankScorer( DoubleBigArrays.wrap( new double[] { 0.0, 1.0, 2.0, 3.0 } ) );
		DocumentIterator diffDocumentIterator = DifferenceDocumentIterator.getInstance( minuendIterator, subtrahendIterator );
		Reference2DoubleMap<Index> weightMap = new Reference2DoubleArrayMap<Index>();
		weightMap.defaultReturnValue( 1.0 );
		drs.setWeights( weightMap );
		drs.wrap( diffDocumentIterator );
		assertEquals( 1, drs.nextDocument() );
		assertEquals( 1.0, drs.score(), 0 );
		assertEquals( 2, drs.nextDocument() );
		assertEquals( 2.0, drs.score(), 0 );
		assertEquals( END_OF_LIST, drs.nextDocument() );
		assertEquals( END_OF_LIST, drs.nextDocument() );
	}

	@Test
	public void testSpuriousReset() throws IOException {
		DocumentIterator minuendIterator = AndDocumentIterator.getInstance( 
				new IntArrayDocumentIterator( new long[] { 0 }, 
						new int[][][] { 
						{ { 0 } }, 
				} ),
				new IntArrayDocumentIterator( new long[] { 0 }, 
						new int[][][] { 
						{ { 1 } }, 
				} )
		);

		DocumentIterator subtrahendIterator = new IntArrayDocumentIterator( new long[] { 1 }, new int[][][] { {} } );
		DocumentIterator differenceIterator = DifferenceDocumentIterator.getInstance( minuendIterator, subtrahendIterator );
		assertTrue( differenceIterator.mayHaveNext() );
		assertEquals( 0, differenceIterator.nextDocument() );
		assertEquals( Interval.valueOf( 0, 1 ), differenceIterator.intervalIterator().nextInterval() );
		assertEquals( END_OF_LIST, differenceIterator.nextDocument() );
		assertEquals( END_OF_LIST, differenceIterator.nextDocument() );
	}

	@Test
	public void testEnlargment() throws IOException {
		DocumentIterator minuendIterator = new IntArrayDocumentIterator( new long[] { 0, 1, 2, 3 }, 
				new int[][][] { 
				{ { 0, 1 }, { 1, 2 } }, 
				{ { 0, 1 }, { 1, 2 } },
				{ {} }, 
				{ { 0, 1 }, { 1, 2 }, { 3, 5 }, { 4, 6 } },
				} );
		DocumentIterator subtrahendIterator = new IntArrayDocumentIterator( new long[] { 0, 1, 2, 3 }, 
				new int[][][] { 
				{ {} }, 
				{},
				{ { 0, 1 }, { 1, 2 } }, 
				{ { 0 }, { 1 }, { 4 } },
				} );
		DocumentIterator differenceIterator = DifferenceDocumentIterator.getInstance( minuendIterator, subtrahendIterator, 1, 1 );
		assertTrue( differenceIterator.mayHaveNext() );
		assertEquals( 1, differenceIterator.nextDocument() );
		assertEquals( Interval.valueOf( 0, 1 ), differenceIterator.intervalIterator().nextInterval() );
		assertEquals( Interval.valueOf( 1, 2 ), differenceIterator.intervalIterator().nextInterval() );
		assertNull( differenceIterator.intervalIterator().nextInterval() );
		assertEquals( 2, differenceIterator.nextDocument() );
		assertEquals( IntervalIterators.TRUE, differenceIterator.intervalIterator() );
		assertEquals( 3, differenceIterator.nextDocument() );
		assertEquals( Interval.valueOf( 0, 1 ), differenceIterator.intervalIterator().nextInterval() );
		assertEquals( Interval.valueOf( 1, 2 ), differenceIterator.intervalIterator().nextInterval() );
		assertEquals( Interval.valueOf( 4, 6 ), differenceIterator.intervalIterator().nextInterval() );
		assertNull( differenceIterator.intervalIterator().nextInterval() );
		assertFalse( differenceIterator.mayHaveNext() );
		assertEquals( END_OF_LIST, differenceIterator.nextDocument() );
		assertEquals( END_OF_LIST, differenceIterator.nextDocument() );
	}
	
	@Test
	public void testVarious() throws IOException {
		long[] documentPointers = { 0, 10, 20, 30, 40, 50 };
		boolean[][] keepArray = { { true, false, false, true, false, true } };
		for ( boolean[] keep: keepArray ) {
			ObjectArrayList<int[][]> minuendSuperList = new ObjectArrayList<int[][]>();
			ObjectArrayList<int[][]> subtrahendSuperList = new ObjectArrayList<int[][]>();
			for ( int i = 0; i < documentPointers.length; i++ ) {
				ObjectArrayList<int[]> minuendList = new ObjectArrayList<int[]>();
				ObjectArrayList<int[]> subtrahendList = new ObjectArrayList<int[]>();
				long documentPointer = documentPointers[ i ];
				boolean shouldKeep = keep[ i ];
				Random from = new Random( documentPointer );
				Random length = new Random( documentPointer + 1 );
				int ff = 1, tt = 5;
				for ( int j = 0; j < 10; j++ ) {
					int left = ff + from.nextInt( 10 );
					int right = Math.max( left + length.nextInt( 10 ) + 5, tt );
					ff = left + 1;
					tt = right + 1;
					minuendList.add( new int[] { left, right } );
					if ( !shouldKeep ) subtrahendList.add( new int[] { left + 1, right - 2 } );
				}
				int[][] minuendArray = new int[ minuendList.size() ][];
				int[][] subtrahendArray = new int[ subtrahendList.size() ][];
				minuendList.toArray( minuendArray );
				subtrahendList.toArray( subtrahendArray );
				minuendSuperList.add( minuendArray );
				subtrahendSuperList.add( subtrahendArray );
			}
			int n = minuendSuperList.size();
			int[][][] first = new int[ n ][][];
			int[][][] second = new int[ n ][][];
			minuendSuperList.toArray( first );
			subtrahendSuperList.toArray( second );
			
			DocumentIterator firstDocumentIterator = new IntArrayDocumentIterator( documentPointers, first );
			DocumentIterator secondDocumentIterator = new IntArrayDocumentIterator( documentPointers, second );
			DocumentIterator result = new DifferenceDocumentIterator( firstDocumentIterator, secondDocumentIterator, 1, 2 );
			IntArrayDocumentIterator compareDocumentIterator = new IntArrayDocumentIterator( documentPointers, first );
			
			for ( int i = 0; i < documentPointers.length; i++ ) {
				compareDocumentIterator.nextDocument();
				if ( !keep[ i ] ) continue;
				assertTrue( result.mayHaveNext() );
				assertEquals( documentPointers[ i ], result.nextDocument() );
				
				IntervalIterator resultIterator = result.intervalIterator();
				IntervalIterator expectedIterator = compareDocumentIterator.intervalIterator();
				assertEquals( documentPointers[ i ], compareDocumentIterator.document() );
				Interval expectedInterval, resultInterval = null;
				while ( ( expectedInterval = expectedIterator.nextInterval() ) != null & ( resultInterval = resultIterator.nextInterval() ) != null ) {
					assertEquals( expectedInterval, resultInterval );
				}
				assertTrue( expectedInterval + " != " + resultInterval, ( expectedInterval == null ) == ( resultInterval == null ) );
			}
			assertFalse( result.mayHaveNext() );

			assertEquals( END_OF_LIST, result.nextDocument() );
			assertEquals( END_OF_LIST, result.nextDocument() );

			Random r = new Random( 0 );
			for ( int i = 0; i < 100; i++ ) {
				firstDocumentIterator = new IntArrayDocumentIterator( documentPointers, first );
				secondDocumentIterator = new IntArrayDocumentIterator( documentPointers, second );
				result = new DifferenceDocumentIterator( firstDocumentIterator, secondDocumentIterator, 1, 2 );
				long j = documentPointers[ r.nextInt( documentPointers.length ) ];
				long skipToResult = result.skipTo( j );
				int t = 0;
				while ( t < documentPointers.length && ( !keep[ t ] || documentPointers[ t ] < j ) ) t++;
				if ( t == documentPointers.length )
					assertEquals( DocumentIterator.END_OF_LIST, skipToResult );
				else
					assertEquals( documentPointers[ t ], skipToResult );
				assertEquals( skipToResult, result.document() );
				for ( t++; t < documentPointers.length; t++ ) {
					if ( !keep[ t ] ) continue;
					assertTrue( result.mayHaveNext() );
					assertEquals( documentPointers[ t ], result.nextDocument() );
				}
				assertFalse( result.mayHaveNext() );	
			}
			
			assertEquals( END_OF_LIST, result.nextDocument() );
			assertEquals( END_OF_LIST, result.nextDocument() );
		}
	}
}