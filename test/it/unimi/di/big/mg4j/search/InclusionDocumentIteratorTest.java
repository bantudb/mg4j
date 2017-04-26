package it.unimi.di.big.mg4j.search;

import static it.unimi.di.big.mg4j.search.IntArrayIndexIterator.INDEX;
import static it.unimi.di.big.mg4j.search.DocumentIterator.END_OF_LIST;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import it.unimi.dsi.util.Interval;

import java.io.IOException;

import org.junit.Test;

public class InclusionDocumentIteratorTest {

	@Test
	public void testInclusion() throws IOException {
		IntArrayDocumentIterator firstIterator = new IntArrayDocumentIterator( new long[] { 0, 1, 2, 3, 4 }, 
				new int[][][] { 
				{ { 0, 1 } }, 
				{ { 0, 1 }, { 1, 2 }, { 2, 3 } },
				{ { 0, 1 }, { 1, 2 }, { 2, 3 } },
				{ { 0 } },
				{ { 1, 3 }, { 3, 5 } }
				} );
		IntArrayDocumentIterator secondIterator = new IntArrayDocumentIterator( new long[] { 1, 2, 4 }, 
				new int[][][] { 
				{ { 0, 2 }, { 2, 4 } },
				{ { 1, 3 } },
				{ { 0, 2 }, { 2, 4 }, { 4, 6 } }
				} );
		DocumentIterator inclusionIterator = InclusionDocumentIterator.getInstance( firstIterator, secondIterator );
		assertTrue( inclusionIterator.mayHaveNext() );
		assertTrue( inclusionIterator.mayHaveNext() ); // To increase coverage
		assertEquals( 1, inclusionIterator.nextDocument() );
		assertEquals( 1, inclusionIterator.document() );
		assertEquals( Interval.valueOf( 0, 1 ), inclusionIterator.intervalIterator().nextInterval() );
		assertEquals( Interval.valueOf( 1, 2 ), inclusionIterator.intervalIterator().nextInterval() );
		assertEquals( Interval.valueOf( 2, 3 ), inclusionIterator.intervalIterator().nextInterval() );
		assertNull( inclusionIterator.intervalIterator().nextInterval() );
		assertEquals( 2, inclusionIterator.nextDocument() );
		assertEquals( 2, inclusionIterator.document() );
		assertEquals( Interval.valueOf( 1, 2 ), inclusionIterator.intervalIterator().nextInterval() );
		assertEquals( Interval.valueOf( 2, 3 ), inclusionIterator.intervalIterator().nextInterval() );
		assertNull( inclusionIterator.intervalIterator().nextInterval() );
		assertEquals( END_OF_LIST, inclusionIterator.nextDocument() );
		
		firstIterator.reset();
		secondIterator.reset();
		
		inclusionIterator = InclusionDocumentIterator.getInstance( firstIterator, secondIterator );
		assertTrue( inclusionIterator.mayHaveNext() );
		assertTrue( inclusionIterator.mayHaveNext() ); // To increase coverage
		assertEquals( 1, inclusionIterator.nextDocument() );
		assertEquals( 1, inclusionIterator.document() );
		assertEquals( Interval.valueOf( 0, 1 ), inclusionIterator.intervalIterator( INDEX ).nextInterval() );
		assertEquals( Interval.valueOf( 1, 2 ), inclusionIterator.intervalIterator( INDEX ).nextInterval() );
		assertEquals( Interval.valueOf( 2, 3 ), inclusionIterator.intervalIterator( INDEX ).nextInterval() );
		assertNull( inclusionIterator.intervalIterator( INDEX ).nextInterval() );
		assertEquals( 2, inclusionIterator.nextDocument() );
		assertEquals( 2, inclusionIterator.document() );
		assertEquals( Interval.valueOf( 1, 2 ), inclusionIterator.intervalIterator( INDEX ).nextInterval() );
		assertEquals( Interval.valueOf( 2, 3 ), inclusionIterator.intervalIterator( INDEX ).nextInterval() );
		assertNull( inclusionIterator.intervalIterator( INDEX ).nextInterval() );
		assertEquals( END_OF_LIST, inclusionIterator.nextDocument() );
		
	} 

	@Test
	public void testskipTo() throws IOException {
		IntArrayDocumentIterator firstIterator = new IntArrayDocumentIterator( new long[] { 0, 1, 2, 3, 4 }, 
				new int[][][] { 
				{ { 0, 1 } }, 
				{ { 0, 1 }, { 1, 2 } },
				{ { 0, 1 }, { 1, 2 }, { 2, 3 } },
				{ { 0 } },
				{ { 0 } }
				} );
		IntArrayDocumentIterator secondIterator = new IntArrayDocumentIterator( new long[] { 1, 2, 3, 4 }, 
				new int[][][] { 
				{ { 1 } },
				{ { 1, 3 } },
				{ {} },
				{ {} },
				} );
		DocumentIterator inclusionIterator = InclusionDocumentIterator.getInstance( firstIterator, secondIterator );
		assertEquals( 2, inclusionIterator.skipTo( 1 ) );
		assertEquals( Interval.valueOf( 1, 2 ), inclusionIterator.intervalIterator().nextInterval() );
		assertEquals( Interval.valueOf( 2, 3 ), inclusionIterator.intervalIterator().nextInterval() );
		
		assertEquals( DocumentIterator.END_OF_LIST, inclusionIterator.skipTo( 4 ) );
		assertEquals( END_OF_LIST, inclusionIterator.nextDocument() );

		firstIterator.reset();
		secondIterator.reset();
		inclusionIterator = InclusionDocumentIterator.getInstance( firstIterator, secondIterator );
		assertEquals( 2, inclusionIterator.skipTo( 2 ) );
		assertEquals( Interval.valueOf( 1, 2 ), inclusionIterator.intervalIterator().nextInterval() );
		assertEquals( Interval.valueOf( 2, 3 ), inclusionIterator.intervalIterator().nextInterval() );
		
		firstIterator.reset();
		secondIterator.reset();
		inclusionIterator = InclusionDocumentIterator.getInstance( firstIterator, secondIterator );
		assertEquals( DocumentIterator.END_OF_LIST, inclusionIterator.skipTo( 10 ) );
		assertEquals( END_OF_LIST, inclusionIterator.nextDocument() );
	
		firstIterator.reset();
		secondIterator.reset();

		inclusionIterator = InclusionDocumentIterator.getInstance( firstIterator, secondIterator );
		assertEquals( 2, inclusionIterator.skipTo( 1 ) );
		assertEquals( Interval.valueOf( 1, 2 ), inclusionIterator.intervalIterator( INDEX ).nextInterval() );
		assertEquals( Interval.valueOf( 2, 3 ), inclusionIterator.intervalIterator( INDEX ).nextInterval() );

		assertEquals( DocumentIterator.END_OF_LIST, inclusionIterator.skipTo( 4 ) );
		assertEquals( END_OF_LIST, inclusionIterator.nextDocument() );

		firstIterator.reset();
		secondIterator.reset();
		inclusionIterator = InclusionDocumentIterator.getInstance( firstIterator, secondIterator );
		assertEquals( 2, inclusionIterator.skipTo( 2 ) );
		assertEquals( Interval.valueOf( 1, 2 ), inclusionIterator.intervalIterator( INDEX ).nextInterval() );
		assertEquals( Interval.valueOf( 2, 3 ), inclusionIterator.intervalIterator( INDEX ).nextInterval() );

		firstIterator.reset();
		secondIterator.reset();
		inclusionIterator = InclusionDocumentIterator.getInstance( firstIterator, secondIterator );
		assertEquals( DocumentIterator.END_OF_LIST, inclusionIterator.skipTo( 10 ) );
	}

	@Test
	public void testtrueFalseDifference() throws IOException {
		IntArrayDocumentIterator firstIterator = new IntArrayDocumentIterator( new long[] { 0, 1, 2, 3 }, 
				new int[][][] { 
				{ {} }, 
				{},
				{ {} }, 
				{},
				} );
		IntArrayDocumentIterator secondIterator = new IntArrayDocumentIterator( new long[] { 0, 1, 2, 3 }, 
				new int[][][] { 
				{ {} }, 
				{ {} }, 
				{},
				{},
				} );
		DocumentIterator inclusionIterator = InclusionDocumentIterator.getInstance( firstIterator, secondIterator );
		assertTrue( inclusionIterator.mayHaveNext() );
		assertEquals( 0, inclusionIterator.nextDocument() );
		assertEquals( IntervalIterators.TRUE, inclusionIterator.intervalIterator() );
		assertEquals( END_OF_LIST, inclusionIterator.nextDocument() );

		firstIterator.reset();
		secondIterator.reset();

		inclusionIterator = InclusionDocumentIterator.getInstance( firstIterator, secondIterator );
		assertTrue( inclusionIterator.mayHaveNext() );
		assertEquals( 0, inclusionIterator.nextDocument() );
		assertEquals( IntervalIterators.TRUE, inclusionIterator.intervalIterator( INDEX ) );
		assertEquals( END_OF_LIST, inclusionIterator.nextDocument() );	
	}

	@Test
	public void testTrueFalseOtherDifference() throws IOException {
		IntArrayDocumentIterator firstIterator = new IntArrayDocumentIterator( new long[] { 0, 1, 2, 3 }, 
				new int[][][] { 
				{ { 0, 1 }, { 1, 2 } }, 
				{ { 0, 1 }, { 1, 2 } },
				{ {} }, 
				{},
				} );
		IntArrayDocumentIterator secondIterator = new IntArrayDocumentIterator( new long[] { 0, 1, 2, 3 }, 
				new int[][][] { 
				{ {} }, 
				{},
				{ { 0, 1 }, { 1, 2 } }, 
				{ { 0, 1 }, { 1, 2 } },
				} );
		DocumentIterator inclusionIterator = InclusionDocumentIterator.getInstance( firstIterator, secondIterator );
		assertTrue( inclusionIterator.mayHaveNext() );
		assertEquals( 2, inclusionIterator.nextDocument() );
		assertEquals( IntervalIterators.TRUE, inclusionIterator.intervalIterator() );
		assertEquals( END_OF_LIST, inclusionIterator.nextDocument() );

		firstIterator.reset();
		secondIterator.reset();

		inclusionIterator = InclusionDocumentIterator.getInstance( firstIterator, secondIterator );
		assertTrue( inclusionIterator.mayHaveNext() );
		assertEquals( 2, inclusionIterator.nextDocument() );
		assertEquals( IntervalIterators.TRUE, inclusionIterator.intervalIterator( INDEX ) );
		assertEquals( END_OF_LIST, inclusionIterator.nextDocument() );
}

	@Test
	public void testEnlargment() throws IOException {
		IntArrayDocumentIterator firstIterator = new IntArrayDocumentIterator( new long[] { 0, 1, 2 }, 
				new int[][][] { 
				{ { 0, 1 }, { 1, 2 } }, 
				{ { 0, 1 }, { 1, 2 } },
				{ { 0, 3 } }, 
				} );
		IntArrayDocumentIterator secondIterator = new IntArrayDocumentIterator( new long[] { 0, 1, 2 }, 
				new int[][][] { 
				{ {} }, 
				{ { 0, 0 } }, 
				{ { 1, 2 }, { 4 } },
				} );
		DocumentIterator inclusionIterator = InclusionDocumentIterator.getInstance( firstIterator, secondIterator, 1, 1 );
		assertTrue( inclusionIterator.mayHaveNext() );
		assertEquals( 1, inclusionIterator.nextDocument() );
		assertEquals( Interval.valueOf( 0, 1 ), inclusionIterator.intervalIterator().nextInterval() );
		assertNull( inclusionIterator.intervalIterator().nextInterval() );
		assertEquals( 2, inclusionIterator.nextDocument() );
		assertEquals( Interval.valueOf( 0, 3 ), inclusionIterator.intervalIterator().nextInterval() );
		assertNull( inclusionIterator.intervalIterator().nextInterval() );
		assertFalse( inclusionIterator.mayHaveNext() );
		assertEquals( END_OF_LIST, inclusionIterator.nextDocument() );

		firstIterator.reset();
		secondIterator.reset();

		inclusionIterator = InclusionDocumentIterator.getInstance( firstIterator, secondIterator, 1, 1 );
		assertTrue( inclusionIterator.mayHaveNext() );
		assertEquals( 1, inclusionIterator.nextDocument() );
		assertEquals( Interval.valueOf( 0, 1 ), inclusionIterator.intervalIterator( INDEX ).nextInterval() );
		assertNull( inclusionIterator.intervalIterator( INDEX ).nextInterval() );
		assertEquals( 2, inclusionIterator.nextDocument() );
		assertEquals( Interval.valueOf( 0, 3 ), inclusionIterator.intervalIterator( INDEX ).nextInterval() );
		assertNull( inclusionIterator.intervalIterator( INDEX ).nextInterval() );
		assertFalse( inclusionIterator.mayHaveNext() );
		assertEquals( END_OF_LIST, inclusionIterator.nextDocument() );

	}

	@Test
	public void testAlignment() throws IOException {
		IntArrayDocumentIterator firstIterator = new IntArrayDocumentIterator( new long[] { 0, 2, 4, 8, 9, 12 }, 
				new int[][][] { 
				{ {} }, 
				{ {} }, 
				{ {} }, 
				{ {} }, 
				{ {} }, 
				{ {} }, 
				} );
		IntArrayDocumentIterator secondIterator = new IntArrayDocumentIterator( new long[] { 1, 2, 3, 5, 8, 10, 12, 13 }, 
				new int[][][] { 
				{ {} }, 
				{ {} }, 
				{ {} }, 
				{ {} }, 
				{ {} }, 
				{ {} }, 
				{ {} }, 
				{ {} }, 
				} );
		DocumentIterator inclusionIterator = InclusionDocumentIterator.getInstance( firstIterator, secondIterator );
		assertTrue( inclusionIterator.mayHaveNext() );
		assertEquals( 2, inclusionIterator.nextDocument() );
		assertEquals( 8, inclusionIterator.nextDocument() );
		assertEquals( 12, inclusionIterator.nextDocument() );
	}

}