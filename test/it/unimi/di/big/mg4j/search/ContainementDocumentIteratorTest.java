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

public class ContainementDocumentIteratorTest {

	@Test
	public void testContainment() throws IOException {
		IntArrayDocumentIterator firstIterator = new IntArrayDocumentIterator( new long[] { 1, 2, 4 }, 
				new int[][][] { 
				{ { 0, 2 }, { 2, 4 } },
				{ { 1, 3 } },
				{ { 0, 2 }, { 2, 4 }, { 4, 6 } }
				} );
		IntArrayDocumentIterator secondIterator =new IntArrayDocumentIterator( new long[] { 0, 1, 2, 3, 4 }, 
				new int[][][] { 
				{ { 0, 1 } }, 
				{ { 0, 1 }, { 1, 2 }, { 2, 3 } },
				{ { 0, 1 }, { 1, 2 }, { 2, 3 } },
				{ { 0 } },
				{ { 1, 3 }, { 3, 5 } }
				} ); 
		DocumentIterator containmentIterator = ContainmentDocumentIterator.getInstance( firstIterator, secondIterator );
		assertTrue( containmentIterator.mayHaveNext() );
		assertTrue( containmentIterator.mayHaveNext() ); // To increase coverage
		assertEquals( 1, containmentIterator.nextDocument() );
		assertEquals( 1, containmentIterator.document() );
		assertEquals( Interval.valueOf( 0, 2 ), containmentIterator.intervalIterator().nextInterval() );
		assertEquals( Interval.valueOf( 2, 4 ), containmentIterator.intervalIterator().nextInterval() );
		assertNull( containmentIterator.intervalIterator().nextInterval() );
		assertEquals( 2, containmentIterator.nextDocument() );
		assertEquals( 2, containmentIterator.document() );
		assertEquals( Interval.valueOf( 1, 3 ), containmentIterator.intervalIterator().nextInterval() );
		assertNull( containmentIterator.intervalIterator().nextInterval() );
		assertEquals( END_OF_LIST, containmentIterator.nextDocument() );
		
		firstIterator.reset();
		secondIterator.reset();
		
		containmentIterator = ContainmentDocumentIterator.getInstance( firstIterator, secondIterator );
		assertTrue( containmentIterator.mayHaveNext() );
		assertTrue( containmentIterator.mayHaveNext() ); // To increase coverage
		assertEquals( 1, containmentIterator.nextDocument() );
		assertEquals( 1, containmentIterator.document() );
		assertEquals( Interval.valueOf( 0, 2 ), containmentIterator.intervalIterator().nextInterval() );
		assertEquals( Interval.valueOf( 2, 4 ), containmentIterator.intervalIterator().nextInterval() );
		assertNull( containmentIterator.intervalIterator( INDEX ).nextInterval() );
		assertEquals( 2, containmentIterator.nextDocument() );
		assertEquals( 2, containmentIterator.document() );
		assertEquals( Interval.valueOf( 1, 3 ), containmentIterator.intervalIterator( INDEX ).nextInterval() );
		assertNull( containmentIterator.intervalIterator( INDEX ).nextInterval() );
		assertEquals( END_OF_LIST, containmentIterator.nextDocument() );
		
	} 

	@Test
	public void testskipTo() throws IOException {
		IntArrayDocumentIterator firstIterator = new IntArrayDocumentIterator( new long[] { 1, 2, 3, 4 }, 
				new int[][][] { 
				{ { 1 } },
				{ { 1, 3 } },
				{ {} },
				{ {} },
				} ); 
		IntArrayDocumentIterator secondIterator = new IntArrayDocumentIterator( new long[] { 0, 1, 2, 3, 4 }, 
				new int[][][] { 
				{ { 0, 1 } }, 
				{ { 0, 1 }, { 1, 2 } },
				{ { 0, 1 }, { 1, 2 }, { 2, 3 } },
				{ { 0 } },
				{ { 0 } }
				} );
		DocumentIterator containmentIterator = ContainmentDocumentIterator.getInstance( firstIterator, secondIterator );
		assertEquals( 2, containmentIterator.skipTo( 1 ) );
		assertEquals( Interval.valueOf( 1, 3 ), containmentIterator.intervalIterator().nextInterval() );
		
		assertEquals( DocumentIterator.END_OF_LIST, containmentIterator.skipTo( 4 ) );
		assertEquals( END_OF_LIST, containmentIterator.nextDocument() );

		firstIterator.reset();
		secondIterator.reset();
		containmentIterator = ContainmentDocumentIterator.getInstance( firstIterator, secondIterator );
		assertEquals( 2, containmentIterator.skipTo( 2 ) );
		assertEquals( Interval.valueOf( 1, 3 ), containmentIterator.intervalIterator().nextInterval() );
		
		firstIterator.reset();
		secondIterator.reset();
		containmentIterator = ContainmentDocumentIterator.getInstance( firstIterator, secondIterator );
		assertEquals( DocumentIterator.END_OF_LIST, containmentIterator.skipTo( 10 ) );
		assertEquals( END_OF_LIST, containmentIterator.nextDocument() );
	
		firstIterator.reset();
		secondIterator.reset();

		containmentIterator = ContainmentDocumentIterator.getInstance( firstIterator, secondIterator );
		assertEquals( 2, containmentIterator.skipTo( 1 ) );
		assertEquals( Interval.valueOf( 1, 3 ), containmentIterator.intervalIterator().nextInterval() );

		assertEquals( DocumentIterator.END_OF_LIST, containmentIterator.skipTo( 4 ) );
		assertEquals( END_OF_LIST, containmentIterator.nextDocument() );

		firstIterator.reset();
		secondIterator.reset();
		containmentIterator = ContainmentDocumentIterator.getInstance( firstIterator, secondIterator );
		assertEquals( 2, containmentIterator.skipTo( 2 ) );
		assertEquals( Interval.valueOf( 1, 3 ), containmentIterator.intervalIterator().nextInterval() );

		firstIterator.reset();
		secondIterator.reset();
		containmentIterator = ContainmentDocumentIterator.getInstance( firstIterator, secondIterator );
		assertEquals( DocumentIterator.END_OF_LIST, containmentIterator.skipTo( 10 ) );
	}

	@Test
	public void testtrueFalseDifference() throws IOException {
		IntArrayDocumentIterator firstIterator = new IntArrayDocumentIterator( new long[] { 0, 1, 2, 3 }, 
				new int[][][] { 
				{ {} }, 
				{ {} }, 
				{},
				{},
				} );
		IntArrayDocumentIterator secondIterator =  new IntArrayDocumentIterator( new long[] { 0, 1, 2, 3 }, 
				new int[][][] { 
				{ {} }, 
				{},
				{ {} }, 
				{},
				} );
		DocumentIterator containmentIterator = ContainmentDocumentIterator.getInstance( firstIterator, secondIterator );
		assertTrue( containmentIterator.mayHaveNext() );
		assertEquals( 0, containmentIterator.nextDocument() );
		assertEquals( IntervalIterators.TRUE, containmentIterator.intervalIterator() );
		assertEquals( END_OF_LIST, containmentIterator.nextDocument() );

		firstIterator.reset();
		secondIterator.reset();

		containmentIterator = ContainmentDocumentIterator.getInstance( firstIterator, secondIterator );
		assertTrue( containmentIterator.mayHaveNext() );
		assertEquals( 0, containmentIterator.nextDocument() );
		assertEquals( IntervalIterators.TRUE, containmentIterator.intervalIterator( INDEX ) );
		assertEquals( END_OF_LIST, containmentIterator.nextDocument() );	
	}

	@Test
	public void testTrueFalseOtherDifference() throws IOException {
		IntArrayDocumentIterator firstIterator = new IntArrayDocumentIterator( new long[] { 0, 1, 2, 3 }, 
				new int[][][] { 
				{ {} }, 
				{},
				{ { 0, 1 }, { 1, 2 } }, 
				{ { 0, 1 }, { 1, 2 } },
				} );
		IntArrayDocumentIterator secondIterator =  new IntArrayDocumentIterator( new long[] { 0, 1, 2, 3 }, 
				new int[][][] { 
				{ { 0, 1 }, { 1, 2 } }, 
				{ { 0, 1 }, { 1, 2 } },
				{ {} }, 
				{},
				} );
		DocumentIterator containmentIterator = ContainmentDocumentIterator.getInstance( firstIterator, secondIterator );
		assertTrue( containmentIterator.mayHaveNext() );
		assertEquals( 2, containmentIterator.nextDocument() );
		assertEquals( Interval.valueOf( 0, 1 ), containmentIterator.intervalIterator().nextInterval() );
		assertEquals( Interval.valueOf( 1, 2 ), containmentIterator.intervalIterator().nextInterval() );
		assertEquals( END_OF_LIST, containmentIterator.nextDocument() );

		firstIterator.reset();
		secondIterator.reset();

		containmentIterator = ContainmentDocumentIterator.getInstance( firstIterator, secondIterator );
		assertTrue( containmentIterator.mayHaveNext() );
		assertEquals( 2, containmentIterator.nextDocument() );
		assertEquals( Interval.valueOf( 0, 1 ), containmentIterator.intervalIterator().nextInterval() );
		assertEquals( Interval.valueOf( 1, 2 ), containmentIterator.intervalIterator().nextInterval() );
		assertEquals( END_OF_LIST, containmentIterator.nextDocument() );
}

	@Test
	public void testEnlargment() throws IOException {
		IntArrayDocumentIterator firstIterator = new IntArrayDocumentIterator( new long[] { 0, 1, 2 }, 
				new int[][][] { 
				{ {} }, 
				{ { 0, 0 } }, 
				{ { 1, 2 }, { 4 } },
				} );
		IntArrayDocumentIterator secondIterator = new IntArrayDocumentIterator( new long[] { 0, 1, 2 }, 
				new int[][][] { 
				{ { 0, 1 }, { 1, 2 } }, 
				{ { 0, 1 }, { 1, 2 } },
				{ { 0, 3 } }, 
				} );
		DocumentIterator containmentIterator = ContainmentDocumentIterator.getInstance( firstIterator, secondIterator, 1, 1 );
		assertTrue( containmentIterator.mayHaveNext() );
		assertEquals( 1, containmentIterator.nextDocument() );
		assertEquals( Interval.valueOf( 0, 0 ), containmentIterator.intervalIterator().nextInterval() );
		assertNull( containmentIterator.intervalIterator().nextInterval() );
		assertEquals( 2, containmentIterator.nextDocument() );
		assertEquals( Interval.valueOf( 1, 2 ), containmentIterator.intervalIterator().nextInterval() );
		assertNull( containmentIterator.intervalIterator().nextInterval() );
		assertFalse( containmentIterator.mayHaveNext() );
		assertEquals( END_OF_LIST, containmentIterator.nextDocument() );

		firstIterator.reset();
		secondIterator.reset();

		containmentIterator = ContainmentDocumentIterator.getInstance( firstIterator, secondIterator, 1, 1 );
		assertTrue( containmentIterator.mayHaveNext() );
		assertEquals( 1, containmentIterator.nextDocument() );
		assertEquals( Interval.valueOf( 0, 0 ), containmentIterator.intervalIterator( INDEX ).nextInterval() );
		assertNull( containmentIterator.intervalIterator( INDEX ).nextInterval() );
		assertEquals( 2, containmentIterator.nextDocument() );
		assertEquals( Interval.valueOf( 1, 2 ), containmentIterator.intervalIterator( INDEX ).nextInterval() );
		assertNull( containmentIterator.intervalIterator( INDEX ).nextInterval() );
		assertFalse( containmentIterator.mayHaveNext() );
		assertEquals( END_OF_LIST, containmentIterator.nextDocument() );

	}

	@Test
	public void testAlignment() throws IOException {
		IntArrayDocumentIterator firstIterator = new IntArrayDocumentIterator( new long[] { 1, 2, 3, 5, 8, 10, 12, 13 }, 
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
		IntArrayDocumentIterator secondIterator = new IntArrayDocumentIterator( new long[] { 0, 2, 4, 8, 9, 12 }, 
				new int[][][] { 
				{ {} }, 
				{ {} }, 
				{ {} }, 
				{ {} }, 
				{ {} }, 
				{ {} }, 
				} );
		DocumentIterator containmentIterator = ContainmentDocumentIterator.getInstance( firstIterator, secondIterator );
		assertTrue( containmentIterator.mayHaveNext() );
		assertEquals( 2, containmentIterator.nextDocument() );
		assertEquals( 8, containmentIterator.nextDocument() );
		assertEquals( 12, containmentIterator.nextDocument() );
	}

}