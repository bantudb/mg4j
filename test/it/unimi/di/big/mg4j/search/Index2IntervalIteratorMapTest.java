package it.unimi.di.big.mg4j.search;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import it.unimi.di.big.mg4j.index.Index;
import it.unimi.di.big.mg4j.index.IndexReader;
import it.unimi.dsi.fastutil.longs.LongSet;
import it.unimi.dsi.util.Interval;

import java.io.IOException;

import org.junit.Test;

public class Index2IntervalIteratorMapTest {

	private final static class FakeIndex extends Index {
		private static final long serialVersionUID = 1L;

		private FakeIndex() {
			super( 0, 0, 0, 0, 0, null, false, false, null, null, null, null, null, null );
		}

		public IndexReader getReader( int bufferSize ) throws IOException {
			return null;
		}
	}

	private final static class FakeIntervalIterator implements IntervalIterator {
		@Override
		public IntervalIterator reset() throws IOException {
			return this;
		}
		@Override
		public Interval nextInterval() throws IOException {
			return null;
		}
		@Override
		public void intervalTerms( final LongSet terms ) {
		}
		@Override
		public int extent() {
			return 0;
		}
	}

	@Test
	public void testMap() {
		Index index0 = new FakeIndex();
		Index index1 = new FakeIndex();
		IntervalIterator intervalIterator0 = new FakeIntervalIterator();
		IntervalIterator intervalIterator1 = new FakeIntervalIterator();
		
		Index2IntervalIteratorMap map = new Index2IntervalIteratorMap( 2 );
		
		assertNull( map.put( index0,  intervalIterator0 ) );
		assertSame( intervalIterator0, map.get( index0 ) );
		assertSame( intervalIterator0, map.put( index0, intervalIterator0 ) );
		map.add( index0, intervalIterator0 );
		assertSame( intervalIterator0, map.get( index0 ) );

		assertNull( map.put( index1,  intervalIterator1 ) );
		assertSame( intervalIterator1, map.get( index1 ) );
		assertSame( intervalIterator1, map.put( index1, intervalIterator0 ) );
		assertSame( intervalIterator0, map.get( index0 ) );
		assertSame( intervalIterator0, map.put( index0, intervalIterator0 ) );
		map.add( index1, intervalIterator1 );
		assertSame( intervalIterator1, map.get( index1 ) );
		assertSame( intervalIterator0, map.get( index0 ) );
		assertEquals( intervalIterator0, map.remove( index0 ) );
		assertNull( map.get( index0 ) );
		assertSame( intervalIterator1, map.get( index1 ) );
		
		map.clear();
		assertNull( map.get( index0 ) );
		assertNull( map.get( index1 ) );
		map.add( index0, intervalIterator0 );
		assertSame( intervalIterator0, map.get( index0 ) );
		map.add( index1, intervalIterator1 );
	}
}
