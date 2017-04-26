package it.unimi.di.big.mg4j.tool;

import static it.unimi.di.big.mg4j.index.CompressionFlags.DEFAULT_PAYLOAD_INDEX;
import static it.unimi.di.big.mg4j.index.DiskBasedIndex.FREQUENCIES_EXTENSION;
import static it.unimi.di.big.mg4j.index.DiskBasedIndex.INDEX_EXTENSION;
import static it.unimi.di.big.mg4j.index.DiskBasedIndex.OCCURRENCIES_EXTENSION;
import static it.unimi.di.big.mg4j.index.DiskBasedIndex.OFFSETS_EXTENSION;
import static it.unimi.di.big.mg4j.index.DiskBasedIndex.PROPERTIES_EXTENSION;
import static it.unimi.di.big.mg4j.index.DiskBasedIndex.SIZES_EXTENSION;
import static it.unimi.di.big.mg4j.index.DiskBasedIndex.TERMMAP_EXTENSION;
import static it.unimi.di.big.mg4j.index.DiskBasedIndex.TERMS_EXTENSION;
import static it.unimi.di.big.mg4j.search.DocumentIterator.END_OF_LIST;
import static it.unimi.di.big.mg4j.tool.Combine.IndexType.HIGH_PERFORMANCE;
import static it.unimi.di.big.mg4j.tool.Combine.IndexType.INTERLEAVED;
import static it.unimi.di.big.mg4j.tool.Combine.IndexType.QUASI_SUCCINCT;
import static it.unimi.dsi.logging.ProgressLogger.DEFAULT_LOG_INTERVAL;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import it.unimi.di.big.mg4j.document.AbstractDocumentSequence;
import it.unimi.di.big.mg4j.document.CompositeDocumentSequence;
import it.unimi.di.big.mg4j.document.DateArrayDocumentCollection;
import it.unimi.di.big.mg4j.document.Document;
import it.unimi.di.big.mg4j.document.DocumentCollection;
import it.unimi.di.big.mg4j.document.DocumentFactory;
import it.unimi.di.big.mg4j.document.DocumentFactory.FieldType;
import it.unimi.di.big.mg4j.document.DocumentIterator;
import it.unimi.di.big.mg4j.document.DocumentSequence;
import it.unimi.di.big.mg4j.document.IdentityDocumentFactory;
import it.unimi.di.big.mg4j.document.InputStreamDocumentSequence;
import it.unimi.di.big.mg4j.document.IntArrayDocumentCollection;
import it.unimi.di.big.mg4j.document.MapVirtualDocumentCollection;
import it.unimi.di.big.mg4j.document.SimpleCompressedDocumentCollectionBuilder;
import it.unimi.di.big.mg4j.document.StringArrayDocumentCollection;
import it.unimi.di.big.mg4j.document.ZipDocumentCollectionBuilder;
import it.unimi.di.big.mg4j.index.BitStreamIndex;
import it.unimi.di.big.mg4j.index.CompressionFlags;
import it.unimi.di.big.mg4j.index.CompressionFlags.Coding;
import it.unimi.di.big.mg4j.index.CompressionFlags.Component;
import it.unimi.di.big.mg4j.index.DiskBasedIndex;
import it.unimi.di.big.mg4j.index.DowncaseTermProcessor;
import it.unimi.di.big.mg4j.index.Index;
import it.unimi.di.big.mg4j.index.IndexIterator;
import it.unimi.di.big.mg4j.index.IndexIterators;
import it.unimi.di.big.mg4j.index.IndexReader;
import it.unimi.di.big.mg4j.index.TermProcessor;
import it.unimi.di.big.mg4j.index.cluster.DocumentalPartitioningStrategy;
import it.unimi.di.big.mg4j.index.cluster.DocumentalStrategies;
import it.unimi.di.big.mg4j.index.cluster.IndexCluster;
import it.unimi.di.big.mg4j.index.cluster.LexicalPartitioningStrategy;
import it.unimi.di.big.mg4j.index.cluster.LexicalStrategies;
import it.unimi.di.big.mg4j.io.IOFactory;
import it.unimi.di.big.mg4j.tool.Combine.IndexType;
import it.unimi.di.big.mg4j.tool.Scan.VirtualDocumentFragment;
import it.unimi.dsi.big.io.FileLinesCollection;
import it.unimi.dsi.big.io.FileLinesCollection.FileLinesIterator;
import it.unimi.dsi.big.util.ShiftAddXorSignedStringMap;
import it.unimi.dsi.big.util.StringMap;
import it.unimi.dsi.bits.TransformationStrategies;
import it.unimi.dsi.fastutil.Arrays;
import it.unimi.dsi.fastutil.Swapper;
import it.unimi.dsi.fastutil.ints.AbstractIntComparator;
import it.unimi.dsi.fastutil.ints.Int2ObjectArrayMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntIterators;
import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.objects.Reference2ObjectOpenHashMap;
import it.unimi.dsi.io.FastBufferedReader;
import it.unimi.dsi.io.InputBitStream;
import it.unimi.dsi.io.WordReader;
import it.unimi.dsi.lang.MutableString;
import it.unimi.dsi.sux4j.mph.GOV3Function;
import it.unimi.dsi.util.Properties;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.Reader;
import java.lang.reflect.InvocationTargetException;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.Map;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.filefilter.FileFilterUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class IndexTest {

	private static StringMap<? extends CharSequence> createMap( String basename ) throws IOException {
		FileLinesCollection flc = new FileLinesCollection( basename, "UTF-8" );
		return new ShiftAddXorSignedStringMap( flc.iterator(), new GOV3Function.Builder<CharSequence>().keys(flc).transform(TransformationStrategies.utf16()).build() );
	}
	
	
	private String basename;

	private final int NUMBER_OF_DOCUMENTS = 100;

	private final int[] INTEGER_DOCUMENT = new int[ NUMBER_OF_DOCUMENTS ];

	private final Date[] DATE_DOCUMENT = new Date[ NUMBER_OF_DOCUMENTS ];

	@SuppressWarnings("unchecked")
	private final Int2ObjectMap<String>[] VIRTUAL_DOCUMENT = new Int2ObjectMap[ NUMBER_OF_DOCUMENTS ];
	{
		for ( int i = INTEGER_DOCUMENT.length; i-- != 0; )
			INTEGER_DOCUMENT[ i ] = i;
		for ( int i = DATE_DOCUMENT.length; i-- != 0; )
			DATE_DOCUMENT[ i ] = new Date( i * 86400000L );
		for ( int i = VIRTUAL_DOCUMENT.length; i-- != 0; ) {
			VIRTUAL_DOCUMENT[ i ] = new Int2ObjectArrayMap<String>();
			VIRTUAL_DOCUMENT[ i ].put( i - 1, "link _ to previous document link" );
			VIRTUAL_DOCUMENT[ i ].put( i, "link to this document link" );
			VIRTUAL_DOCUMENT[ i ].put( i + 1, "link to next document link" );
		}
	}

	private final VirtualDocumentResolver RESOLVER = new MapVirtualDocumentCollection.TrivialVirtualDocumentResolver( NUMBER_OF_DOCUMENTS );

	public static Reference2ObjectOpenHashMap<Component, Coding> defaultStandardIndex() {
		return new Reference2ObjectOpenHashMap<Component, Coding>( CompressionFlags.DEFAULT_STANDARD_INDEX );
	}
	
	public static Reference2ObjectOpenHashMap<Component, Coding> defaultQuasiSuccinctIndex() {
		return new Reference2ObjectOpenHashMap<Component, Coding>( CompressionFlags.DEFAULT_QUASI_SUCCINCT_INDEX );
	}
	
	public final static TermProcessor KILL_A_PROCESSOR = KillATermProcessor.getInstance();

	public final static class KillATermProcessor implements TermProcessor {
		private static final long serialVersionUID = 1L;

		private static final KillATermProcessor INSTANCE = new KillATermProcessor();

		public TermProcessor copy() {
			return this;
		}

		public static TermProcessor getInstance() {
			return INSTANCE;
		}

		public boolean processPrefix( MutableString prefix ) {
			return true;
		}

		public boolean processTerm( MutableString term ) {
			return term.indexOf( 'a' ) == -1;
		}
	};

	final static int[] INDEXED_FIELD = { 0, 1, 2 };

	/**
	 * Checks that the two provided indices are byte-by-byte the same, and that property files
	 * coincide except for the provided property keys.
	 * 
	 * @param basename0 the basename of an index.
	 * @param basename1 the basename of an index.
	 * @param excludedProperty a list of property keys that will not be considered when evaluating
	 * the equality of property fields.
	 */
	private void sameIndex( final String basename0, final String basename1, final String... excludedProperty ) throws IOException, ConfigurationException {
		// The two indices must be byte-by-byte identical in all components
		for ( String ext : new String[] { INDEX_EXTENSION, OFFSETS_EXTENSION, TERMS_EXTENSION, SIZES_EXTENSION, FREQUENCIES_EXTENSION, OCCURRENCIES_EXTENSION } ) {
			File f0 = new File( basename0 + ext );
			File f1 = new File( basename1 + ext );
			assertEquals( ext, Boolean.valueOf( f0.exists() ), Boolean.valueOf( f1.exists() ) );
			if ( ext != SIZES_EXTENSION && f0.exists() ) assertTrue( ext, IOUtils.contentEquals( new FileInputStream( f0 ), new FileInputStream( f1 ) ) );
		}

		Properties properties0 = new Properties( basename0 + PROPERTIES_EXTENSION );
		Properties properties1 = new Properties( basename1 + PROPERTIES_EXTENSION );
		for ( String p : excludedProperty ) {
			properties0.setProperty( p, null );
			properties1.setProperty( p, null );
		}

		assertEquals( properties0, properties1 );
	}

	public static void sameContent( CharSequence basename0, CharSequence basename1, it.unimi.dsi.big.io.FileLinesCollection.FileLinesIterator fileLinesIterator ) throws ConfigurationException, SecurityException, IOException, URISyntaxException,
			ClassNotFoundException, InstantiationException, IllegalAccessException, InvocationTargetException, NoSuchMethodException {
		sameContent( it.unimi.di.big.mg4j.index.Index.getInstance( basename0 ), it.unimi.di.big.mg4j.index.Index.getInstance( basename1 ), fileLinesIterator );
	}

	public static void sameContent( CharSequence basename0, CharSequence basename1 ) throws ConfigurationException, SecurityException, IOException, URISyntaxException, ClassNotFoundException,
			InstantiationException, IllegalAccessException, InvocationTargetException, NoSuchMethodException {
		sameContent( basename0, basename1, null );
	}

	public static void sameContent( it.unimi.di.big.mg4j.index.Index index0, it.unimi.di.big.mg4j.index.Index index1 ) throws IOException {
		sameContent( index0, index1, null );
	}

	public static void sameContent( it.unimi.di.big.mg4j.index.Index index0, it.unimi.di.big.mg4j.index.Index index1, final FileLinesIterator terms ) throws IOException {
		assertEquals( Boolean.valueOf( index0.hasCounts ), Boolean.valueOf( index1.hasCounts ) );
		assertEquals( Boolean.valueOf( index0.hasPositions ), Boolean.valueOf( index1.hasPositions ) );
		assertEquals( Boolean.valueOf( index0.hasPayloads ), Boolean.valueOf( index1.hasPayloads ) );
		assertEquals( index0.numberOfTerms, index1.numberOfTerms );
		assertEquals( index0.numberOfDocuments, index1.numberOfDocuments );

		final long numTerms = index0.numberOfTerms;
		long document;
		boolean hasCounts = index0.hasCounts, hasPositions = index0.hasPositions;
		final IndexReader reader0 = index0.getReader(), reader1 = index1.getReader();
		IndexIterator i0, i1;
		for ( int i = 0; i < numTerms; i++ ) {
			if ( terms != null ) {
				final CharSequence term = terms.next();
				i0 = reader0.documents( term );
				i1 = reader1.documents( term );
			}
			else {
				i0 = reader0.documents( i );
				i1 = reader1.documents( i );
			}

			while ( i0.mayHaveNext() && i1.mayHaveNext() ) {
				assertEquals( "term " + i, document = i0.nextDocument(), i1.nextDocument() );
				if ( document == END_OF_LIST ) break;
				if ( hasCounts ) {
					assertEquals( "term " + i + ", document " + document, i0.count(), i1.count() );
					if ( hasPositions ) for ( int p = 0; p <= i0.count(); p++ ) assertEquals( "term " + i + ", document " + document + ", position " + p, i0.nextPosition(), i1.nextPosition() );
				}
			}
			
			assertEquals( "term " + i, i0.document(), i1.document() );
		}
		reader0.close();
		reader1.close();
	}


	public static int processDocument( WordReader wordReader, int documentIndex, int startPos, Object2ObjectOpenHashMap<MutableString, ObjectArrayList<int[]>> termMap, TermProcessor termProcessor )
			throws IOException {
		assertTrue( documentIndex >= 0 );
		Object2ObjectOpenHashMap<MutableString, IntArrayList> terms = new Object2ObjectOpenHashMap<MutableString, IntArrayList>();
		MutableString word = new MutableString(), nonWord = new MutableString();

		int pos = startPos;
		while ( wordReader.next( word, nonWord ) ) {
			if ( word.length() == 0 ) continue;
			if ( !termProcessor.processTerm( word ) ) {
				pos++;
				continue;
			}
			IntArrayList positions = terms.get( word );
			if ( positions == null ) terms.put( word.copy(), positions = new IntArrayList() );
			positions.add( pos++ );
		}

		for ( MutableString term : terms.keySet() ) {
			ObjectArrayList<int[]> list = termMap.get( term );
			IntArrayList positions = terms.get( term );
			if ( list == null ) termMap.put( term, list = new ObjectArrayList<int[]>() );

			int[] t = new int[ positions.size() + 1 ];
			t[ 0 ] = documentIndex;
			System.arraycopy( positions.elements(), 0, t, 1, positions.size() );
			list.add( t );
		}

		return pos;
	}

	/**
	 * Checks that the fields indexed by the given indices have been indexed correctly by performing
	 * a mock index construction over the given sequence.
	 * 
	 * @param sequence a document sequence.
	 * @param map a renumbering for the documents, or <code>null</code>.
	 * @param resolver the virtual document resolver used to index the collection (we assume the
	 * same for all virtual fields), or <code>null</code>.
	 * @param gap the virtual document gap (we assume the same for all virtual fields; it is
	 * immaterial if no field is virtual).
	 * @param basename the basename of the indices. 
	 * @param index a list of indices that have indexed one or more fields of <code>sequence</code>.
	 */
	@SuppressWarnings("unchecked")
	public static void checkAgainstContent( DocumentSequence sequence, int[] map, VirtualDocumentResolver resolver, int gap, CharSequence basename, Index... index ) throws IOException {
		DocumentIterator iterator = sequence.iterator();
		DocumentFactory factory = sequence.factory();
		Document document;
		final int n = index.length;
		final int[] field = new int[ n ];
		final int[][] currMaxPos = new int[ n ][];
		final int[] maxDoc = new int[ n ];
		java.util.Arrays.fill( maxDoc, -1 );
		final Object2ObjectOpenHashMap<MutableString, ObjectArrayList<int[]>>[] termMap = new Object2ObjectOpenHashMap[ n ];
		final IntArrayList[] payloadPointers = new IntArrayList[ n ];
		final ObjectArrayList<Object>[] payloadContent = new ObjectArrayList[ n ];

		for ( int i = 0; i < n; i++ ) {
			field[ i ] = factory.fieldIndex( index[ i ].field );
			switch ( factory.fieldType( field[ i ] ) ) {
			case VIRTUAL:
				currMaxPos[ i ] = new int[ (int)resolver.numberOfDocuments() ];
			case TEXT:
				termMap[ i ] = new Object2ObjectOpenHashMap<MutableString, ObjectArrayList<int[]>>();
				break;
			case DATE:
			case INT:
				payloadPointers[ i ] = new IntArrayList();
				payloadContent[ i ] = new ObjectArrayList<Object>();
			}
		}

		int documentIndex = 0;

		while ( ( document = iterator.nextDocument() ) != null ) {
			for ( int i = 0; i < field.length; i++ ) {
				switch ( factory.fieldType( field[ i ] ) ) {
				case TEXT:
					processDocument( document.wordReader( field[ i ] ).setReader( (Reader)document.content( field[ i ] ) ), map == null ? documentIndex : map[ documentIndex ], 0, termMap[ i ],
							index[ i ].termProcessor );
					break;
				case VIRTUAL:
					ObjectArrayList<VirtualDocumentFragment> fragments = (ObjectArrayList<VirtualDocumentFragment>)document.content( field[ i ] );
					resolver.context( document );
					for ( VirtualDocumentFragment fragment : fragments ) {
						int d = (int)resolver.resolve( fragment.documentSpecifier() );

						if ( d != -1 ) {
							if ( map != null ) d = map[ d ];
							if ( maxDoc[ i ] < d ) maxDoc[ i ] = d;
							currMaxPos[ i ][ d ] = processDocument( document.wordReader( field[ i ] ).setReader( new FastBufferedReader( fragment.text() ) ), d, currMaxPos[ i ][ d ], termMap[ i ],
									index[ i ].termProcessor )
									+ gap;
						}
					}
					break;
				case INT:
				case DATE:
					Object x = document.content( field[ i ] );
					if ( x != null ) {
						payloadPointers[ i ].add( map == null ? documentIndex : map[ documentIndex ] );
						payloadContent[ i ].add( x );
					}
				default:
				}
			}
			document.close();
			documentIndex++;
		}

		iterator.close();

		final MutableString[][] term = new MutableString[ n ][];
		for( int i = 0; i < n; i++ ) if ( termMap[ i ] != null ) java.util.Arrays.sort( term[ i ] = termMap[ i ].keySet().toArray( new MutableString[ termMap[ i ].size() ] ) );
		
		for ( int i = 0; i < n; i++ ) {
			if ( termMap[ i ] != null ) for ( ObjectArrayList<int[]> list : termMap[ i ].values() ) {
				// We sort in all cases, just to reduce the possible execution paths
				Collections.sort( list, new Comparator<int[]>() {
					public int compare( int[] p0, int[] p1 ) {
						return p0[ 0 ] - p1[ 0 ];
					}
				} );

				switch ( factory.fieldType( field[ i ] ) ) {
				case VIRTUAL:
					// We coalesce the list
					ObjectArrayList<int[]> newList = new ObjectArrayList<int[]>();
					for ( int k = 0; k < list.size(); ) {
						int s;
						for ( s = k + 1; s < list.size(); s++ )
							if ( list.get( k )[ 0 ] != list.get( s )[ 0 ] ) break;
						int count = 0;
						for ( int t = k; t < s; t++ )
							count += list.get( t ).length - 1;
						int[] posting = new int[ count + 1 ];
						posting[ 0 ] = list.get( k )[ 0 ];
						count = 1;
						for ( int t = k; t < s; t++ ) {
							System.arraycopy( list.get( t ), 1, posting, count, list.get( t ).length - 1 );
							count += list.get( t ).length - 1;
						}
						k = s;
						newList.add( posting );
					}
					list.clear();
					list.addAll( newList );
					break;
				default:
				}
			}
			if ( payloadPointers[ i ] != null ) {
				final int p[] = payloadPointers[ i ].elements();
				final Object[] b = payloadContent[ i ].elements();
				Arrays.quickSort( 0, payloadPointers[ i ].size(), new AbstractIntComparator() {
					private static final long serialVersionUID = 1L;

					public int compare( int i0, int i1 ) {
						return p[ i0 ] - p[ i1 ];
					}
				}, new Swapper() {
					public void swap( int i0, int i1 ) {
						final int t = p[ i0 ];
						p[ i0 ] = p[ i1 ];
						p[ i1 ] = t;
						final Object o = b[ i0 ];
						b[ i0 ] = b[ i1 ];
						b[ i1 ] = o;
					}
				} );
			}
		}


		for ( int i = 0; i < n; i++ ) {
			assertEquals( index[ i ].toString(), factory.fieldType( field[ i ] ) == FieldType.VIRTUAL ? maxDoc[ i ] + 1 : documentIndex, index[ i ].numberOfDocuments );
			switch ( factory.fieldType( field[ i ] ) ) {
			case TEXT:
			case VIRTUAL:
				assertEquals( termMap[ i ].size(), index[ i ].numberOfTerms );
				int postings = 0,
				occurrences = 0;
				for ( ObjectArrayList<int[]> l : termMap[ i ].values() ) {
					postings += l.size();
					for ( int[] p : l )
						occurrences += p.length - 1;
				}
				assertEquals( index[ i ].toString(), postings, index[ i ].numberOfPostings );
				assertEquals( occurrences, index[ i ].numberOfOccurrences );
				IndexReader indexReader = index[ i ].getReader();
				for ( MutableString t: term[ i ] ) {
					String msg = index[ i ] + ":" + term;
					IndexIterator indexIterator = indexReader.documents( t );
					ObjectArrayList<int[]> list = termMap[ i ].get( t );
					int k = 0;
					for( long d; ( d = indexIterator.nextDocument() ) != END_OF_LIST; ) {
						assertEquals( msg, list.get( k )[ 0 ], d ); // Document
																								// pointer
						if ( index[ i ].hasCounts ) assertEquals( msg, list.get( k ).length - 1, indexIterator.count() ); // Count
						if ( index[ i ].hasPositions ) {
							final int[] position = IndexIterators.positionArray( indexIterator );
							for ( int p = 0; p < indexIterator.count(); p++ )
								assertEquals( msg, list.get( k )[ p + 1 ], position[ p ] ); // Positions
						}
						k++;
					}
					assertEquals( k, list.size() ); // This implicitly checks the frequency
				}
				indexReader.close();
				break;
			case INT:
			case DATE:
				assertEquals( index[ i ].toString(), payloadPointers[ i ].size(), index[ i ].numberOfPostings );
				assertEquals( index[ i ].toString(), documentIndex != 0 ? 1 : 0, index[ i ].numberOfTerms );
				assertEquals( index[ i ].toString(), -1, index[ i ].numberOfOccurrences );
				if ( documentIndex != 0 ) {
					IndexIterator indexIterator = index[ i ].documents( 0 );
					int k = 0;
					while ( indexIterator.mayHaveNext() ) {
						assertEquals( payloadPointers[ i ].getInt( k ), indexIterator.nextDocument() );
						if ( factory.fieldType( field[ i ] ) == FieldType.INT ) assertEquals( ( (Number)payloadContent[ i ].get( k ) ).longValue(), ( (Number)indexIterator.payload().get() )
								.longValue() );
						else assertEquals( payloadContent[ i ].get( k ), indexIterator.payload().get() );
						k++;
					}
					indexIterator.dispose();
					assertEquals( k, payloadContent[ i ].size() );
				}
			}
		}

		if ( basename != null ) {
			final InputBitStream[] occurrencies = new InputBitStream[ n ];
			final InputBitStream[] sumsMaxPos = new InputBitStream[ n ];
			final InputBitStream[] frequencies = new InputBitStream[ n ];
			for( int i = 0; i < n; i++ ) {
				if ( index[ i ].hasCounts ) occurrencies[ i ] = new InputBitStream( basename + "-" + index[ i ].field + DiskBasedIndex.OCCURRENCIES_EXTENSION );
    				if ( index[ i ].hasPositions ) sumsMaxPos[ i ] = new InputBitStream( basename + "-" + index[ i ].field + DiskBasedIndex.SUMS_MAX_POSITION_EXTENSION );
				frequencies[ i ] = new InputBitStream( basename + "-" + index[ i ].field + DiskBasedIndex.FREQUENCIES_EXTENSION );
			}

			for ( int i = 0; i < n; i++ ) {
				switch ( factory.fieldType( field[ i ] ) ) {
				case TEXT:
				case VIRTUAL:
					if ( termMap[ i ] != null ) { 
						for ( MutableString t: term[ i ] ) {
							final ObjectArrayList<int[]> list = termMap[ i ].get( t );
							long occurrency = 0;
							long sumMaxPos = 0;
							for( int position[]: list ) {
								occurrency += position.length - 1;
								sumMaxPos += position[ position.length - 1 ];
							}
							assertEquals( index[ i ].toString(), list.size(), frequencies[ i ].readGamma() );
							if ( index[ i ].hasPositions ) assertEquals( index[ i ].toString(), sumMaxPos, sumsMaxPos[ i ].readLongDelta() );
							if ( index[ i ].hasCounts ) assertEquals( index[ i ].toString(), occurrency, occurrencies[ i ].readLongGamma() );
						}
					}
				default:
				}
				if ( index[ i ].hasCounts ) occurrencies[ i ].close();
				if ( index[ i ].hasPositions ) sumsMaxPos[ i ].close();
				frequencies[ i ].close();
			}
		}
	}

	/**
	 * Checks skips in the given index.
	 * 
	 * @param basename an index basename.
	 */
	public void checkSkips( final CharSequence basename ) throws IOException, ConfigurationException, SecurityException, URISyntaxException, ClassNotFoundException, InstantiationException, IllegalAccessException, InvocationTargetException, NoSuchMethodException {
		checkSkips( basename, basename );
	}

	/**
	 * Checks skips in the given index.
	 * 
	 * @param basename an index basename.
	 * @param termsBasename an alternative basename to locate the list of terms.
	 */
	public void checkSkips( final CharSequence basename, final CharSequence termsBasename ) throws IOException, ConfigurationException, SecurityException, URISyntaxException, ClassNotFoundException, InstantiationException, IllegalAccessException, InvocationTargetException, NoSuchMethodException {
		checkSkips( Index.getInstance( basename ), new FileLinesCollection( termsBasename + DiskBasedIndex.TERMS_EXTENSION, "UTF-8" ).iterator() );
	}

	/**
	 * Checks skips in the given index.
	 * 
	 * @param index an index.
	 * @param termsBasename an alternative basename to locate the list of terms.
	 */
	public void checkSkips( final Index index, final CharSequence termsBasename ) throws IOException, SecurityException {
		checkSkips( index, new FileLinesCollection( termsBasename + DiskBasedIndex.TERMS_EXTENSION, "UTF-8" ).iterator() );
	}

	/**
	 * Checks skips in the given index.
	 * 
	 * @param index an index.
	 * @param terms an alternative iterator on the index terms.
	 */
	public void checkSkips( final Index index, final FileLinesCollection.FileLinesIterator terms ) throws IOException {
		checkSkips( index, BitStreamIndex.DEFAULT_BUFFER_SIZE, terms );
	}
	
	/**
	 * Checks skips in the given index.
	 * 
	 * @param index an index.
	 * @param bufferSize a buffer size of index readers.
	 * @param termsBasename an alternative basename to locate the list of terms.
	 */
	public void checkSkips( final Index index, final int bufferSize, final CharSequence termsBasename ) throws IOException, SecurityException {
		checkSkips( index, bufferSize, new FileLinesCollection( termsBasename + DiskBasedIndex.TERMS_EXTENSION, "UTF-8" ).iterator() );
	}

	/**
	 * Checks skips in the given index.
	 * 
	 * @param index an index.
	 * @param bufferSize a buffer size of index readers.
	 * @param terms an alternative iterator on the index terms.
	 */
	public void checkSkips( final Index index, final int bufferSize, final FileLinesCollection.FileLinesIterator terms ) throws IOException {
		int start = 0, end = 0;
		long result;
		final LongArrayList l = new LongArrayList();
		final ObjectArrayList<int[]> positions = new ObjectArrayList<int[]>();

				final IndexReader indexReader = index.getReader( bufferSize );
				for (int t = 0; t < index.numberOfTerms; t++) {
					final MutableString term = terms.next();
					l.clear();
					positions.clear();
					final IndexIterator documents = indexReader.documents( term );
					long d;
					while( ( d = documents.nextDocument() ) != END_OF_LIST ) {
						l.add( d );
						if ( index.hasPositions ) positions.add( IndexIterators.positionArray( documents ) );
					}
					
					for( start = 0; start < l.size(); start++ ) {
						for( end = start + 1; end < l.size(); end++ ) {
							IndexIterator indexIterator = indexReader.documents( term );
							
							result = indexIterator.skipTo( l.getLong( start ) );
							assertEquals( l.getLong( start ), indexIterator.document() );
							assertEquals( l.getLong( start ), result );
							result = indexIterator.skipTo( l.getLong( end ) );
							assertEquals( l.getLong( end ), indexIterator.document() );
							assertEquals( l.getLong( end ), result );
							
							if ( index.hasPositions ) {
								// This catches wrong state reconstruction after skips.
								indexIterator = indexReader.documents( term );
								indexIterator.skipTo( l.getLong( start ) );
								assertEquals( l.getLong( start ), indexIterator.document() );
								assertEquals( positions.get( start ).length, indexIterator.count() );
								assertArrayEquals( positions.get( start ), IndexIterators.positionArray( indexIterator ) );
								indexIterator.skipTo( l.getLong( end ) );
								assertEquals( l.getLong( end ), indexIterator.document() );
								assertEquals( positions.get( end ).length, indexIterator.count() );
								assertArrayEquals( positions.get( end ), IndexIterators.positionArray( indexIterator ) );
							}
							
						}
						
						IndexIterator indexIterator = indexReader.documents( term );
						
						result = indexIterator.skipTo( l.getLong( start ) );
						assertEquals( l.getLong( start ), indexIterator.document() );
						assertEquals( l.getLong( start ), result );
						result = indexIterator.skipTo( it.unimi.di.big.mg4j.search.DocumentIterator.END_OF_LIST );
						assertEquals( it.unimi.di.big.mg4j.search.DocumentIterator.END_OF_LIST, result );
					}
				}
				
				indexReader.close();
	}
	
	
	@Before
	public void setUp() throws IOException {
		basename = File.createTempFile( IndexTest.class.getSimpleName(), "test" ).getCanonicalPath();
	}

	@After
	public void tearDown() throws IOException {
		for ( Object f : FileUtils.listFiles( new File( basename ).getParentFile(), FileFilterUtils.prefixFileFilter( IndexTest.class.getSimpleName() ), null ) )
			( (File)f ).delete();
		if ( lastSequence != null ) lastSequence.close();
	}

	// We keep track of the last returned sequence to close it without cluttering the test code
	private DocumentSequence lastSequence;

	public DocumentSequence getSequence() throws ConfigurationException, IOException {
		if ( lastSequence != null ) lastSequence.close();
		return lastSequence = new CompositeDocumentSequence( new InputStreamDocumentSequence( this.getClass().getResourceAsStream( "documents.data" ), 10, new IdentityDocumentFactory(
				new String[] { "encoding=UTF-8" } ), NUMBER_OF_DOCUMENTS ), new IntArrayDocumentCollection( INTEGER_DOCUMENT ), new DateArrayDocumentCollection( DATE_DOCUMENT ),
				new MapVirtualDocumentCollection( VIRTUAL_DOCUMENT ) );
	}

	public DocumentSequence getEmptySequence() throws ConfigurationException, IOException {
		if ( lastSequence != null ) lastSequence.close();
		return lastSequence = new CompositeDocumentSequence( new StringArrayDocumentCollection(), new IntArrayDocumentCollection(), new DateArrayDocumentCollection(),
				new MapVirtualDocumentCollection() );
	}

	public void testIndex( IndexType indexType, Map<Component, Coding> flags, int quantum, int height, TermProcessor termProcessor ) throws ConfigurationException, SecurityException, IOException, URISyntaxException, ClassNotFoundException,
			InstantiationException, IllegalAccessException, InvocationTargetException, NoSuchMethodException {

		// Vanilla indexing
		new IndexBuilder( basename, getSequence() ).standardWriterFlags( flags ).quasiSuccinctWriterFlags( flags ).termProcessor( termProcessor ).skipBufferSize( 1024 ).pasteBufferSize( 1024 ).indexType( indexType ).skips( quantum != 0 ).quantum( quantum )
				.height( height ).virtualDocumentResolver( 3, RESOLVER ).documentsPerBatch( 20 ).run();
		checkAgainstContent( getSequence(), null, RESOLVER, Scan.DEFAULT_VIRTUAL_DOCUMENT_GAP, basename, Index.getInstance( basename + "-text" ), Index.getInstance( basename + "-int" ), Index
				.getInstance( basename + "-date" ), Index.getInstance( basename + "-virtual" ) );

		final String basenameZipped = basename + "-zipped";
		if ( indexType == INTERLEAVED && flags.get( Component.POSITIONS ) != null ) flags.put( Component.POSITIONS, Coding.GOLOMB );
		// Vanilla indexing generating a zipped collection (we also use Golomb coding to test the usage of sizes in combinations).
		ZipDocumentCollectionBuilder zipBuilder = new ZipDocumentCollectionBuilder( basenameZipped, getSequence().factory(), true );
		new IndexBuilder( basename, getSequence() ).standardWriterFlags( flags ).quasiSuccinctWriterFlags( flags ).termProcessor( termProcessor ).skipBufferSize( 1024 ).pasteBufferSize( 1024 ).indexType( indexType ).skips( quantum != 0 ).quantum( quantum )
				.height( height ).virtualDocumentResolver( 3, RESOLVER ).documentsPerBatch( 20 ).builder( zipBuilder ).run();
		// Vanilla indexing using the zipped collection
		new IndexBuilder( basenameZipped, AbstractDocumentSequence.load( basenameZipped + DocumentCollection.DEFAULT_EXTENSION ) ).standardWriterFlags( flags ).quasiSuccinctWriterFlags( flags ).termProcessor( termProcessor ).indexedFields( 0, 1, 2, 3 ).skipBufferSize( 1024 )
				.pasteBufferSize( 1024 ).indexType( indexType ).skips( quantum != 0 ).quantum( quantum ).height( height ).virtualDocumentResolver( 3, RESOLVER ).documentsPerBatch( 20 ).run();

		// The two indices must be byte-by-byte identical (and we keep the zipped index for future
		// reference)
		sameIndex( basename + "-text", basenameZipped + "-text" );
		sameIndex( basename + "-int", basenameZipped + "-int", "batches" );
		sameIndex( basename + "-date", basenameZipped + "-date", "batches" );
		sameIndex( basename + "-virtual", basenameZipped + "-virtual", "batches" );

		checkSkips( basename + "-text" );
		checkSkips( basename + "-int" );
		checkSkips( basename + "-date" );
		checkSkips( basename + "-virtual" );

		final String basenameSimple = basename + "-simple";

		// Vanilla indexing generating a simple compressed collection
		SimpleCompressedDocumentCollectionBuilder simpleBuilder = new SimpleCompressedDocumentCollectionBuilder( basenameSimple, getSequence().factory(), true );
		new IndexBuilder( basename, getSequence() ).standardWriterFlags( flags ).quasiSuccinctWriterFlags( flags ).termProcessor( termProcessor ).skipBufferSize( 1024 ).pasteBufferSize( 1024 ).indexType( indexType ).skips( quantum != 0 ).quantum( quantum )
				.height( height ).virtualDocumentResolver( 3, RESOLVER ).documentsPerBatch( 20 ).builder( simpleBuilder ).run();
		// Vanilla indexing using the simple compressed collection
		new IndexBuilder( basenameSimple, AbstractDocumentSequence.load( basenameSimple + DocumentCollection.DEFAULT_EXTENSION ) ).standardWriterFlags( flags ).quasiSuccinctWriterFlags( flags ).termProcessor( termProcessor ).indexedFields( 0, 1, 2, 3 ).skipBufferSize( 1024 )
				.pasteBufferSize( 1024 ).indexType( indexType ).skips( quantum != 0 ).quantum( quantum ).height( height ).virtualDocumentResolver( 3, RESOLVER ).documentsPerBatch( 20 ).run();

		// The two indices must be byte-by-byte identical (and we keep the zipped index for future
		// reference)
		sameIndex( basename + "-text", basenameSimple + "-text" );
		sameIndex( basename + "-int", basenameSimple + "-int", "batches" );
		sameIndex( basename + "-date", basenameSimple + "-date", "batches" );
		sameIndex( basename + "-virtual", basenameSimple + "-virtual", "batches" );


		// Indexing with just one batch
		new IndexBuilder( basename + "-onebatch", getSequence() ).standardWriterFlags( flags ).quasiSuccinctWriterFlags( flags ).termProcessor( termProcessor ).skipBufferSize( 1024 ).pasteBufferSize( 1024 ).indexType( indexType ).skips( quantum != 0 )
				.quantum( quantum ).height( height ).virtualDocumentResolver( 3, RESOLVER ).documentsPerBatch( NUMBER_OF_DOCUMENTS ).run();

		if ( quantum >= 0 ) {
			// The two indices must be byte-by-byte identical
			sameIndex( basename + "-text", basename + "-onebatch-text", "batches" );
			sameIndex( basename + "-int", basename + "-onebatch-int", "batches" );
			sameIndex( basename + "-date", basename + "-onebatch-date", "batches" );
			sameIndex( basename + "-virtual", basename + "-onebatch-virtual", "batches" );
		}
		else {
			// The two indices must have the same content, as a different division
			// in batches can lead to a different quantum estimate. 
			sameContent( basename + "-text", basename + "-onebatch-text" );
			sameContent( basename + "-int", basename + "-onebatch-int" );
			sameContent( basename + "-date", basename + "-onebatch-date" );
			sameContent( basename + "-virtual", basename + "-onebatch-virtual" );
		}
	}

	public void testIndex( IndexType indexType, int quantum, int height ) throws ConfigurationException, SecurityException, IOException, URISyntaxException, ClassNotFoundException, InstantiationException,
			IllegalAccessException, InvocationTargetException, NoSuchMethodException {
		testIndex( indexType, defaultStandardIndex(), quantum, height, DowncaseTermProcessor.getInstance() );
	}

	public void testIndex( IndexType indexType, Map<Component, Coding> flags, int quantum, int height ) throws ConfigurationException, SecurityException, IOException, URISyntaxException, ClassNotFoundException, InstantiationException, IllegalAccessException, InvocationTargetException, NoSuchMethodException {
		testIndex( indexType, flags, quantum, height, DowncaseTermProcessor.getInstance() );
	}
	
	@Test
	public void testIndex() throws ConfigurationException, SecurityException, IOException, URISyntaxException, ClassNotFoundException, InstantiationException, IllegalAccessException,
			InvocationTargetException, NoSuchMethodException {

		Reference2ObjectOpenHashMap<Component, Coding> flags = new Reference2ObjectOpenHashMap<Component, Coding>( defaultQuasiSuccinctIndex() );
		flags.remove( Component.POSITIONS );
		testIndex( QUASI_SUCCINCT, flags, 4, 0 );

		testIndex( QUASI_SUCCINCT, defaultQuasiSuccinctIndex(), 1, 0, KILL_A_PROCESSOR );
		testIndex( QUASI_SUCCINCT, 1, 0 );
		testIndex( QUASI_SUCCINCT, 4, 0 );
		testIndex( QUASI_SUCCINCT, 8, 0 );

		flags = new Reference2ObjectOpenHashMap<Component, Coding>( defaultStandardIndex() );
		flags.remove( Component.POSITIONS );
		testIndex( INTERLEAVED, flags, 4, 4 );
		testIndex( INTERLEAVED, flags, -4, 4 );
		flags.remove( Component.COUNTS );
		testIndex( INTERLEAVED, flags, 4, 4 );
		testIndex( INTERLEAVED, flags, -4, 4 );

		
		testIndex( INTERLEAVED, 0, 0 );
		testIndex( INTERLEAVED, defaultStandardIndex(), 0, 0, KILL_A_PROCESSOR );
		testIndex( INTERLEAVED, 1, 1 );
		testIndex( INTERLEAVED, 8, 4 );
		testIndex( INTERLEAVED, -1, 1 );
		testIndex( INTERLEAVED, -8, 4 );

		testIndex( HIGH_PERFORMANCE, 1, 0 );
		testIndex( HIGH_PERFORMANCE, defaultStandardIndex(), 1, 0, KILL_A_PROCESSOR );
		testIndex( HIGH_PERFORMANCE, 1, 1 );
		testIndex( HIGH_PERFORMANCE, 8, 4 );
		testIndex( HIGH_PERFORMANCE, -1, 1 );
		testIndex( HIGH_PERFORMANCE, -8, 4 );

	}

	public void testRemappedIndex( IndexType indexType, Map<Component, Coding> flags, int quantum, int height, TermProcessor termProcessor ) throws IOException, ConfigurationException, SecurityException, URISyntaxException, ClassNotFoundException,
			InstantiationException, IllegalAccessException, InvocationTargetException, NoSuchMethodException {

		final String basenameMapped = basename + "-map";
		int[] map = IntIterators.unwrap( BinIO.asIntIterator( new DataInputStream( this.getClass().getResourceAsStream( "documents.permutation.data" ) ) ) );
		String mapFile = File.createTempFile( this.getClass().getSimpleName(), "map" ).toString();
		BinIO.storeInts( map, mapFile );

		// Remapped index
		new IndexBuilder( basenameMapped, getSequence() ).standardWriterFlags( flags ).quasiSuccinctWriterFlags( flags ).termProcessor( termProcessor ).skipBufferSize( 1024 ).pasteBufferSize( 1024 ).indexType( indexType ).skips( quantum != 0 ).quantum(
				quantum ).height( height ).virtualDocumentResolver( 3, RESOLVER ).documentsPerBatch( 20 ).mapFile( mapFile ).run();
		checkAgainstContent( getSequence(), map, RESOLVER, Scan.DEFAULT_VIRTUAL_DOCUMENT_GAP, basenameMapped, Index.getInstance( basenameMapped + "-text" ), Index.getInstance( basenameMapped + "-int" ), Index
				.getInstance( basenameMapped + "-date" ), Index.getInstance( basenameMapped + "-virtual" ) );

		// Remapped index, one batch
		new IndexBuilder( basenameMapped + "-onebatch", getSequence() ).standardWriterFlags( flags ).quasiSuccinctWriterFlags( flags ).termProcessor( termProcessor ).skipBufferSize( 1024 ).pasteBufferSize( 1024 ).indexType( indexType ).skips( quantum != 0 )
				.quantum( quantum ).height( height ).virtualDocumentResolver( 3, RESOLVER ).documentsPerBatch( NUMBER_OF_DOCUMENTS ).mapFile( mapFile ).run();

		if ( quantum >= 0 ) {
			// The two indices must be byte-by-byte identical
			sameIndex( basenameMapped + "-text", basenameMapped + "-onebatch-text", "batches" );
			sameIndex( basenameMapped + "-int", basenameMapped + "-onebatch-int", "batches" );
			sameIndex( basenameMapped + "-date", basenameMapped + "-onebatch-date", "batches" );
			sameIndex( basenameMapped + "-virtual", basenameMapped + "-onebatch-virtual", "batches" );
		}
		else {
			// The two indices must have the same content, as a different division
			// in batches can lead to a different quantum estimate. 
			sameContent( basenameMapped + "-text", basenameMapped + "-onebatch-text" );
			sameContent( basenameMapped + "-int", basenameMapped + "-onebatch-int" );
			sameContent( basenameMapped + "-date", basenameMapped + "-onebatch-date" );
			sameContent( basenameMapped + "-virtual", basenameMapped + "-onebatch-virtual" );
		}
	}

	public void testRemappedIndex( IndexType indexType, int quantum, int height ) throws ConfigurationException, SecurityException, IOException, URISyntaxException, ClassNotFoundException, InstantiationException,
			IllegalAccessException, InvocationTargetException, NoSuchMethodException {
		testRemappedIndex( indexType, defaultStandardIndex(), quantum, height, DowncaseTermProcessor.getInstance() );
	}
	public void testRemappedIndex( IndexType indexType, Map<Component, Coding> flags, int quantum, int height ) throws IOException, ConfigurationException, SecurityException, URISyntaxException, ClassNotFoundException, InstantiationException, IllegalAccessException, InvocationTargetException, NoSuchMethodException {
		testRemappedIndex( indexType, flags, quantum, height, DowncaseTermProcessor.getInstance() );
	}
	
	@Test
	public void testRemappedIndex() throws ConfigurationException, SecurityException, IOException, URISyntaxException, ClassNotFoundException, InstantiationException, IllegalAccessException,
			InvocationTargetException, NoSuchMethodException {

		Reference2ObjectOpenHashMap<Component, Coding> flags = new Reference2ObjectOpenHashMap<Component, Coding>( defaultQuasiSuccinctIndex() );
		flags.remove( Component.POSITIONS );
		testRemappedIndex( QUASI_SUCCINCT, flags, 4, 0 );
		
		testRemappedIndex( QUASI_SUCCINCT, defaultQuasiSuccinctIndex(), 1, 0, KILL_A_PROCESSOR );
		testRemappedIndex( QUASI_SUCCINCT, 1, 0 );
		testRemappedIndex( QUASI_SUCCINCT, 4, 0 );
		testRemappedIndex( QUASI_SUCCINCT, 8, 0 );

		flags = new Reference2ObjectOpenHashMap<Component, Coding>( defaultStandardIndex() );
		flags.remove( Component.POSITIONS );
		testRemappedIndex( INTERLEAVED, flags, 4, 4 );
		testRemappedIndex( INTERLEAVED, flags, -4, 4 );
		flags.remove( Component.COUNTS );
		testRemappedIndex( INTERLEAVED, flags, 4, 4 );
		testRemappedIndex( INTERLEAVED, flags, -4, 4 );
		
		testRemappedIndex( INTERLEAVED, 0, 0 );
		testRemappedIndex( INTERLEAVED, defaultStandardIndex(), 0, 0, KILL_A_PROCESSOR );
		testRemappedIndex( INTERLEAVED, 1, 1 );
		testRemappedIndex( INTERLEAVED, 8, 4 );
		testRemappedIndex( INTERLEAVED, -1, 1 );
		testRemappedIndex( INTERLEAVED, -8, 4 );

		testRemappedIndex( HIGH_PERFORMANCE, 1, 0 );
		testRemappedIndex( HIGH_PERFORMANCE, defaultStandardIndex(), 1, 0, KILL_A_PROCESSOR );
		testRemappedIndex( HIGH_PERFORMANCE, 1, 1 );
		testRemappedIndex( HIGH_PERFORMANCE, 8, 4 );
		testRemappedIndex( HIGH_PERFORMANCE, -1, 1 );
		testRemappedIndex( HIGH_PERFORMANCE, -8, 4 );
	}

	public void testPartitionConcatenate( IndexType indexType, Map<Component, Coding> flags, int quantum, int height ) throws Exception {
		// Vanilla indexing
		if ( indexType == INTERLEAVED && flags.get( Component.POSITIONS ) != null ) flags.put( Component.POSITIONS, Coding.GOLOMB );
		new IndexBuilder( basename, getSequence() ).standardWriterFlags( flags ).quasiSuccinctWriterFlags( flags ).skipBufferSize( 1024 ).pasteBufferSize( 1024 ).indexType( indexType ).skips( quantum != 0 ).quantum( quantum ).height( height )
				.virtualDocumentResolver( 3, RESOLVER ).run();

		// We partition
		BinIO.storeObject( DocumentalStrategies.uniform( 3, NUMBER_OF_DOCUMENTS ), basename + "-strategy" );

		new PartitionDocumentally( basename + "-text", basename + "-text-part", DocumentalStrategies.uniform( 3, NUMBER_OF_DOCUMENTS ), basename + "-strategy", 0, 1024, flags, 
				indexType, quantum != 0, Math.abs( quantum ), height, 1024 * 1024, DEFAULT_LOG_INTERVAL ).run();
		new PartitionDocumentally( basename + "-int", basename + "-int-part", DocumentalStrategies.uniform( 3, NUMBER_OF_DOCUMENTS ), basename + "-strategy", 0, 1024, DEFAULT_PAYLOAD_INDEX,
				INTERLEAVED, quantum != 0, Math.abs( quantum ), height, 1024 * 1024, DEFAULT_LOG_INTERVAL ).run();
		new PartitionDocumentally( basename + "-date", basename + "-date-part", DocumentalStrategies.uniform( 3, NUMBER_OF_DOCUMENTS ), basename + "-strategy", 0, 1024, DEFAULT_PAYLOAD_INDEX,
				INTERLEAVED, quantum != 0, Math.abs( quantum ), height, 1024 * 1024, DEFAULT_LOG_INTERVAL ).run();
		new PartitionDocumentally( basename + "-virtual", basename + "-virtual-part", DocumentalStrategies.uniform( 3, NUMBER_OF_DOCUMENTS ), basename + "-strategy", 0, 1024, flags,
				indexType, quantum != 0, Math.abs( quantum ), height, 1024 * 1024, DEFAULT_LOG_INTERVAL ).run();

		// For the text part, we need term maps to call sameIndex()
		String[] localIndex = new Properties( basename + "-text-part" + PROPERTIES_EXTENSION ).getStringArray( IndexCluster.PropertyKeys.LOCALINDEX );
		for ( String index : localIndex ) BinIO.storeObject( createMap( index + TERMS_EXTENSION ), index + TERMMAP_EXTENSION );

		sameContent( basename + "-text", basename + "-text-part", new FileLinesCollection( basename + "-text" + TERMS_EXTENSION, "UTF-8" ).iterator() );

		localIndex = new Properties( basename + "-int-part" + PROPERTIES_EXTENSION ).getStringArray( IndexCluster.PropertyKeys.LOCALINDEX );
		for ( String index : localIndex ) BinIO.storeObject( createMap( index + TERMS_EXTENSION ), index + TERMMAP_EXTENSION );

		sameContent( basename + "-int", basename + "-int-part" );
		
		localIndex = new Properties( basename + "-date-part" + PROPERTIES_EXTENSION ).getStringArray( IndexCluster.PropertyKeys.LOCALINDEX );
		for ( String index : localIndex ) BinIO.storeObject( createMap( index + TERMS_EXTENSION ), index + TERMMAP_EXTENSION );

		sameContent( basename + "-date", basename + "-date-part" );

		localIndex = new Properties( basename + "-virtual-part" + PROPERTIES_EXTENSION ).getStringArray( IndexCluster.PropertyKeys.LOCALINDEX );
		for ( String index : localIndex ) BinIO.storeObject( createMap( index + TERMS_EXTENSION ), index + TERMMAP_EXTENSION );

		sameContent( basename + "-virtual", basename + "-virtual-part", new FileLinesCollection( basename + "-virtual" + TERMS_EXTENSION, "UTF-8" ).iterator() );

		checkSkips( basename + "-text-part", basename + "-text" );
		checkSkips( basename + "-int-part", basename + "-int" );
		checkSkips( basename + "-date-part", basename + "-date" );
		checkSkips( basename + "-virtual-part", basename + "-virtual" );

		localIndex = new Properties( basename + "-text-part" + PROPERTIES_EXTENSION ).getStringArray( IndexCluster.PropertyKeys.LOCALINDEX );
		new Concatenate( IOFactory.FILESYSTEM_FACTORY, basename + "-text-merged", localIndex, false, 1024, flags, indexType, quantum != 0, quantum, height, 1024 * 1024, DEFAULT_LOG_INTERVAL ).run();
		if ( quantum >= 0 ) sameIndex( basename + "-text", basename + "-text-merged", flags.containsKey( Component.COUNTS ) ? new String[] { "batches" } : new String[] { "batches", "maxcount", "occurrences" } );
		sameContent( basename + "-text", basename + "-text-merged" );

		localIndex = new Properties( basename + "-int-part" + PROPERTIES_EXTENSION ).getStringArray( IndexCluster.PropertyKeys.LOCALINDEX );
		new Concatenate( IOFactory.FILESYSTEM_FACTORY, basename + "-int-merged", localIndex, false, 1024, DEFAULT_PAYLOAD_INDEX, INTERLEAVED, quantum != 0, quantum, height, 1024 * 1024, DEFAULT_LOG_INTERVAL ).run();
		if ( quantum >= 0 ) sameIndex( basename + "-int", basename + "-int-merged", flags.containsKey( Component.COUNTS ) ? new String[] { "batches" } : new String[] { "batches", "maxcount", "occurrences" } );
		sameContent( basename + "-int", basename + "-int-merged" );

		localIndex = new Properties( basename + "-date-part" + PROPERTIES_EXTENSION ).getStringArray( IndexCluster.PropertyKeys.LOCALINDEX );
		new Concatenate( IOFactory.FILESYSTEM_FACTORY, basename + "-date-merged", localIndex, false, 1024, DEFAULT_PAYLOAD_INDEX, INTERLEAVED, quantum != 0, quantum, height, 1024 * 1024, DEFAULT_LOG_INTERVAL ).run();
		if ( quantum >= 0 ) sameIndex( basename + "-date", basename + "-date-merged", flags.containsKey( Component.COUNTS ) ? new String[] { "batches" } : new String[] { "batches", "maxcount", "occurrences" } );
		sameContent( basename + "-date", basename + "-date-merged" );

		localIndex = new Properties( basename + "-virtual-part" + PROPERTIES_EXTENSION ).getStringArray( IndexCluster.PropertyKeys.LOCALINDEX );
		new Concatenate( IOFactory.FILESYSTEM_FACTORY, basename + "-virtual-merged", localIndex, false, 1024, flags, indexType, quantum != 0, quantum, height, 1024 * 1024, DEFAULT_LOG_INTERVAL ).run();
		if ( quantum >= 0 ) sameIndex( basename + "-virtual", basename + "-virtual-merged", flags.containsKey( Component.COUNTS ) ? new String[] { "batches" } : new String[] { "batches", "maxcount", "occurrences" } );
		sameContent( basename + "-virtual", basename + "-virtual-merged" );
	}

	@Test
	public void testPartitionConcatenate() throws Exception {
		Reference2ObjectOpenHashMap<Component, Coding> flags;
		
		flags = new Reference2ObjectOpenHashMap<Component, Coding>( defaultQuasiSuccinctIndex() );
		flags.remove( Component.POSITIONS );
		testPartitionConcatenate( QUASI_SUCCINCT, flags, 4, 0 );

		testPartitionConcatenate( QUASI_SUCCINCT, defaultQuasiSuccinctIndex(), 1, 0 );
		testPartitionConcatenate( QUASI_SUCCINCT, defaultQuasiSuccinctIndex(), 4, 0 );
		testPartitionConcatenate( QUASI_SUCCINCT, defaultQuasiSuccinctIndex(), 8, 0 );

		flags = new Reference2ObjectOpenHashMap<Component, Coding>( defaultStandardIndex() );
		flags.remove( Component.POSITIONS );
		testPartitionConcatenate( INTERLEAVED, flags, 4, 4 );
		testPartitionConcatenate( INTERLEAVED, flags, -4, 4 );
		flags.remove( Component.COUNTS );
		testPartitionConcatenate( INTERLEAVED, flags, 4, 4 );
		testPartitionConcatenate( INTERLEAVED, flags, -4, 4 );

		testPartitionConcatenate( INTERLEAVED, defaultStandardIndex(), 0, 0 );
		testPartitionConcatenate( INTERLEAVED, defaultStandardIndex(), 1, 1 );
		testPartitionConcatenate( INTERLEAVED, defaultStandardIndex(), 8, 4 );
		testPartitionConcatenate( INTERLEAVED, defaultStandardIndex(), -1, 1 );
		testPartitionConcatenate( INTERLEAVED, defaultStandardIndex(), -8, 4 );

		testPartitionConcatenate( HIGH_PERFORMANCE, defaultStandardIndex(), 1, 0 );
		testPartitionConcatenate( HIGH_PERFORMANCE, defaultStandardIndex(), 1, 1 );
		testPartitionConcatenate( HIGH_PERFORMANCE, defaultStandardIndex(), 8, 4 );
		testPartitionConcatenate( HIGH_PERFORMANCE, defaultStandardIndex(), -1, 1 );
		testPartitionConcatenate( HIGH_PERFORMANCE, defaultStandardIndex(), -8, 4 );
	}



	public void testPartitionMerge( IndexType indexType, Map<Component, Coding> flags, int quantum, int height ) throws ConfigurationException, SecurityException, IOException, ClassNotFoundException, InstantiationException, IllegalAccessException,
			Exception {
		
		if ( indexType == INTERLEAVED && flags.get( Component.POSITIONS ) != null ) flags.put( Component.POSITIONS, Coding.GOLOMB );
		
		// Vanilla indexing
		new IndexBuilder( basename, getSequence() ).standardWriterFlags( flags ).quasiSuccinctWriterFlags( flags ).skipBufferSize( 1024 ).pasteBufferSize( 1024 ).indexType( indexType ).skips( quantum != 0 ).quantum( quantum ).height( height )
				.virtualDocumentResolver( 3, RESOLVER ).run();

		// Now we use a crazy strategy moving around documents using modular arithmetic
		final DocumentalPartitioningStrategy modulo3 = new Modulo3DocumentalClusteringStrategy( NUMBER_OF_DOCUMENTS );
		BinIO.storeObject( modulo3, basename + "-strategy" );

		new PartitionDocumentally( basename + "-text", basename + "-text-part", modulo3, basename + "-strategy", 0, 1024, flags, indexType, quantum != 0, Math.abs( quantum ), height,
				1024 * 1024, DEFAULT_LOG_INTERVAL ).run();
		new PartitionDocumentally( basename + "-int", basename + "-int-part", modulo3, basename + "-strategy", 0, 1024, DEFAULT_PAYLOAD_INDEX, INTERLEAVED, quantum != 0, Math.abs( quantum ), height, 1024 * 1024,
				DEFAULT_LOG_INTERVAL ).run();
		new PartitionDocumentally( basename + "-date", basename + "-date-part", modulo3, basename + "-strategy", 0, 1024, DEFAULT_PAYLOAD_INDEX, INTERLEAVED, quantum != 0, Math.abs( quantum ), height,
				1024 * 1024, DEFAULT_LOG_INTERVAL ).run();
		new PartitionDocumentally( basename + "-virtual", basename + "-virtual-part", modulo3, basename + "-strategy", 0, 1024, flags, indexType, quantum != 0, Math.abs( quantum ), height,
				1024 * 1024, DEFAULT_LOG_INTERVAL ).run();

		String[] localIndex = new Properties( basename + "-text-part" + PROPERTIES_EXTENSION ).getStringArray( IndexCluster.PropertyKeys.LOCALINDEX );
		for ( String index : localIndex ) BinIO.storeObject( createMap( index + TERMS_EXTENSION ), index + TERMMAP_EXTENSION );
		sameContent( basename + "-text", basename + "-text-part", new FileLinesCollection( basename + "-text" + TERMS_EXTENSION, "UTF-8" ).iterator() );

		localIndex = new Properties( basename + "-int-part" + PROPERTIES_EXTENSION ).getStringArray( IndexCluster.PropertyKeys.LOCALINDEX );
		for ( String index : localIndex ) BinIO.storeObject( createMap( index + TERMS_EXTENSION ), index + TERMMAP_EXTENSION );

		sameContent( basename + "-int", basename + "-int-part" );

		localIndex = new Properties( basename + "-date-part" + PROPERTIES_EXTENSION ).getStringArray( IndexCluster.PropertyKeys.LOCALINDEX );
		for ( String index : localIndex ) BinIO.storeObject( createMap( index + TERMS_EXTENSION ), index + TERMMAP_EXTENSION );

		sameContent( basename + "-date", basename + "-date-part" );
		
		localIndex = new Properties( basename + "-virtual-part" + PROPERTIES_EXTENSION ).getStringArray( IndexCluster.PropertyKeys.LOCALINDEX );
		for ( String index : localIndex ) BinIO.storeObject( createMap( index + TERMS_EXTENSION ), index + TERMMAP_EXTENSION );
		sameContent( basename + "-virtual", basename + "-virtual-part", new FileLinesCollection( basename + "-virtual" + TERMS_EXTENSION, "UTF-8" ).iterator() );

		checkSkips( basename + "-text-part", basename + "-text" );
		checkSkips( basename + "-int-part", basename + "-int" );
		checkSkips( basename + "-date-part", basename + "-date" );
		checkSkips( basename + "-virtual-part", basename + "-virtual" );

		localIndex = new Properties( basename + "-text-part" + PROPERTIES_EXTENSION ).getStringArray( IndexCluster.PropertyKeys.LOCALINDEX );

		new Merge( IOFactory.FILESYSTEM_FACTORY, basename + "-text-merged", localIndex, false, 1024, flags, indexType, quantum != 0, quantum, height, 1024 * 1024, DEFAULT_LOG_INTERVAL ).run();
		if ( indexType != INTERLEAVED && quantum >= 0 ) sameIndex( basename + "-text", basename + "-text-merged", "batches" );
		else sameContent( basename + "-text", basename + "-text-merged" );
		localIndex = new Properties( basename + "-int-part" + PROPERTIES_EXTENSION ).getStringArray( IndexCluster.PropertyKeys.LOCALINDEX );
		new Merge( IOFactory.FILESYSTEM_FACTORY, basename + "-int-merged", localIndex, false, 1024, DEFAULT_PAYLOAD_INDEX, INTERLEAVED, quantum != 0, quantum, height, 1024 * 1024, DEFAULT_LOG_INTERVAL ).run();
		if ( indexType != INTERLEAVED && quantum >= 0 ) sameIndex( basename + "-int", basename + "-int-merged", "batches" );
		else sameContent( basename + "-int", basename + "-int-merged" );
		localIndex = new Properties( basename + "-date-part" + PROPERTIES_EXTENSION ).getStringArray( IndexCluster.PropertyKeys.LOCALINDEX );
		new Merge( IOFactory.FILESYSTEM_FACTORY, basename + "-date-merged", localIndex, false, 1024, DEFAULT_PAYLOAD_INDEX, INTERLEAVED, quantum != 0, quantum, height, 1024 * 1024, DEFAULT_LOG_INTERVAL ).run();
		if ( indexType != INTERLEAVED && quantum >= 0 ) sameIndex( basename + "-date", basename + "-date-merged", "batches" );
		else sameContent( basename + "-date", basename + "-date-merged" );
		localIndex = new Properties( basename + "-virtual-part" + PROPERTIES_EXTENSION ).getStringArray( IndexCluster.PropertyKeys.LOCALINDEX );
		new Merge( IOFactory.FILESYSTEM_FACTORY, basename + "-virtual-merged", localIndex, false, 1024, flags, indexType, quantum != 0, quantum, height, 1024 * 1024, DEFAULT_LOG_INTERVAL ).run();
		if ( indexType != INTERLEAVED && quantum >= 0 ) sameIndex( basename + "-virtual", basename + "-virtual-merged", "batches" );
		else sameContent( basename + "-virtual", basename + "-virtual-merged" );
	}

	@Test
	public void testPartitionMerge() throws Exception {
		Reference2ObjectOpenHashMap<Component, Coding> flags = new Reference2ObjectOpenHashMap<Component, Coding>( defaultQuasiSuccinctIndex() );
		flags.remove( Component.POSITIONS );
		testPartitionMerge( QUASI_SUCCINCT, flags, 4, 0 );
		
		testPartitionMerge( QUASI_SUCCINCT, defaultQuasiSuccinctIndex(), 1, 0 );
		testPartitionMerge( QUASI_SUCCINCT, defaultQuasiSuccinctIndex(), 4, 0 );
		testPartitionMerge( QUASI_SUCCINCT, defaultQuasiSuccinctIndex(), 8, 0 );

		flags = new Reference2ObjectOpenHashMap<Component, Coding>( defaultStandardIndex() );
		flags.remove( Component.POSITIONS );
		testPartitionMerge( INTERLEAVED, flags, 4, 4 );
		testPartitionMerge( INTERLEAVED, flags, -4, 4 );
		flags.remove( Component.COUNTS );
		testPartitionMerge( INTERLEAVED, flags, 4, 4 );
		testPartitionMerge( INTERLEAVED, flags, -4, 4 );
		
		testPartitionMerge( INTERLEAVED, defaultStandardIndex(), 0, 0 );
		testPartitionMerge( INTERLEAVED, defaultStandardIndex(), 1, 1 );
		testPartitionMerge( INTERLEAVED, defaultStandardIndex(), 8, 4 );
		testPartitionMerge( INTERLEAVED, defaultStandardIndex(), -1, 1 );
		testPartitionMerge( INTERLEAVED, defaultStandardIndex(), -8, 4 );

		testPartitionMerge( HIGH_PERFORMANCE, defaultStandardIndex(), 1, 0 );
		testPartitionMerge( HIGH_PERFORMANCE, defaultStandardIndex(), 1, 1 );
		testPartitionMerge( HIGH_PERFORMANCE, defaultStandardIndex(), 8, 4 );
		testPartitionMerge( HIGH_PERFORMANCE, defaultStandardIndex(), -1, 1 );
		testPartitionMerge( HIGH_PERFORMANCE, defaultStandardIndex(), -8, 4 );
	}

	public void testLexicalPartitioning( IndexType indexType, Map<Component, Coding> flags ) throws ConfigurationException, SecurityException, IOException, ClassNotFoundException, InstantiationException, IllegalAccessException,
			Exception {
		// Vanilla indexing
		new IndexBuilder( basename, getSequence() ).standardWriterFlags( flags ).quasiSuccinctWriterFlags( flags ).indexType( indexType ).skipBufferSize( 1024 ).pasteBufferSize( 1024 ).virtualDocumentResolver( 3, RESOLVER ).run();

		// Now we use a crazy strategy moving around documents using modular arithmetic
		final LexicalPartitioningStrategy uniform = LexicalStrategies.uniform( 3, DiskBasedIndex.getInstance( basename + "-text" ) );
		BinIO.storeObject( uniform, basename + "-strategy" );

		new PartitionLexically( basename + "-text", basename + "-text-part", uniform, basename + "-strategy", 1024, DEFAULT_LOG_INTERVAL ).run();
		new PartitionLexically( basename + "-virtual", basename + "-virtual-part", uniform, basename + "-strategy", 1024, DEFAULT_LOG_INTERVAL ).run();

		String[] localIndex = new Properties( basename + "-text-part" + PROPERTIES_EXTENSION ).getStringArray( IndexCluster.PropertyKeys.LOCALINDEX );
		for ( String index : localIndex )
			BinIO.storeObject( createMap( index + TERMS_EXTENSION ), index + TERMMAP_EXTENSION );
		sameContent( basename + "-text", basename + "-text-part", new FileLinesCollection( basename + "-text" + TERMS_EXTENSION, "UTF-8" ).iterator() );
		sameContent( basename + "-virtual", basename + "-virtual-part" );
	} 

	@Test
	public void testLexicalPartitioning() throws ConfigurationException, SecurityException, IOException, ClassNotFoundException, InstantiationException, IllegalAccessException, Exception {
		testLexicalPartitioning( QUASI_SUCCINCT, defaultQuasiSuccinctIndex() );
		testLexicalPartitioning( INTERLEAVED, defaultStandardIndex() );
		testLexicalPartitioning( HIGH_PERFORMANCE, defaultStandardIndex() );
		Reference2ObjectOpenHashMap<Component, Coding> flags = new Reference2ObjectOpenHashMap<Component, Coding>( defaultStandardIndex() );
		flags.remove( Component.POSITIONS );
		testLexicalPartitioning( INTERLEAVED, flags );
		flags.remove( Component.COUNTS );
		testLexicalPartitioning( INTERLEAVED, flags );
	}

	
	public void testEmpty( IndexType indexType, Map<Component, Coding> flags, int quantum, int height ) throws ConfigurationException, SecurityException, IOException, URISyntaxException, ClassNotFoundException, InstantiationException,
			IllegalAccessException, InvocationTargetException, NoSuchMethodException {
		// Vanilla indexing
		new IndexBuilder( basename, getEmptySequence() ).standardWriterFlags( flags ).quasiSuccinctWriterFlags( flags ).skipBufferSize( 1024 ).pasteBufferSize( 1024 ).indexType( indexType ).skips( quantum != 0 ).quantum( quantum ).height( height )
				.virtualDocumentResolver( 3, RESOLVER ).documentsPerBatch( 20 ).run();
		checkAgainstContent( getEmptySequence(), null, RESOLVER, Scan.DEFAULT_VIRTUAL_DOCUMENT_GAP, basename, Index.getInstance( basename + "-text" ), Index.getInstance( basename + "-int" ), Index
				.getInstance( basename + "-date" ), Index.getInstance( basename + "-virtual" ) );

		// Permuted indexing
		String mapFile = File.createTempFile( this.getClass().getSimpleName(), "permutation" ).toString();
		new IndexBuilder( basename + "-mapped", getEmptySequence() ).standardWriterFlags( flags ).quasiSuccinctWriterFlags( flags ).skipBufferSize( 1024 ).pasteBufferSize( 1024 ).indexType( indexType ).skips( quantum != 0 ).quantum( quantum ).height( height )
				.virtualDocumentResolver( 3, RESOLVER ).mapFile( mapFile ).documentsPerBatch( 20 ).run();

		sameIndex( basename + "-text", basename + "-mapped-text" );
		sameIndex( basename + "-int", basename + "-mapped-int" );
		sameIndex( basename + "-date", basename + "-mapped-date" );
		sameIndex( basename + "-virtual", basename + "-mapped-virtual" );
	}

	@Test
	public void testEmpty() throws Exception {
		final Reference2ObjectOpenHashMap<Component, Coding> flags = new Reference2ObjectOpenHashMap<Component, Coding>( defaultStandardIndex() );
		flags.remove( Component.POSITIONS );
		testEmpty( INTERLEAVED, flags, 4, 4 );
		testEmpty( INTERLEAVED, flags, -4, 4 );
		flags.remove( Component.COUNTS );
		testEmpty( INTERLEAVED, flags, 4, 4 );
		testEmpty( INTERLEAVED, flags, -4, 4 );

		testEmpty( INTERLEAVED, defaultStandardIndex(), 0, 0 );
		testEmpty( INTERLEAVED, defaultStandardIndex(), 1, 1 );
		testEmpty( INTERLEAVED, defaultStandardIndex(), 8, 4 );
		testEmpty( INTERLEAVED, defaultStandardIndex(), -1, 1 );
		testEmpty( INTERLEAVED, defaultStandardIndex(), -8, 4 );

		testEmpty( HIGH_PERFORMANCE, defaultStandardIndex(), 1, 0 );
		testEmpty( HIGH_PERFORMANCE, defaultStandardIndex(), 1, 1 );
		testEmpty( HIGH_PERFORMANCE, defaultStandardIndex(), 8, 4 );
		testEmpty( HIGH_PERFORMANCE, defaultStandardIndex(), -1, 1 );
		testEmpty( HIGH_PERFORMANCE, defaultStandardIndex(), -8, 4 );

		testEmpty( QUASI_SUCCINCT, defaultQuasiSuccinctIndex(), 1, 0 );
		testEmpty( QUASI_SUCCINCT, defaultQuasiSuccinctIndex(), 4, 0 );
		testEmpty( QUASI_SUCCINCT, defaultQuasiSuccinctIndex(), 8, 0 );

	}

	public void testLoadOptions( IndexType indexType, int quantum, int height ) throws ConfigurationException, SecurityException, IOException, URISyntaxException, ClassNotFoundException, InstantiationException,
			IllegalAccessException, InvocationTargetException, NoSuchMethodException {
		// Vanilla indexing
		new IndexBuilder( basename, getSequence() ).skipBufferSize( 1024 ).pasteBufferSize( 1024 ).indexType( indexType ).skips( quantum != 0 ).quantum( quantum ).height( height )
				.virtualDocumentResolver( 3, RESOLVER ).documentsPerBatch( 20 ).run();
		for ( String options : new String[] { "inmemory=1", "mapped=1", "offsetstep=0", "offsetstep=-2" } )
			checkAgainstContent( getSequence(), null, RESOLVER, Scan.DEFAULT_VIRTUAL_DOCUMENT_GAP, basename, Index.getInstance( basename + "-text?" + options ),
					Index.getInstance( basename + "-int?" + options ), Index.getInstance( basename + "-date?" + options ), Index.getInstance( basename + "-virtual?" + options ) );
	}

	@Test
	public void testLoadOptions() throws Exception {
		testLoadOptions( INTERLEAVED, 0, 0 );
		testLoadOptions( INTERLEAVED, 1, 1 );
		testLoadOptions( INTERLEAVED, -1, 1 );

		testLoadOptions( HIGH_PERFORMANCE, 1, 0 );
		testLoadOptions( HIGH_PERFORMANCE, 1, 1 );
		testLoadOptions( HIGH_PERFORMANCE, -1, 1 );

		testLoadOptions( QUASI_SUCCINCT, 1, 0 );
		testLoadOptions( QUASI_SUCCINCT, 4, 0 );
	}
}
