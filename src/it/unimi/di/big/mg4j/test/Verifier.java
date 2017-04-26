package it.unimi.di.big.mg4j.test;

import static it.unimi.di.big.mg4j.search.DocumentIterator.END_OF_LIST;
import it.unimi.di.big.mg4j.document.Document;
import it.unimi.di.big.mg4j.document.DocumentFactory;
import it.unimi.di.big.mg4j.document.DocumentSequence;
import it.unimi.di.big.mg4j.document.IdentityDocumentFactory;
import it.unimi.di.big.mg4j.index.BitStreamIndex;
import it.unimi.di.big.mg4j.index.DiskBasedIndex;
import it.unimi.di.big.mg4j.index.Index;
import it.unimi.di.big.mg4j.index.IndexIterator;
import it.unimi.di.big.mg4j.index.IndexIterators;
import it.unimi.di.big.mg4j.index.IndexReader;
import it.unimi.di.big.mg4j.index.QuasiSuccinctIndex;
import it.unimi.di.big.mg4j.index.payload.Payload;
import it.unimi.di.big.mg4j.search.AndDocumentIterator;
import it.unimi.di.big.mg4j.search.DocumentIterator;
import it.unimi.di.big.mg4j.search.OrDocumentIterator;
import it.unimi.di.big.mg4j.util.MG4JClassParser;
import it.unimi.dsi.Util;
import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntArrays;
import it.unimi.dsi.fastutil.ints.IntIterator;
import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.fastutil.longs.LongAVLTreeSet;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongBidirectionalIterator;
import it.unimi.dsi.fastutil.longs.LongLinkedOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongSet;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.io.FileLinesCollection;
import it.unimi.dsi.io.InputBitStream;
import it.unimi.dsi.io.WordReader;
import it.unimi.dsi.lang.MutableString;
import it.unimi.dsi.logging.ProgressLogger;

import java.io.File;
import java.io.IOException;
import java.io.Reader;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.martiansoftware.jsap.FlaggedOption;
import com.martiansoftware.jsap.JSAP;
import com.martiansoftware.jsap.JSAPResult;
import com.martiansoftware.jsap.Parameter;
import com.martiansoftware.jsap.SimpleJSAP;
import com.martiansoftware.jsap.Switch;
import com.martiansoftware.jsap.UnflaggedOption;

/** Verifies that an index matches a collection. */

final public class Verifier {
	private static final Logger LOGGER = LoggerFactory.getLogger( Verifier.class );
	
	private Verifier() {}

	private static void pour( IndexIterator indexIterator, LongSet s ) throws IOException {
		for( long d; ( d = indexIterator.nextDocument() ) != END_OF_LIST; ) s.add( d );
	}

	@SuppressWarnings("unchecked")
	public static void main( final String[] arg ) throws Throwable {

		SimpleJSAP jsap = new SimpleJSAP( Verifier.class.getName(), "Scans an index and associated files, checking internal coherence. Optionally, compares the index with a document sequence.",
				new Parameter[] {
					new FlaggedOption( "sequence", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'S', "sequence", "A serialised document sequence that will be used instead of stdin." ),
					new FlaggedOption( "factory", MG4JClassParser.getParser(), IdentityDocumentFactory.class.getName(), JSAP.NOT_REQUIRED, 'f', "factory", "A document factory with a standard constructor." ),
					new FlaggedOption( "property", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'p', "property", "A 'key=value' specification, or the name of a property file" ).setAllowMultipleDeclarations( true ),
					new FlaggedOption( "indexedField", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'I', "indexed-field", "The field(s) of the document factory that will be indexed. (default: all fields)" ).setAllowMultipleDeclarations( true ),
					new Switch( "allFields", 'a', "all-fields", "Verify also all virtual fields; has no effect if indexedField has been used at least once." ),
					new FlaggedOption( "bufferSize", JSAP.INTSIZE_PARSER, Util.formatBinarySize( it.unimi.di.big.mg4j.tool.Scan.DEFAULT_BUFFER_SIZE ), JSAP.NOT_REQUIRED, 'b', "buffer-size", "The size of an I/O buffer." ),
					new FlaggedOption( "delimiter", JSAP.INTEGER_PARSER, Integer.toString( it.unimi.di.big.mg4j.tool.Scan.DEFAULT_DELIMITER ), JSAP.NOT_REQUIRED, 'd', "delimiter", "The document delimiter." ),
					new FlaggedOption( "renumber", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'r', "renumber", "The filename of a document renumbering." ),
					new FlaggedOption( "logInterval", JSAP.LONG_PARSER, Long.toString( ProgressLogger.DEFAULT_LOG_INTERVAL ), JSAP.NOT_REQUIRED, 'l', "log-interval", "The minimum time interval between activity logs in milliseconds." ),
					new Switch( "termLists", 't', "term-lists", "Instead of assuming each index knows its terms, read a term file stemmed from the index name." ),
					new Switch( "stem", 's', "stem", "Stem basename using field names from the collection." ),
					new Switch( "random", 'R', "random", "Perform random access checks; requires a collection (will use stdin if none is specified)." ),
					new Switch( "virtual", JSAP.NO_SHORTFLAG, "virtual", "Virtual collection; skip document size/occurrences check and random access check." ),
					new Switch( "noSeq", JSAP.NO_SHORTFLAG, "no-seq", "Skip sequential check." ),
					new Switch( "noSkip", JSAP.NO_SHORTFLAG, "no-skip", "Skip \"all-skips\" check." ),
					new Switch( "noComp", JSAP.NO_SHORTFLAG, "no-comp", "Skip composite iterator check." ),
					new UnflaggedOption( "basename", JSAP.STRING_PARSER, JSAP.REQUIRED, "The basename of the index." )
			});
		
		JSAPResult jsapResult = jsap.parse( arg );
		if ( jsap.messagePrinted() ) return;
		
		DocumentSequence documentSequence = it.unimi.di.big.mg4j.tool.Scan.getSequence( jsapResult.getString( "sequence" ), jsapResult.getClass( "factory" ), jsapResult.getStringArray( "property" ), jsapResult.getInt( "delimiter" ), LOGGER );
		
		final DocumentFactory factory = documentSequence.factory();
		final boolean stem = jsapResult.getBoolean( "stem" );
		final boolean termLists = jsapResult.getBoolean( "termLists" );
		final int[] indexedField = it.unimi.di.big.mg4j.tool.Scan.parseFieldNames( jsapResult.getStringArray( "indexedField" ), factory, jsapResult.getBoolean( "allFields" )  );
		
		LOGGER.debug( "Parsed indexed field: " + IntArrayList.wrap( indexedField ) );
		
		final String basename = jsapResult.getString( "basename" ); 
		final String permutationFile = jsapResult.getString( "renumber" );

		final boolean isVirtual = jsapResult.getBoolean( "virtual" );

		int i, t = 0;

		final ProgressLogger pl = new ProgressLogger( LOGGER, jsapResult.getLong( "logInterval" ), TimeUnit.MILLISECONDS, "ints" );
		final Index[] index = stem ? new Index[ indexedField.length ] : new Index[ 1 ];
		final long numberOfTerms[] = new long[ indexedField.length ];
		final ObjectArrayList<MutableString>[] terms = new ObjectArrayList[ indexedField.length ];
		final IndexReader[] indexReader = new IndexReader[ index.length ];
		final InputBitStream[] frequencies = new InputBitStream[ index.length ];
		final int[][] count = new int[ index.length ][];
		final int[] permutation = permutationFile != null ? BinIO.loadInts( permutationFile ) : null;
		final int[][] occ = new int[ index.length ][];
		final int[][] wordInPos = new int[ index.length ][];
		final Int2IntMap[] termsInDoc = new Int2IntOpenHashMap[ index.length ];
		int totalTerms = 0;
		
		boolean allBitStreamIndices = true;
		
		for( i = 0; i < index.length; i++ ) {
			final String basenameField = basename + (stem ? "-" + factory.fieldName( indexedField[ i ] ) : "" );
			index[ i ] = Index.getInstance( basenameField );
			if ( ! ( index[ i ] instanceof BitStreamIndex ) && ! ( index[ i ] instanceof QuasiSuccinctIndex ) ) allBitStreamIndices = false;
			
			if ( termLists ) {
				terms[ i ] = new ObjectArrayList<MutableString>( new FileLinesCollection( basenameField + DiskBasedIndex.TERMS_EXTENSION, "UTF-8" ).allLines() );
				numberOfTerms[ i ] = terms[ i ].size();
			}
			else numberOfTerms[ i ] = index[ i ].numberOfTerms;
			totalTerms += numberOfTerms[ i ];
			
			// This will be matched with the number of occurrences per document
			if ( index[ i ].numberOfDocuments <= Integer.MAX_VALUE ) count[ i ] = new int[ (int)index[ i ].numberOfDocuments ];
			else LOGGER.warn( "Index " + index[ i ] + " has too many documents: positions will not be checked" );

			occ[ i ] = index[ i ].maxCount > 0 ? new int[ index[ i ].maxCount ] : IntArrays.EMPTY_ARRAY;
			wordInPos[ i ] = new int[ Math.max( 0, index[ i ].properties.getInt( Index.PropertyKeys.MAXDOCSIZE ) ) ];
			indexReader[ i ] = index[ i ].getReader();
			
			if ( new File( basenameField + DiskBasedIndex.FREQUENCIES_EXTENSION ).exists() ) frequencies[ i ] = new InputBitStream( basenameField + DiskBasedIndex.FREQUENCIES_EXTENSION );
			termsInDoc[ i ] = new Int2IntOpenHashMap();
		}


		int currDoc = 0,
		// Term position in the current document.
		pos = 0, f = 0;
		long p;

		pl.itemsName = "lists";
		pl.expectedUpdates = totalTerms;
		
		long indexFrequency = -1;
		
		// Sequential scan
		if ( !jsapResult.getBoolean( "noSeq" ) ) {
			try {
				for ( i = 0; i < index.length; i++ ) {
					int numberOfPostings = 0;
					pl.expectedUpdates = numberOfTerms[ i ];
					pl.start( "Verifying sequentially index " + index[ i ] + "..." );

					if ( allBitStreamIndices ) {
						for ( t = 0; t < numberOfTerms[ i ]; t++ ) {
							pl.update();
							IndexIterator indexIterator = indexReader[ i ].nextIterator();
							indexFrequency = indexIterator.frequency();
							numberOfPostings += indexFrequency;
							if ( frequencies[ i ] != null && indexFrequency != ( f = frequencies[ i ].readGamma() ) ) {
								System.err.println( "Error in frequency for term " + t + ": expected " + f + " documents, found " + indexFrequency );
								return;
							}

							while ( indexFrequency-- != 0 ) {
								p = indexIterator.nextDocument();
								if ( p > Integer.MAX_VALUE ) throw new IndexOutOfBoundsException();
								if (index[i].hasCounts) count[i][(int)p] += indexIterator.count();
								if (index[i].hasPositions) while( indexIterator.nextPosition() != IndexIterator.END_OF_POSITIONS ); // Just to force reading in high-performance indices
							}
							if ( indexIterator.nextDocument() != END_OF_LIST ) throw new AssertionError( "nextDocument() is not END_OF_LIST after exhaustive iteration" );
						}
						
						// Check document sizes. We to this ONLY if the number of documents is not a long.
						if ( ! isVirtual && index[ i ].sizes != null && index[ i ].hasCounts && index[ i ].numberOfDocuments < Integer.MAX_VALUE )
							for ( p = 0; p < index[ i ].numberOfDocuments; p++ )
								if ( index[ i ].sizes.getInt( p ) != count[ i ][ (int)p ] )
									System.err.println( "Document " + p + " has size " + index[ i ].sizes.getInt( p ) + " but " + count[ i ][ (int)p ] + " occurrences have been stored." );
						
					}
					else { // Non-bitstream indices
						for (t = 0; t < numberOfTerms[ i ]; t++) {
							pl.update();
							IndexIterator indexIterator = termLists ? indexReader[ i ].documents( terms[ i ].get( t ) ) : indexReader[ i ].documents( t );
							indexFrequency = indexIterator.frequency();
							numberOfPostings += indexFrequency;
							if (frequencies[i] != null && indexFrequency != (f = frequencies[i].readGamma())) {
								System.err.println("Error in frequency for term " + t
										+ ": expected " + f + " documents, found "
										+ indexFrequency);
								return;
							}
							
							long prevp = -1;
							while (indexFrequency-- != 0) {
								p = indexIterator.nextDocument();
								if ( prevp >= p ) throw new AssertionError( "previous pointer: " + prevp + "; current pointer: " + p );
								prevp = p;
								if (index[i].hasCounts &&  index[ i ].numberOfDocuments < Integer.MAX_VALUE ) count[i][(int)p] += indexIterator.count();
							}
						}
					}
					pl.done();
					
					if ( ! isVirtual && numberOfPostings != index[ i ].numberOfPostings ) System.err.println( "Index declares " + index[ i ].numberOfPostings + " postings, but we found " + numberOfPostings );
					long numberOfOccurrences = 0;
					if ( index[ i ].hasCounts && index[ i ].numberOfDocuments < Integer.MAX_VALUE ) {
						for ( p = 0; p < index[ i ].numberOfDocuments; p++ ) numberOfOccurrences += count[ i ][ (int)p ];
						if ( numberOfOccurrences != index[ i ].numberOfOccurrences ) System.err.println( "Index declares " + index[ i ].numberOfOccurrences + " occurrences, but we found " + numberOfOccurrences );
					}
				}
			} catch ( Exception e ) {
				System.err.println( "Exception while scanning sequentially term " + t + " of index " + index[ i ] );
				System.err.println( "Term frequency was " + f + " and position " + ( f - indexFrequency - 1 ) );
				throw e;
			}
		}
	
		LongArrayList l = new LongArrayList();
		ObjectArrayList<int[]> positions = new ObjectArrayList<int[]>();
		
		if ( ! jsapResult.getBoolean( "noSkip" ) ) {
			int start = 0, end = 0;
			long result;
			try {
				for (i = 0; i < index.length; i++) {
					
					pl.expectedUpdates = numberOfTerms[ i ];
					pl.start("Verifying all skips in " + index[i] + "...");

					for (t = 0; t < numberOfTerms[ i ]; t++) {
						l.clear();
						positions.clear();
						IndexIterator documents = termLists ? indexReader[ i ].documents( terms[ i ].get( t ) ) : indexReader[ i ].documents( t );
						long d;
						while( ( d = documents.nextDocument() ) != END_OF_LIST ) {
							l.add( d );
							if ( index[ i ].hasPositions ) positions.add( IndexIterators.positionArray( documents ) );
						}
						
						for( start = 0; start < l.size(); start++ ) {
							for( end = start + 1; end < l.size(); end++ ) {
								IndexIterator indexIterator = termLists ? indexReader[ i ].documents( terms[ i ].get( t ) ) : indexReader[ i ].documents( t );
								
								result = indexIterator.skipTo( l.getLong( start ) );
								if ( indexIterator.document() != l.getLong( start ) || result != l.getLong( start ) ) throw new AssertionError( "Trying to skip to document " + l.getLong( start ) + " (term " + t + ") moved to " + indexIterator.document() + "(skipTo() returned " + result + ")" );
								result = indexIterator.skipTo( l.getLong( end ) );
								if ( indexIterator.document() != l.getLong( end ) || result != l.getLong( end ) ) throw new AssertionError( "Trying to skip to document " + l.getLong( end ) + " (term " + t + ") after a skip to " + start + " moved to " + indexIterator.document() + "(skipTo() returned " + result + ")" );
								
								if ( index[ i ].hasPositions ) {
									// This catches wrong state reconstruction after skips.
									indexIterator = termLists ? indexReader[ i ].documents( terms[ i ].get( t ) ) : indexReader[ i ].documents( t );
									indexIterator.skipTo( l.getLong( start ) );
									if ( indexIterator.document() != l.getLong( start ) ) throw new AssertionError(indexIterator.document() + " != " + l.getLong( start ) );
									if ( indexIterator.count() != positions.get( start ).length ) throw new AssertionError(indexIterator.count() + " != " + positions.get( start ).length );
									int[] position = IndexIterators.positionArray( indexIterator );
									if ( ! Arrays.equals( positions.get( start ), position ) ) throw new AssertionError(Arrays.toString( positions.get( start ) ) + "!=" + Arrays.toString( position ) );
									indexIterator.skipTo( l.getLong( end ) );
									if ( indexIterator.document() != l.getLong( end )  ) throw new AssertionError(indexIterator.document() + " != " + l.getLong( end ) );
									if ( indexIterator.count() != positions.get( end ).length ) throw new AssertionError(indexIterator.count() + " != " + positions.get( end ).length );
									position = IndexIterators.positionArray( indexIterator );
									if ( ! Arrays.equals( positions.get( end ), position ) ) throw new AssertionError(Arrays.toString( positions.get( end ) ) + "!=" + Arrays.toString( position ) );
								}
								
							}
							
							IndexIterator indexIterator = termLists ? indexReader[ i ].documents( terms[ i ].get( t ) ) : indexReader[ i ].documents( t );
							
							result = indexIterator.skipTo( l.getLong( start ) );
							if ( indexIterator.document() != l.getLong( start ) || result != l.getLong( start ) ) throw new AssertionError("Trying to skip to document " + l.getLong( start ) + " (term " + t + ") moved to " + indexIterator.document() + "(skipTo() returned " + result + ")" );
							result = indexIterator.skipTo( DocumentIterator.END_OF_LIST );
							if ( indexIterator.mayHaveNext() || result != DocumentIterator.END_OF_LIST ) throw new AssertionError("Trying to skip beyond end of list (term " + t + ") after a skip to " + start + " returned " + result + " (mayHaveNext()=" + indexIterator.mayHaveNext() + ")" );
						}
						pl.update();
					}
					pl.done();
				}
			}
			catch( Throwable e  ) {
				System.err.println( "Exception during all-skip test (index=" + index[ i ] + ", term=" + t + ", start=" + start + ", end=" + end + ")" );
				throw e;
			}
 		}
		

		if ( ! jsapResult.getBoolean( "noComp" ) ) {
			IndexReader additionalReader;
			LongLinkedOpenHashSet s0 = new LongLinkedOpenHashSet();
			LongLinkedOpenHashSet s1 = new LongLinkedOpenHashSet();
			LongAVLTreeSet s2 = new LongAVLTreeSet();
			LongBidirectionalIterator it;
			IndexIterator indexIterator, additionalIterator;
			it.unimi.di.big.mg4j.search.DocumentIterator documentIterator;
			int u = 0;
			
			try {
				for (i = 0; i < index.length; i++) {
					pl.expectedUpdates = numberOfTerms[ i ];
					pl.start("Verifying composite iterators in " + index[i] + "...");
					additionalReader = index[ i ].getReader();
					
					for (t = 0; t < numberOfTerms[ i ]; t++) {
						for (u = 0; u < numberOfTerms[ i ]; u++) {
							s0.clear();
							s1.clear();
							// TODO: in case we have positions, we should check them, too
							pour( termLists ? indexReader[ i ].documents( terms[ i ].get( t ) ) : indexReader[ i ].documents( t ), s0 );
							pour( termLists ? indexReader[ i ].documents( terms[ i ].get( u ) ) : indexReader[ i ].documents( u ), s1 );
							s0.retainAll( s1 );
							indexIterator =  termLists ? indexReader[ i ].documents( terms[ i ].get( t ) ) : indexReader[ i ].documents( t );
							additionalIterator = termLists ? additionalReader.documents( terms[ i ].get( u ) ) : additionalReader.documents( u );
							it = s0.iterator();
							documentIterator = AndDocumentIterator.getInstance( indexIterator, additionalIterator );
							for( int j = s0.size(); j-- != 0; ) if ( it.nextLong() != documentIterator.nextDocument() ) throw new AssertionError();
							
							s2.clear();
							pour( termLists ? indexReader[ i ].documents( terms[ i ].get( t ) ) : indexReader[ i ].documents( t ), s2 );
							pour( termLists ? indexReader[ i ].documents( terms[ i ].get( u ) ) : indexReader[ i ].documents( u ), s2 );

							indexIterator =  termLists ? indexReader[ i ].documents( terms[ i ].get( t ) ) : indexReader[ i ].documents( t );
							additionalIterator = termLists ? additionalReader.documents( terms[ i ].get( u ) ) : additionalReader.documents( u );

							it = s2.iterator();
							documentIterator = OrDocumentIterator.getInstance( indexIterator, additionalIterator ); 
							for( int j = s2.size(); j-- != 0; ) if ( it.nextLong() != documentIterator.nextDocument() ) throw new AssertionError();
							
						}	
						pl.update();
					}
					pl.done();
					additionalReader.close();
				}
			}
			catch( Throwable e  ) {
				System.err.println( "Exception during composite iterator test (index=" + index[ i ] + ", first term=" + t + ", second term =" + u + ")" );
				throw e;
			}	
		}
		
		if ( ! isVirtual && jsapResult.getBoolean( "random" ) ) {
			
			// Random access scan
			pl.expectedUpdates = index[ 0 ].numberOfDocuments;
			pl.itemsName = "documents";
			pl.start( "Verifying random access..." );

			if ( allBitStreamIndices ) {
				it.unimi.di.big.mg4j.document.DocumentIterator documentIterator = documentSequence.iterator();
				Document document;
				Reader reader;
				WordReader wordReader;
				
				final MutableString word = new MutableString(), nonWord = new MutableString();
				
				int docCounter = 0;
				
				while( ( document = documentIterator.nextDocument() ) != null ) {
					currDoc = permutation != null ? permutation[ docCounter ] : docCounter;

					for( i = 0; i < index.length; i++ ) {
						Object content = document.content( stem || index[ i ].field == null ? indexedField[ i ] : factory.fieldIndex( index[ i ].field ) );
						if ( index[ i ].hasPayloads ) {
							// TODO: write tests for the other case
							if ( allBitStreamIndices ) {
								IndexIterator indexIterator = indexReader[ i ].documents( 0 );
								long pointer = indexIterator.skipTo( currDoc );
								if ( pointer == currDoc ) {
									Payload payload = indexIterator.payload();
									if ( ! payload.get().equals( content ) ) LOGGER.error( index[ i ] + ": Document " + currDoc + " has payload " + content + " but the index says " + payload );  
								}
								else LOGGER.error( index[ i ] + ": Document " + currDoc + " does not appear in the inverted list of term " + t );
							}
							else {
								IndexIterator indexIterator = indexReader[ i ].documents(  0  );
								if ( indexIterator.skipTo( currDoc ) == currDoc ) {
									if ( ! indexIterator.payload().get().equals( content ) )
										LOGGER.error( index[ i ] + ": Document " + currDoc + " has payload " + content + " but the index says " + indexIterator.payload().get() );
								} 
								else LOGGER.error( index[ i ] + ": Document " + currDoc + " does not appear in the inverted list of term " + t );
							}
						}
						else {
							// text index
							pos = 0;
							termsInDoc[ i ].clear();
							reader = (Reader)content;
							wordReader = document.wordReader( stem || index[ i ].field == null ? indexedField[ i ] : factory.fieldIndex( index[ i ].field ) );
							wordReader.setReader( reader );
							while( wordReader.next( word, nonWord ) ) {
								if ( word.length() == 0 || index[ i ].termProcessor != null && ! index[ i ].termProcessor.processTerm( word ) ) continue;
								if ( ( t = (int)index[ i ].termMap.getLong( word ) ) == -1 ) LOGGER.error( index[ i ] + ": Could not find term " + word + " in term index" );
								else {
									if ( index[ i ].hasCounts ) termsInDoc[ i ].put( t, termsInDoc[ i ].get( t ) + 1 );
									if ( index[ i ].hasPositions ) wordInPos[ i ][ pos++ ] = t;
								}
							}

							if ( allBitStreamIndices ) {
								for( IntIterator x = termsInDoc[ i ].keySet().iterator(); x.hasNext(); ) {
									t = x.nextInt();

									IndexIterator indexIterator = indexReader[ i ].documents( t );

									long pointer = indexIterator.skipTo( currDoc );
									if ( pointer == currDoc ) {
										if ( index[ i ].hasCounts ) {
											int c = indexIterator.count();
											if ( termsInDoc[ i ].get( t ) !=  c ) 
												LOGGER.error( index[ i ] + ": The count for term " + t + " in document " + currDoc + " is " + c + " instead of " + termsInDoc[ i ].get( t ) );
											else {
												if ( index[ i ].hasPositions ) {
													occ[ i ] = IndexIterators.positionArray( indexIterator );

													for( int j = 0; j < c; j++ ) 
														if ( wordInPos[ i ][ occ[ i ][ j ] ] != t )  
															LOGGER.error( index[ i ] + ": The occurrence of index " + i + " of term " + t + " (position " + occ[ i ] +") in document " + currDoc + " is occupied instead by term " + wordInPos[ i ][ occ[ i ][ j ] ] );
												}
											}
										} 
									}
									else LOGGER.error( index[ i ] + ": Document " + currDoc + " does not appear in the inverted list of term " + t + "(skipTo returned " + pointer + ")" );
								}
							}
							else {
								for( IntIterator x = termsInDoc[ i ].keySet().iterator(); x.hasNext(); ) {
									t = x.nextInt();
									IndexIterator indexIterator = termLists ? indexReader[ i ].documents( terms[ i ].get( t ) ) : indexReader[ i ].documents( t );

									if ( indexIterator.skipTo( currDoc ) == currDoc ) {
										if ( index[ i ].hasCounts ) {
											int c = indexIterator.count();
											if ( termsInDoc[ i ].get( t ) !=  c ) 
												LOGGER.error( index[ i ] + ": The count for term " + t + " in document " + currDoc + " is " + c + " instead of " + termsInDoc[ i ].get( t ) );
											else {
												if ( index[ i ].hasPositions ) {
													occ[ i ] = IndexIterators.positionArray( indexIterator );

													for( int j = 0; j < c; j++ ) 
														if ( wordInPos[ i ][ occ[ i ][ j ] ] != t )  
															LOGGER.error( index[ i ] + ": The occurrence of index " + i + " of term " + t + " (position " + occ[ i ] +") in document " + currDoc + " is occupied instead by term " + wordInPos[ i ][ occ[ i ][ j ] ] );
												}
											}
										}
									} 
									else LOGGER.error( index[ i ] + ": Document " + currDoc + " does not appear in the inverted list of term " + t );
								}
							}
						}
					}
					docCounter++;
					document.close();
					pl.update();
				}
			}
			else {
				LOGGER.warn( "Random access tests require very slow single-term scanning as not all indices are disk based" );

				it.unimi.di.big.mg4j.document.DocumentIterator documentIterator = documentSequence.iterator();
				Document document;
				Reader reader;
				WordReader wordReader;
				
				final MutableString word = new MutableString(), nonWord = new MutableString();
				
				int docCounter = 0;
				
				while( ( document = documentIterator.nextDocument() ) != null ) {
					currDoc = permutation != null ? permutation[ docCounter ] : docCounter;

					for( i = 0; i < index.length; i++ ) {
						Object content = document.content( stem || index[ i ].field == null ? indexedField[ i ] : factory.fieldIndex( index[ i ].field ) );
						if ( index[ i ].hasPayloads ) {
							if ( allBitStreamIndices ) {
								IndexIterator indexIterator = indexReader[ i ].documents( 0 );
								long pointer = indexIterator.skipTo( currDoc );
								if ( pointer == currDoc ) {
									Payload payload = indexIterator.payload();
									if ( ! payload.get().equals( content ) ) LOGGER.error( index[ i ] + ": Document " + currDoc + " has payload " + content + " but the index says " + payload );  
								}
								else LOGGER.error( index[ i ] + ": Document " + currDoc + " does not appear in the inverted list of term " + t );
							}
							else {
								IndexIterator indexIterator = indexReader[ i ].documents( "#" );
								if ( indexIterator.skipTo( currDoc ) == currDoc ) {
									if ( ! indexIterator.payload().get().equals( content ) )
										LOGGER.error( index[ i ] + ": Document " + currDoc + " has payload " + content + " but the index says " + indexIterator.payload().get() );
								} 
								else LOGGER.error( index[ i ] + ": Document " + currDoc + " does not appear in the inverted list of term " + t );
							}
						}
						else {
							pos = 0;
							reader = (Reader)content;
							wordReader = document.wordReader( stem || index[ i ].field == null ? indexedField[ i ] : factory.fieldIndex( index[ i ].field ) );
							wordReader.setReader( reader );
							while( wordReader.next( word, nonWord ) ) {
								if ( word.length() == 0 || index[ i ].termProcessor != null && ! index[ i ].termProcessor.processTerm( word ) ) continue;
								IndexIterator indexIterator = indexReader[ i ].documents( word );
								if ( currDoc != indexIterator.skipTo( currDoc ) )
									LOGGER.error( index[ i ] + ": Document " + currDoc + " does not appear in the inverted list of term " + word );
								else if ( index[ i ].hasPositions ) {
									occ[ i ] = IndexIterators.positionArray( indexIterator );
									if ( IntArrayList.wrap( occ[ i ], indexIterator.count() ).indexOf( pos ) == -1 )
										LOGGER.error( index[ i ] + ": Position " + pos + " does not appear in the position list of term " + word + " in document " + currDoc );
								}
								pos++;
							}
						}
					}
					document.close();
					pl.update();
					docCounter++;
				}
			}

			pl.done();
		}
		
		for( IndexReader ir : indexReader ) ir.close();
	}

}
