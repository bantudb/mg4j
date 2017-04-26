package it.unimi.di.big.mg4j.document;

/*		 
 * MG4J: Managing Gigabytes for Java (big)
 *
 * Copyright (C) 2008-2016 Paolo Boldi and Sebastiano Vigna 
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

import it.unimi.di.big.mg4j.document.PropertyBasedDocumentFactory.MetadataKeys;
import it.unimi.dsi.fastutil.bytes.ByteArrays;
import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.fastutil.io.FastBufferedInputStream;
import it.unimi.dsi.fastutil.io.FastByteArrayInputStream;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.objects.Reference2ObjectArrayMap;
import it.unimi.dsi.fastutil.objects.Reference2ObjectMap;
import it.unimi.dsi.fastutil.objects.Reference2ObjectOpenHashMap;
import it.unimi.dsi.io.FastBufferedReader;
import it.unimi.dsi.io.MultipleInputStream;
import it.unimi.dsi.logging.ProgressLogger;
import it.unimi.dsi.sux4j.util.EliasFanoMonotoneLongBigList;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.net.URLEncoder;
import java.util.Arrays;
import java.util.zip.GZIPInputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.martiansoftware.jsap.JSAP;
import com.martiansoftware.jsap.JSAPException;
import com.martiansoftware.jsap.JSAPResult;
import com.martiansoftware.jsap.Parameter;
import com.martiansoftware.jsap.SimpleJSAP;
import com.martiansoftware.jsap.Switch;
import com.martiansoftware.jsap.UnflaggedOption;

/** A {@link it.unimi.di.big.mg4j.document.DocumentCollection} corresponding to
 *  a given set of files in the Yahoo! Wikipedia format.
 *  
 * <P><strong>Warning</strong>: this class has no connection whatsoever with
 * {@link WikipediaDocumentSequence}.
 * 
 * <P>This class provides a main method with a flexible syntax that serialises
 * into a document collection a list of (possibly gzip'd) files given on the command line or
 * piped into standard input. The files are to be taken from the 
 * <a href="http://barcelona.research.yahoo.net/dokuwiki/doku.php?id=semantically_annotated_snapshot_of_wikipedia">semantically 
 * annotated snapshot of the english wikipedia</a> distributed by Yahoo!. 
 * The position of each record is stored using an {@link EliasFanoMonotoneLongBigList} per file, which gives us
 * random access with very little overhead.
 * 
 * <p>Each column of the collection is indexed in parallel, and is accessible using its label as field name. For instance,
 * a query like
 * <pre>
 * Washington ^ WSJ:(B\-E\:PERSON | B\-I\:PERSON)
 * </pre>
 * will search for &ldquo;Washington&rdquo;, but only if the term has been annotated as a person name (note the
 * escaping, which is necessary if you use the standard parser). See
 * the {@link it.unimi.di.big.mg4j.search} package for more info about the operators available.
 * 
 * <p>See the collection page for more information about the tagging process. 
 */
public class WikipediaDocumentCollection extends AbstractDocumentCollection implements Serializable {
	private final static Logger LOGGER = LoggerFactory.getLogger( WikipediaDocumentCollection.class );
	
	private static final long serialVersionUID = 1L;
	
	private static final byte[] META_MARKER = "%%#".getBytes();
	private static final byte[] DOC_MARKER = "%%#DOC".getBytes();
	private static final byte[] PAGE_MARKER = "%%#PAGE".getBytes();
	private static final byte[] SENTENCE_MARKER = "%%#SEN".getBytes();
	
	private final static int NUM_FIELDS = 10;
	private final static String[] FIELD_NAME = { "token", "POS", "lemma", "CONL", "WNSS", "WSJ", "ana", "head", "deplabel", "link" };
 
	/** The files in this collection. */
	private final String[] file;
	/** The files in {@link #file} are gzip'd. */
	private boolean gzipped;
	/** The factory to be used by this collection. */
	private final DocumentFactory factory;
	/** A list of lists of pointers parallel to {@link #file}. Each list contains the
	 * starting pointer of each document (within its file), plus a final pointer at the end of the file. */
	private final ObjectArrayList<EliasFanoMonotoneLongBigList> pointers;
	/** The number of documents in this collection. */
	private final int size;
	/** Whether this index contains phrases (as opposed to documents). */
	private final boolean phrase;
	/** An array parallel to {@link #file} containing the index of the first
	 * document within each file, plus a final entry equal to {@link #size}. */
	private final long[] firstDocument;
	/** Byte array buffers used to reconstruct each field for random access. */
	private transient byte[][] buffer;
	/** Line buffer. */
	private transient byte[] lineBuffer;
	/** An array parallel to {@link #buffer} specifying the number of valid bytes. */
	private transient int[] bufferSize;
	/** The metadata of the last document. */
	private transient Reference2ObjectMap<Enum<?>,Object> metadata;
	/** The last document read, or -1 if no document has been read. */
	private transient int lastDocument;
	
	private final void initBuffers() {
		bufferSize = new int[ NUM_FIELDS ];
		buffer = new byte[ NUM_FIELDS ][];
		lineBuffer = ByteArrays.EMPTY_ARRAY;
		lastDocument = -1;
		metadata = new Reference2ObjectArrayMap<Enum<?>, Object>();
		for( int i = NUM_FIELDS; i-- != 0; ) buffer[ i ] = ByteArrays.EMPTY_ARRAY;
	}
	
	/** Builds a document collection corresponding to a given set of Wikipedia files specified as an array.
	 * 
	 *  <p><strong>Beware.</strong> This class is not guaranteed to work if files are
	 *  deleted or modified after creation!
	 * 
	 * @param file an array containing the files that will be contained in the collection.
	 * @param factory the factory that will be used to create documents.
	 * @param phrase whether phrases should be indexed instead of documents.
	 */
	public WikipediaDocumentCollection( final String[] file, final DocumentFactory factory, final boolean phrase ) throws IOException {
		this( file, factory, phrase, false );
	}
	
	/** Builds a document collection corresponding to a given set of (possibly gzip'd) Wikipedia files specified as an array.
	 * 
	 *  <p><strong>Beware.</strong> This class is not guaranteed to work if files are
	 *  deleted or modified after creation!
	 * 
	 * @param file an array containing the files that will be contained in the collection.
	 * @param factory the factory that will be used to create documents.
	 * @param phrase whether phrases should be indexed instead of documents.
	 * @param gzipped the files in <code>file</code> are gzip'd.
	 */
	public WikipediaDocumentCollection( final String[] file, final DocumentFactory factory, final boolean phrase, final boolean gzipped ) throws IOException {
		this.file = file;
		this.factory = factory;
		this.gzipped = gzipped;
		this.phrase = phrase;
		
		initBuffers();

		LongArrayList p = new LongArrayList();
		pointers = new ObjectArrayList<EliasFanoMonotoneLongBigList>( file.length );
		firstDocument = new long[ file.length + 1 ];
		int count = 0;
		
		final ProgressLogger pl = new ProgressLogger( LOGGER );
		pl.expectedUpdates = file.length;
		pl.itemsName = "files";
		pl.start( "Scanning files..." );
		
		// Scan files and retrieve page pointers
		for( String f : file ) {
			p.clear();
			final FastBufferedInputStream fbis = gzipped ? new FastBufferedInputStream( new GZIPInputStream( new FileInputStream( f ) ) ) : new FastBufferedInputStream( new FileInputStream( f ) );
			long position;
			for(;;) {
				position = fbis.position();
				if ( readLine( fbis ) == -1 ) break;
				if ( startsWith( lineBuffer, DOC_MARKER ) ) p.add( position );
				if ( phrase && startsWith( lineBuffer, SENTENCE_MARKER ) ) p.add( position );
			}
			
			count += p.size();
			p.add( fbis.position() );
			fbis.close();
			
			pointers.add( new EliasFanoMonotoneLongBigList( p ) );
			firstDocument[ pointers.size() ] = count;
			
			pl.update();
		}
		
		pl.done();
		
		size = count;
	}

	private final int readLine( final FastBufferedInputStream fbis ) throws IOException {
		int start = 0, len;
		while( ( len = fbis.readLine( lineBuffer, start, lineBuffer.length - start, FastBufferedInputStream.ALL_TERMINATORS ) ) == lineBuffer.length - start ) {
			start += len;
			lineBuffer = ByteArrays.grow( lineBuffer, lineBuffer.length + 1 );
		}
		
		if ( len != -1 ) start += len;
		
		return len == -1 ? -1 : start;
	}


	public static class WhitespaceWordReader extends FastBufferedReader {
		private static final long serialVersionUID = 1L;

		@Override
		protected boolean isWordConstituent( final char c ) {
			return ! Character.isWhitespace( c );
		}
	}
	
	protected WikipediaDocumentCollection( String[] file, DocumentFactory factory, ObjectArrayList<EliasFanoMonotoneLongBigList> pointers, int size, long[] firstDocument, boolean phrase, boolean gzipped ) {
		this.file = file;
		this.factory = factory;
		this.pointers = pointers;
		this.size = size;
		this.firstDocument = firstDocument;
		this.gzipped = gzipped;
		this.phrase = phrase;
		initBuffers();
	}

	private static boolean startsWith( byte[] array, byte[] pattern ) {
		int length = pattern.length;
		if ( array.length < length ) return false;
		while( length-- != 0 ) if ( array[ length ] != pattern[ length ] ) return false;
		return true;
	}

	public DocumentFactory factory() {
		return factory;
	}
	
	public long size() {
		return size;
	}

	public Reference2ObjectMap<Enum<?>,Object> metadata( final long index ) throws IOException {
		readDocument( index, -1, null );
		if ( ! metadata.containsKey( MetadataKeys.TITLE ) ) metadata.put(  MetadataKeys.TITLE, "Sentence #" + ( index + 1 ) );
		return metadata;
	}

	public Document document( final long index ) throws IOException {
		return factory.getDocument( stream( index ), metadata( index ) );
	}
	
	public InputStream stream( final long index ) throws IOException {
		readDocument( index, -1, null );
		FastByteArrayInputStream[] is = new FastByteArrayInputStream[ NUM_FIELDS ]; 
		for( int i = 0; i < NUM_FIELDS; i++ ) is[ i ] = new FastByteArrayInputStream( buffer[ i ], 0, bufferSize[ i ] );
		return MultipleInputStream.getStream( is );
	}
	
	
	@Override
	public DocumentIterator iterator() throws IOException {
		return new AbstractDocumentIterator() {
			private int index = 0;
			private int f = 0;
			private FastBufferedInputStream fbis = new FastBufferedInputStream( new FileInputStream( file[ 0 ] ) );
			
			public void close() throws IOException {
				super.close();
				if( fbis != null ) {
					fbis.close();
					fbis = null;
				}
			}

			public Document nextDocument() throws IOException {
				if ( index == size ) return null;
				if ( index == firstDocument[ f + 1 ] ) {
					fbis.close();
					fbis = new FastBufferedInputStream( new FileInputStream( file[ ++f ] ) );
				}
				
				readDocument( index, f, fbis );
				return document( index++ );
			}
		};
	}


	private void readDocument( final long index, int f, FastBufferedInputStream fbis ) throws IOException {
		ensureDocumentIndex( index );
		if ( index == lastDocument ) return;

		final boolean openStream = fbis == null;
		if ( openStream ){
			f = Arrays.binarySearch( firstDocument, index );
			if ( f < 0 ) f = -f - 2;
			fbis = new FastBufferedInputStream( new FileInputStream( file[ f ] ) );
		}

		long start = pointers.get( f ).getLong( index - firstDocument[ f ] );
		fbis.position( start );
		final long end = pointers.get( f ).getLong( index - firstDocument[ f ] + 1 );
		Arrays.fill( bufferSize, 0 );
		metadata.clear();
		int l, field;
		boolean startOfPage, startOfSentence;
		String title;
		
		while( fbis.position() < end ) {
			l = readLine( fbis );
			if ( startsWith( lineBuffer, META_MARKER ) ) {
				startOfPage = startOfSentence = false;
				if ( startsWith( lineBuffer, DOC_MARKER ) && phrase ) return;
				if ( startsWith( lineBuffer, PAGE_MARKER ) ) startOfPage = true;
				else if ( startsWith( lineBuffer, SENTENCE_MARKER ) ) startOfSentence = true;
				if ( startOfPage ) {
					title = new String( lineBuffer, Math.min( PAGE_MARKER.length + 1, l ), Math.max( l - PAGE_MARKER.length - 1, 0 ), "UTF-8" ).trim();
					metadata.put( MetadataKeys.TITLE, title );
					metadata.put( MetadataKeys.URI, "http://en.wikipedia.org/wiki/" + URLEncoder.encode( title, "UTF-8" ) );
				}
				if ( ( startOfPage || startOfSentence ) && ! phrase ) {
					for( int i = 0; i < NUM_FIELDS; i++ ) {
						// Add a paragraph symbol (UTF-8: 0xC2 0xB6).
						buffer[ i ] = ByteArrays.grow( buffer[ i ], bufferSize[ i ] + 3 );
						buffer[ i ][ bufferSize[ i ]++ ] = (byte)0xc2;
						buffer[ i ][ bufferSize[ i ]++ ] = (byte)0xb6;
						buffer[ i ][ bufferSize[ i ]++ ] = '\n';
					}
				}
			}
			else for( int i = field = 0; i < l; i++ ) {
				if ( lineBuffer[ i ] == '\t' ) {
					field++;
				}
				else {
					buffer[ field ] = ByteArrays.grow( buffer[ field ], bufferSize[ field ] + 2 );
					buffer[ field ][ bufferSize[ field ]++ ] = lineBuffer[ i ];
					
					if ( i == l - 1 || lineBuffer[ i + 1 ] == '\t' ) 
						buffer[ field ][ bufferSize[ field ]++ ] = ' ';  
				}
			}
		}
		
		if ( openStream ) fbis.close();
	}
	
	public WikipediaDocumentCollection copy() {
		return new WikipediaDocumentCollection( file, factory.copy(), pointers, size, firstDocument, phrase, gzipped );  
	}
	
	private void readObject( final ObjectInputStream s ) throws IOException, ClassNotFoundException {
		s.defaultReadObject();
		initBuffers();
	}	
	
	public static void main( final String[] arg ) throws IOException, JSAPException, InstantiationException, IllegalAccessException, InvocationTargetException, NoSuchMethodException {

		SimpleJSAP jsap = new SimpleJSAP( WikipediaDocumentCollection.class.getName(), "Saves a serialised document collection based on a set of files.",
				new Parameter[] {
					// Don't know what this was for...
					//new FlaggedOption( "uris", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'u', "uris", "A file containing a list of URIs in ASCII encoding, one per line, that will be associated with each file" ),
					new Switch( "sentence", 's', "sentence", "Index sentences rather than documents." ),
					new Switch( "gzipped", 'z', "gzipped", "The files are gzipped." ),
					new UnflaggedOption( "collection", JSAP.STRING_PARSER, JSAP.REQUIRED, "The filename for the serialised collection." ),
					new UnflaggedOption( "file", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, JSAP.GREEDY, "A list of files that will be indexed. If missing, a list of files will be read from standard input." )
				}
		);
		

		JSAPResult jsapResult = jsap.parse( arg );
		if ( jsap.messagePrinted() ) return;
		
		/*String uri[] = null;
		if ( jsapResult.getString( "uris" ) != null ) {
			Collection<MutableString> lines = new FileLinesCollection( jsapResult.getString( "uris" ), "ASCII" ).allLines();
			uri = new String[ lines.size() ];
			int i = 0;
			for( Object l: lines ) uri[ i++ ] = l.toString();
		}*/
		
		final DocumentFactory factory = new IdentityDocumentFactory( new Reference2ObjectOpenHashMap<Enum<?>,Object>( 
				new PropertyBasedDocumentFactory.MetadataKeys[] { PropertyBasedDocumentFactory.MetadataKeys.ENCODING, PropertyBasedDocumentFactory.MetadataKeys.WORDREADER }, 
				new Object[] { "UTF-8", WhitespaceWordReader.class.getName() } ) ); 
		
		String[] file = (String[])jsapResult.getObjectArray( "file", new String[ 0 ] );
		if ( file.length == 0 ) {
			final ObjectArrayList<String> files = new ObjectArrayList<String>();
			BufferedReader bufferedReader = new BufferedReader( new InputStreamReader( System.in ) );
			String s;
			while( ( s = bufferedReader.readLine() ) != null ) files.add( s );
			file = files.toArray( new String[ 0 ] );
		}
		
		if ( file.length == 0 ) System.err.println( "WARNING: empty file set." );
		//if ( uri != null && file.length != uri.length ) throw new IllegalArgumentException( "The number of files (" + file.length + ") and the number of URIs (" + uri.length + ") differ" );
		BinIO.storeObject( new WikipediaDocumentCollection( file, ReplicatedDocumentFactory.getFactory( factory, NUM_FIELDS, FIELD_NAME ), jsapResult.getBoolean( "sentence"), jsapResult.getBoolean( "gzipped" ) ), jsapResult.getString( "collection" ) );
	}
}
