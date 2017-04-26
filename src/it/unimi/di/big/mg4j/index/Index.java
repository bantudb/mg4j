package it.unimi.di.big.mg4j.index;

/*		 
 * MG4J: Managing Gigabytes for Java
 *
 * Copyright (C) 2004-2016 Sebastiano Vigna 
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

import it.unimi.di.big.mg4j.document.PropertyBasedDocumentFactory;
import it.unimi.di.big.mg4j.index.cluster.IndexCluster;
import it.unimi.di.big.mg4j.index.payload.Payload;
import it.unimi.di.big.mg4j.io.IOFactories;
import it.unimi.di.big.mg4j.io.IOFactory;
import it.unimi.di.big.mg4j.search.DocumentIterator;
import it.unimi.di.big.mg4j.search.IntervalIterator;
import it.unimi.di.big.mg4j.search.visitor.DocumentIteratorVisitor;
import it.unimi.di.big.mg4j.util.MG4JClassParser;
import it.unimi.dsi.big.util.PrefixMap;
import it.unimi.dsi.big.util.StringMap;
import it.unimi.dsi.big.util.StringMaps;
import it.unimi.dsi.fastutil.ints.IntBigList;
import it.unimi.dsi.fastutil.longs.LongIterator;
import it.unimi.dsi.fastutil.objects.Reference2ReferenceMap;
import it.unimi.dsi.fastutil.objects.ReferenceSet;
import it.unimi.dsi.fastutil.objects.ReferenceSets;
import it.unimi.dsi.lang.ObjectParser;
import it.unimi.dsi.sux4j.util.EliasFanoLongBigList;
import it.unimi.dsi.util.ImmutableExternalPrefixMap;
import it.unimi.dsi.util.LongInterval;
import it.unimi.dsi.util.LongIntervals;
import it.unimi.dsi.util.Properties;

import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.EnumMap;

import org.apache.commons.configuration.ConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;





/** An abstract representation of an index.
 *
 * <P>Concrete subclasses of this class represent abstract index access
 * information: for instance, the basename or IP address/port,
 * flags, etc. It allows to build easily {@linkplain IndexReader index readers} over the index:
 * in turn, index readers provide {@linkplain it.unimi.di.big.mg4j.search.DocumentIterator document iterators}.
 * 
 * <P>This class contains just methods declarations,
 * and attributes for all data that is common to any form of index.
 * Note that we use an abstract class, rather than an interface, because
 * interfaces do not allow to declare attributes. 
 * 
 * <P>We provide static factory methods (e.g., {@link #getInstance(CharSequence)})
 * that return an index given a suitable URI string. If the scheme part is <samp>mg4j</samp>, then
 * the URI is assumed to point at a remote index. Otherwise, it is assumed to be the
 * basename of a local index. In both cases, a query part introduced by <samp>?</samp> can
 * specify additional parameters (<samp><var>key</var>=<var>value</var></samp> pairs separated
 * by <samp>;</samp>). For instance, the URI <samp>example?inmemory=1</samp> will load
 * the index with basename <samp>example</samp>, caching its content in core memory.
 * Please have a look at constants in {@link Index.UriKeys} 
 * (and analogous enums in subclasses) for additional parameters.
 * 
 * <p>If the index is local, by convention this class will locate a property file with extension
 * {@link DiskBasedIndex#PROPERTIES_EXTENSION} that is expected to contain a number
 * of {@linkplain PropertyKeys key/value pairs} (which are quite informative and can be examined
 * manually). In particular, the key {@link PropertyKeys#INDEXCLASS} explain which kind of
 * index class should be used to read the index. The file might contain additional keys
 * depending on the value of {@link PropertyKeys#INDEXCLASS} (e.g., {@link QuasiSuccinctIndex.PropertyKeys#BYTEORDER}).
 *
 * <A>An index usually exposes {@linkplain #termMap term}
 * or {@linkplain #prefixMap prefix} maps and the {@linkplain #sizes size list} but this is not compulsory
 * (the latter, in particular, is necessary with certain codings).
 * 
 * <h2>Thread safety</h2>
 * 
 * <p>Indices are a natural candidate for multithreaded access. An instance of this class
 * <strong>must</strong> be thread safe as long as external data structures provided to its
 * constructors are. For instance, the tool {@link it.unimi.di.big.mg4j.tool.IndexBuilder} generates
 * a {@linkplain StringMaps#synchronize(PrefixMap) synchronized} {@link ImmutableExternalPrefixMap}
 * so that by default the resulting index is thread safe.
 * 
 * <p>For instance, a {@link it.unimi.di.big.mg4j.index.DiskBasedIndex} requires a list of
 * term offsets, term maps, etc. As long as all these data structures are thread safe, the
 * same is true of the index. Data structures created by static factory methods such as
 * {@link it.unimi.di.big.mg4j.index.DiskBasedIndex#getInstance(CharSequence)} are thread safe.
 * 
 * <p>Note that {@link it.unimi.di.big.mg4j.index.IndexReader}s returned by {@link #getReader()}
 * are <em>not</em> thread safe (even if the method {@link #getReader()} is). The logic behind
 * this arrangement is that you create as many reader as you need, and then {@link java.io.Closeable#close()} them. In a multithreaded
 * environment, a pool of index readers can be created, and a custom {@link it.unimi.di.big.mg4j.query.nodes.QueryBuilderVisitor}
 * can be used to build {@link it.unimi.di.big.mg4j.search.DocumentIterator}s using the given pool of readers. In
 * this case readers are not closed, but rather reused.
 * 
 * <h2>Read-once load</h2>
 * 
 * <p>Implementations of this class are strongly encouraged to offer <em>read-once</em> constructors
 * and factory methods: property files and other data related to the index (but not to an {@link it.unimi.di.big.mg4j.index.IndexReader}
 * should be read exactly once, and sequentially. This feature is very useful when 
 * {@linkplain it.unimi.di.big.mg4j.tool.Combine combining indices}.
 * 
 * @author Paolo Boldi
 * @author Sebastiano Vigna
 * @since 0.9
 */

public abstract class Index implements Serializable {
	private static final Logger LOGGER = LoggerFactory.getLogger( Index.class );
	private static final long serialVersionUID = 0;

	/** Symbolic names for properties of a {@link it.unimi.di.big.mg4j.index.Index}. */
	public static enum PropertyKeys {
		/** The number of documents in the collection. */
		DOCUMENTS,
		/** The number of terms in the collection. */
		TERMS,
		/** The number of occurrences in the collection, or -1 if the number of occurrences is not known. */
		OCCURRENCES,
		/** The number of postings (pairs term/document) in the collection. */
		POSTINGS,
		/** The number of batches this index was (or should be) built from. */
		BATCHES,
		/** The maximum count, or -1 is the maximum count is not known. */
		MAXCOUNT,
		/** The maximum size (in words) of a document, or -1 if the maximum document size is not known. */
		MAXDOCSIZE,
		/** The {@link TermProcessor} used to build this index. */
		TERMPROCESSOR,
		/** A class for the payloads of this index. */
		PAYLOADCLASS,
		/** The specification of a compressiong flag. This property can be specified
		 * as many time as necessary (e.g., <samp>FREQUENCIES:GAMMA</samp>, <samp>POINTERS:GOLOMB</samp>, <samp>POSITIONS:NONE</samp>, etc.).
		 * Note that different type of indices have different allowable combinations. */
		CODING,		
		/** The name of the {@link Index} class that should read this index. */
		INDEXCLASS,			
		/** The name of the field indexed by this index, if any. */
		FIELD,			
		/** The size in bits of the index. */
		SIZE
	}
	
	/** Keys to be used (downcased) in specifiying additional parameters to a MG4J URI. */
	
	public static enum UriKeys {
		/** When set, forces loading a local index into core memory. */
		INMEMORY,
		/** When set, forces to map a local index into core memory. */
		MAPPED,
		/** The step used for creating the offset {@link it.unimi.di.big.mg4j.util.SemiExternalOffsetBigList}. If
		 * set to zero, the offset list will be entirely loaded into core memory. If negative, the list
		 * will be memory-mapped, and the absolute value will be used as step. */
		OFFSETSTEP,
		/** When set, sizes are loaded in a succinct format (more precisely, using an {@linkplain EliasFanoLongBigList Elias&ndash;Fano compressed list}. */
		SUCCINCTSIZES,
		/** The name of a sizes file that will be loaded in case of an {@link IndexCluster}. */
		SIZES,
	}

	/** The field indexed by this index, or <code>null</code>. */
	public final String field;
	/** The properties of this index. It is stored here for convenience (for instance,
	 * if custom keys are added to the property file), but it may be <code>null</code>. */
	public final Properties properties;
	/** The number of documents of the collection. */
	public final long numberOfDocuments;
	/** The number of terms of the collection. This field might be set to -1 in some cases 
	 * (for instance, in certain documental clusters). */
	public final long numberOfTerms;
	/** The number of occurrences of the collection, or possibly -1 if it is unknown. */
	public final long numberOfOccurrences;
	/** The number of postings (pairs term/document) of the collection. */
	public final long numberOfPostings;
	/** The maximum number of positions in an position list, or possibly -1 if this index does not have positions. */
	public final int maxCount;
	/** The payload for this index, or <code>null</code>. */
	public final Payload payload;
	/** Whether this index contains payloads; if true, {@link #payload} is non-<code>null</code>. */
	public final boolean hasPayloads;
	/** Whether this index contains counts. */
	public final boolean hasCounts;
	/** Whether this index contains positions. */
	public final boolean hasPositions;
	/** The term processor used to build this index. */
	public final TermProcessor termProcessor;
	/** An immutable singleton set containing just {@link #keyIndex}. */
	public ReferenceSet<Index> singletonSet;
	/** The index used as a key to retrieve intervals. Usually equal to <code>this</code>, but it is {@linkplain #keyIndex(Index) settable}. */
	public Index keyIndex;
	/** The term map for this index, or <code>null</code> if the term map was not loaded. */
	public final StringMap<? extends CharSequence> termMap;
	/** The prefix map for this index, or <code>null</code> if the prefix map was not loaded. */
	public final PrefixMap<? extends CharSequence> prefixMap;
	/** The size of each document, or <code>null</code> if sizes are not necessary or not loaded in this index. */
	public final IntBigList sizes;

	/** Creates a new instance, initialising all fields. */
	protected Index( final long numberOfDocuments, final long numberOfTerms, final long numberOfPostings,
			final long numberOfOccurrences, final int maxCount,
			final Payload payload, final boolean hasCounts, final boolean hasPositions, final TermProcessor termProcessor,
			final String field, StringMap<? extends CharSequence> termMap, final PrefixMap<? extends CharSequence> prefixMap, final IntBigList sizes, final Properties properties ) {
		this.numberOfDocuments = numberOfDocuments;
		this.numberOfTerms = numberOfTerms;
		this.numberOfPostings = numberOfPostings;
		this.numberOfOccurrences = numberOfOccurrences;
		this.maxCount = maxCount;
		this.payload = payload;
		this.hasPayloads = payload != null;
		this.hasCounts = hasCounts;
		this.hasPositions = hasPositions;
		this.termProcessor = termProcessor;
		this.field = field;
		this.properties = properties;
		this.keyIndex = this;
		this.singletonSet = ReferenceSets.singleton( this );
		this.termMap = termMap;
		this.prefixMap = prefixMap;
		this.sizes = sizes;
	}

	protected static TermProcessor getTermProcessor( final Properties properties ) {
		try {
			// Catch old property files
			if ( properties.getProperty( Index.PropertyKeys.TERMPROCESSOR ) == null ) 
				throw new IllegalArgumentException( "No term processor has been specified (most likely, because of an obsolete property file)" );
			return ObjectParser.fromSpec( properties.getString( Index.PropertyKeys.TERMPROCESSOR ).replace( ".dsi.big.mg4j.", ".di.big.mg4j." ), TermProcessor.class, MG4JClassParser.PACKAGE, new String[] { "getInstance" } );
		}
		catch ( Exception e ) {
			throw new RuntimeException( e );
		}
	}

	/** Returns a new index using the given URI.
	 * 
	 * @param ioFactory the factory that will be used to perform I/O, or <code>null</code> (implying the {@link IOFactory#FILESYSTEM_FACTORY} for disk-based indices).
	 * @param uri the URI defining the index.
	 * @param randomAccess whether the index should be accessible randomly.
	 * @param documentSizes if true, document sizes will be loaded (note that sometimes document sizes
	 * might be loaded anyway because the compression method for positions requires it).
	 * @param maps if true, {@linkplain StringMap term} and {@linkplain PrefixMap prefix} maps will be guessed and loaded (this
	 * feature might not be available with some kind of index). 
	 */
	public static Index getInstance( IOFactory ioFactory, final CharSequence uri, final boolean randomAccess, final boolean documentSizes, final boolean maps ) throws IOException, ConfigurationException, URISyntaxException, ClassNotFoundException, SecurityException, InstantiationException, IllegalAccessException, InvocationTargetException, NoSuchMethodException {
		/* If the scheme is mg4j, then we are creating a remote
		 * index. If it is null, we assume it is a property file and load it. Otherwise, we
		 * assume it is a valid property file URI and try to download it. */
		
		final String uriString = uri.toString();
		/*if ( uriString.startsWith( "mg4j:" ) ) {
			if ( ioFactory != null ) throw new IllegalAccessError( "You cannot specify a factory for a remote index" );
			final URI u = new URI( uriString );
			return IndexServer.getIndex( u.getHost(), u.getPort(), randomAccess, documentSizes );
		}*/

		final String basename, query;
		if ( ioFactory == null ) ioFactory = IOFactory.FILESYSTEM_FACTORY;
		
		if ( uriString.startsWith( "file:" ) ) {
			final URI u = new URI( uriString );
			basename = u.getPath();
			query = u.getQuery();
		}
		else {
			final int questionMarkPos = uriString.indexOf( '?' );
			basename = questionMarkPos == -1 ? uriString : uriString.substring( 0, questionMarkPos );
			query = questionMarkPos == -1 ? null : uriString.substring( questionMarkPos + 1 );
		}
		
		LOGGER.debug( "Searching for an index with basename " + basename + "..." );
		final Properties properties = IOFactories.loadProperties( ioFactory, basename + DiskBasedIndex.PROPERTIES_EXTENSION );
		LOGGER.debug( "Properties: " + properties );
					
		// We parse the key/value pairs appearing in the query part.
		final EnumMap<UriKeys,String> queryProperties = new EnumMap<UriKeys,String>( UriKeys.class );
		if ( query != null ) {
			String[] keyValue = query.split( ";" );
			for( int i = 0; i < keyValue.length; i++ ) {
				String[] piece = keyValue[ i ].split( "=" );
				if ( piece.length != 2 ) throw new IllegalArgumentException( "Malformed key/value pair: "  + keyValue[ i ] );
				// Convert to standard keys
				boolean found = false;
				for( UriKeys key: UriKeys.values() )  
					if ( found = PropertyBasedDocumentFactory.sameKey( key, piece[ 0 ] ) ) {
						queryProperties.put( key, piece[ 1 ] );
						break;
					}
				if ( ! found ) throw new IllegalArgumentException( "Unknown key: " + piece[ 0 ] );
			}
		}

		// Compatibility with previous versions
		String className = properties.getString( Index.PropertyKeys.INDEXCLASS, "(missing index class)" ).replace( ".dsi.big.mg4j.", ".di.big.mg4j." );
		Class<?> indexClass = Class.forName( className );

		// It is a cluster.
		if ( IndexCluster.class.isAssignableFrom( indexClass ) ) return IndexCluster.getInstance( basename, randomAccess, documentSizes, queryProperties );
		// It is a disk-based index.
		return DiskBasedIndex.getInstance( ioFactory, basename, properties, randomAccess, documentSizes, maps, queryProperties );
	}

	/** Returns a new index using the given URI and no {@link IOFactory}.
	 * 
	 * @param uri the URI defining the index.
	 * @param randomAccess whether the index should be accessible randomly.
	 * @param documentSizes if true, document sizes will be loaded (note that sometimes document sizes
	 * might be loaded anyway because the compression method for positions requires it).
	 * @param maps if true, {@linkplain StringMap term} and {@linkplain PrefixMap prefix} maps will be guessed and loaded (this
	 * feature might not be available with some kind of index). 
	 */
	public static Index getInstance( final CharSequence uri, final boolean randomAccess, final boolean documentSizes, final boolean maps ) throws IOException, ConfigurationException, URISyntaxException, ClassNotFoundException, SecurityException, InstantiationException, IllegalAccessException, InvocationTargetException, NoSuchMethodException {
		return getInstance( null, uri, randomAccess, documentSizes, maps );
	}
	
	/** Returns a new index using the given URI, searching dynamically for term and prefix maps.
	 * 
	 * @param uri the URI defining the index.
	 * @param randomAccess whether the index should be accessible randomly.
	 * @param documentSizes if true, document sizes will be loaded (note that sometimes document sizes
	 * might be loaded anyway because the compression method for positions requires it).
	 * @see #getInstance(CharSequence, boolean, boolean, boolean)
	 */
	public static Index getInstance( final CharSequence uri, final boolean randomAccess, final boolean documentSizes ) throws IOException, ConfigurationException, URISyntaxException, ClassNotFoundException, SecurityException, InstantiationException, IllegalAccessException, InvocationTargetException, NoSuchMethodException {
		return getInstance( uri, randomAccess, documentSizes, true );
	}

	/** Returns a new index using the given URI, searching dynamically for term and prefix maps and loading
	 * document sizes only if it is necessary.   
	 * 
	 * @param uri the URI defining the index.
	 * @param randomAccess whether the index should be accessible randomly.
	 * @see #getInstance(CharSequence, boolean, boolean)
	 */
	public static Index getInstance( final CharSequence uri, final boolean randomAccess ) throws ConfigurationException, IOException, URISyntaxException, ClassNotFoundException, SecurityException, InstantiationException, IllegalAccessException, InvocationTargetException, NoSuchMethodException {
		return getInstance( uri, randomAccess, false );
	}

	/** Returns a new index using the given URI, searching dynamically for term and prefix maps, loading offsets but loading
	 * document sizes only if it is necessary.   
	 * 
	 * @param uri the URI defining the index.
	 * @see #getInstance(CharSequence, boolean)
	 */
	public static Index getInstance( final CharSequence uri ) throws ConfigurationException, IOException, URISyntaxException, ClassNotFoundException, SecurityException, InstantiationException, IllegalAccessException, InvocationTargetException, NoSuchMethodException {
		return getInstance( uri, true );
	}

	/** An iterator returning no documents based on this index. 
	 * 
	 * <P>Note that {@link #accept(DocumentIteratorVisitor)} does nothing
	 * and returns true, whereas {@link #acceptOnTruePaths(DocumentIteratorVisitor)}
	 * throws an {@link IllegalStateException}, {@link #weight()} returns 1
	 * and {@link #weight(double)} is a no-op.
	 */
	public class EmptyIndexIterator implements IndexIterator, Serializable {
		private static final long serialVersionUID = 0;
		public String term;
		public double weight = 1;
		public int id;
		public long termNumber;
		
		public long document() { return -1; }
		public ReferenceSet<Index> indices() { return Index.this.singletonSet; }
		public IntervalIterator intervalIterator() { throw new IllegalStateException(); }
		public Reference2ReferenceMap<Index,IntervalIterator> intervalIterators() { throw new IllegalStateException(); }
		public IntervalIterator intervalIterator( final Index index ) { throw new IllegalStateException(); }
		public long nextDocument() { return END_OF_LIST; }
		public boolean mayHaveNext() { return false; }
		public long skipTo( final long n ) { return END_OF_LIST; }
		public long frequency() { return 0; }
		public Payload payload() { throw new IllegalStateException(); }
		public int count() { throw new IllegalStateException(); }
		public void dispose() {}
		public Index index() { return Index.this; };
		public <T> T accept( DocumentIteratorVisitor<T> visitor ) throws IOException { return visitor.visit( this ); }
		public <T> T acceptOnTruePaths( DocumentIteratorVisitor<T> visitor ) { throw new IllegalStateException(); }
		public String term() { return term; }
		public EmptyIndexIterator term( final CharSequence term ) { this.term = term == null ? null : term.toString(); return this; };
		public int id() { return id; }
		public IndexIterator id( final int id ) { this.id = id; return this; }
		public long termNumber() { return termNumber; }
		public IndexIterator termNumber( final long term ) { this.termNumber = term; return this; }
		public double weight() { return weight; }
		public IndexIterator weight( final double weight ) { this.weight = weight; return this; }
		@Override
		public int nextPosition() throws IOException { throw new IllegalStateException(); }
	}

	public IndexIterator getEmptyIndexIterator() {
		return new EmptyIndexIterator();
	}

	public IndexIterator getEmptyIndexIterator( final long term ) {
		return new EmptyIndexIterator().termNumber( term );
	}

	public IndexIterator getEmptyIndexIterator( final CharSequence term ) {
		return new EmptyIndexIterator().term( term );
	}

	public IndexIterator getEmptyIndexIterator( final CharSequence term, final long termNumber ) {
		return new EmptyIndexIterator().term( term ).termNumber( termNumber );
	}

	/** Creates and returns a new {@link IndexReader} based on this index, using
	 * the default buffer size. After that, you can use the reader to read this index.
	 * 
	 * @return a new {@link IndexReader} to read this index.
	 */
	public IndexReader getReader() throws IOException {
		return getReader( -1 );
	}

	
	/** Creates and returns a new {@link IndexReader} based on this index. After that, you
	 *  can use the reader to read this index.
	 * 
	 * @param bufferSize the size of the buffer to be used accessing the reader, or -1
	 * for a default buffer size.
	 * @return a new {@link IndexReader} to read this index.
	 */
	public abstract IndexReader getReader( final int bufferSize ) throws IOException;

	
	/** Creates a new {@link IndexReader} for this index and uses it to return 
	 * an index iterator over the documents containing a term.
	 *
	 * <p>Since the reader is created from scratch, it is essential
	 * to {@linkplain it.unimi.di.big.mg4j.search.DocumentIterator#dispose() dispose} the
	 * returned iterator after usage. See {@link IndexReader#documents(long)}
	 * for a method with the same semantics, but making reader reuse possible.
	 * 
	 * @param term a term.
	 * @throws IOException if an exception occurred while accessing the index.
	 * @throws UnsupportedOperationException if this index is not accessible by term
	 * number.
	 * @see IndexReader#documents(long)
	 */
	public IndexIterator documents( final long term ) throws IOException {
		final IndexReader indexReader = getReader();
		final IndexIterator indexIterator = indexReader.documents( term );
		if ( indexIterator instanceof EmptyIndexIterator ) indexReader.close();
		return indexIterator;
	}

	/** Creates a new {@link IndexReader} for this index and uses it to return 
	 * an index iterator over the documents containing a term; the term is
	 *  given explicitly, and the index {@linkplain StringMap term map} is used, if present.
	 *
	 * <p>Since the reader is created from scratch, it is essential
	 * to {@linkplain it.unimi.di.big.mg4j.search.DocumentIterator#dispose() dispose} the
	 * returned iterator after usage. See {@link IndexReader#documents(long)}
	 * for a method with the same semantics, but making reader reuse possible.
	 * 
	 * <p>Unless the {@linkplain Index#termProcessor term processor} of
	 * this index is <code>null</code>, words coming from a query will
	 * have to be processed before being used with this method.
	 * 
	 * @param term a term.
	 * @throws IOException if an exception occurred while accessing the index.
	 * @throws UnsupportedOperationException if the {@linkplain StringMap term map} is not 
	 * available for this index.
	 * @see IndexReader#documents(CharSequence)
	 */
	public IndexIterator documents( final CharSequence term ) throws IOException {
		final IndexReader indexReader = getReader();
		final IndexIterator indexIterator = indexReader.documents( term );
		if ( indexIterator instanceof EmptyIndexIterator ) indexReader.close();
		return indexIterator;
	}

	/** Creates a number of instances of {@link IndexReader} for this index and uses them to return 
	 * a {@link MultiTermIndexIterator} over the documents containing any term our of a set of terms defined
	 *  by a prefix; the prefix is given explicitly, and unless the index has a 
	 *  {@linkplain PrefixMap prefix map}, an {@link UnsupportedOperationException}
	 *  will be thrown. 
	 *
	 * @param prefix a prefix.
	 * @param limit a limit on the number of terms that will be used to resolve
	 * the prefix query; if the terms starting with <code>prefix</code> are more than
	 * <code>limit</code>, a {@link TooManyTermsException} will be thrown. 
	 * @throws UnsupportedOperationException if this index cannot resolve prefixes.
	 * @throws TooManyTermsException if there are more than <code>limit</code> terms starting with <code>prefix</code>.
	 */
	public IndexIterator documents( final CharSequence prefix, final int limit ) throws IOException, TooManyTermsException {
		if ( prefixMap != null ) {
			final LongInterval interval = prefixMap.rangeMap().get( prefix );
			if ( interval == LongIntervals.EMPTY_INTERVAL ) return new Index.EmptyIndexIterator();
			final IndexIterator result;
			
			if ( interval.length() > limit ) throw new TooManyTermsException( interval.length() );
			
			if ( interval.length() == 1 ) result = documents( interval.left );
			else {
				IndexIterator[] baseIterator = new IndexIterator[ (int)interval.length()];
				int k = 0;
				for( LongIterator i = interval.iterator(); i.hasNext(); ) baseIterator[ k++ ] = documents( i.nextLong() );
			
				result = MultiTermIndexIterator.getInstance( this, baseIterator );
			}
			result.term( prefix + "*" );
			return result;
		}
		else throw new UnsupportedOperationException( "Index " + this + " has no prefix map" );
	}

	
	/** Sets the index used as a key to retrieve intervals from iterators generated from this index.
	 * 
	 * <P>This setter is a compromise between clarity of design and efficiency.
	 * Each index iterator is based on an index, and when that index is passed
	 * to {@link DocumentIterator#intervalIterator(Index)}, intervals corresponding
	 * to the positions of the term in the current document are returned. Analogously,
	 * {@link it.unimi.di.big.mg4j.search.DocumentIterator#indices()} returns a singleton
	 * set containing the index. However, when composing indices into clusters, 
	 * often iterators generated by a local index must act as if they really belong
	 * to the global index. This method allows to set the index that is used as
	 * a key to return intervals, and that is contained in {@link #singletonSet}.   
	 *
	 * <P>Note that setting this value will only influence {@linkplain IndexReader index readers}
	 * created afterwards.
	 * 
	 * @param newKeyIndex the new index to be used as a key for interval retrieval.
	 */
	
	public void keyIndex( Index newKeyIndex ) {
		keyIndex = newKeyIndex;
		singletonSet = ReferenceSets.singleton( keyIndex );
	}

}
