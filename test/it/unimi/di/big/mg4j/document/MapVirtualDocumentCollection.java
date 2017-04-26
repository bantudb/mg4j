package it.unimi.di.big.mg4j.document;

import it.unimi.di.big.mg4j.tool.VirtualDocumentResolver;
import it.unimi.di.big.mg4j.util.parser.callback.AnchorExtractor.Anchor;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.objects.Reference2ObjectMap;
import it.unimi.dsi.fastutil.objects.Reference2ObjectMaps;
import it.unimi.dsi.io.FastBufferedReader;
import it.unimi.dsi.io.WordReader;
import it.unimi.dsi.lang.MutableString;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

/** A virtual document collection explicitly defined by an array of maps and a trivial resolver (mainly useful for testing).
 * 
 * <p>Every map in the provided sequence is considered a virtual document, and the only
 * field virtual. The factory is built-in.
 * 
 * <p>For simple testing, it is suggested that the provided document specifiers are just integers (as strings),
 * and that an instance of {@link MapVirtualDocumentCollection.TrivialVirtualDocumentResolver} is used to resolve them.
 */

public class MapVirtualDocumentCollection extends AbstractDocumentCollection {
	/** A trivial resolver that just parses document specifier as integers. */
	public final static class TrivialVirtualDocumentResolver implements VirtualDocumentResolver {
		private static final long serialVersionUID = 1L;
		private long numberOfDocuments;
		public long numberOfDocuments() { return numberOfDocuments; }
		public void context( Document document ) {}
		public long resolve( CharSequence virtualDocumentSpec ) { 
			final long d = Long.parseLong( virtualDocumentSpec.toString() );
			return d < 0 || d >= numberOfDocuments ? -1 : d;
		}
		public TrivialVirtualDocumentResolver( final long numberOfDocuments ) {
			this.numberOfDocuments = numberOfDocuments;
		}
	};
	
	public final static class MapVirtualDocumentFactory extends AbstractDocumentFactory {
		private static final long serialVersionUID = 1L;

		public DocumentFactory copy() { return this; }

		public int fieldIndex( String fieldName ) {
			if ( "virtual".equals( fieldName ) ) return 0;
			return -1;
		}

		public String fieldName( int field ) {
			ensureFieldIndex( field );
			return "virtual";
		}

		public FieldType fieldType( int field ) {
			ensureFieldIndex( field );
			return FieldType.VIRTUAL;
		}

		public Document getDocument( InputStream rawContent, Reference2ObjectMap<Enum<?>,Object> metadata ) throws IOException {
			throw new UnsupportedOperationException();
		}

		public int numberOfFields() { return 1; }
	}

	final public Int2ObjectMap<? extends CharSequence>[] virtual;
	final DocumentFactory factory;

	@SafeVarargs
	public MapVirtualDocumentCollection( final Int2ObjectMap<? extends CharSequence>... virtual ) {
		factory = new MapVirtualDocumentFactory();
		this.virtual = virtual;
	}

	public long size() {
		return virtual.length;
	}

	public Document document( final long index ) {
		return new Document() {
			public void close() {}
			public Object content( int field ) throws IOException {
				ensureDocumentIndex( index );
				ObjectArrayList<Anchor> result = new ObjectArrayList<Anchor>();
				for( Map.Entry<Integer, ? extends CharSequence> entry: virtual[ (int)index ].entrySet() )
					result.add( new Anchor( new MutableString( entry.getKey().toString() ), new MutableString( entry.getValue() ) ) );
				return result; 
			}
			public CharSequence title() { return null; }
			public CharSequence uri() { return null; }
			public WordReader wordReader( int field ) { return new FastBufferedReader(); }
		};
	}

	public InputStream stream( final long index ) throws IOException {
		throw new UnsupportedOperationException();
	}

	@SuppressWarnings("unchecked")
	public Reference2ObjectMap<Enum<?>, Object> metadata( long index ) throws IOException {
		return Reference2ObjectMaps.EMPTY_MAP;
	}

	public DocumentCollection copy() {
		return this;
	}

	public DocumentFactory factory() {
		return factory;
	};
}
