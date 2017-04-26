package it.unimi.di.big.mg4j.document;

/*		 
 * MG4J: Managing Gigabytes for Java (big)
 *
 * Copyright (C) 2005-2016 Paolo Boldi  
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
import it.unimi.di.big.mg4j.tool.Scan.VirtualDocumentFragment;
import it.unimi.di.big.mg4j.util.parser.callback.AnchorExtractor;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.objects.Reference2ObjectArrayMap;
import it.unimi.dsi.fastutil.objects.Reference2ObjectMap;
import it.unimi.dsi.io.FastBufferedReader;
import it.unimi.dsi.io.WordReader;
import it.unimi.dsi.lang.MutableString;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.Reader;
import java.io.Serializable;
import java.util.NoSuchElementException;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import java.util.zip.ZipInputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/** A {@linkplain it.unimi.di.big.mg4j.document.DocumentCollection document collection} stored in a {@linkplain ZipFile zip file}.
 * 
 * <p>Each instance of this class has an associated zip file. Each Zip entry corresponds to a document: 
 * the title is recorded in the comment field, whereas the 
 * URI is written with {@link MutableString#writeSelfDelimUTF8(java.io.OutputStream)}
 * directly to the zipped output stream. When building an <em>exact</em>
 * {@linkplain it.unimi.di.big.mg4j.document.ZipDocumentCollection} 
 * subsequent word/nonword pairs are written in the same way, and
 * delimited by two empty strings. If the collection is not exact, just words are written,
 * and delimited by an empty string. Non-text fields are written directly to the zipped output stream
 * as serialised objects.
 *
 * <p>The collection will produce the same documents as the original sequence whence it
 * was produced, in the following sense:
 * 
 *  <ul>
 *    <li>the resulting collection has as many document as the original sequence, in the same order, with
 *     the same titles and URI;
 *    <li>every document has the same number of fields, with the same names and types;
 *    <li>non-textual non-virtual fields will be written out as objects, so they need to be serializable;
 *    <li>virtual fields will be written as a sequence of {@linkplain MutableString#writeSelfDelimUTF8(java.io.DataOutput) self-delimiting UTF-8 mutable strings}
 *     starting with the number of fragments (converted into a string with {@link String#valueOf(int)}),
 *     followed by a pair of strings for each fragment (the first string being the document specifier,
 *     and the second being the associated text);
 *    <li>textual fields will be written out in such a way that, when reading them, the same sequence
 *     of words and non-words will be produced; alternatively, one may produce a collection that only
 *     copies words (non-words are not copied). 
 *  </ul>
 * 
 * <p>The collection will be, as any other collection, serialized on a file, but it will refer to another
 * zip file that is going to contain the documents themselves. Please use {@link AbstractDocumentSequence#load(CharSequence)}
 * to load instances of this collection.
 * 
 * <p>Note that the zip format is not designed for a large number of files. This class is mainly a useful example,
 * and a handy way to build quickly a collection containing all fields at indexing time. For a more efficient
 * kind of collection, see {@link SimpleCompressedDocumentCollection}. 
 * 
 * <p><strong>Warning:</strong> the {@link java.io.Reader} returned by {@link it.unimi.di.big.mg4j.document.Document#content(int)}
 * for documents produced by this factory is just obtained as the concatenation of words and non-words returned by
 * the word reader for that field. In case the collection is not exact, nonwords are substituted by a space.
 */
public class ZipDocumentCollection extends AbstractDocumentCollection implements Serializable {
	private static final long serialVersionUID = 2L;

	public final static String ZIP_EXTENSION = ".zip";
	
	/** Symbolic names for common properties of a {@link it.unimi.di.big.mg4j.document.DocumentCollection}. */
	public static enum PropertyKeys {
		/** The serialised collection. */
		COLLECTION,
	}

	private static final Logger LOGGER = LoggerFactory.getLogger( ZipDocumentCollection.class );
	private static final boolean DEBUG = false;
	
	/** The name of the zip collection file. */
	private String zipFilename;
	/** The zip collection file. */
	private transient ZipFile zipFile;
	/** The factory used for the original document sequence. */
	private final DocumentFactory underlyingFactory;
	/** The factory used for this document collection. */
	private final DocumentFactory factory;
	/** The number of documents. */
	private final long numberOfDocuments;
	/** <code>true</code> iff this is an exact reproduction of the original sequence (i.e., if also non-words are preserved). */
	private final boolean exact;
	
	
	/** A factory tightly coupled to a {@link ZipDocumentCollection}. */
	protected static class ZipFactory extends AbstractDocumentFactory {
		private static final long serialVersionUID = 1L;

		private final boolean exact;
		private final DocumentFactory underlyingFactory;

		protected ZipFactory( final DocumentFactory underlyingFactory, final boolean exact ) {
			this.underlyingFactory = underlyingFactory;
			this.exact = exact;
		}

		public ZipFactory copy() {
			return this;
		}
		
		public int numberOfFields() {
			return underlyingFactory.numberOfFields();
		}

		public String fieldName( final int field ) {
			ensureFieldIndex( field );
			return underlyingFactory.fieldName( field );
		}

		public int fieldIndex( final String fieldName ) {
			return underlyingFactory.fieldIndex( fieldName );
		}

		public FieldType fieldType( final int field ) {
			ensureFieldIndex( field );
			return underlyingFactory.fieldType( field );
		}

		public Document getDocument( final InputStream rawContent, final Reference2ObjectMap<Enum<?>,Object> metadata ) throws IOException {
			return new AbstractDocument() {
				final DataInputStream rawContentDataInputStream = new DataInputStream( rawContent );
				int nextFieldToRead = 0;
				final MutableString uri = new MutableString();
				
				{
					uri.readSelfDelimUTF8( rawContent ).compact();
				}
				
				@Override
				public void close() throws IOException {
					super.close();
					rawContent.close();
				}
				
				public CharSequence title() {
					return (CharSequence)metadata.get( MetadataKeys.TITLE );
				}
				
				public String toString() {
					return title().toString();
				}

				public CharSequence uri() {
					return uri.length() == 0 ? null : uri;
				}
				
				/** Skips until the end of the current field, and increments <code>nextFieldToRead</code>.
				 * @throws ClassNotFoundException
				 * @throws IOException
				 */
				private void skipOneField() throws IOException, ClassNotFoundException {
					switch( fieldType( nextFieldToRead ) ) {
					case TEXT:
						MutableString word = new MutableString();
						MutableString nonWord = new MutableString();
						do {
							word.readSelfDelimUTF8( rawContent );
							if ( exact ) nonWord.readSelfDelimUTF8( rawContent );
						} while ( word.length() > 0 || ( exact && nonWord.length() > 0 ) );
						break;
					case VIRTUAL: 
						final int nfrag = rawContentDataInputStream.readInt();
						for ( int i = 0; i < 2 * nfrag; i++ ) MutableString.skipSelfDelimUTF8( rawContent );
						break;
					default: // Non-text and non-virtual
						new ObjectInputStream( rawContent ).readObject();
					}
					nextFieldToRead++;
				}
				
				/** Skips to the given field.
				 * 
				 * @param field the field to skip to.
				 * @throws IOException
				 * @throws ClassNotFoundException
				 */
				private void skipToField( final int field ) throws IOException, ClassNotFoundException {
					if ( nextFieldToRead > field ) throw new IllegalStateException( "Trying to skip to field " + field + " after " + nextFieldToRead );
					while ( nextFieldToRead < field ) skipOneField();
				}

				public Object content( final int field ) {
					ensureFieldIndex( field );
					Object result = null;
					if ( DEBUG ) LOGGER.debug( "Called content(" + field + "); nextField:" + nextFieldToRead );
					try {
						skipToField( field );
						if ( fieldType( nextFieldToRead ) == FieldType.VIRTUAL ) {
							final int nfrag = rawContentDataInputStream.readInt();
							MutableString doc = new MutableString();
							MutableString text = new MutableString();
							VirtualDocumentFragment[] fragArray = new VirtualDocumentFragment[ nfrag ];
							for ( int i = 0; i < nfrag; i++ ) {
								doc.readSelfDelimUTF8( rawContent );
								text.readSelfDelimUTF8( rawContent );
								fragArray[ i ] = new AnchorExtractor.Anchor( doc.copy(), text.copy() );
							}
							result = new ObjectArrayList<VirtualDocumentFragment>( fragArray );
						}
						else if ( fieldType( nextFieldToRead ) != FieldType.TEXT ) {
							result = new ObjectInputStream( rawContent ).readObject();
							if ( DEBUG ) LOGGER.debug( "Read " + result + " from field " + fieldName( nextFieldToRead ) + " of object " + title() );
							nextFieldToRead++;
						}
						else {
							if ( DEBUG ) LOGGER.debug( "Returning reader for " + field );
							result = new Reader() {
								FastBufferedReader fbr = null;
								int f = field;
								public void close() {}
								public int read( final char[] cbuf, final int off, final int len ) throws IOException {
									if ( fbr == null ) {
										if ( DEBUG ) LOGGER.debug( "Initialising reader for content " + f );
										MutableString text = new MutableString();
										MutableString word = new MutableString();
										MutableString nonWord = new MutableString(); 
										do {
											text.append( word.readSelfDelimUTF8( rawContent ) );
											if ( exact ) text.append( nonWord.readSelfDelimUTF8( rawContent ) );
											else text.append( ' ' );
										} while ( word.length() > 0 || ( exact && nonWord.length() > 0 ) );
										fbr = new FastBufferedReader( text );
										nextFieldToRead++;
									}
									return fbr.read( cbuf, off, len );
								}
							};
						}
					} catch ( IOException e ) {
						throw new RuntimeException( e );
					} catch (ClassNotFoundException e) {
						throw new RuntimeException( e );
					} 
					return result;
				}

				public WordReader wordReader( final int field )  {
					ensureFieldIndex( field );
					if ( DEBUG ) LOGGER.debug( "Called wordReader(" + field + ")" );
					try {
						skipToField( field );
					} catch ( Exception e ) {
						throw new RuntimeException( e );
					} 
					//logger.debug( "Asked for a new word reader for field " + fieldName( field ) );
					switch ( fieldType( field ) ) {
					case TEXT:
						return new WordReader() {
							private static final long serialVersionUID = 1L;
							public boolean next( final MutableString word, final MutableString nonWord ) throws IOException {
								try {
									word.readSelfDelimUTF8( rawContent );
								}
								catch( EOFException e ) {
									return false; // TODO: a bit raw
								}
								nonWord.length( 0 );
								
								if ( exact ) {
									try {
										nonWord.readSelfDelimUTF8( rawContent );
									}
									catch( EOFException e ) {
										return true; // TODO: a bit raw
									}
								}
								else nonWord.append( ' ' );

								final boolean goOn = word.length() != 0 || ( exact && nonWord.length() != 0 );
								if ( DEBUG ) LOGGER.debug( "Got word <" + word + "|" + nonWord + "> exact=" + exact + " returning " + goOn );
								if ( ! goOn ) nextFieldToRead++;
								return goOn;
							}
							public WordReader setReader( final Reader reader ) {
								return this;
							}
							public WordReader copy() {
								throw new UnsupportedOperationException();
							}
						};
					case VIRTUAL:
						return new FastBufferedReader();
					default:
						return null;
					}

				}
			};
		}
	}
	
	private void initZipFile() {
		try {
			zipFile = new ZipFile( zipFilename );
		}
		// We leave the possibility for a filename() to fix the problem and load the right zipfile.
		catch( Exception e ) {}
	}
	
	private void ensureZipFile() {
		if ( zipFile == null ) throw new IllegalStateException( "The .zip file used by this " + ZipDocumentCollection.class.getSimpleName() + " has not been loaded correctly; please use " + AbstractDocumentSequence.class.getName() + ".load() or call filename() after deserialising this instance" );
	}
	
	/** Constructs a document collection (for reading) corresponding to a given zip collection file.
	 * 
	 * @param zipFilename the filename of the zip collection.
	 * @param underlyingFactory the underlying document factory.
	 * @param numberOfDocuments2 the number of documents.
	 * @param exact <code>true</code> iff this is an exact reproduction of the original sequence.
	 */
	public ZipDocumentCollection( final String zipFilename, final DocumentFactory underlyingFactory, final long numberOfDocuments2, final boolean exact )  {
		this.zipFilename = zipFilename;
		this.underlyingFactory = underlyingFactory;
		this.numberOfDocuments = numberOfDocuments2;
		this.exact = exact;
		// Creates the factory
		factory = new ZipFactory( underlyingFactory, exact );
		initZipFile();
	}

	@Override
	public void filename( CharSequence filename ) throws IOException {
		/* If we don't have a zipFile, we try to get it relatively to the basename.
		 * We also store the resulting filename, so copy() should work. */
		if ( zipFile == null ) {
			zipFilename = new File( new File( filename.toString() ).getParentFile(), zipFilename ).toString();
			zipFile = new ZipFile( zipFilename );
		}
	}

	private void readObject( final ObjectInputStream s ) throws IOException, ClassNotFoundException {
		s.defaultReadObject();
		initZipFile();
	}
		
	public ZipDocumentCollection copy() {
		return new ZipDocumentCollection( zipFilename, underlyingFactory, numberOfDocuments, exact );
	}
	
	public DocumentFactory factory() {
		return factory;
	}
	
	public long size() {
		return numberOfDocuments;
	}

	private ZipEntry getEntry( final int index ) {
		ensureDocumentIndex( index );
		ensureZipFile();
		final ZipEntry entry = zipFile.getEntry( Integer.toString( index ) );
		if ( entry == null ) throw new NoSuchElementException( "Failure retrieving entry " + index );
		return entry;
	}
	
	public Document document( final long index ) throws IOException {
		ensureDocumentIndex( index );
		final ZipEntry entry = getEntry( (int)index );
		final Reference2ObjectMap<Enum<?>,Object> metadata = metadata( index, entry );
		InputStream is = zipFile.getInputStream( entry );
		return factory.getDocument( is, metadata );
	}
	

	private Reference2ObjectMap<Enum<?>,Object> metadata( final long index, ZipEntry entry ) {
		ensureDocumentIndex( index );
		if ( entry == null ) entry = getEntry( (int)index );
		final Reference2ObjectArrayMap<Enum<?>,Object> metadata = new Reference2ObjectArrayMap<Enum<?>,Object>( 1 );
		metadata.put( MetadataKeys.TITLE, entry.getComment() );
		return metadata;
	}
	
	public Reference2ObjectMap<Enum<?>,Object> metadata( final long index ) {
		return metadata( index, null );
	}
	
	public InputStream stream( final long index ) throws IOException {
		ensureDocumentIndex( index );
		final ZipEntry entry = getEntry ( (int)index );
		entry.getComment(); // Just skip title
		InputStream is = zipFile.getInputStream( entry );
		return is;
	}
	
	public DocumentIterator iterator() {
			try {
				return new AbstractDocumentIterator() {
					final Reference2ObjectArrayMap<Enum<?>,Object> metadata = new Reference2ObjectArrayMap<Enum<?>,Object>( new Enum[ 1 ], new Object[ 1 ] );

					ZipInputStream zis = new ZipInputStream( new FileInputStream( zipFile.getName() ) ); 
					public Document nextDocument() throws IOException {
						ZipEntry entry;
						String name;
						do {
							entry = zis.getNextEntry();
							if ( entry == null ) return null;
							name = entry.getName();
						} while ( !Character.isDigit( name.charAt( 0 ) ) );  
						String title = entry.getComment();
						if ( DEBUG ) LOGGER.debug( "Reading sequentially document " + title + ", name: " + entry.getName() );
						InputStream is = zipFile.getInputStream( entry );
						metadata.put( MetadataKeys.TITLE, title );
						return factory.getDocument( is, metadata );
					}
				};
			} catch ( FileNotFoundException e ) {
				throw new RuntimeException( e );
			}
	}
	
	public void close() throws IOException {
		super.close();
		if ( zipFile != null ) zipFile.close();
	}
}
