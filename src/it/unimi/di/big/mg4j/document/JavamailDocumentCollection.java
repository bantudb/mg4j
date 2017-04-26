package it.unimi.di.big.mg4j.document;

/*		 
 * MG4J: Managing Gigabytes for Java (big)
 *
 * Copyright (C) 2005-2016 Paolo Boldi and Sebastiano Vigna 
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
import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import it.unimi.dsi.fastutil.objects.Reference2ObjectArrayMap;
import it.unimi.dsi.fastutil.objects.Reference2ObjectMap;
import it.unimi.dsi.io.FastBufferedReader;
import it.unimi.dsi.io.NullReader;
import it.unimi.dsi.io.WordReader;
import it.unimi.dsi.lang.MutableString;
import it.unimi.dsi.util.Properties;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.io.Serializable;
import java.io.StringReader;
import java.lang.reflect.InvocationTargetException;
import java.nio.charset.Charset;
import java.text.ParseException;
import java.util.Date;

import javax.mail.Address;
import javax.mail.Folder;
import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.Session;
import javax.mail.Store;
import javax.mail.URLName;
import javax.mail.internet.AddressException;
import javax.mail.internet.MailDateFormat;

import org.apache.commons.configuration.ConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.martiansoftware.jsap.FlaggedOption;
import com.martiansoftware.jsap.JSAP;
import com.martiansoftware.jsap.JSAPException;
import com.martiansoftware.jsap.JSAPResult;
import com.martiansoftware.jsap.Parameter;
import com.martiansoftware.jsap.SimpleJSAP;
import com.martiansoftware.jsap.UnflaggedOption;

/** A {@link it.unimi.di.big.mg4j.document.DocumentCollection} corresponding to
 *  a Javamail {@link javax.mail.Store}.
 *  
 *  <p>This class is very simple: for instance, it will not understand correctly
 *  multipart MIME messages, which will seen as without content. You are invited
 *  to extend it.
 *  
 *  <p>This implementation is an example of a document collection that does not use a
 *  factory: more precisely, there is an internal class that act as a wired factory. This
 *  structure is made necessary by the fact that Javamail provide no means to parse messages
 *  starting from an {@link java.io.InputStream}, which makes a separate implementation
 *  of {@link it.unimi.di.big.mg4j.document.DocumentFactory#getDocument(InputStream,Reference2ObjectMap)}
 *  impossible.
 *  
 *  <p>Note that to be able to use this class you must configure properly Javamail:
 *  this involves setting up a <samp>javamail.properties</samp> file describing the
 *  providers you want to use for the various access schemes. GNU Javamail, for instance, contains
 *  providers for files, IMAP, POP, etc. 
 */

public class JavamailDocumentCollection extends AbstractDocumentCollection implements Serializable {
	private final static Logger LOGGER = LoggerFactory.getLogger( JavamailDocumentCollection.class );

	/** A special date (actually, 1 January 1970) representing no date. */
	public final static Date NO_DATE = new Date( 0 );
	
	private static final long serialVersionUID = 2L;
	/** Our only session . */
	private final static Session SESSION = Session.getDefaultInstance( new java.util.Properties() );
	/** The number of messages. */
	private final int numberOfMessages;
	/** The factory to be used by this collection. */
	private final JavamailDocumentFactory factory;
	/** The URL for the store. */
	private final String storeUrl;
	/** The folder name. */
	private final String folderName;
	/** The javamail store we are reading. */
	private final transient Store store;
	/** The javamail folder we are reading. */
	private final transient Folder folder;
	
	/** Builds a document collection corresponding to a given store URL and folder name.
	 * 
	 *  <p><strong>Beware.</strong> This class is not suited for large mbox files!
	 * 
	 * @param storeUrl the javamail URL of the store.
	 * @param folderName the folder name.
	 * @param factory the factory that will be used to create documents.
	 * @throws MessagingException 
	 */
	protected JavamailDocumentCollection( final String storeUrl, final String folderName, final JavamailDocumentFactory factory ) throws MessagingException {
		this.storeUrl = storeUrl;
		this.folderName = folderName;
		this.factory = factory;

		this.store = SESSION.getStore( new URLName( storeUrl ) );
		store.connect();
		
		this.folder = store.getDefaultFolder().getFolder( folderName );
		folder.open( Folder.READ_ONLY );
		
		this.numberOfMessages = folder.getMessageCount();
	}

	public JavamailDocumentCollection( final String storeUrl, final String folderName ) throws MessagingException {
		this( storeUrl, folderName, new JavamailDocumentFactory() );
	}

	public JavamailDocumentCollection( final String storeUrl, final String folderName, final Properties properties ) throws MessagingException, ConfigurationException {
		this( storeUrl, folderName, new JavamailDocumentFactory( properties ) );
	}

	public JavamailDocumentCollection( final String storeUrl, final String folderName, final String[] property ) throws MessagingException, ConfigurationException {
		this( storeUrl, folderName, new JavamailDocumentFactory( property ) );
	}

	public JavamailDocumentCollection( final String storeUrl, final String folderName, final Reference2ObjectMap<Enum<?>,Object> defaultMetadata ) throws MessagingException {
		this( storeUrl, folderName, new JavamailDocumentFactory( defaultMetadata ) );
	}

	public JavamailDocumentCollection copy() {
		try {
			return new JavamailDocumentCollection( storeUrl, folderName, factory.copy() );
		}
		catch ( MessagingException e ) {
			throw new RuntimeException( e );
		}
	}	
	
	private final static class JavamailDocumentFactory extends PropertyBasedDocumentFactory {
		private static final long serialVersionUID = 1L;

		/** The field names (each also corresponds to a header, except for the 0-th). */
		private static final String[] FIELD_NAME = { "body", "subject", "from", "to", "date", "cc", "bcc", "content-type" };
		/** The field types. */
		private static final FieldType[] FIELD_TYPE = { FieldType.TEXT, FieldType.TEXT, FieldType.TEXT, FieldType.TEXT, FieldType.DATE, FieldType.TEXT, FieldType.TEXT, FieldType.TEXT };
		/** The map from field names to field indices. */
		private static final Object2IntOpenHashMap<String> FIELD2INDEX;

		static {
			FIELD2INDEX = new Object2IntOpenHashMap<String>( FIELD_NAME.length, .5f );
			FIELD2INDEX.defaultReturnValue( -1 );
			for( int i = 0; i < FIELD_NAME.length; i++ ) FIELD2INDEX.put( FIELD_NAME[ i ], i );
		}

		/** The word reader used for all documents. */
		private WordReader wordReader = new FastBufferedReader();

		protected boolean parseProperty( final String key, final String[] values, final Reference2ObjectMap<Enum<?>,Object> metadata ) throws ConfigurationException {
			if ( sameKey( MetadataKeys.ENCODING, key) ) {
				metadata.put( MetadataKeys.ENCODING, Charset.forName( ensureJustOne( key, values ) ).toString() );
				return true;
			}
			
			return super.parseProperty( key, values, metadata );
		}

		
		public JavamailDocumentFactory() {
			init();
		}
		
		public JavamailDocumentFactory( final Properties properties ) throws ConfigurationException {
			super( properties );
			init();
		}

		public JavamailDocumentFactory( final Reference2ObjectMap<Enum<?>,Object> defaultMetadata ) {
			super( defaultMetadata );
			init();
		}

		public JavamailDocumentFactory( final String[] property ) throws ConfigurationException {
			super( property );
			init();
		}
		
		private void init() {
			wordReader = new FastBufferedReader();
		}

		public JavamailDocumentFactory copy() {
			return new JavamailDocumentFactory( defaultMetadata );
		}
		
		public int numberOfFields() {
			return FIELD_NAME.length;
		}
		
		public String fieldName( final int field ) {
			ensureFieldIndex( field );
			return FIELD_NAME[ field ];
		}
		
		public FieldType fieldType( final int field ) {
			ensureFieldIndex( field );
			return FIELD_TYPE[ field ];
		}
		
		public int fieldIndex( final String fieldName ) {
			return FIELD2INDEX.getInt( fieldName );
		}
		
		public Document getDocument( final InputStream rawContent, final Reference2ObjectMap<Enum<?>,Object> metadata )  {
			throw new UnsupportedOperationException();	
		}
	}
	
	
	public DocumentFactory factory() {
		return factory;
	}

	
	public long size() {
		return numberOfMessages;
	}

	public void close() throws IOException {
		super.close();
		try {
			folder.close( false );
			store.close();
		}
		catch( MessagingException e ) {
			throw new IOException( e.toString() );
		}
	}
	
	private Object readResolve() throws MessagingException, IOException {
		super.close(); // To avoid spurious warnings about unclosed collected objects.
		return new JavamailDocumentCollection( storeUrl, folderName, factory );
	}
	
	public Document document( final long index ) throws IOException {
			try {
				return new AbstractDocument() {
					// Can you believe that? Javamail numbers messages from 1...
					final Message message = folder.getMessage( (int)( index + 1 ) );

					public CharSequence title() {
						final String subject; 
						try {
							subject = message.getSubject();
						}
						catch ( MessagingException e ) {
							throw new RuntimeException( e.toString() );
						}
						if ( subject == null ) return (CharSequence)factory.resolve( MetadataKeys.TITLE, factory.defaultMetadata );
						else return subject; 
					}
					
					public CharSequence uri() {
						try {
							return folder.getURLName() + "#" + message.getMessageNumber();
						}
						catch ( MessagingException e ) {
							throw new RuntimeException( e );
						} 
					}

					private Reader joinAddresses( final Address address[] ) {
						if ( address == null ) return NullReader.getInstance();
						final MutableString s = new MutableString();
						if ( address != null ) {
							for( int i = 0; i < address.length; i++ ) {
								if ( i > 0 ) s.append( ", " );
								s.append( address[ i ] );
							}
						}
						return new FastBufferedReader( s );
					}
					
					public Object content( final int field ) throws IOException {
						factory.ensureFieldIndex( field );
						try {
							switch ( field ) {
							case 0: // body
								// TODO: analyze multipart messages
								Object content = null;
								try {
									content = message.getContent();
								}
								catch( Exception e ) {
									LOGGER.warn( "Message " + message.getMessageNumber() + " cannot be decoded; content will be empty", e );
								}
								
								if ( content != null && content instanceof String ) return new StringReader( (String)content );
								
								return NullReader.getInstance();
							case 1: // subject
								return message.getSubject() == null ? NullReader.getInstance() : new StringReader( message.getSubject() );
							case 2: // from
								return joinAddresses( message.getFrom() );
							case 3: // to 
								return joinAddresses( message.getRecipients( Message.RecipientType.TO ) );
							case 4: // date
								final String[] date = message.getHeader( "date" );
								if ( date == null || date.length == 0 ) return NO_DATE;
								final MailDateFormat mailDateFormat = new MailDateFormat();
								try {
									return mailDateFormat.parse( date[ 0 ] );
								}
								catch ( ParseException e ) {
									LOGGER.warn( "Error parsing date " + date[ 0 ] );
									return NO_DATE;
								}

							case 5: // cc
								return joinAddresses( message.getRecipients( Message.RecipientType.CC ) );
							case 6: // bcc
								return joinAddresses( message.getRecipients( Message.RecipientType.BCC ) );
							case 7: // content-type
								return new StringReader( message.getContentType() );
							}
						}
						catch ( MessagingException e ) {
							// A simple error
							if ( e instanceof AddressException ) {
								LOGGER.warn( "Error while parsing address", e );
								return NullReader.getInstance();
							}
							throw new IOException( e.toString() );
						}
						throw new IllegalStateException();
					}

					public WordReader wordReader( final int field ) {
						factory.ensureFieldIndex( field );
						return factory.wordReader; 
					}
				};
			}
			catch ( MessagingException e ) {
				throw new IOException( e.toString() );
			}
		}

	
	public Reference2ObjectMap<Enum<?>,Object> metadata( final long index ) {
		ensureDocumentIndex( index );
		final Reference2ObjectArrayMap<Enum<?>,Object> metadata = new Reference2ObjectArrayMap<Enum<?>,Object>( 2 );
		metadata.put( MetadataKeys.TITLE, "Message #" + index );
		metadata.put( MetadataKeys.URI, storeUrl + folder + "#" + index );
		return metadata;
	}

	
	public InputStream stream( final long index ) throws IOException {
		ensureDocumentIndex( index );
		try {
			// Can you believe that? Javamail numbers messages from 1...
			return folder.getMessage( (int)( index + 1 ) ).getInputStream();
		}
		catch ( MessagingException e ) {
			throw new IOException( e.toString() );
		}
	}
	
	public static void main( final String[] arg ) throws IOException, JSAPException, IllegalAccessException, InvocationTargetException, NoSuchMethodException, InstantiationException, MessagingException, ConfigurationException {

		SimpleJSAP jsap = new SimpleJSAP( JavamailDocumentCollection.class.getName(), "Saves a serialised mbox collection based on a given mbox file.",
				new Parameter[] {
					new FlaggedOption( "property", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'p', "property", "A 'key=value' specification, or the name of a property file" ).setAllowMultipleDeclarations( true ),
					new UnflaggedOption( "collection", JSAP.STRING_PARSER, JSAP.REQUIRED, "The filename for the serialised collection." ),
					new UnflaggedOption( "storeUrl", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, JSAP.NOT_GREEDY, "The javamail store." ),
					new UnflaggedOption( "folder", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, JSAP.NOT_GREEDY, "The folder to be read." )
				}
		);
		
		JSAPResult jsapResult = jsap.parse( arg );
		if ( jsap.messagePrinted() ) return;
		
		BinIO.storeObject( new JavamailDocumentCollection( jsapResult.getString( "storeUrl" ), jsapResult.getString( "folder" ), jsapResult.getStringArray( "property" ) ), jsapResult.getString( "collection" ) );
	}
}
