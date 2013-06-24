package org.culturegraph.mf.search.pipe;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Fieldable;
import org.culturegraph.mf.framework.DefaultObjectPipe;
import org.culturegraph.mf.framework.DefaultStreamPipe;
import org.culturegraph.mf.framework.StreamReceiver;
import org.culturegraph.mf.search.IndexConstants;
import org.culturegraph.mf.stream.converter.CGEntityDecoder;



/**
 * Writes {@link Document} to a {@link StreamReceiver}. 
 * 
 * @author Markus Michael Geipel
 *
 */
public final class LuceneDocDecoder extends DefaultObjectPipe<Document,StreamReceiver> {
	
	
	@Override
	public void process(final Document doc){
		read(doc, getReceiver());
	}

	public static <R extends StreamReceiver> R read(final Document doc, final R receiver) {
		receiver.startRecord(doc.get(IndexConstants.ID_NAME));
		final RemoveRecordBoundery removeRecordBoundery = new RemoveRecordBoundery();
		removeRecordBoundery.setReceiver(receiver);
		for (Fieldable field : doc.getFields()) {
			
			final Field field2 = (Field) field;
			final String name = field2.name();
			if(!name.startsWith("_")){
				receiver.literal(name, field2.stringValue());
			}else if(IndexConstants.SERIALIZED.equals(name)){
				CGEntityDecoder.process(field2.stringValue(), removeRecordBoundery);
			}
			
		}
		receiver.endRecord();
		return receiver;
	}
	
	protected static final class RemoveRecordBoundery extends DefaultStreamPipe<StreamReceiver>{
		
		@Override
		public void startEntity(final String name) {
			getReceiver().startEntity(name);
		}
		
		@Override
		public void endEntity() {
			getReceiver().endEntity();
		}
		
		@Override
		public void literal(final String name, final String value) {
			getReceiver().literal(name, value);
		}
	}
	
}
