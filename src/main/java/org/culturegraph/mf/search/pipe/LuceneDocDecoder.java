package org.culturegraph.mf.search.pipe;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.culturegraph.mf.formeta.FormetaDecoder;
import org.apache.lucene.index.IndexableField;
import org.culturegraph.mf.framework.StreamReceiver;
import org.culturegraph.mf.framework.helpers.DefaultObjectPipe;
import org.culturegraph.mf.framework.helpers.DefaultStreamPipe;
import org.culturegraph.mf.search.IndexConstants;

/**
 * Writes {@link Document} to a {@link StreamReceiver}.
 *
 * @author Markus Michael Geipel
 *
 */
public final class LuceneDocDecoder
		extends DefaultObjectPipe<Document,StreamReceiver> {

	@Override
	public void process(final Document doc){
		read(doc, getReceiver());
	}

	public static <R extends StreamReceiver> R read(final Document doc,
			final R receiver) {
		receiver.startRecord(doc.get(IndexConstants.ID_NAME));
		final FormetaDecoder formetaDecoder = new FormetaDecoder();
		final RemoveRecordBoundery removeRecordBoundery = new RemoveRecordBoundery();
		formetaDecoder
				.setReceiver(removeRecordBoundery)
				.setReceiver(receiver);
		for (IndexableField field : doc.getFields()) {
			final String name = field.name();
			if(!name.startsWith("_")){
				receiver.literal(name, field.stringValue());
			}else if(IndexConstants.SERIALIZED.equals(name)){
				formetaDecoder.process(field.stringValue());
			}
		}
		receiver.endRecord();
		return receiver;
	}

	protected static final class RemoveRecordBoundery
			extends DefaultStreamPipe<StreamReceiver> {

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
