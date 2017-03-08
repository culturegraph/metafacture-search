package org.culturegraph.mf.search.pipe;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.culturegraph.mf.formeta.FormetaDecoder;
import org.apache.lucene.index.IndexableField;
import org.culturegraph.mf.framework.ObjectReceiver;
import org.culturegraph.mf.framework.StreamReceiver;
import org.culturegraph.mf.framework.helpers.DefaultObjectPipe;
import org.culturegraph.mf.framework.helpers.DefaultStreamPipe;
import org.culturegraph.mf.mangling.StreamEventDiscarder;
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
		final ObjectReceiver<String> newDecoder = createFormetaDecoder(receiver);
		final ObjectReceiver<String> legacyDecoder = createCGEntityDecoder(receiver);
		for (IndexableField field : doc.getFields()) {
			final String name = field.name();
			if(!name.startsWith("_")){
				receiver.literal(name, field.stringValue());
			}else if(IndexConstants.SERIALIZED.equals(name)){
				final String value = field.stringValue();
				if (isProbablyCGEntity(value)) {
					legacyDecoder.process(value);
				} else {
					newDecoder.process(value);
				}
			}
		}
		receiver.endRecord();
		return receiver;
	}

	private static boolean isProbablyCGEntity(String value) {
		final String fieldDelimiter = String.valueOf(CGEntityDecoder.FIELD_DELIMITER);
		final String subfieldDelimiter = String.valueOf(CGEntityDecoder.SUB_DELIMITER);
		return value.contains(fieldDelimiter) && value.contains(subfieldDelimiter);
	}

	private static ObjectReceiver<String> createFormetaDecoder(final StreamReceiver receiver) {
		final FormetaDecoder formetaDecoder = new FormetaDecoder();
		final StreamEventDiscarder discarder = new StreamEventDiscarder();
		discarder.setDiscardRecordEvents(true);
		formetaDecoder
				.setReceiver(discarder)
				.setReceiver(receiver);
		return formetaDecoder;
	}

	private static ObjectReceiver<String> createCGEntityDecoder(final StreamReceiver receiver) {
		final CGEntityDecoder cgEntityDecoder = new CGEntityDecoder();
		final StreamEventDiscarder discarder = new StreamEventDiscarder();
		discarder.setDiscardRecordEvents(true);
		cgEntityDecoder
				.setReceiver(discarder)
				.setReceiver(receiver);
		return cgEntityDecoder;
	}

}
