/*
 *  Copyright 2013, 2014, 2017 Deutsche Nationalbibliothek
 *
 *  Licensed under the Apache License, Version 2.0 the "License";
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.culturegraph.mf.search.pipe;

import java.util.regex.Pattern;

import org.culturegraph.mf.framework.FormatException;
import org.culturegraph.mf.framework.StreamReceiver;
import org.culturegraph.mf.framework.helpers.DefaultObjectPipe;

/**
 * Reads Strings CGEntity format.
 *
 * @author Markus Michael Geipel, Christoph Böhme
 *
 */
final class CGEntityDecoder
        extends DefaultObjectPipe<String, StreamReceiver> {

    static final char NEWLINE_ESC = '\u001d';
    static final char NEWLINE = '\n';
    static final char LITERAL_MARKER = '-';
    static final char ENTITY_START_MARKER = '<';
    static final char ENTITY_END_MARKER = '>';
    static final char FIELD_DELIMITER = '\u001e';
    static final char SUB_DELIMITER = '\u001f';

    private static final Pattern FIELD_PATTERN = Pattern.compile(
            String.valueOf(FIELD_DELIMITER), Pattern.LITERAL);
    private static final Pattern SUBFIELD_PATTERN = Pattern.compile(
            String.valueOf(SUB_DELIMITER), Pattern.LITERAL);

    @Override
    public void process(final String record) {
        process(record, getReceiver());
    }

    private static void process(final String record, final StreamReceiver receiver) {
        try {
            final String[] fields = FIELD_PATTERN.split(record);
            receiver.startRecord(fields[0]);
            for (int i = 1; i < fields.length; ++i) {
                final char firstChar = fields[i].charAt(0);
                if (firstChar == LITERAL_MARKER) {
                    final String[] parts = SUBFIELD_PATTERN
                            .split(fields[i], -1);
                    receiver.literal(
                            parts[0].substring(1),
                            parts[1].replace(NEWLINE_ESC,
                                    NEWLINE));
                } else if (firstChar == ENTITY_START_MARKER) {
                    receiver.startEntity(fields[i].substring(1));
                } else if (firstChar == ENTITY_END_MARKER) {
                    receiver.endEntity();
                } else if (firstChar == NEWLINE) {
                    // Ignore
                } else {
                    throw new FormatException(record);
                }
            }
            receiver.endRecord();
        } catch (IndexOutOfBoundsException exception) {
            throw new FormatException(record, exception);
        }
    }

}
