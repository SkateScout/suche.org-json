package org.suche.json;

import java.io.IOException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.Temporal;
import java.time.zone.ZoneRules;

class JsonDateTime {
	static final ZoneId    BERLIN = ZoneId.of("Europe/Berlin");
	static final ZoneRules RULES  = BERLIN.getRules();

	interface Format { void apply(JsonOutputStream s,Temporal t) throws IOException; }

	public enum TimeFormat {
		EPOCH_DAY((s,v)-> s.writeNumber((char)0, Instant.from(v).getEpochSecond()/(24*3600))),
		EPOCH_SEC((s,v)-> s.writeNumber((char)0, Instant.from(v).getEpochSecond())),
		EPOCH_MIN((s,v)-> s.writeNumber((char)0, Instant.from(v).getEpochSecond()/60)),
		EPOCH_MS ((s,v)-> s.writeNumber((char)0, Instant.from(v).toEpochMilli  ())),
		TXT      (JsonOutputStream::writeTimestampasText);
		Format format;
		TimeFormat(final Format pFormat) { this.format = pFormat; }
	}

	static void writeTimestamp(final JsonOutputStream s, final Temporal t, final TimeFormat tf) throws IOException {
		if (tf == TimeFormat.TXT) { s.writeTimestampasText(t); return; }
		if (t == null) { s.write(JsonOutputStream.NULL_BYTES); return; }
		long sec;
		int  nano;
		switch (t) {
		case final Instant   v -> { sec = v.getEpochSecond(); nano = v.getNano(); }
		case final LocalDate v -> {
			final var ldt    = v.atStartOfDay();
			final var offset = RULES.getOffset(ldt);
			sec  = ldt.toEpochSecond(offset);
			nano = 0;
		}
		case final LocalDateTime v -> {
			final var offset = RULES.getOffset(v);
			sec  = v.toEpochSecond(offset);
			nano = v.getNano();
		}
		case final ZonedDateTime v -> { sec = v.toEpochSecond(); nano = v.getNano(); }
		case final OffsetDateTime v -> { sec = v.toEpochSecond(); nano = v.getNano(); }
		default -> throw new IllegalStateException(t.getClass().getName());
		}
		final var outVal = switch (tf) {
		case EPOCH_DAY -> {
			if (t instanceof final LocalDate ld) yield ld.toEpochDay();
			final var offsetSec = switch(t) {
			case final Instant        i -> RULES.getOffset(i).getTotalSeconds();
			case final LocalDateTime  l -> RULES.getOffset(l).getTotalSeconds();
			case final ZonedDateTime  z -> z.getOffset().getTotalSeconds();
			case final OffsetDateTime o -> o.getOffset().getTotalSeconds();
			default                     -> RULES.getOffset(Instant.ofEpochSecond(sec)).getTotalSeconds();
			};
			yield Math.floorDiv(sec + offsetSec, 86400);
		}
		case EPOCH_SEC -> sec;
		case EPOCH_MIN -> sec / 60L;
		default        -> (sec * 1000) + (nano / 1_000_000);
		};
		s.writeNumber((char)0, outVal);
	}
}