package org.suche.json;

import static java.lang.classfile.ClassFile.ACC_FINAL;
import static java.lang.classfile.ClassFile.ACC_PRIVATE;
import static java.lang.classfile.ClassFile.ACC_PUBLIC;

import java.io.IOException;
import java.lang.classfile.ClassFile;
import java.lang.classfile.CodeBuilder;
import java.lang.classfile.Label;
import java.lang.classfile.instruction.SwitchCase;
import java.lang.constant.ClassDesc;
import java.lang.constant.MethodTypeDesc;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodHandles.Lookup;
import java.util.ArrayList;
import java.util.function.UnaryOperator;

public class ObjectSerializerFactory {

	public interface ObjectSerializer {
		// idx=0 for start, return next or -1 if done
		int serialize(JsonOutputStream s, Object o, int idx) throws IOException;
	}

	// Die Metadaten, die du beim Erstellen für jedes Feld reinreichst
	public record PropMeta(String name, Class<?> type, boolean isField, String memberName) {}

	private static final ClassDesc      CD_SERIALIZER    = ClassDesc.ofDescriptor(ObjectSerializer.class.descriptorString());
	private static final ClassDesc      CD_JSON_OUT      = ClassDesc.ofDescriptor(JsonOutputStream.class.descriptorString());
	private static final ClassDesc      CD_OBJECT        = ClassDesc.ofDescriptor(Object.class.descriptorString());
	private static final ClassDesc      CD_UNARY_OP      = ClassDesc.ofDescriptor(UnaryOperator.class.descriptorString());
	private static final ClassDesc      CD_BYTE_ARR_2D   = ClassDesc.ofDescriptor("[[B");
	private static final ClassDesc      CD_BYTE_ARR      = ClassDesc.ofDescriptor("[B");
	private static final ClassDesc      CD_UNARY_OP_ARR  = ClassDesc.ofDescriptor("[Ljava/util/function/UnaryOperator;");
	private static final ClassDesc      CD_VOID          = ClassDesc.ofDescriptor("V");
	private static final ClassDesc      CD_STRING        = ClassDesc.ofDescriptor(String.class.descriptorString());
	private static final MethodTypeDesc	MT_VI            = MethodTypeDesc.of(CD_VOID, ClassDesc.ofDescriptor("I"));
	private static final MethodTypeDesc	MT_VJ            = MethodTypeDesc.of(CD_VOID, ClassDesc.ofDescriptor("J"));
	private static final MethodTypeDesc MT_INIT          = MethodTypeDesc.of(CD_VOID, CD_BYTE_ARR_2D, CD_UNARY_OP_ARR);
	private static final MethodTypeDesc MT_SERIALIZE     = MethodTypeDesc.of(ClassDesc.ofDescriptor("I"), CD_JSON_OUT, CD_OBJECT, ClassDesc.ofDescriptor("I"));

	private static final Lookup LOOKUP = MethodHandles.lookup();

	public static ObjectSerializer build(final Class<?> cls, final PropMeta[] props, final byte[][] keys, final UnaryOperator<Object>[] transformers) throws Exception {
		final var className  = cls.getName() + "$$InternalSerializer";
		final var classDesc  = ClassDesc.ofDescriptor("L" + className.replace('.', '/') + ";");
		final var targetDesc = ClassDesc.ofDescriptor(cls.descriptorString());

		final var bytes = ClassFile.of().build(classDesc, classBuilder -> {
			classBuilder.withFlags(ACC_PUBLIC | ACC_FINAL);
			classBuilder.withInterfaceSymbols(CD_SERIALIZER);
			// Felder für Keys und Transformer anlegen
			classBuilder.withField("keys"        , CD_BYTE_ARR_2D , ACC_PRIVATE | ACC_FINAL);
			classBuilder.withField("transformers", CD_UNARY_OP_ARR, ACC_PRIVATE | ACC_FINAL);
			// Konstruktor: public <init>(byte[][] keys, UnaryOperator[] transformers)
			classBuilder.withMethodBody("<init>", MT_INIT, ACC_PUBLIC, cb -> {
				cb.aload(0);
				cb.invokespecial(CD_OBJECT, "<init>", MethodTypeDesc.ofDescriptor("()V"));
				cb.aload(0);
				cb.aload(1);
				cb.putfield(classDesc, "keys", CD_BYTE_ARR_2D);
				cb.aload(0);
				cb.aload(2);
				cb.putfield(classDesc, "transformers", CD_UNARY_OP_ARR);
				cb.return_();
			});

			// State Machine: public int serialize(JsonOutputStream s, Object o, int idx)
			classBuilder.withMethodBody("serialize", MT_SERIALIZE, ACC_PUBLIC, cb -> {
				final var numFields = props.length;

				// Cast des Objects auf den echten Typ -> Löst Virtual Dispatch ein für alle Mal auf!
				cb.aload(2);
				cb.checkcast(targetDesc);
				cb.astore(4); // Local 4 = targetObject

				// Labels und SwitchCases für den Tableswitch vorbereiten
				final var caseLabels  = new Label[numFields];
				final var switchCases = new ArrayList<SwitchCase>(numFields);
				// Binde den Case-Index an das entsprechende Label
				for (var i = 0; i < numFields; i++) switchCases.add(SwitchCase.of(i, caseLabels[i]=cb.newLabel()));

				final var defaultLabel = cb.newLabel();
				final var endLabel     = cb.newLabel();

				cb.iload(3);		// switch(idx)
				if (numFields > 0) cb.tableswitch(0, numFields - 1, defaultLabel, switchCases);
				else               cb.goto_(endLabel);

				for (var i = 0; i < numFields; i++) {		// Bytecode für jedes Feld weben
					cb.labelBinding(caseLabels[i]);
					final var prop = props[i];
					final var propDesc = ClassDesc.ofDescriptor(prop.type().descriptorString());
					cb.aload(1);		// out.writeCommaIfNeeded();
					cb.invokevirtual(CD_JSON_OUT, "writeCommaIfNeeded", MethodTypeDesc.ofDescriptor("()V"));
					cb.aload(1);		// out.write(this.keys[i]);
					cb.aload(0);
					cb.getfield(classDesc, "keys", CD_BYTE_ARR_2D);
					pushInt(cb, i);
					cb.aaload();
					cb.invokevirtual(CD_JSON_OUT, "write", MethodTypeDesc.of(CD_VOID, CD_BYTE_ARR));

					cb.aload(4);		// Wert auslesen: targetObj.getter() oder targetObj.field
					if (prop.isField()) cb.getfield     (targetDesc, prop.memberName(), propDesc);
					else                cb.invokevirtual(targetDesc, prop.memberName(), MethodTypeDesc.of(propDesc));

					// Wert in Stream schreiben
					if    (prop.type() == int.class || prop.type() == Integer.class) {
						if (!prop.type().isPrimitive()) { cb.invokevirtual(ClassDesc.ofDescriptor("Ljava/lang/Integer;"), "intValue", MethodTypeDesc.ofDescriptor("()I")); }
						cb.invokevirtual(CD_JSON_OUT, "writeNumber", MT_VI);
					}
					else if (prop.type() == long  .class  || prop.type() == Long.class) {
						if (!prop.type().isPrimitive()) { cb.invokevirtual(ClassDesc.ofDescriptor("Ljava/lang/Long;"), "longValue", MethodTypeDesc.ofDescriptor("()J")); }
						cb.invokevirtual(CD_JSON_OUT, "writeNumber", MT_VJ);
					}
					else if (prop.type() == double.class  || prop.type() == Double.class) {
						if (!prop.type().isPrimitive()) { cb.invokevirtual(ClassDesc.ofDescriptor("Ljava/lang/Double;"), "doubleValue", MethodTypeDesc.ofDescriptor("()D")); }
						cb.invokevirtual(CD_JSON_OUT, "writeDouble", MethodTypeDesc.of(CD_VOID, ClassDesc.ofDescriptor("D")));
					}
					else if (prop.type() == boolean.class || prop.type() == Boolean.class) {
						if (!prop.type().isPrimitive()) { cb.invokevirtual(ClassDesc.ofDescriptor("Ljava/lang/Boolean;"), "booleanValue", MethodTypeDesc.ofDescriptor("()Z")); }
						cb.invokevirtual(CD_JSON_OUT, "writeBoolean", MethodTypeDesc.of(ClassDesc.ofDescriptor("V"), ClassDesc.ofDescriptor("Z")));
					}
					else if (prop.type() == String.class) {
						cb.invokevirtual(CD_JSON_OUT, "writeEscapedString", MethodTypeDesc.of(ClassDesc.ofDescriptor("V"), CD_STRING));
					}
					else objectPropertyHanlder(cb, classDesc, numFields);
					// Am Ende eines Primitives oder Strings fällt der Code automatisch in das Label des nächsten Cases durch (Fallthrough).
				}

				cb.labelBinding(endLabel);
				cb.labelBinding(defaultLabel);
				cb.iconst_m1();
				cb.ireturn();
			});
		});

		final var definedClass = MethodHandles.privateLookupIn(cls, LOOKUP)
				.defineHiddenClass(bytes, true, MethodHandles.Lookup.ClassOption.NESTMATE)
				.lookupClass();

		return (ObjectSerializer) definedClass
				.getConstructor(byte[][].class, UnaryOperator[].class)
				.newInstance(keys, transformers);
	}

	static void objectPropertyHanlder(final CodeBuilder cb, final ClassDesc classDesc, final int i) {
		// COMPLEX OBJECT STATE MACHINE YIELD!
		cb.astore(5); // Speichere komplexes Objekt in Local 5
		// if (this.transformers[i] != null)
		cb.aload(0);
		cb.getfield(classDesc, "transformers", CD_UNARY_OP_ARR);
		pushInt(cb, i);
		cb.aaload();
		cb.dup(); // Stack: [transformer, transformer]
		final var isNullLabel       = cb.newLabel();
		final var endTransformLabel = cb.newLabel();
		cb.ifnull(isNullLabel); // Prüft und verbraucht 1x. Stack: [transformer]
		// --- NICHT NULL PFAD ---
		// val = transformers[i].apply(val)
		cb.aload(5); // Stack: [transformer, val]
		cb.invokeinterface(CD_UNARY_OP, "apply", MethodTypeDesc.of(CD_OBJECT, CD_OBJECT)); // Stack: [newVal]
		cb.astore(5); // Stack: []
		cb.goto_(endTransformLabel); // HIER IST DAS GOTO WICHTIG! Springt über pop() hinweg.
		cb.labelBinding(isNullLabel); // Stack: [null]
		cb.pop(); // Pop den nutzlosen null-Transformer vom Stack. Stack: []
		cb.labelBinding(endTransformLabel); // Stack ist in BEIDEN Pfaden jetzt exakt []
		cb.aload(1);		// if (!out.isSkipped(val))
		cb.aload(5);
		cb.invokevirtual(CD_JSON_OUT, "isSkipped", MethodTypeDesc.of(ClassDesc.ofDescriptor("Z"), CD_OBJECT));
		final var skipLabel = cb.newLabel();
		cb.ifne(skipLabel); // Wenn true (isSkipped), springe über das Yield hinweg
		cb.aload(1);	// out.queuedComplex = val;
		cb.aload(5);
		cb.putfield(CD_JSON_OUT, "queuedComplex", CD_OBJECT);
		pushInt(cb, i + 1);	// return i + 1; (YIELD!)
		cb.ireturn();
		cb.labelBinding(skipLabel); // Hier landet er, wenn skipped, und fällt ins nächste Case!

	}


	private static void pushInt(final CodeBuilder cb, final int value) {
		switch (value) {
		case -1 -> cb.iconst_m1();
		case 0  -> cb.iconst_0();
		case 1  -> cb.iconst_1();
		case 2  -> cb.iconst_2();
		case 3  -> cb.iconst_3();
		case 4  -> cb.iconst_4();
		case 5  -> cb.iconst_5();
		default -> {
			if      (value >= Byte.MIN_VALUE && value <= Byte.MAX_VALUE) cb.bipush(value);
			else if (value >= Short.MIN_VALUE && value <= Short.MAX_VALUE) cb.sipush(value);
			else    cb.ldc(value);
		}
		}
	}
}