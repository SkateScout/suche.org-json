package org.suche.json;

import static java.lang.classfile.ClassFile.ACC_FINAL;
import static java.lang.classfile.ClassFile.ACC_PUBLIC;

import java.lang.classfile.ClassFile;
import java.lang.classfile.CodeBuilder;
import java.lang.constant.ClassDesc;
import java.lang.constant.MethodTypeDesc;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodHandles.Lookup;
import java.lang.reflect.Constructor;
import java.lang.reflect.RecordComponent;
import java.util.List;

public class ConstructorGenerator {
	public interface ObjectArrayFactory { Object create(Object[] objects, long[] primitives); }

	private static final ClassDesc      CD_OBJECT       = ClassDesc.ofDescriptor(Object .class.descriptorString());
	private static final ClassDesc      CD_OBJECT_ARRAY = ClassDesc.ofDescriptor(Object[].class.descriptorString());
	private static final ClassDesc      CD_LONG_ARRAY   = ClassDesc.ofDescriptor(long  [].class.descriptorString());
	private static final ClassDesc      CD_DOUBLE       = ClassDesc.ofDescriptor(Double .class.descriptorString());
	private static final ClassDesc      CD_FLOAT        = ClassDesc.ofDescriptor(Float  .class.descriptorString());
	private static final MethodTypeDesc MT_CREATE       = MethodTypeDesc.of(CD_OBJECT, CD_OBJECT_ARRAY, CD_LONG_ARRAY);
	private static final MethodTypeDesc MT_INIT         = MethodTypeDesc.of(ClassDesc.ofDescriptor("V"));
	private static final MethodTypeDesc MT_DJ           = MethodTypeDesc.of(ClassDesc.ofDescriptor("D"), ClassDesc.ofDescriptor("J"));
	private static final MethodTypeDesc MT_FI           = MethodTypeDesc.of(ClassDesc.ofDescriptor("F"), ClassDesc.ofDescriptor("I"));
	private static final ClassDesc      IF_NAME         = ClassDesc.ofDescriptor(ObjectArrayFactory.class.descriptorString());
	private static final Lookup         LOOKUP          = MethodHandles.lookup();

	public static ObjectArrayFactory generate(final Class<?> cls, final Class<?>[] args) throws Exception {
		final var className  = cls.getName() + "$$InternalFactory";
		final var classDesc  = ClassDesc.ofDescriptor("L" + className.replace('.', '/') + ";");
		final var recordDesc = ClassDesc.ofDescriptor(cls.descriptorString());
		final var bytes      = ClassFile.of().build(classDesc, classBuilder -> {
			classBuilder.withFlags(ACC_PUBLIC | ACC_FINAL);
			classBuilder.withInterfaceSymbols(IF_NAME);
			classBuilder.withMethodBody("<init>", MT_INIT, ACC_PUBLIC, codeBuilder -> {
				codeBuilder.aload(0);
				codeBuilder.invokespecial(CD_OBJECT, "<init>", MT_INIT);
				codeBuilder.return_();
			});
			classBuilder.withMethodBody("create", MT_CREATE, ACC_PUBLIC, codeBuilder -> {
				codeBuilder.new_(recordDesc);
				codeBuilder.dup();
				for (var i = 0; i < args.length; i++) {
					final var type = args[i];
					if (type.isPrimitive()) {
						codeBuilder.aload(2);
						pushArgument(codeBuilder, i);
						codeBuilder.laload();
						emitPrimitiveCast(codeBuilder, type);
					} else {
						codeBuilder.aload(1);
						pushArgument(codeBuilder, i);
						codeBuilder.aaload();
						codeBuilder.checkcast(ClassDesc.ofDescriptor(type.descriptorString()));
					}
				}
				final var constructorDesc = MethodTypeDesc.of(ClassDesc.ofDescriptor("V"), List.of(args).stream().map(c -> ClassDesc.ofDescriptor(c.descriptorString())).toList());
				codeBuilder.invokespecial(recordDesc, "<init>", constructorDesc);
				codeBuilder.areturn();
			});
		});
		final var definedClass = MethodHandles.privateLookupIn(cls, LOOKUP).defineHiddenClass(bytes, true, MethodHandles.Lookup.ClassOption.NESTMATE).lookupClass();
		return (ObjectArrayFactory) definedClass.getConstructor().newInstance();
	}

	public static ObjectArrayFactory generate(final Constructor<?> ctor) throws Exception {
		return generate(ctor.getDeclaringClass(), ctor.getParameterTypes());
	}

	public static ObjectArrayFactory generate(final Class<? extends Record> cls) throws Exception {
		return generate(cls, List.of(cls.getRecordComponents()).stream().map(RecordComponent::getType).toList().toArray(new Class[0]));
	}

	private static void pushArgument(final CodeBuilder cb, final int value) {
		if (value >= -1 && value <= 5)
			switch (value) {
			case -1 -> cb.iconst_m1();
			case 0  -> cb.iconst_0();
			case 1  -> cb.iconst_1();
			case 2  -> cb.iconst_2();
			case 3  -> cb.iconst_3();
			case 4  -> cb.iconst_4();
			case 5  -> cb.iconst_5();
			}
		else if (value >= Byte .MIN_VALUE && value <= Byte .MAX_VALUE) cb.bipush(value);
		else if (value >= Short.MIN_VALUE && value <= Short.MAX_VALUE) cb.sipush(value);
		else                                                           cb.ldc   (value);
	}

	private static void emitPrimitiveCast(final CodeBuilder cb, final Class<?> target) {
		if (target != long.class) switch (target.getName()) {
		case "int"     ->   cb.l2i();
		case "boolean" ->   cb.l2i();
		case "double"  ->             cb.invokestatic(CD_DOUBLE, "longBitsToDouble", MT_DJ);
		case "float"   -> { cb.l2i(); cb.invokestatic(CD_FLOAT , "intBitsToFloat"  , MT_FI); }
		case "short"   -> { cb.l2i(); cb.i2s(); }
		case "byte"    -> { cb.l2i(); cb.i2b(); }
		case "char"    -> { cb.l2i(); cb.i2c(); }
		default        -> throw new IllegalArgumentException("Unknown primitive: " + target);
		}
	}
}