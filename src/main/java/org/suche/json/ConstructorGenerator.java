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
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.RecordComponent;
import java.util.List;

public class ConstructorGenerator {
	private static final String CONSTRUCTOR_NAME = "<init>";
	public interface ObjectArrayFactory { Object create(Object[] objects, long[] primitives); }

	public record PropDef(String name, Class<?> type, boolean isField) {}

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

	private static void addFactoryArgs(final CodeBuilder codeBuilder,final Class<?>[] factoryArgs) {
		for (var i = 0; i < factoryArgs.length; i++) {
			final var type = factoryArgs[i];
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
	}

	public static ObjectArrayFactory generate(final Class<?> cls, final String methodName, final Class<?>[] factoryArgs, final PropDef[] setters) throws IllegalAccessException, InstantiationException, IllegalArgumentException, InvocationTargetException, NoSuchMethodException  {
		final var className  = cls.getName() + "$$InternalFactory";
		final var classDesc  = ClassDesc.ofDescriptor("L" + className.replace('.', '/') + ";");
		final var recordDesc = ClassDesc.ofDescriptor(cls.descriptorString());
		final var bytes      = ClassFile.of().build(classDesc, classBuilder -> {
			classBuilder.withFlags(ACC_PUBLIC | ACC_FINAL);
			classBuilder.withInterfaceSymbols(IF_NAME);
			classBuilder.withMethodBody(CONSTRUCTOR_NAME, MT_INIT, ACC_PUBLIC, codeBuilder -> {
				codeBuilder.aload(0);
				codeBuilder.invokespecial(CD_OBJECT, CONSTRUCTOR_NAME, MT_INIT);
				codeBuilder.return_();
			});
			classBuilder.withMethodBody("create", MT_CREATE, ACC_PUBLIC, codeBuilder -> {
				final var isConstructor = CONSTRUCTOR_NAME.equals(methodName);

				if (isConstructor) {
					codeBuilder.new_(recordDesc);
					codeBuilder.dup();
				}
				addFactoryArgs(codeBuilder, factoryArgs);

				final var argDescs = List.of(factoryArgs).stream().map(c -> ClassDesc.ofDescriptor(c.descriptorString())).toList();

				if (isConstructor) {
					final var constructorDesc = MethodTypeDesc.of(ClassDesc.ofDescriptor("V"), argDescs);
					codeBuilder.invokespecial(recordDesc, CONSTRUCTOR_NAME, constructorDesc);
				} else {
					final var factoryDesc = MethodTypeDesc.of(recordDesc, argDescs);
					codeBuilder.invokestatic(recordDesc, methodName, factoryDesc);
				}

				if (setters != null)
					for (var i = 0; i < setters.length; i++)
						callSetter(codeBuilder, recordDesc, factoryArgs.length + i, setters[i]);


				codeBuilder.areturn();
			});
		});
		final var definedClass = MethodHandles.privateLookupIn(cls, LOOKUP).defineHiddenClass(bytes, true, MethodHandles.Lookup.ClassOption.NESTMATE).lookupClass();
		return (ObjectArrayFactory) definedClass.getConstructor().newInstance();
	}

	private static void callSetter(final CodeBuilder codeBuilder, final ClassDesc recordDesc, final int idx, final PropDef prop) {
		final var type = prop.type();
		final var typeDesc = ClassDesc.ofDescriptor(type.descriptorString());
		codeBuilder.dup();
		if (type.isPrimitive()) {
			codeBuilder.aload(2);
			pushArgument(codeBuilder, idx);
			codeBuilder.laload();
			emitPrimitiveCast(codeBuilder, type);
			if (prop.isField()) {
				codeBuilder.putfield(recordDesc, prop.name(), typeDesc);
			} else {
				codeBuilder.invokevirtual(recordDesc, prop.name(), MethodTypeDesc.of(ClassDesc.ofDescriptor("V"), typeDesc));
			}
		} else {
			codeBuilder.aload(1);
			pushArgument(codeBuilder, idx);
			codeBuilder.aaload();
			codeBuilder.dup();

			final var skip = codeBuilder.newLabel();
			codeBuilder.ifnull(skip);

			codeBuilder.checkcast(typeDesc);
			if (prop.isField()) {
				codeBuilder.putfield(recordDesc, prop.name(), typeDesc);
			} else {
				codeBuilder.invokevirtual(recordDesc, prop.name(), MethodTypeDesc.of(ClassDesc.ofDescriptor("V"), typeDesc));
			}
			final var end = codeBuilder.newLabel();
			codeBuilder.goto_(end);

			codeBuilder.labelBinding(skip);
			codeBuilder.pop();
			codeBuilder.pop();

			codeBuilder.labelBinding(end);
		}
	}

	public static ObjectArrayFactory generate(final Class<?> cls, final Class<?>[] args) throws IllegalAccessException, InstantiationException, IllegalArgumentException, InvocationTargetException, NoSuchMethodException {
		return generate(cls, CONSTRUCTOR_NAME, args, null);
	}

	public static ObjectArrayFactory generate(final Constructor<?> ctor) throws IllegalAccessException, InstantiationException, IllegalArgumentException, InvocationTargetException, NoSuchMethodException {
		return generate(ctor.getDeclaringClass(), CONSTRUCTOR_NAME, ctor.getParameterTypes(), null);
	}

	public static ObjectArrayFactory generate(final Method factoryMethod) throws IllegalAccessException, InstantiationException, IllegalArgumentException, InvocationTargetException, NoSuchMethodException  {
		if (!Modifier.isStatic(factoryMethod.getModifiers())) throw new IllegalArgumentException("Factory method must be static");
		return generate(factoryMethod.getReturnType(), factoryMethod.getName(), factoryMethod.getParameterTypes(), null);
	}

	public static ObjectArrayFactory generate(final Class<? extends Record> cls) throws IllegalAccessException, InstantiationException, IllegalArgumentException, InvocationTargetException, NoSuchMethodException {
		return generate(cls, CONSTRUCTOR_NAME, List.of(cls.getRecordComponents()).stream().map(RecordComponent::getType).toList().toArray(new Class[0]), null);
	}

	private static void pushArgument(final CodeBuilder cb, final int value) {
		switch (value) {
		case -1 -> cb.iconst_m1();
		case 0  -> cb.iconst_0();
		case 1  -> cb.iconst_1();
		case 2  -> cb.iconst_2();
		case 3  -> cb.iconst_3();
		case 4  -> cb.iconst_4();
		case 5  -> cb.iconst_5();
		default -> {
			if      (value >= Byte .MIN_VALUE && value <= Byte .MAX_VALUE) cb.bipush(value);
			else if (value >= Short.MIN_VALUE && value <= Short.MAX_VALUE) cb.sipush(value);
			else                                                           cb.ldc   (value);
		}
		}
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