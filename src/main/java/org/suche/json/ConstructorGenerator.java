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
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

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

	private static final class BytecodeLoader extends ClassLoader {
		BytecodeLoader(final ClassLoader parent) { super(parent); }
		Class<?> def(final String name, final byte[] b) {
			synchronized (getClassLoadingLock(name)) {
				var c = findLoadedClass(name);
				if (c == null) c = defineClass(name, b, 0, b.length);
				return c;
			}
		}
		Class<?> get(final String name) {
			synchronized (getClassLoadingLock(name)) { return findLoadedClass(name); }
		}
	}

	private static final Map<ClassLoader,BytecodeLoader> BYTE_LOADERS = new ConcurrentHashMap<>();

	private static final Class<?> viaByteLoader(final ClassLoader cl, final String name, final byte[] bytes) {
		return BYTE_LOADERS.computeIfAbsent(cl, BytecodeLoader::new).def(name, bytes);
	}

	private static final Class<?> viaByteLoader(final ClassLoader cl, final String name) {
		return BYTE_LOADERS.computeIfAbsent(cl, BytecodeLoader::new).get(name);
	}

	public static ObjectArrayFactory generate(final Class<?> cls, final String methodName, final Class<?>[] factoryArgs, final PropDef[] setters) throws IllegalAccessException, InstantiationException, IllegalArgumentException, InvocationTargetException, NoSuchMethodException  {
		var isPublicTarget = false;
		try {
			var mods = cls.getModifiers();
			for (Class<?> c = cls.getEnclosingClass(); c != null; c = c.getEnclosingClass()) mods &= c.getModifiers();
			if (CONSTRUCTOR_NAME.equals(methodName)) mods &= cls.getDeclaredConstructor(factoryArgs).getModifiers();
			else                                     mods &= cls.getDeclaredMethod(methodName, factoryArgs).getModifiers();
			if (setters != null) {
				for (final var setter : setters) {
					if (setter.isField()) mods &= cls.getDeclaredField(setter.name()).getModifiers();
					else                  mods &= cls.getDeclaredMethod(setter.name(), setter.type()).getModifiers();
				}
			}
			isPublicTarget = Modifier.isPublic(mods);
		} catch (final NoSuchMethodException | NoSuchFieldException _) { }

		// 1. Exception-freier ClassLoader Visibility Check (Parent-Chain Traversal)
		var isVisibleToLookup = false;
		final var callerLoader = LOOKUP.lookupClass().getClassLoader();
		final var targetLoader = cls.getClassLoader();

		if (targetLoader == null || callerLoader == targetLoader) {
			// null = Bootstrap ClassLoader (z.B. java.lang.*), für alle sichtbar.
			// Identische Referenz = gleicher ClassLoader.
			isVisibleToLookup = true;
		} else if (callerLoader != null) {
			// Hierarchie nach oben wandern: Ist der Target-Loader ein Parent des Caller-Loaders?
			var p = callerLoader.getParent();
			while (p != null) {
				if (p == targetLoader) {
					isVisibleToLookup = true;
					break;
				}
				p = p.getParent();
			}
		}

		final var useJsonLookup = isPublicTarget && isVisibleToLookup;
		// 2. SICHEREN Klassennamen ohne '$' generieren (verhindert NoClassDefFoundError beim Linking)
		final var pkg = cls.getPackageName();
		// z.B. "HoymilesApi$IdPort"
		final var nameWithoutPkg = pkg.isEmpty() ? cls.getName() : cls.getName().substring(pkg.length() + 1);
		// '$' durch '_' ersetzen -> "HoymilesApi_IdPort_InternalFactory"
		final var safeName = nameWithoutPkg.replace('$', '_') + "_InternalFactory";
		final var className = useJsonLookup ? "org.suche.json." + safeName : pkg.isEmpty() ? safeName : pkg + "." + safeName;

		if(isPublicTarget && viaByteLoader(cls.getClassLoader(), className) instanceof final Class<?> definedClass) return (ObjectArrayFactory) definedClass.getConstructor().newInstance();
		final var classDesc  = ClassDesc.ofDescriptor("L" + className.replace('.', '/') + ";");
		final var recordDesc = ClassDesc.ofDescriptor(cls.descriptorString());
		final var bytes      = buildBytes(classDesc, methodName, recordDesc, factoryArgs, setters);
		try {
			// 2. JPMS Modul-Sicherheit: Dürfen wir überhaupt privateLookupIn aufrufen?
			final var targetModule = cls.getModule();
			final var callerModule = LOOKUP.lookupClass().getModule();
			final var canPrivateLookup = targetModule == callerModule || targetModule.isOpen(cls.getPackageName(), callerModule);

			MethodHandles.Lookup targetLookup = null;
			if (useJsonLookup) {
				targetLookup = LOOKUP;
			} else if (canPrivateLookup) {
				targetLookup = MethodHandles.privateLookupIn(cls, LOOKUP);
			}

			// 3. Exception-freier Pre-Check: Hat das Lookup das "FullPrivilege" für defineHiddenClass behalten?
			if (targetLookup != null && targetLookup.hasFullPrivilegeAccess()) {
				final var definedClass = targetLookup.defineHiddenClass(bytes, true, MethodHandles.Lookup.ClassOption.NESTMATE).lookupClass();
				return (ObjectArrayFactory) definedClass.getConstructor().newInstance();
			}

			// 4. Regulärer Fallback für ClassLoader- & Modul-Grenzen (z.B. Servlet-Container)
			if (isPublicTarget) return (ObjectArrayFactory) viaByteLoader(cls.getClassLoader(), className, bytes).getConstructor().newInstance();
			throw new IllegalAccessException("Keine Berechtigung zur Bytecode-Generierung für " + cls.getName() + " (Nicht public und JPMS-Modul blockiert)");
		} catch(final Exception e) {
			final var x = new IllegalAccessException("generate("+cls+" , "+methodName+" , factoryArgs , setters) => "+e.getMessage());
			x.setStackTrace(e.getStackTrace());
			throw x;
		}
	}

	private static byte[] buildBytes(final ClassDesc classDesc, final String methodName, final ClassDesc recordDesc, final Class<?>[] factoryArgs, final PropDef[] setters) {
		return ClassFile.of().build(classDesc, classBuilder -> {
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
				// Replaced Stream execution with direct array mappings for zero-allocation
				final var argDescs = new ClassDesc[factoryArgs.length];
				for (var i = 0; i < factoryArgs.length; i++) argDescs[i] = ClassDesc.ofDescriptor(factoryArgs[i].descriptorString());
				if (isConstructor) {
					final var constructorDesc = MethodTypeDesc.of(ClassDesc.ofDescriptor("V"), argDescs);
					codeBuilder.invokespecial(recordDesc, CONSTRUCTOR_NAME, constructorDesc);
				} else {
					final var factoryDesc = MethodTypeDesc.of(recordDesc, argDescs);
					codeBuilder.invokestatic(recordDesc, methodName, factoryDesc);
				}
				if (setters != null) for (var i = 0; i < setters.length; i++) callSetter(codeBuilder, recordDesc, factoryArgs.length + i, setters[i]);
				codeBuilder.areturn();
			});
		});

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
		// Replacing Stream traversal with a loop array population to evade internal allocations
		final var components = cls.getRecordComponents();
		final var args = new Class<?>[components.length];
		for (var i = 0; i < components.length; i++) args[i] = components[i].getType();
		return generate(cls, CONSTRUCTOR_NAME, args, null);
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