(function attachDeepCloneCallableRegistryToGlobalObject(globalObject){
"use strict";

/*
  deep_clone_callable_registry_module_hardened.js

  Purpose:
    Provide a hardened deep-clone engine for ordinary data and for callable
    behavior reconstructed from a trusted registry.

  Core rule:
    State is cloneable data. Behavior is registered and reconstructed.

  Security posture:
    Raw functions are rejected by default. Registered callable factories are the
    only accepted executable reconstruction boundary.

  Module style:
    ECMAScript module, no third-party dependencies.
*/

/* Constant: canonical marker used for serialized callable descriptors. */
const CALLABLE_DESCRIPTOR_KIND = "RegisteredCallableDescriptor";

/* Constant: default version marker for descriptors created by this module. */
const CALLABLE_DESCRIPTOR_SCHEMA_VERSION = 1;

/* Constant: maximum array-index key according to ordinary JavaScript array semantics. */
const MAX_ARRAY_INDEX = 2 ** 32 - 2;

/* Constant: object tag lookup is centralized so helper code is testable and uniform. */
const objectToString = Object.prototype.toString;

/*
  DeepCloneError:
    Base error for clone failures caused by unsupported values or invalid clone policy.
*/
class DeepCloneError extends TypeError {
  /* Function: construct a clone-specific error. */
  constructor(message, options = {}) {
    /* Assignment: initialize the native TypeError portion of this custom error. */
    super(message);

    /* Assignment: expose a stable error name for callers and tests. */
    this.name = "DeepCloneError";

    /* Assignment: attach optional structured diagnostic details. */
    this.details = options.details ?? null;
  }
}

/*
  CallableRegistryError:
    Base error for callable registration and reconstruction failures.
*/
class CallableRegistryError extends Error {
  /* Function: construct a callable-registry-specific error. */
  constructor(message, options = {}) {
    /* Assignment: initialize the native Error portion of this custom error. */
    super(message);

    /* Assignment: expose a stable error name for callers and tests. */
    this.name = "CallableRegistryError";

    /* Assignment: attach optional structured diagnostic details. */
    this.details = options.details ?? null;
  }
}

/*
  assertNonEmptyString:
    Validate that a value is a non-empty string.
*/
function assertNonEmptyString(value, label) {
  /* Conditional: reject non-string values and empty identifiers. */
  if (typeof value !== "string" || value.length === 0) {
    throw new TypeError(`${label} must be a non-empty string.`);
  }
}

/*
  assertPlainOptionsObject:
    Validate an options value enough to protect public entry points from null misuse.
*/
function assertPlainOptionsObject(value, label) {
  /* Conditional: allow omitted options through undefined only at call sites that default it. */
  if (value === null || typeof value !== "object" || Array.isArray(value)) {
    throw new TypeError(`${label} must be an object.`);
  }
}

/*
  isObjectLike:
    Return true for objects and functions, because both can carry identity.
*/
function isObjectLike(value) {
  /* Assignment: capture the primitive type tag once for stable branching. */
  const valueType = typeof value;

  /* Return: identity-bearing values are objects or functions and not null. */
  return value !== null && (valueType === "object" || valueType === "function");
}

/*
  getObjectTag:
    Return the internal [[Class]] style tag, for example [object Date].
*/
function getObjectTag(value) {
  /* Return: delegate to Object.prototype.toString without trusting instance methods. */
  return objectToString.call(value);
}

/*
  isArrayIndexKey:
    Return true when a property key is a canonical array index string.
*/
function isArrayIndexKey(propertyKey) {
  /* Conditional: symbols and non-strings cannot be array-index keys. */
  if (typeof propertyKey !== "string") {
    return false;
  }

  /* Assignment: parse the candidate property key once. */
  const numericIndex = Number(propertyKey);

  /* Return: enforce canonical array-index constraints. */
  return (
    Number.isInteger(numericIndex) &&
    numericIndex >= 0 &&
    numericIndex <= MAX_ARRAY_INDEX &&
    String(numericIndex) === propertyKey
  );
}

/*
  isRegisteredCallableDescriptor:
    Identify descriptor objects accepted by the callable rehydration path.
*/
function isRegisteredCallableDescriptor(value) {
  /* Conditional: reject primitives, null, and functions before reading fields. */
  if (value === null || typeof value !== "object") {
    return false;
  }

  /* Return: require the kind marker and callable type id. */
  return (
    value.kind === CALLABLE_DESCRIPTOR_KIND &&
    typeof value.callableTypeId === "string" &&
    value.callableTypeId.length > 0
  );
}

/*
  defineHiddenProperty:
    Define a non-enumerable, non-configurable helper property when needed.
*/
function defineHiddenProperty(targetObject, propertyName, propertyValue) {
  /* Assignment: build the descriptor explicitly to avoid accidental enumerable metadata. */
  const descriptor = {
    value: propertyValue,
    writable: false,
    enumerable: false,
    configurable: false
  };

  /* Assignment: define the property on the target object. */
  Object.defineProperty(targetObject, propertyName, descriptor);

  /* Return: provide fluent access to the same target object. */
  return targetObject;
}

/*
  clonePropertyDescriptors:
    Copy property descriptors from source to target while cloning data values.
    Accessor functions are copied by reference because JavaScript does not expose
    a safe general clone operation for getter/setter functions.
*/
function clonePropertyDescriptors({ sourceObject, targetObject, cloneValue, seen, skipArrayIndexes = false }) {
  /* Assignment: read all own descriptors, including non-enumerable and symbol properties. */
  const descriptors = Object.getOwnPropertyDescriptors(sourceObject);

  /* Loop: visit every own property descriptor from the source object. */
  for (const propertyKey of Reflect.ownKeys(descriptors)) {
    /* Conditional: arrays already clone indexed entries in order, so skip duplicate index copying. */
    if (skipArrayIndexes && isArrayIndexKey(propertyKey)) {
      continue;
    }

    /* Assignment: select the descriptor currently being copied. */
    const descriptor = descriptors[propertyKey];

    /* Conditional: data descriptors have a concrete value that must be deep-cloned. */
    if ("value" in descriptor) {
      /* Assignment: replace the original value with the cloned value in the descriptor copy. */
      descriptor.value = cloneValue(descriptor.value, seen);
    }

    /* Assignment: define the cloned or preserved descriptor on the target object. */
    Object.defineProperty(targetObject, propertyKey, descriptor);
  }
}

/*
  createCallableDescriptor:
    Build a normalized descriptor for registered callable reconstruction.
*/
function createCallableDescriptor({ callableTypeId, constructorData = {}, internalData = {}, metadata = null }) {
  /* Conditional: ensure descriptor IDs cannot be omitted or malformed. */
  assertNonEmptyString(callableTypeId, "callableTypeId");

  /* Assignment: assemble the public descriptor with explicit schema fields. */
  const descriptor = {
    kind: CALLABLE_DESCRIPTOR_KIND,
    schemaVersion: CALLABLE_DESCRIPTOR_SCHEMA_VERSION,
    callableTypeId,
    constructorData,
    internalData
  };

  /* Conditional: include metadata only when the caller intentionally provided it. */
  if (metadata !== null && metadata !== undefined) {
    /* Assignment: attach caller metadata as data, not executable behavior. */
    descriptor.metadata = metadata;
  }

  /* Return: provide the normalized descriptor to the caller. */
  return descriptor;
}

/*
  CallableRegistry:
    Trusted map from callable type IDs to factories.
*/
class CallableRegistry {
  /* Function: construct an empty callable registry. */
  constructor(options = {}) {
    /* Conditional: reject malformed options early. */
    assertPlainOptionsObject(options, "CallableRegistry options");

    /* Assignment: store registered factories by stable callable type ID. */
    this.factoriesById = new Map();

    /* Assignment: store optional metadata by stable callable type ID. */
    this.metadataById = new Map();

    /* Assignment: record whether duplicate registration should fail. */
    this.rejectDuplicateRegistration = options.rejectDuplicateRegistration ?? true;
  }

  /* Function: register a callable factory. */
  register(callableTypeId, factory, metadata = null) {
    /* Conditional: validate the callable type id before using it as a map key. */
    assertNonEmptyString(callableTypeId, "callableTypeId");

    /* Conditional: validate that the registered behavior constructor is executable. */
    if (typeof factory !== "function") {
      throw new TypeError("factory must be a function.");
    }

    /* Conditional: prevent accidental overwrite unless registry policy allows it. */
    if (this.factoriesById.has(callableTypeId) && this.rejectDuplicateRegistration) {
      throw new CallableRegistryError(`Callable factory already registered: ${callableTypeId}`);
    }

    /* Assignment: store the factory under its stable callable type id. */
    this.factoriesById.set(callableTypeId, factory);

    /* Assignment: store optional registry metadata for diagnostics or tooling. */
    this.metadataById.set(callableTypeId, metadata);

    /* Return: provide this registry for fluent setup code. */
    return this;
  }

  /* Function: check whether a callable type id is registered. */
  has(callableTypeId) {
    /* Conditional: malformed ids are not present. */
    if (typeof callableTypeId !== "string" || callableTypeId.length === 0) {
      return false;
    }

    /* Return: delegate to the internal factory map. */
    return this.factoriesById.has(callableTypeId);
  }

  /* Function: list registered callable type ids. */
  listIds() {
    /* Return: expose a defensive array rather than the mutable internal map. */
    return Array.from(this.factoriesById.keys());
  }

  /* Function: retrieve registry metadata for one callable type id. */
  getMetadata(callableTypeId) {
    /* Conditional: validate the callable type id before map access. */
    assertNonEmptyString(callableTypeId, "callableTypeId");

    /* Return: return metadata or null when absent. */
    return this.metadataById.get(callableTypeId) ?? null;
  }

  /* Function: retrieve a factory or throw a registry-specific error. */
  getFactoryOrThrow(callableTypeId) {
    /* Conditional: validate the callable type id before map access. */
    assertNonEmptyString(callableTypeId, "callableTypeId");

    /* Assignment: retrieve the factory once for stable validation and return. */
    const factory = this.factoriesById.get(callableTypeId);

    /* Conditional: fail loudly when reconstruction cannot be trusted. */
    if (typeof factory !== "function") {
      throw new CallableRegistryError(`Unknown callable type id: ${callableTypeId}`);
    }

    /* Return: provide the registered factory to the caller. */
    return factory;
  }

  /* Function: create a runtime callable from registered behavior and data. */
  create(callableTypeId, constructorData = {}, internalData = {}, options = {}) {
    /* Conditional: validate the options wrapper. */
    assertPlainOptionsObject(options, "CallableRegistry.create options");

    /* Assignment: retrieve the trusted factory for this callable type. */
    const factory = this.getFactoryOrThrow(callableTypeId);

    /* Return: construct a registered callable through the public wrapper. */
    return new RegisteredCallable({
      registry: this,
      callableTypeId,
      constructorData,
      internalData,
      factory,
      cloneEngine: options.cloneEngine ?? null,
      metadata: options.metadata ?? null
    });
  }

  /* Function: create a runtime callable from a descriptor. */
  createFromDescriptor(descriptor, options = {}) {
    /* Conditional: validate the descriptor marker before using its fields. */
    if (!isRegisteredCallableDescriptor(descriptor)) {
      throw new TypeError("Callable descriptor must be a RegisteredCallableDescriptor object.");
    }

    /* Conditional: validate the options wrapper. */
    assertPlainOptionsObject(options, "CallableRegistry.createFromDescriptor options");

    /* Return: reconstruct the callable from descriptor data. */
    return this.create(
      descriptor.callableTypeId,
      descriptor.constructorData ?? {},
      descriptor.internalData ?? {},
      options
    );
  }
}

/*
  RegisteredCallable:
    Runtime wrapper around registered behavior and cloneable state.
*/
class RegisteredCallable {
  /* Function: construct a registered callable from a trusted registry and factory. */
  constructor({ registry, callableTypeId, constructorData = {}, internalData = {}, factory, cloneEngine = null, metadata = null }) {
    /* Conditional: ensure the registry is the expected trusted registry type. */
    if (!(registry instanceof CallableRegistry)) {
      throw new TypeError("registry must be a CallableRegistry.");
    }

    /* Conditional: validate the callable type id before storage. */
    assertNonEmptyString(callableTypeId, "callableTypeId");

    /* Conditional: validate the factory before invoking it. */
    if (typeof factory !== "function") {
      throw new TypeError("factory must be a function.");
    }

    /* Assignment: store the registry used for future reconstruction. */
    this.registry = registry;

    /* Assignment: store the stable behavior identifier. */
    this.callableTypeId = callableTypeId;

    /* Assignment: store optional tool/debug metadata as non-executable data. */
    this.metadata = metadata;

    /* Assignment: select the clone engine used to isolate constructor and internal data. */
    this.cloneEngine = cloneEngine ?? createDefaultDeepCloneEngine(registry);

    /* Assignment: clone constructor data so callers cannot mutate this callable from outside. */
    this.constructorData = this.cloneEngine.clone(constructorData);

    /* Assignment: clone internal data so callers cannot mutate this callable from outside. */
    this.internalData = this.cloneEngine.clone(internalData);

    /* Assignment: invoke the factory to create the executable runtime function. */
    this.execute = factory({
      constructorData: this.constructorData,
      internalData: this.internalData,
      callable: this
    });

    /* Conditional: factories must return an executable function. */
    if (typeof this.execute !== "function") {
      throw new TypeError(`Factory for ${callableTypeId} must return a function.`);
    }
  }

  /* Function: create an uninitialized callable shell for cycle-preserving clone hydration. */
  static createUninitializedShell({ registry, callableTypeId, cloneEngine, metadata = null }) {
    /* Conditional: ensure the registry is the expected trusted registry type. */
    if (!(registry instanceof CallableRegistry)) {
      throw new TypeError("registry must be a CallableRegistry.");
    }

    /* Conditional: validate the callable type id before storage. */
    assertNonEmptyString(callableTypeId, "callableTypeId");

    /* Assignment: create an object with the correct prototype without running the constructor. */
    const callable = Object.create(RegisteredCallable.prototype);

    /* Assignment: store the registry used for reconstruction. */
    callable.registry = registry;

    /* Assignment: store the stable behavior identifier. */
    callable.callableTypeId = callableTypeId;

    /* Assignment: store the clone engine used during hydration. */
    callable.cloneEngine = cloneEngine;

    /* Assignment: store optional metadata as data. */
    callable.metadata = metadata;

    /* Assignment: initialize constructor data to null until hydration completes. */
    callable.constructorData = null;

    /* Assignment: initialize internal data to null until hydration completes. */
    callable.internalData = null;

    /* Assignment: install a temporary throwing execute function to catch premature use. */
    callable.execute = function executeBeforeHydration() {
      throw new CallableRegistryError("RegisteredCallable used before clone hydration completed.");
    };

    /* Return: provide the partially initialized callable shell. */
    return callable;
  }

  /* Function: finish hydration of an uninitialized callable shell. */
  hydrateFromClonedData({ constructorData, internalData, factory }) {
    /* Conditional: validate the factory before invoking it. */
    if (typeof factory !== "function") {
      throw new TypeError("factory must be a function.");
    }

    /* Assignment: attach already-cloned constructor data. */
    this.constructorData = constructorData;

    /* Assignment: attach already-cloned internal data. */
    this.internalData = internalData;

    /* Assignment: reconstruct executable behavior from the trusted factory. */
    this.execute = factory({
      constructorData: this.constructorData,
      internalData: this.internalData,
      callable: this
    });

    /* Conditional: factories must return executable functions. */
    if (typeof this.execute !== "function") {
      throw new TypeError(`Factory for ${this.callableTypeId} must return a function.`);
    }

    /* Return: provide this callable after successful hydration. */
    return this;
  }

  /* Function: call the reconstructed executable behavior. */
  call(...args) {
    /* Return: delegate all arguments to the registered executable function. */
    return this.execute(...args);
  }

  /* Function: alias for call, useful when wrappers expect an execute method. */
  run(...args) {
    /* Return: delegate to call so behavior remains centralized. */
    return this.call(...args);
  }

  /* Function: clone this registered callable through its clone engine. */
  clone() {
    /* Return: delegate to the clone engine so cycle and type policy remain centralized. */
    return this.cloneEngine.clone(this);
  }

  /* Function: create a plain descriptor for serialization or later rehydration. */
  toDescriptor() {
    /* Return: build a descriptor with cloned data so callers cannot mutate this callable. */
    return createCallableDescriptor({
      callableTypeId: this.callableTypeId,
      constructorData: this.cloneEngine.clone(this.constructorData),
      internalData: this.cloneEngine.clone(this.internalData),
      metadata: this.metadata === null ? null : this.cloneEngine.clone(this.metadata)
    });
  }
}

/*
  CallableRef:
    Lightweight serializable wrapper around a callable descriptor.
*/
class CallableRef {
  /* Function: construct a descriptor wrapper. */
  constructor(descriptor) {
    /* Conditional: validate descriptor shape before wrapping. */
    if (!isRegisteredCallableDescriptor(descriptor)) {
      throw new TypeError("CallableRef requires a RegisteredCallableDescriptor object.");
    }

    /* Assignment: store the descriptor as data. */
    this.descriptor = descriptor;
  }

  /* Function: rehydrate the referenced callable with a registry. */
  rehydrate(registry, options = {}) {
    /* Conditional: validate registry type before reconstruction. */
    if (!(registry instanceof CallableRegistry)) {
      throw new TypeError("registry must be a CallableRegistry.");
    }

    /* Return: delegate descriptor reconstruction to the registry. */
    return registry.createFromDescriptor(this.descriptor, options);
  }
}

/*
  DeepCloneEngine:
    Policy-driven deep clone engine with callable reconstruction support.
*/
class DeepCloneEngine {
  /* Function: construct a clone engine with explicit policy options. */
  constructor(options = {}) {
    /* Conditional: reject malformed options early. */
    assertPlainOptionsObject(options, "DeepCloneEngine options");

    /* Assignment: store the registry used for callable descriptor rehydration. */
    this.callableRegistry = options.callableRegistry ?? null;

    /* Assignment: store whether raw functions should be rejected. */
    this.rejectUnregisteredFunctions = options.rejectUnregisteredFunctions ?? true;

    /* Assignment: store whether accessor descriptors are permitted. */
    this.allowAccessors = options.allowAccessors ?? true;

    /* Assignment: store whether cloned plain objects should preserve prototypes. */
    this.preservePrototypes = options.preservePrototypes ?? true;
  }

  /* Function: deep clone a value according to this engine's policy. */
  clone(value) {
    /* Assignment: create per-operation cycle tracking. */
    const seen = new WeakMap();

    /* Return: start recursive cloning with an empty path. */
    return this.#cloneValue(value, seen, "$");
  }

  /* Function: clone an array of values as a convenience wrapper. */
  cloneMany(values) {
    /* Conditional: enforce array input for predictable result ordering. */
    if (!Array.isArray(values)) {
      throw new TypeError("cloneMany expects an array.");
    }

    /* Return: clone the array and every reachable value. */
    return this.clone(values);
  }

  /* Function: rehydrate a callable descriptor directly. */
  rehydrateCallable(descriptor) {
    /* Conditional: callable descriptor rehydration requires a registry. */
    if (!this.callableRegistry) {
      throw new DeepCloneError("Cannot rehydrate callable descriptor without a callable registry.");
    }

    /* Return: delegate to the trusted callable registry. */
    return this.callableRegistry.createFromDescriptor(descriptor, { cloneEngine: this });
  }

  /* Function: recursively clone one value. */
  #cloneValue(value, seen, path) {
    /* Conditional: null is a primitive-like terminal value. */
    if (value === null) {
      return null;
    }

    /* Assignment: capture the value type once for stable branching. */
    const valueType = typeof value;

    /* Conditional: non-object and non-function values are copied by value. */
    if (valueType !== "object" && valueType !== "function") {
      return value;
    }

    /* Conditional: functions are behavior, not state, unless wrapped. */
    if (valueType === "function") {
      return this.#cloneRawFunction(value, path);
    }

    /* Conditional: reuse already cloned values to preserve circular references. */
    if (seen.has(value)) {
      return seen.get(value);
    }

    /* Conditional: registered callable wrappers require behavior reconstruction. */
    if (value instanceof RegisteredCallable) {
      return this.#cloneRegisteredCallable(value, seen, path);
    }

    /* Conditional: descriptor objects can be rehydrated when a registry is attached. */
    if (isRegisteredCallableDescriptor(value)) {
      return this.#cloneRegisteredCallableDescriptor(value, seen, path);
    }

    /* Conditional: callable references are wrappers around descriptors. */
    if (value instanceof CallableRef) {
      return this.#cloneCallableRef(value, seen, path);
    }

    /* Assignment: inspect the stable object tag for built-in clone branches. */
    const valueTag = getObjectTag(value);

    /* Conditional: clone Date values by timestamp. */
    if (valueTag === "[object Date]") {
      return new Date(value.getTime());
    }

    /* Conditional: clone RegExp values by source, flags, and lastIndex. */
    if (valueTag === "[object RegExp]") {
      return this.#cloneRegExp(value);
    }

    /* Conditional: clone Map values while preserving key/value graph structure. */
    if (value instanceof Map) {
      return this.#cloneMap(value, seen, path);
    }

    /* Conditional: clone Set values while preserving graph structure. */
    if (value instanceof Set) {
      return this.#cloneSet(value, seen, path);
    }

    /* Conditional: clone arrays with index-specific handling. */
    if (Array.isArray(value)) {
      return this.#cloneArray(value, seen, path);
    }

    /* Conditional: clone ArrayBuffer values by byte copy. */
    if (value instanceof ArrayBuffer) {
      return value.slice(0);
    }

    /* Conditional: clone SharedArrayBuffer when the runtime supports slicing it. */
    if (typeof SharedArrayBuffer !== "undefined" && value instanceof SharedArrayBuffer) {
      return this.#cloneSharedArrayBuffer(value, path);
    }

    /* Conditional: clone typed array and DataView objects after buffers are handled. */
    if (ArrayBuffer.isView(value)) {
      return this.#cloneArrayBufferView(value, seen, path);
    }

    /* Conditional: reject WeakMap because its entries cannot be enumerated. */
    if (value instanceof WeakMap) {
      throw new DeepCloneError("Cannot deep clone WeakMap because entries are intentionally not enumerable.", { details: { path } });
    }

    /* Conditional: reject WeakSet because its entries cannot be enumerated. */
    if (value instanceof WeakSet) {
      throw new DeepCloneError("Cannot deep clone WeakSet because entries are intentionally not enumerable.", { details: { path } });
    }

    /* Conditional: reject Promise because it represents pending runtime behavior. */
    if (value instanceof Promise) {
      throw new DeepCloneError("Cannot deep clone Promise because it represents pending runtime behavior.", { details: { path } });
    }

    /* Conditional: clone Error objects with stack, cause, and custom fields. */
    if (value instanceof Error) {
      return this.#cloneError(value, seen, path);
    }

    /* Conditional: clone URL objects when available. */
    if (typeof URL !== "undefined" && value instanceof URL) {
      return new URL(value.href);
    }

    /* Conditional: clone URLSearchParams objects when available. */
    if (typeof URLSearchParams !== "undefined" && value instanceof URLSearchParams) {
      return new URLSearchParams(value.toString());
    }

    /* Return: use the generic object clone path for remaining ordinary objects and class instances. */
    return this.#cloneObject(value, seen, path);
  }

  /* Function: handle raw functions according to policy. */
  #cloneRawFunction(value, path) {
    /* Conditional: strict mode rejects raw functions. */
    if (this.rejectUnregisteredFunctions) {
      throw new DeepCloneError("Cannot deep clone raw function. Wrap behavior in RegisteredCallable.", { details: { path } });
    }

    /* Return: permissive mode preserves the function reference. */
    return value;
  }

  /* Function: clone registered callable while preserving cycles into its data. */
  #cloneRegisteredCallable(value, seen, path) {
    /* Assignment: retrieve the trusted factory for the callable type id. */
    const factory = value.registry.getFactoryOrThrow(value.callableTypeId);

    /* Assignment: create a callable shell before cloning nested data, preserving cycles. */
    const clonedCallable = RegisteredCallable.createUninitializedShell({
      registry: value.registry,
      callableTypeId: value.callableTypeId,
      cloneEngine: this,
      metadata: value.metadata
    });

    /* Assignment: store the shell immediately for circular reference preservation. */
    seen.set(value, clonedCallable);

    /* Assignment: clone constructor data with shared cycle tracking. */
    const clonedConstructorData = this.#cloneValue(value.constructorData, seen, `${path}.constructorData`);

    /* Assignment: clone internal data with shared cycle tracking. */
    const clonedInternalData = this.#cloneValue(value.internalData, seen, `${path}.internalData`);

    /* Assignment: hydrate the callable shell with cloned data and reconstructed behavior. */
    clonedCallable.hydrateFromClonedData({
      constructorData: clonedConstructorData,
      internalData: clonedInternalData,
      factory
    });

    /* Return: provide the fully hydrated callable clone. */
    return clonedCallable;
  }

  /* Function: clone and rehydrate registered callable descriptor objects. */
  #cloneRegisteredCallableDescriptor(value, seen, path) {
    /* Conditional: descriptor rehydration requires a callable registry. */
    if (!this.callableRegistry) {
      throw new DeepCloneError("Cannot rehydrate callable descriptor without a callable registry.", { details: { path } });
    }

    /* Assignment: clone descriptor as data before rehydration. */
    const clonedDescriptor = this.#cloneObject(value, seen, path);

    /* Return: reconstruct behavior from the trusted registry. */
    return this.callableRegistry.createFromDescriptor(clonedDescriptor, { cloneEngine: this });
  }

  /* Function: clone a CallableRef wrapper as data, not as executable behavior. */
  #cloneCallableRef(value, seen, path) {
    /* Assignment: create a wrapper shell around a placeholder descriptor. */
    const clonedRef = Object.create(CallableRef.prototype);

    /* Assignment: store wrapper in cycle map before cloning descriptor. */
    seen.set(value, clonedRef);

    /* Assignment: clone the descriptor field. */
    clonedRef.descriptor = this.#cloneValue(value.descriptor, seen, `${path}.descriptor`);

    /* Return: provide the cloned callable reference wrapper. */
    return clonedRef;
  }

  /* Function: clone a RegExp object. */
  #cloneRegExp(value) {
    /* Assignment: create a new regular expression from source and flags. */
    const clonedRegExp = new RegExp(value.source, value.flags);

    /* Assignment: preserve lastIndex for stateful global/sticky regex use cases. */
    clonedRegExp.lastIndex = value.lastIndex;

    /* Return: provide the cloned regular expression. */
    return clonedRegExp;
  }

  /* Function: clone a Map object. */
  #cloneMap(value, seen, path) {
    /* Assignment: create an empty Map clone before cloning entries. */
    const clonedMap = new Map();

    /* Assignment: store the clone immediately to preserve circular references. */
    seen.set(value, clonedMap);

    /* Assignment: start a diagnostic index for map entries. */
    let entryIndex = 0;

    /* Loop: clone every map key and value pair. */
    for (const [mapKey, mapValue] of value.entries()) {
      /* Assignment: clone the current key with cycle tracking. */
      const clonedKey = this.#cloneValue(mapKey, seen, `${path}.<mapKey:${entryIndex}>`);

      /* Assignment: clone the current value with cycle tracking. */
      const clonedValue = this.#cloneValue(mapValue, seen, `${path}.<mapValue:${entryIndex}>`);

      /* Assignment: store the cloned key/value pair in the cloned map. */
      clonedMap.set(clonedKey, clonedValue);

      /* Assignment: advance the diagnostic entry index. */
      entryIndex += 1;
    }

    /* Return: provide the cloned map. */
    return clonedMap;
  }

  /* Function: clone a Set object. */
  #cloneSet(value, seen, path) {
    /* Assignment: create an empty Set clone before cloning entries. */
    const clonedSet = new Set();

    /* Assignment: store the clone immediately to preserve circular references. */
    seen.set(value, clonedSet);

    /* Assignment: start a diagnostic index for set entries. */
    let entryIndex = 0;

    /* Loop: clone every set value. */
    for (const setValue of value.values()) {
      /* Assignment: clone the current set value with cycle tracking. */
      const clonedValue = this.#cloneValue(setValue, seen, `${path}.<setValue:${entryIndex}>`);

      /* Assignment: store the cloned value in the cloned set. */
      clonedSet.add(clonedValue);

      /* Assignment: advance the diagnostic entry index. */
      entryIndex += 1;
    }

    /* Return: provide the cloned set. */
    return clonedSet;
  }

  /* Function: clone an Array object. */
  #cloneArray(value, seen, path) {
    /* Assignment: create an array with the same length to preserve sparse structure. */
    const clonedArray = new Array(value.length);

    /* Assignment: store the clone immediately to preserve circular references. */
    seen.set(value, clonedArray);

    /* Loop: clone each own array index that actually exists. */
    for (let index = 0; index < value.length; index += 1) {
      /* Conditional: preserve holes in sparse arrays by skipping absent indexes. */
      if (!Object.prototype.hasOwnProperty.call(value, index)) {
        continue;
      }

      /* Assignment: clone the indexed array entry. */
      clonedArray[index] = this.#cloneValue(value[index], seen, `${path}[${index}]`);
    }

    /* Assignment: copy non-index own properties and descriptors. */
    clonePropertyDescriptors({
      sourceObject: value,
      targetObject: clonedArray,
      cloneValue: (nestedValue, nestedSeen) => this.#cloneValue(nestedValue, nestedSeen, `${path}.<property>`),
      seen,
      skipArrayIndexes: true
    });

    /* Return: provide the cloned array. */
    return clonedArray;
  }

  /* Function: clone SharedArrayBuffer with runtime capability checks. */
  #cloneSharedArrayBuffer(value, path) {
    /* Conditional: require slice support to avoid sharing the original memory. */
    if (typeof value.slice !== "function") {
      throw new DeepCloneError("Cannot deep clone SharedArrayBuffer in this runtime without slice support.", { details: { path } });
    }

    /* Return: copy the shared buffer bytes. */
    return value.slice(0);
  }

  /* Function: clone typed array or DataView values. */
  #cloneArrayBufferView(value, seen, path) {
    /* Conditional: DataView needs explicit buffer, offset, and length reconstruction. */
    if (value instanceof DataView) {
      /* Assignment: clone the underlying buffer through the main clone path. */
      const clonedBuffer = this.#cloneValue(value.buffer, seen, `${path}.buffer`);

      /* Return: rebuild the DataView against the cloned buffer. */
      return new DataView(clonedBuffer, value.byteOffset, value.byteLength);
    }

    /* Assignment: clone the underlying buffer through the main clone path. */
    const clonedBuffer = this.#cloneValue(value.buffer, seen, `${path}.buffer`);

    /* Return: rebuild the typed array with same offset and length over cloned buffer. */
    return new value.constructor(clonedBuffer, value.byteOffset, value.length);
  }

  /* Function: clone Error objects while preserving useful diagnostic fields. */
  #cloneError(value, seen, path) {
    /* Assignment: create an error object with the same prototype. */
    const clonedError = Object.create(Object.getPrototypeOf(value));

    /* Assignment: store the clone immediately to preserve circular custom fields. */
    seen.set(value, clonedError);

    /* Assignment: clone all own fields, including message, stack, cause, and custom fields. */
    clonePropertyDescriptors({
      sourceObject: value,
      targetObject: clonedError,
      cloneValue: (nestedValue, nestedSeen) => this.#cloneValue(nestedValue, nestedSeen, `${path}.<errorProperty>`),
      seen
    });

    /* Return: provide the cloned error object. */
    return clonedError;
  }

  /* Function: clone ordinary objects and class instances. */
  #cloneObject(value, seen, path) {
    /* Assignment: select the target prototype according to engine policy. */
    const targetPrototype = this.preservePrototypes ? Object.getPrototypeOf(value) : Object.prototype;

    /* Assignment: create an object shell before cloning fields. */
    const clonedObject = Object.create(targetPrototype);

    /* Assignment: store the clone immediately to preserve circular references. */
    seen.set(value, clonedObject);

    /* Assignment: copy all own properties and descriptors. */
    clonePropertyDescriptors({
      sourceObject: value,
      targetObject: clonedObject,
      cloneValue: (nestedValue, nestedSeen) => this.#cloneValue(nestedValue, nestedSeen, `${path}.<property>`),
      seen
    });

    /* Return: provide the cloned object. */
    return clonedObject;
  }
}

/*
  createDefaultDeepCloneEngine:
    Convenience wrapper for the strict recommended engine policy.
*/
function createDefaultDeepCloneEngine(callableRegistry = null) {
  /* Return: create a strict clone engine with callable rehydration support. */
  return new DeepCloneEngine({
    callableRegistry,
    rejectUnregisteredFunctions: true,
    allowAccessors: true,
    preservePrototypes: true
  });
}

/*
  createPermissiveDeepCloneEngine:
    Convenience wrapper for tooling contexts that intentionally preserve raw functions by reference.
*/
function createPermissiveDeepCloneEngine(callableRegistry = null) {
  /* Return: create a clone engine that allows raw function references. */
  return new DeepCloneEngine({
    callableRegistry,
    rejectUnregisteredFunctions: false,
    allowAccessors: true,
    preservePrototypes: true
  });
}

/*
  createCallableRegistryWithDefaults:
    Convenience wrapper for quick setup and later extension.
*/
function createCallableRegistryWithDefaults(options = {}) {
  /* Assignment: create a registry with caller-supplied policy. */
  const registry = new CallableRegistry(options);

  /* Return: provide the empty registry for caller registration. */
  return registry;
}

/*
  deepClone:
    One-shot helper for callers that do not need to hold an engine instance.
*/
function deepClone(value, options = {}) {
  /* Conditional: reject malformed options early. */
  assertPlainOptionsObject(options, "deepClone options");

  /* Assignment: select or create a clone engine. */
  const cloneEngine = options.cloneEngine ?? new DeepCloneEngine(options);

  /* Return: delegate cloning to the engine. */
  return cloneEngine.clone(value);
}

/*
  selfTestDeepCloneModule:
    Small dependency-free smoke test useful during integration.
*/
function selfTestDeepCloneModule() {
  /* Assignment: create a trusted callable registry. */
  const registry = createCallableRegistryWithDefaults();

  /* Assignment: create a strict clone engine wired to the registry. */
  const cloneEngine = createDefaultDeepCloneEngine(registry);

  /* Assignment: register a simple stateful callable factory. */
  registry.register("math.add.v1", ({ constructorData, internalData }) => {
    /* Return: factory returns the executable behavior. */
    return function addValue(inputNumber) {
      /* Assignment: update callable-owned internal state. */
      internalData.executionCount += 1;

      /* Return: expose result and current execution count. */
      return {
        result: inputNumber + constructorData.amountToAdd,
        executionCount: internalData.executionCount
      };
    };
  });

  /* Assignment: create the original callable. */
  const originalCallable = registry.create(
    "math.add.v1",
    { amountToAdd: 5 },
    { executionCount: 0 },
    { cloneEngine }
  );

  /* Assignment: execute original once to mutate internal state. */
  const firstResult = originalCallable.call(10);

  /* Assignment: clone original callable after mutation. */
  const clonedCallable = cloneEngine.clone(originalCallable);

  /* Assignment: execute clone once to verify independent state. */
  const cloneResult = clonedCallable.call(10);

  /* Assignment: execute original again to verify separate state path. */
  const secondOriginalResult = originalCallable.call(10);

  /* Conditional: validate the first result. */
  if (firstResult.result !== 15 || firstResult.executionCount !== 1) {
    throw new Error("Self-test failed: original first execution result mismatch.");
  }

  /* Conditional: validate clone starts from copied state and then diverges. */
  if (cloneResult.result !== 15 || cloneResult.executionCount !== 2) {
    throw new Error("Self-test failed: clone execution result mismatch.");
  }

  /* Conditional: validate original continues independently. */
  if (secondOriginalResult.result !== 15 || secondOriginalResult.executionCount !== 2) {
    throw new Error("Self-test failed: original second execution result mismatch.");
  }

  /* Assignment: create a circular object for graph preservation testing. */
  const circularObject = { name: "root" };

  /* Assignment: attach a self-reference to the circular object. */
  circularObject.self = circularObject;

  /* Assignment: clone the circular object. */
  const clonedCircularObject = cloneEngine.clone(circularObject);

  /* Conditional: verify circular structure was preserved. */
  if (clonedCircularObject.self !== clonedCircularObject) {
    throw new Error("Self-test failed: circular reference not preserved.");
  }

  /* Return: provide a compact test report. */
  return {
    ok: true,
    tested: [
      "registered callable clone",
      "callable internal state copy",
      "post-clone state divergence",
      "circular object clone"
    ]
  };
}


/*
  Minimal example:

  import {
    CallableRegistry,
    createDefaultDeepCloneEngine
  } from "./deep_clone_callable_registry_module_hardened.js";

  const registry = new CallableRegistry();
  const cloneEngine = createDefaultDeepCloneEngine(registry);

  registry.register("math.add.v1", ({ constructorData, internalData }) => {
    return function addValue(inputNumber) {
      internalData.executionCount += 1;
      return inputNumber + constructorData.amountToAdd;
    };
  });

  const originalCallable = registry.create(
    "math.add.v1",
    { amountToAdd: 5 },
    { executionCount: 0 },
    { cloneEngine }
  );

  const clonedCallable = cloneEngine.clone(originalCallable);
*/


/* Assignment: publish a browser/global namespace that mirrors the ESM export surface. */
globalObject.PipelineCallableRegistry=Object.freeze({
  CALLABLE_DESCRIPTOR_KIND,
  CALLABLE_DESCRIPTOR_SCHEMA_VERSION,
  CallableRef,
  CallableRegistry,
  CallableRegistryError,
  DeepCloneEngine,
  DeepCloneError,
  RegisteredCallable,
  createCallableDescriptor,
  createCallableRegistryWithDefaults,
  createDefaultDeepCloneEngine,
  createPermissiveDeepCloneEngine,
  deepClone,
  isRegisteredCallableDescriptor,
  selfTestDeepCloneModule
});

if(typeof module!=="undefined"&&module.exports){
  module.exports=globalObject.PipelineCallableRegistry;
}

})(globalThis);
