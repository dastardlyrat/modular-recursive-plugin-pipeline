(function attachMergedPipelineCoreToGlobalObject(globalObject){
"use strict";

// Assignment: define the finite set of control signals accepted by the runtime.
const pipelineSignalNameByAction=Object.freeze({START:"START",STOP:"STOP",PAUSE:"PAUSE",RESUME:"RESUME",RESET:"RESET"});

// Assignment: define stable runtime states for display, diagnostics, and control flow.
const pipelineRuntimeStateNameByStatus=Object.freeze({IDLE:"idle",RUNNING:"running",PAUSED:"paused",STOPPED:"stopped",ERROR:"error"});

// Assignment: define node categories used by the registry, dependency validator, and UI.
const pipelineNodeTypeNameByRole=Object.freeze({SYSTEM_SENTINEL:"system-sentinel",STANDARD_PLUGIN:"stateful-plugin",STREAM_PLUGIN:"stream-plugin",EXTENSION_PLUGIN:"extension-plugin",EXTERNAL_PLUGIN:"external-plugin"});

// Assignment: define data support markers used by plugin metadata.
const supportedPipelineDataFormat=Object.freeze({UNIVERSAL:"universal",JSON:"json"});

// Assignment: define conservative runtime execution-budget defaults for hardening.
const DEFAULT_MAXIMUM_NODES_PER_PASS=1000;
const DEFAULT_MAXIMUM_ELAPSED_MS_PER_PASS=10000;
const DEFAULT_MAXIMUM_BRANCH_ENTRIES_PER_PASS=50;

// Function/class: central security utility for cloning, validation, parsing, and regex creation.
class PipelineSecurityUtility{
  // Function: compatibility alias; retained for older callers that expect the runtime-safe clone.
  static deepCloneJsonSafeValue(valueToClone){
    return PipelineSecurityUtility.cloneRuntimeSafeValue(valueToClone);
  }

  // Function: clone trusted runtime values, including Maps/Sets/Dates/RegExps, without executing user code.
  static cloneRuntimeSafeValue(valueToClone){
    // Conditional: prefer the platform clone when it is available and succeeds.
    if(typeof globalObject.structuredClone==="function"){
      try{
        // Assignment: cloned value returned by the platform implementation.
        const platformClonedValue=globalObject.structuredClone(valueToClone);
        return platformClonedValue;
      }catch(structuredCloneError){
        // Assignment: intentionally preserve the error only for debugger visibility, then use manual cloning.
        const ignoredStructuredCloneError=structuredCloneError;
        void ignoredStructuredCloneError;
      }
    }

    // Assignment: weak map tracks already-cloned objects so circular references do not recurse forever.
    const objectReferenceToCloneReferenceMap=new WeakMap();
    return PipelineSecurityUtility.deepCloneManuallyWithSeenMap(valueToClone,objectReferenceToCloneReferenceMap);
  }

  // Function: clone only strict JSON-compatible data for pipeline data, signals, hook payloads, diagnostics, and output.
  static cloneStrictJsonValue(valueToClone,jsonPath="$",objectReferenceSet=new WeakSet()){
    // Conditional: wrapped pipeline documents are reduced to their current plain JSON root before cloning.
    if(typeof PipelineJsonDocumentFactory!=="undefined"&&PipelineJsonDocumentFactory.isPipelineJsonDocument(valueToClone)){
      return PipelineSecurityUtility.cloneStrictJsonValue(valueToClone.toPlainObject(),jsonPath,objectReferenceSet);
    }

    // Conditional: null is valid JSON.
    if(valueToClone===null){
      return null;
    }

    // Assignment: primitive type label.
    const valueType=typeof valueToClone;

    // Conditional: JSON scalar primitives are accepted.
    if(valueType==="string"||valueType==="boolean"){
      return valueToClone;
    }

    // Conditional: JSON numbers must be finite.
    if(valueType==="number"){
      if(!Number.isFinite(valueToClone)){
        throw new TypeError(jsonPath+" must be a finite JSON number.");
      }

      return valueToClone;
    }

    // Conditional: undefined, bigint, symbol, and function cannot be represented in strict JSON.
    if(valueType!=="object"){
      throw new TypeError(jsonPath+" contains non-JSON value of type "+valueType+".");
    }

    // Conditional: circular references cannot be represented in strict JSON.
    if(objectReferenceSet.has(valueToClone)){
      throw new TypeError(jsonPath+" contains a circular reference, which is not valid JSON.");
    }

    // Assignment: mark object before descending.
    objectReferenceSet.add(valueToClone);

    // Conditional: arrays must be dense and recursively JSON-compatible.
    if(Array.isArray(valueToClone)){
      const clonedArray=[];

      for(let arrayItemIndex=0;arrayItemIndex<valueToClone.length;arrayItemIndex+=1){
        if(!Object.prototype.hasOwnProperty.call(valueToClone,arrayItemIndex)){
          throw new TypeError(jsonPath+"["+arrayItemIndex+"] cannot be a sparse JSON array slot.");
        }

        clonedArray.push(PipelineSecurityUtility.cloneStrictJsonValue(valueToClone[arrayItemIndex],jsonPath+"["+arrayItemIndex+"]",objectReferenceSet));
      }

      objectReferenceSet.delete(valueToClone);
      return clonedArray;
    }

    // Conditional: only plain objects are accepted at strict JSON boundaries.
    if(!PipelineSecurityUtility.isPlainObject(valueToClone)){
      throw new TypeError(jsonPath+" contains non-JSON object value "+Object.prototype.toString.call(valueToClone)+".");
    }

    // Assignment: strict clone uses ordinary plain objects after rejecting prototype-polluting keys.
    const clonedObject={};
    const propertyDescriptorMap=Object.getOwnPropertyDescriptors(valueToClone);

    for(const propertyKey of Reflect.ownKeys(propertyDescriptorMap)){
      if(typeof propertyKey!=="string"){
        throw new TypeError(jsonPath+" cannot contain symbol keys in JSON data.");
      }

      if(propertyKey==="__proto__"||propertyKey==="prototype"||propertyKey==="constructor"){
        throw new TypeError(jsonPath+" contains blocked prototype-pollution key: "+propertyKey);
      }

      const propertyDescriptor=propertyDescriptorMap[propertyKey];

      if(!propertyDescriptor.enumerable){
        throw new TypeError(jsonPath+"."+propertyKey+" must be an enumerable JSON property.");
      }

      if(!Object.prototype.hasOwnProperty.call(propertyDescriptor,"value")){
        throw new TypeError(jsonPath+"."+propertyKey+" uses an accessor descriptor, which is not allowed in strict JSON data.");
      }

      clonedObject[propertyKey]=PipelineSecurityUtility.cloneStrictJsonValue(propertyDescriptor.value,jsonPath+"."+propertyKey,objectReferenceSet);
    }

    objectReferenceSet.delete(valueToClone);
    return clonedObject;
  }

  // Function: deeply freeze a JSON-safe object or array tree so history snapshots cannot be mutated.
  static deepFreezeJsonSafeValue(valueToFreeze,objectReferenceSet=new WeakSet()){
    // Conditional: primitives and null are already immutable values.
    if(valueToFreeze===null||typeof valueToFreeze!=="object"){
      return valueToFreeze;
    }

    // Conditional: repeated references should not recurse forever.
    if(objectReferenceSet.has(valueToFreeze)){
      return valueToFreeze;
    }

    // Assignment: mark object before descending into children.
    objectReferenceSet.add(valueToFreeze);

    // Loop: freeze every own child before freezing this container.
    for(const ownPropertyKey of Reflect.ownKeys(valueToFreeze)){
      const ownPropertyDescriptor=Object.getOwnPropertyDescriptor(valueToFreeze,ownPropertyKey);
      if(ownPropertyDescriptor&&"value" in ownPropertyDescriptor){
        PipelineSecurityUtility.deepFreezeJsonSafeValue(ownPropertyDescriptor.value,objectReferenceSet);
      }
    }

    return Object.freeze(valueToFreeze);
  }

  // Function: manually clone supported values while rejecting unsafe runtime-only values.
  static deepCloneManuallyWithSeenMap(valueToClone,objectReferenceToCloneReferenceMap){
    // Conditional: functions cannot be represented as JSON-safe data and must not be cloned silently.
    if(typeof valueToClone==="function"){
      throw new TypeError("Cannot deep clone functions.");
    }

    // Conditional: primitive values are already immutable enough for the pipeline data layer.
    if(valueToClone===null||typeof valueToClone!=="object"){
      return valueToClone;
    }

    // Conditional: pipeline JSON documents clone to their current plain object payload.
    if(PipelineJsonDocumentFactory.isPipelineJsonDocument(valueToClone)){
      return PipelineSecurityUtility.deepCloneManuallyWithSeenMap(valueToClone.toPlainObject(),objectReferenceToCloneReferenceMap);
    }

    // Conditional: circular or repeated references reuse the clone already produced for the same source object.
    if(objectReferenceToCloneReferenceMap.has(valueToClone)){
      return objectReferenceToCloneReferenceMap.get(valueToClone);
    }

    // Conditional: WeakMap cannot be enumerated, so cloning it would be dishonest.
    if(valueToClone instanceof WeakMap){
      throw new TypeError("Cannot deep clone WeakMap.");
    }

    // Conditional: WeakSet cannot be enumerated, so cloning it would be dishonest.
    if(valueToClone instanceof WeakSet){
      throw new TypeError("Cannot deep clone WeakSet.");
    }

    // Conditional: Promise represents future execution state, not plain data.
    if(valueToClone instanceof Promise){
      throw new TypeError("Cannot deep clone Promise.");
    }

    // Conditional: Date is cloned by timestamp.
    if(valueToClone instanceof Date){
      return new Date(valueToClone.getTime());
    }

    // Conditional: RegExp is cloned with source, flags, and current scan position.
    if(valueToClone instanceof RegExp){
      // Assignment: reconstructed regular expression instance.
      const clonedRegularExpression=new RegExp(valueToClone.source,valueToClone.flags);
      // Assignment: preserve lastIndex for callers that intentionally use stateful regex scans.
      clonedRegularExpression.lastIndex=valueToClone.lastIndex;
      return clonedRegularExpression;
    }

    // Conditional: ArrayBuffer can be copied directly.
    if(valueToClone instanceof ArrayBuffer){
      // Assignment: copied binary buffer.
      const clonedArrayBuffer=valueToClone.slice(0);
      // Assignment: record clone before return for consistency with other object paths.
      objectReferenceToCloneReferenceMap.set(valueToClone,clonedArrayBuffer);
      return clonedArrayBuffer;
    }

    // Conditional: typed-array and DataView objects share the ArrayBuffer cloning path.
    if(ArrayBuffer.isView(valueToClone)){
      // Assignment: clone the backing buffer once through the shared clone map.
      const clonedBackingArrayBuffer=PipelineSecurityUtility.deepCloneManuallyWithSeenMap(valueToClone.buffer,objectReferenceToCloneReferenceMap);
      // Assignment: placeholder for the reconstructed view.
      let clonedArrayBufferView;

      // Conditional: DataView constructor differs from typed-array constructors.
      if(valueToClone instanceof DataView){
        // Assignment: reconstructed DataView over the cloned buffer.
        clonedArrayBufferView=new DataView(clonedBackingArrayBuffer,valueToClone.byteOffset,valueToClone.byteLength);
      }else{
        // Assignment: reconstructed typed array over the cloned buffer.
        clonedArrayBufferView=new valueToClone.constructor(clonedBackingArrayBuffer,valueToClone.byteOffset,valueToClone.length);
      }

      // Assignment: record the cloned view to handle repeated references.
      objectReferenceToCloneReferenceMap.set(valueToClone,clonedArrayBufferView);
      return clonedArrayBufferView;
    }

    // Conditional: Map requires cloning both keys and values.
    if(valueToClone instanceof Map){
      // Assignment: empty map allocated before recursion so circular references can resolve.
      const clonedMapValue=new Map();
      // Assignment: record the clone before iterating nested values.
      objectReferenceToCloneReferenceMap.set(valueToClone,clonedMapValue);

      // Loop: clone each key/value pair without assuming keys are primitive.
      for(const [sourceMapKey,sourceMapValue] of valueToClone.entries()){
        // Assignment: clone the map key.
        const clonedMapKey=PipelineSecurityUtility.deepCloneManuallyWithSeenMap(sourceMapKey,objectReferenceToCloneReferenceMap);
        // Assignment: clone the map value.
        const clonedMapItemValue=PipelineSecurityUtility.deepCloneManuallyWithSeenMap(sourceMapValue,objectReferenceToCloneReferenceMap);
        // Assignment: store cloned pair in cloned map.
        clonedMapValue.set(clonedMapKey,clonedMapItemValue);
      }

      return clonedMapValue;
    }

    // Conditional: Set requires cloning each member.
    if(valueToClone instanceof Set){
      // Assignment: empty set allocated before recursion so circular references can resolve.
      const clonedSetValue=new Set();
      // Assignment: record the clone before iterating nested values.
      objectReferenceToCloneReferenceMap.set(valueToClone,clonedSetValue);

      // Loop: clone each set item.
      for(const sourceSetValue of valueToClone.values()){
        // Assignment: store the cloned set item.
        clonedSetValue.add(PipelineSecurityUtility.deepCloneManuallyWithSeenMap(sourceSetValue,objectReferenceToCloneReferenceMap));
      }

      return clonedSetValue;
    }

    // Conditional: Array receives a length-preserving sparse-aware clone.
    if(Array.isArray(valueToClone)){
      // Assignment: empty array clone preserves the source length.
      const clonedArrayValue=new Array(valueToClone.length);
      // Assignment: record the clone before iterating nested values.
      objectReferenceToCloneReferenceMap.set(valueToClone,clonedArrayValue);

      // Loop: clone only indexes that actually exist on the source array.
      for(let arrayItemIndex=0;arrayItemIndex<valueToClone.length;arrayItemIndex+=1){
        // Conditional: preserve holes instead of inventing undefined values.
        if(Object.prototype.hasOwnProperty.call(valueToClone,arrayItemIndex)){
          // Assignment: clone an existing array slot.
          clonedArrayValue[arrayItemIndex]=PipelineSecurityUtility.deepCloneManuallyWithSeenMap(valueToClone[arrayItemIndex],objectReferenceToCloneReferenceMap);
        }
      }

      // Function call: copy any extra non-index own properties.
      PipelineSecurityUtility.copyOwnPropertyDescriptors(valueToClone,clonedArrayValue,objectReferenceToCloneReferenceMap,{skipNumericArrayIndexes:true});
      return clonedArrayValue;
    }

    // Assignment: generic object clone preserves prototype and descriptors.
    const clonedObjectValue=Object.create(Object.getPrototypeOf(valueToClone));
    // Assignment: record the clone before iterating nested values.
    objectReferenceToCloneReferenceMap.set(valueToClone,clonedObjectValue);
    // Function call: copy all own descriptors, including symbol keys.
    PipelineSecurityUtility.copyOwnPropertyDescriptors(valueToClone,clonedObjectValue,objectReferenceToCloneReferenceMap);
    return clonedObjectValue;
  }

  // Function: clone and copy all own property descriptors from source to target.
  static copyOwnPropertyDescriptors(sourceObject,targetObject,objectReferenceToCloneReferenceMap,copyOptions={}){
    // Assignment: descriptor map includes non-enumerable and symbol properties.
    const sourcePropertyDescriptorMap=Object.getOwnPropertyDescriptors(sourceObject);

    // Loop: process every own property key from the descriptor map.
    for(const propertyKey of Reflect.ownKeys(sourcePropertyDescriptorMap)){
      // Conditional: array index descriptors were already copied in the array-specific loop.
      if(copyOptions.skipNumericArrayIndexes&&typeof propertyKey==="string"&&String(Number(propertyKey))===propertyKey){
        continue;
      }

      // Assignment: descriptor is cloned in place before definition on target.
      const propertyDescriptor=sourcePropertyDescriptorMap[propertyKey];

      // Conditional: data descriptors need cloned values; accessors keep their getter/setter functions.
      if("value" in propertyDescriptor){
        // Assignment: replace descriptor value with a deep clone.
        propertyDescriptor.value=PipelineSecurityUtility.deepCloneManuallyWithSeenMap(propertyDescriptor.value,objectReferenceToCloneReferenceMap);
      }

      // Assignment: define the cloned descriptor on the target object.
      Object.defineProperty(targetObject,propertyKey,propertyDescriptor);
    }
  }

  // Function: test whether a value is a plain object suitable for JSON object use.
  static isPlainObject(valueToCheck){
    // Conditional: null, arrays, and primitives are not plain objects.
    if(valueToCheck===null||typeof valueToCheck!=="object"||Array.isArray(valueToCheck)){
      return false;
    }

    // Assignment: plain objects use Object.prototype or null prototype.
    const objectPrototype=Object.getPrototypeOf(valueToCheck);
    return objectPrototype===Object.prototype||objectPrototype===null;
  }

  // Function: enforce plain-object shape.
  static assertPlainObject(valueToCheck,diagnosticLabel){
    // Conditional: throw with caller label when the shape is wrong.
    if(!PipelineSecurityUtility.isPlainObject(valueToCheck)){
      throw new TypeError(diagnosticLabel+" must be a plain object.");
    }
  }

  // Function: enforce array of strings shape.
  static assertStringArray(valueToCheck,diagnosticLabel){
    // Conditional: every item must be a string.
    if(!Array.isArray(valueToCheck)||!valueToCheck.every(arrayItem=>typeof arrayItem==="string")){
      throw new TypeError(diagnosticLabel+" must be an array of strings.");
    }
  }

  // Function: parse JSON with a useful label in the thrown syntax error.
  static parseJsonTextSafely(jsonText,diagnosticLabel="JSON text"){
    // Conditional: parser only accepts text.
    if(typeof jsonText!=="string"){
      throw new TypeError(diagnosticLabel+" must be a string.");
    }

    try{
      return JSON.parse(jsonText);
    }catch(jsonParseError){
      throw new SyntaxError(diagnosticLabel+" could not be parsed: "+jsonParseError.message);
    }
  }

  // Function: convert arbitrary input text into a JSON object wrapper payload.
  static normalizeReaderTextIntoJsonValue(inputText){
    // Conditional: input must be textual at the browser adapter boundary.
    if(typeof inputText!=="string"){
      throw new TypeError("Input text must be a string.");
    }

    // Assignment: trim only for JSON detection; the returned text uses the trimmed text for stable behavior.
    const trimmedInputText=inputText.trim();

    // Conditional: empty text becomes an object with an empty text field.
    if(!trimmedInputText){
      return {text:""};
    }

    try{
      return JSON.parse(trimmedInputText);
    }catch(ignoredJsonParseError){
      // Assignment: preserve ignored parse error for debug tooling without changing fallback behavior.
      const ignoredError=ignoredJsonParseError;
      void ignoredError;
      return {text:trimmedInputText};
    }
  }

  // Function: reject common catastrophic-backtracking regex shapes before RegExp construction.
  static assertRegularExpressionSourceIsSafe(regularExpressionSource){
    // Assignment: nested quantified groups such as (a+)+ are high-risk ReDoS patterns.
    const nestedQuantifiedGroupPattern=new RegExp("\\((?:[^()\\\\]|\\\\.)*[+*](?:[^()\\\\]|\\\\.)*\\)\\s*(?:[+*]|\\{)");
    // Assignment: quantified alternation groups can create ambiguous exponential search spaces.
    const quantifiedAlternationPattern=new RegExp("\\((?:[^()\\\\]|\\\\.)*\\|(?:[^()\\\\]|\\\\.)*\\)\\s*(?:[+*]|\\{)");
    // Assignment: repeated wildcard scans are frequently accidental and expensive in browser UIs.
    const repeatedWildcardPattern=new RegExp("(?:\\.\\*){2,}|(?:\\.\\+){2,}");

    if(nestedQuantifiedGroupPattern.test(regularExpressionSource)){
      throw new Error("Potentially unsafe regex source rejected: nested quantified group.");
    }

    if(quantifiedAlternationPattern.test(regularExpressionSource)){
      throw new Error("Potentially unsafe regex source rejected: quantified alternation group.");
    }

    if(repeatedWildcardPattern.test(regularExpressionSource)){
      throw new Error("Potentially unsafe regex source rejected: repeated wildcard quantifiers.");
    }
  }

  // Function: create a bounded regular expression with a strict flag whitelist.
  static createBoundedRegularExpression(regularExpressionSource,regularExpressionFlags){
    // Conditional: source must be a non-empty string.
    if(typeof regularExpressionSource!=="string"||regularExpressionSource.length===0){
      throw new TypeError("Regex source must be non-empty.");
    }

    // Conditional: source length is bounded to reduce pathological regex input risk.
    if(regularExpressionSource.length>500){
      throw new RangeError("Regex source too long.");
    }

    // Conditional: flags must be textual before whitelist validation.
    if(typeof regularExpressionFlags!=="string"){
      throw new TypeError("Regex flags must be a string.");
    }

    // Conditional: reject unsupported or malformed flags before RegExp construction.
    if(!/^[dgimsuvy]*$/.test(regularExpressionFlags)){
      throw new TypeError("Bad regex flags.");
    }

    // Function call: reject known high-risk catastrophic-backtracking shapes before construction.
    PipelineSecurityUtility.assertRegularExpressionSourceIsSafe(regularExpressionSource);

    // Assignment: remove duplicate flags while preserving caller order.
    const deduplicatedRegularExpressionFlags=[...new Set(regularExpressionFlags.split(""))].join("");
    return new RegExp(regularExpressionSource,deduplicatedRegularExpressionFlags);
  }
}

// Function/class: JSON document wrapper that stores both original and current JSON-compatible values.
class PipelineJsonDocument{
  // Function: construct a new document wrapper from JSON-compatible data.
  constructor(jsonCompatibleValue={},documentMetadata={}){
    // Assignment: normalize the root before storing it.
    const normalizedInitialRoot=PipelineJsonDocumentFactory.normalizeJsonCompatibleValue(jsonCompatibleValue,"$");
    // Assignment: immutable original root is a frozen clone of the first accepted JSON input.
    const immutableOriginalRootValue=PipelineSecurityUtility.deepFreezeJsonSafeValue(PipelineSecurityUtility.cloneStrictJsonValue(normalizedInitialRoot));
    // Assignment: current root starts as a frozen clone of the first accepted JSON input.
    const initialFrozenCurrentRoot=PipelineSecurityUtility.deepFreezeJsonSafeValue(PipelineSecurityUtility.cloneStrictJsonValue(normalizedInitialRoot));
    // Assignment: history buffer is a frozen array of frozen JSON snapshots.
    const initialJsonHistoryBuffer=Object.freeze([initialFrozenCurrentRoot]);
    // Assignment: history metadata mirrors the history buffer by index.
    const initialJsonHistoryMetadata=Object.freeze([Object.freeze({historyIndex:0,event:"document-created",createdAt:new Date().toISOString(),...PipelineSecurityUtility.cloneStrictJsonValue(documentMetadata||{})})]);

    // Assignment: immutable root is separate from the mutable current pointer and cannot be rewritten.
    Object.defineProperty(this,"immutableOriginalRoot",{value:immutableOriginalRootValue,writable:false,enumerable:false,configurable:false});

    // Assignment: compatibility alias for existing code; this also points to the immutable original root.
    Object.defineProperty(this,"originalJsonRoot",{value:immutableOriginalRootValue,writable:false,enumerable:false,configurable:false});

    // Assignment: current root is the latest frozen JSON snapshot.
    Object.defineProperty(this,"currentJsonRoot",{value:initialFrozenCurrentRoot,writable:true,enumerable:false,configurable:false});

    // Assignment: append-only JSON history buffer preserves each transformation input/output as frozen snapshots.
    Object.defineProperty(this,"jsonHistoryBuffer",{value:initialJsonHistoryBuffer,writable:true,enumerable:false,configurable:false});

    // Assignment: append-only metadata buffer records why each snapshot was appended.
    Object.defineProperty(this,"jsonHistoryMetadata",{value:initialJsonHistoryMetadata,writable:true,enumerable:false,configurable:false});

    // Assignment: metadata records document creation and caller-provided source information.
    Object.defineProperty(this,"documentMetadata",{value:Object.freeze({createdAt:new Date().toISOString(),historyModel:"append-only-json-history-buffer",...PipelineSecurityUtility.cloneStrictJsonValue(documentMetadata||{})}),writable:false,enumerable:false,configurable:false});

    // Assignment: marker allows helper code to detect wrapped documents without instanceof brittleness.
    Object.defineProperty(this,"isPipelineJsonDocumentMarker",{value:true,writable:false,enumerable:false,configurable:false});

    return new Proxy(this,PipelineJsonDocumentFactory.documentProxyHandler);
  }

  // Function: append a normalized and frozen JSON snapshot to the history buffer and make it current.
  appendTransformationSnapshot(nextJsonCompatibleValue,transformationMetadata={}){
    // Assignment: normalize the incoming root against strict JSON compatibility rules.
    const normalizedNextRoot=PipelineJsonDocumentFactory.normalizeJsonCompatibleValue(nextJsonCompatibleValue,"$");
    // Assignment: freeze stored snapshot to prevent mutation of prior history.
    const frozenNextRoot=PipelineSecurityUtility.deepFreezeJsonSafeValue(PipelineSecurityUtility.cloneStrictJsonValue(normalizedNextRoot));
    // Assignment: next index follows current history length.
    const nextHistoryIndex=Array.isArray(this.jsonHistoryBuffer)?this.jsonHistoryBuffer.length:0;
    // Assignment: current root now points to the latest frozen snapshot.
    this.currentJsonRoot=frozenNextRoot;
    // Assignment: append by replacing the frozen history array with a new frozen history array.
    this.jsonHistoryBuffer=Object.freeze([...(Array.isArray(this.jsonHistoryBuffer)?this.jsonHistoryBuffer:[]),frozenNextRoot]);
    // Assignment: append metadata by replacing the frozen metadata array.
    const nextHistoryMetadataRecord=Object.freeze({historyIndex:nextHistoryIndex,event:"transformation-appended",appendedAt:new Date().toISOString(),...PipelineSecurityUtility.cloneStrictJsonValue(transformationMetadata||{})});
    this.jsonHistoryMetadata=Object.freeze([...(Array.isArray(this.jsonHistoryMetadata)?this.jsonHistoryMetadata:[]),nextHistoryMetadataRecord]);
    return this;
  }

  // Function: return cloned full JSON history array.
  toHistoryPlainArray(){
    return PipelineSecurityUtility.cloneStrictJsonValue(Array.isArray(this.jsonHistoryBuffer)?this.jsonHistoryBuffer:[this.currentJsonRoot]);
  }

  // Function: return cloned JSON history metadata array.
  toHistoryMetadataPlainArray(){
    return PipelineSecurityUtility.cloneStrictJsonValue(Array.isArray(this.jsonHistoryMetadata)?this.jsonHistoryMetadata:[]);
  }

  // Function: return the number of snapshots retained in the history buffer.
  getJsonHistoryLength(){
    return Array.isArray(this.jsonHistoryBuffer)?this.jsonHistoryBuffer.length:1;
  }

  // Function: return the latest history index.
  getLatestHistoryIndex(){
    return this.getJsonHistoryLength()-1;
  }

  // Function: return a cloned plain object/value representing the current root.
  toPlainObject(){
    return PipelineSecurityUtility.cloneStrictJsonValue(this.currentJsonRoot);
  }

  // Function: return a cloned plain object/value representing the immutable original root.
  toOriginalPlainObject(){
    return PipelineSecurityUtility.cloneStrictJsonValue(this.immutableOriginalRoot);
  }

  // Function: standard JSON serialization hook.
  toJSON(){
    return this.toPlainObject();
  }

  // Function: stringify current root with caller-controlled spacing.
  stringifyCurrentJson(spaces=2){
    return JSON.stringify(this.currentJsonRoot,null,spaces);
  }

  // Function: replace the current root by appending a new frozen history snapshot.
  replaceCurrentRoot(nextJsonCompatibleValue,{alsoReplaceOriginalRoot=false,transformationMetadata={}}={}){
    // Assignment: immutable original root is never rewritten; compatibility flag is retained as metadata only.
    return this.appendTransformationSnapshot(nextJsonCompatibleValue,{event:"replace-current-root",alsoReplaceOriginalRootIgnored:Boolean(alsoReplaceOriginalRoot),...transformationMetadata});
  }

  // Function: reset current root from immutable original root by appending a new frozen snapshot.
  resetCurrentRootToOriginalRoot(){
    return this.appendTransformationSnapshot(this.immutableOriginalRoot,{event:"reset-current-root-to-immutable-original"});
  }

  // Function: preserve compatibility while refusing to rewrite the immutable original root.
  commitCurrentRootAsOriginalRoot(){
    return this;
  }

  // Function: test whether a path exists.
  hasValueAtPath(jsonPath){
    // Assignment: split path into navigable parts.
    const jsonPathParts=PipelineJsonDocumentFactory.convertJsonPathToPathParts(jsonPath);
    // Assignment: cursor starts at current root.
    let currentCursor=this.currentJsonRoot;

    // Loop: walk through each path segment.
    for(const pathPart of jsonPathParts){
      // Conditional: missing container or missing key means path does not exist.
      if(!PipelineJsonDocumentFactory.isObjectOrArrayContainer(currentCursor)||!(pathPart in currentCursor)){
        return false;
      }

      // Assignment: move cursor deeper into the document.
      currentCursor=currentCursor[pathPart];
    }

    return true;
  }

  // Function: read a value at path, optionally by reference.
  getValueAtPath(jsonPath,defaultValue=undefined,{cloneReturnedValue=true}={}){
    // Assignment: split path into navigable parts.
    const jsonPathParts=PipelineJsonDocumentFactory.convertJsonPathToPathParts(jsonPath);
    // Assignment: cursor starts at current root.
    let currentCursor=this.currentJsonRoot;

    // Loop: walk through each path segment.
    for(const pathPart of jsonPathParts){
      // Conditional: missing container or missing key returns caller default.
      if(!PipelineJsonDocumentFactory.isObjectOrArrayContainer(currentCursor)||!(pathPart in currentCursor)){
        return defaultValue;
      }

      // Assignment: move cursor deeper into the document.
      currentCursor=currentCursor[pathPart];
    }

    // Conditional: clone unless caller explicitly wants a live reference.
    if(cloneReturnedValue){
      return PipelineSecurityUtility.cloneStrictJsonValue(currentCursor);
    }

    return currentCursor;
  }

  // Function: read a value reference at path.
  getReferenceAtPath(jsonPath,defaultValue=undefined){
    return this.getValueAtPath(jsonPath,defaultValue,{cloneReturnedValue:false});
  }

  // Function: set a value at path by cloning current root, mutating the clone, then appending a frozen snapshot.
  setValueAtPath(jsonPath,nextJsonCompatibleValue,{createMissingContainers=true,transformationMetadata={}}={}){
    // Assignment: split path into navigable parts.
    const jsonPathParts=PipelineJsonDocumentFactory.convertJsonPathToPathParts(jsonPath);
    // Assignment: normalize the value before inserting it.
    const normalizedNextValue=PipelineJsonDocumentFactory.normalizeJsonCompatibleValue(nextJsonCompatibleValue,"$"+(jsonPathParts.length?"."+jsonPathParts.join("."):""));

    // Conditional: empty path replaces the whole root by appending a new root snapshot.
    if(jsonPathParts.length===0){
      return this.appendTransformationSnapshot(normalizedNextValue,{event:"set-root",...transformationMetadata});
    }

    // Assignment: clone current root so frozen history snapshots are never mutated in place.
    let nextRoot=PipelineSecurityUtility.cloneStrictJsonValue(this.currentJsonRoot);

    // Conditional: non-container root can be replaced with object only when creation is allowed.
    if(!PipelineJsonDocumentFactory.isObjectOrArrayContainer(nextRoot)){
      // Conditional: caller can forbid path creation.
      if(!createMissingContainers){
        throw new Error("Cannot set path on a non-container JSON root.");
      }

      // Assignment: create an object root for the path.
      nextRoot={};
    }

    // Assignment: cursor starts at cloned root for mutation.
    let currentCursor=nextRoot;

    // Loop: walk to the parent of the final path segment.
    for(let pathPartIndex=0;pathPartIndex<jsonPathParts.length-1;pathPartIndex+=1){
      // Assignment: current segment.
      const currentPathPart=jsonPathParts[pathPartIndex];
      // Assignment: next segment determines object vs array creation.
      const nextPathPart=jsonPathParts[pathPartIndex+1];

      // Conditional: missing/non-container child can be created when allowed.
      if(!PipelineJsonDocumentFactory.isObjectOrArrayContainer(currentCursor[currentPathPart])){
        // Conditional: caller can forbid missing path creation.
        if(!createMissingContainers){
          throw new Error("Missing JSON path segment: "+jsonPathParts.slice(0,pathPartIndex+1).join("."));
        }

        // Assignment: choose array for numeric next segment; otherwise choose object.
        currentCursor[currentPathPart]=PipelineJsonDocumentFactory.isArrayIndexPart(nextPathPart)?[]:{};
      }

      // Assignment: descend into the child container.
      currentCursor=currentCursor[currentPathPart];
    }

    // Assignment: set the final path segment on the cloned root.
    currentCursor[jsonPathParts[jsonPathParts.length-1]]=normalizedNextValue;
    return this.appendTransformationSnapshot(nextRoot,{event:"set-value-at-path",jsonPath:jsonPathParts.join("."),...transformationMetadata});
  }

  // Function: delete a path by cloning current root, deleting from the clone, then appending a frozen snapshot.
  deleteValueAtPath(jsonPath){
    // Assignment: split path into navigable parts.
    const jsonPathParts=PipelineJsonDocumentFactory.convertJsonPathToPathParts(jsonPath);

    // Conditional: empty path clears root to an empty object through history append.
    if(jsonPathParts.length===0){
      this.appendTransformationSnapshot({},{event:"delete-root"});
      return true;
    }

    // Assignment: clone current root so history snapshots remain immutable.
    const nextRoot=PipelineSecurityUtility.cloneStrictJsonValue(this.currentJsonRoot);
    // Assignment: cursor starts at cloned root for mutation.
    let currentCursor=nextRoot;

    // Loop: walk to parent segment.
    for(let pathPartIndex=0;pathPartIndex<jsonPathParts.length-1;pathPartIndex+=1){
      // Assignment: current segment.
      const currentPathPart=jsonPathParts[pathPartIndex];

      // Conditional: missing path cannot be deleted.
      if(!PipelineJsonDocumentFactory.isObjectOrArrayContainer(currentCursor)||!(currentPathPart in currentCursor)){
        return false;
      }

      // Assignment: descend into next container.
      currentCursor=currentCursor[currentPathPart];
    }

    // Conditional: parent must be a container.
    if(!PipelineJsonDocumentFactory.isObjectOrArrayContainer(currentCursor)){
      return false;
    }

    const finalPathPart=jsonPathParts[jsonPathParts.length-1];

    // Conditional: absent final key means nothing was deleted.
    if(!(finalPathPart in currentCursor)){
      return false;
    }

    // Assignment: delete the final path segment from the cloned root.
    const deleteSucceeded=delete currentCursor[finalPathPart];

    // Conditional: append only after successful deletion.
    if(deleteSucceeded){
      this.appendTransformationSnapshot(nextRoot,{event:"delete-value-at-path",jsonPath:jsonPathParts.join(".")});
    }

    return deleteSucceeded;
  }

  // Function: merge an object into an object path by appending a new frozen snapshot.
  mergeObjectAtPath(objectToMerge,{jsonPath="",overwriteExistingValues=true}={}){
    // Assignment: normalize merge source.
    const normalizedMergeObject=PipelineJsonDocumentFactory.normalizeJsonCompatibleValue(objectToMerge,"$");
    // Function call: enforce object merge shape.
    PipelineSecurityUtility.assertPlainObject(normalizedMergeObject,"Merged JSON value");
    // Assignment: clone current root so history snapshots are not mutated in place.
    let nextRoot=PipelineSecurityUtility.cloneStrictJsonValue(this.currentJsonRoot);
    // Assignment: target path parts.
    const jsonPathParts=PipelineJsonDocumentFactory.convertJsonPathToPathParts(jsonPath);

    // Conditional: non-container root becomes an object for object merge.
    if(!PipelineJsonDocumentFactory.isObjectOrArrayContainer(nextRoot)){
      nextRoot={};
    }

    // Assignment: target object reference inside the cloned root.
    let targetObject=nextRoot;

    // Loop: ensure the target path exists as plain objects.
    for(const jsonPathPart of jsonPathParts){
      if(!PipelineSecurityUtility.isPlainObject(targetObject[jsonPathPart])){
        targetObject[jsonPathPart]={};
      }

      targetObject=targetObject[jsonPathPart];
    }

    // Loop: assign each merge key.
    for(const [mergeKey,mergeValue] of Object.entries(normalizedMergeObject)){
      // Conditional: overwrite only when requested or when the target key is absent.
      if(overwriteExistingValues||!(mergeKey in targetObject)){
        // Assignment: clone value into target object.
        targetObject[mergeKey]=PipelineSecurityUtility.cloneStrictJsonValue(mergeValue);
      }
    }

    return this.appendTransformationSnapshot(nextRoot,{event:"merge-object-at-path",jsonPath,overwriteExistingValues});
  }

  // Function: ensure a path holds a plain object and return its live reference.
  ensureObjectAtPath(jsonPath){
    // Assignment: current live value at requested path.
    const currentValue=this.getReferenceAtPath(jsonPath);

    // Conditional: existing plain object can be reused.
    if(PipelineSecurityUtility.isPlainObject(currentValue)){
      return currentValue;
    }

    // Function call: replace the path with a plain object.
    this.setValueAtPath(jsonPath,{});
    return this.getReferenceAtPath(jsonPath);
  }

  // Function: ensure a path holds an array and return its live reference.
  ensureArrayAtPath(jsonPath){
    // Assignment: current live value at requested path.
    const currentValue=this.getReferenceAtPath(jsonPath);

    // Conditional: existing array can be reused.
    if(Array.isArray(currentValue)){
      return currentValue;
    }

    // Function call: replace the path with an array.
    this.setValueAtPath(jsonPath,[]);
    return this.getReferenceAtPath(jsonPath);
  }

  // Function: append a JSON-compatible value to an array path by appending a new frozen snapshot.
  appendValueToArrayAtPath(jsonPath,jsonCompatibleValue){
    // Assignment: target array path parts.
    const jsonPathParts=PipelineJsonDocumentFactory.convertJsonPathToPathParts(jsonPath);
    // Assignment: normalized value to append.
    const normalizedValueToAppend=PipelineJsonDocumentFactory.normalizeJsonCompatibleValue(jsonCompatibleValue,"$");
    // Assignment: clone current root so history snapshots are not mutated in place.
    let nextRoot=PipelineSecurityUtility.cloneStrictJsonValue(this.currentJsonRoot);

    // Conditional: root path appends into the root array itself.
    if(jsonPathParts.length===0){
      if(!Array.isArray(nextRoot)){
        nextRoot=[];
      }

      nextRoot.push(PipelineSecurityUtility.cloneStrictJsonValue(normalizedValueToAppend));
      return this.appendTransformationSnapshot(nextRoot,{event:"append-value-to-root-array"});
    }

    // Conditional: non-container root becomes an object for nested array path creation.
    if(!PipelineJsonDocumentFactory.isObjectOrArrayContainer(nextRoot)){
      nextRoot={};
    }

    // Assignment: cursor walks cloned root.
    let currentCursor=nextRoot;

    // Loop: ensure parent containers.
    for(let pathPartIndex=0;pathPartIndex<jsonPathParts.length;pathPartIndex+=1){
      const currentPathPart=jsonPathParts[pathPartIndex];
      const isFinalPathPart=pathPartIndex===jsonPathParts.length-1;

      if(isFinalPathPart){
        if(!Array.isArray(currentCursor[currentPathPart])){
          currentCursor[currentPathPart]=[];
        }

        currentCursor=currentCursor[currentPathPart];
      }else{
        const nextPathPart=jsonPathParts[pathPartIndex+1];
        if(!PipelineJsonDocumentFactory.isObjectOrArrayContainer(currentCursor[currentPathPart])){
          currentCursor[currentPathPart]=PipelineJsonDocumentFactory.isArrayIndexPart(nextPathPart)?[]:{};
        }

        currentCursor=currentCursor[currentPathPart];
      }
    }

    // Assignment: append normalized clone into cloned target array.
    currentCursor.push(PipelineSecurityUtility.cloneStrictJsonValue(normalizedValueToAppend));
    return this.appendTransformationSnapshot(nextRoot,{event:"append-value-to-array-at-path",jsonPath:jsonPathParts.join(".")});
  }

  // Function: read preferred text path, falling back to JSON stringification.
  getTextFromPathOrStringifiedDocument(jsonPath="text"){
    // Conditional: prefer a string at the requested path.
    if(this.hasValueAtPath(jsonPath)){
      // Assignment: live value from path.
      const possibleTextValue=this.getReferenceAtPath(jsonPath);

      // Conditional: only string values count as source text.
      if(typeof possibleTextValue==="string"){
        return possibleTextValue;
      }
    }

    return JSON.stringify(this.currentJsonRoot,null,2);
  }

  // Function: set text at path.
  setTextAtPath(textValue,jsonPath="text"){
    // Conditional: text path only accepts strings.
    if(typeof textValue!=="string"){
      throw new TypeError("Text value must be a string.");
    }

    return this.setValueAtPath(jsonPath,textValue);
  }

  // Function: transform text at path through a mapper callback.
  mapTextAtPath(textMapper,jsonPath="text"){
    // Conditional: mapper must be callable.
    if(typeof textMapper!=="function"){
      throw new TypeError("Text mapper must be a function.");
    }

    // Assignment: mapped text result.
    const mappedTextValue=textMapper(this.getTextFromPathOrStringifiedDocument(jsonPath));

    // Conditional: mapper must return text.
    if(typeof mappedTextValue!=="string"){
      throw new TypeError("Text mapper must return a string.");
    }

    return this.setTextAtPath(mappedTextValue,jsonPath);
  }
}

// Function/class: factory and static helpers for PipelineJsonDocument.
class PipelineJsonDocumentFactory{
  // Function: create a wrapped JSON document.
  static createPipelineJsonDocument(jsonCompatibleValue={},documentMetadata={}){
    return new PipelineJsonDocument(jsonCompatibleValue,documentMetadata);
  }

  // Function: detect a wrapped JSON document.
  static isPipelineJsonDocument(valueToCheck){
    return Boolean(valueToCheck&&valueToCheck.isPipelineJsonDocumentMarker===true&&typeof valueToCheck.toPlainObject==="function");
  }

  // Function: return value if wrapped, otherwise wrap it.
  static ensurePipelineJsonDocument(valueToEnsure={},documentMetadata={}){
    // Conditional: existing document is already suitable.
    if(PipelineJsonDocumentFactory.isPipelineJsonDocument(valueToEnsure)){
      return valueToEnsure;
    }

    return PipelineJsonDocumentFactory.createPipelineJsonDocument(valueToEnsure,documentMetadata);
  }

  // Function: normalize and validate JSON-compatible data.
  static normalizeJsonCompatibleValue(valueToNormalize,jsonPath="$"){
    // Assignment: unwrap documents before validating.
    const plainJsonValue=PipelineJsonDocumentFactory.isPipelineJsonDocument(valueToNormalize)
      ? valueToNormalize.toPlainObject()
      : PipelineSecurityUtility.cloneStrictJsonValue(valueToNormalize);

    // Function call: enforce JSON compatibility.
    PipelineJsonDocumentFactory.assertJsonCompatibleValue(plainJsonValue,jsonPath);
    return plainJsonValue;
  }

  // Function: reject non-JSON-compatible values.
  static assertJsonCompatibleValue(valueToCheck,jsonPath="$"){
    // Conditional: null is JSON-compatible.
    if(valueToCheck===null){
      return;
    }

    // Assignment: primitive type label.
    const valueType=typeof valueToCheck;

    // Conditional: strings and booleans are JSON-compatible.
    if(valueType==="string"||valueType==="boolean"){
      return;
    }

    // Conditional: numbers must be finite for valid JSON semantics.
    if(valueType==="number"){
      // Conditional: NaN and Infinity cannot be preserved in strict JSON.
      if(!Number.isFinite(valueToCheck)){
        throw new TypeError(jsonPath+" must be a finite JSON number.");
      }

      return;
    }

    // Conditional: arrays must be dense and recursively compatible.
    if(Array.isArray(valueToCheck)){
      // Loop: validate each array index.
      for(let arrayItemIndex=0;arrayItemIndex<valueToCheck.length;arrayItemIndex+=1){
        // Conditional: sparse slots are rejected because JSON.stringify would silently alter them.
        if(!Object.prototype.hasOwnProperty.call(valueToCheck,arrayItemIndex)){
          throw new TypeError(jsonPath+"["+arrayItemIndex+"] cannot be a sparse JSON array slot.");
        }

        // Function call: recursively validate child item.
        PipelineJsonDocumentFactory.assertJsonCompatibleValue(valueToCheck[arrayItemIndex],jsonPath+"["+arrayItemIndex+"]");
      }

      return;
    }

    // Conditional: plain objects are recursively compatible when all keys are strings and descriptors are ordinary data properties.
    if(PipelineSecurityUtility.isPlainObject(valueToCheck)){
      // Assignment: property descriptors are inspected so getters are not executed accidentally.
      const propertyDescriptorMap=Object.getOwnPropertyDescriptors(valueToCheck);

      // Loop: validate each own key.
      for(const objectKey of Reflect.ownKeys(propertyDescriptorMap)){
        // Conditional: JSON object keys cannot be symbols.
        if(typeof objectKey!=="string"){
          throw new TypeError(jsonPath+" cannot contain symbol keys in JSON data.");
        }

        // Conditional: block keys that can cause prototype-pollution surprises when merged into ordinary objects.
        if(objectKey==="__proto__"||objectKey==="prototype"||objectKey==="constructor"){
          throw new TypeError(jsonPath+" contains blocked prototype-pollution key: "+objectKey);
        }

        // Assignment: descriptor for current key.
        const propertyDescriptor=propertyDescriptorMap[objectKey];

        // Conditional: strict JSON boundaries accept only enumerable data descriptors.
        if(!propertyDescriptor.enumerable||!Object.prototype.hasOwnProperty.call(propertyDescriptor,"value")){
          throw new TypeError(jsonPath+"."+objectKey+" must be an enumerable data property.");
        }

        // Function call: recursively validate child value without invoking accessors.
        PipelineJsonDocumentFactory.assertJsonCompatibleValue(propertyDescriptor.value,jsonPath+"."+objectKey);
      }

      return;
    }

    throw new TypeError(jsonPath+" contains non-JSON value of type "+Object.prototype.toString.call(valueToCheck)+".");
  }

  // Function: convert dot/bracket path into array parts.
  static convertJsonPathToPathParts(jsonPath){
    // Conditional: array paths are accepted directly after normalizing each item.
    if(Array.isArray(jsonPath)){
      return jsonPath.map(pathPart=>typeof pathPart==="number"?pathPart:String(pathPart));
    }

    // Conditional: blank path points to the root.
    if(jsonPath===null||typeof jsonPath==="undefined"||jsonPath===""){
      return [];
    }

    // Conditional: only strings, arrays, null, and undefined are accepted.
    if(typeof jsonPath!=="string"){
      throw new TypeError("JSON path must be a string, array, null, or undefined.");
    }

    return jsonPath
      .replace(/\[(\d+)\]/g,".$1")
      .split(".")
      .filter(Boolean)
      .map(pathPart=>/^\d+$/.test(pathPart)?Number(pathPart):pathPart);
  }

  // Function: test whether a value can contain child values.
  static isObjectOrArrayContainer(valueToCheck){
    return valueToCheck!==null&&typeof valueToCheck==="object";
  }

  // Function: test whether a path part means an array index.
  static isArrayIndexPart(pathPart){
    return Number.isInteger(pathPart)&&pathPart>=0;
  }
}

// Assignment: proxy handler lets simple property access mirror the current JSON root when possible.
PipelineJsonDocumentFactory.documentProxyHandler=Object.freeze({
  // Function: read document methods/properties first, then root properties.
  get(targetDocument,propertyKey,receiver){
    // Conditional: document member wins over JSON payload member.
    if(propertyKey in targetDocument){
      return Reflect.get(targetDocument,propertyKey,receiver);
    }

    // Conditional: proxy current root object/array fields when available.
    if(PipelineJsonDocumentFactory.isObjectOrArrayContainer(targetDocument.currentJsonRoot)&&propertyKey in targetDocument.currentJsonRoot){
      return targetDocument.currentJsonRoot[propertyKey];
    }

    return undefined;
  },

  // Function: set root field unless setting a known document member.
  set(targetDocument,propertyKey,propertyValue){
    // Conditional: internal members remain direct assignments.
    if(propertyKey in targetDocument){
      // Assignment: set target member through Reflect.
      return Reflect.set(targetDocument,propertyKey,propertyValue);
    }

    // Assignment: clone current root so frozen history snapshots are never mutated in place.
    let nextRoot=PipelineSecurityUtility.deepCloneJsonSafeValue(targetDocument.currentJsonRoot);

    // Conditional: non-container root becomes an object for property writes.
    if(!PipelineJsonDocumentFactory.isObjectOrArrayContainer(nextRoot)){
      // Assignment: create object root.
      nextRoot={};
    }

    // Assignment: write normalized value into cloned current root.
    nextRoot[propertyKey]=PipelineJsonDocumentFactory.normalizeJsonCompatibleValue(propertyValue,"$"+String(propertyKey));
    // Function call: append proxy mutation as a history snapshot.
    targetDocument.appendTransformationSnapshot(nextRoot,{event:"proxy-property-set",propertyKey:String(propertyKey)});
    return true;
  },

  // Function: report membership in document or root.
  has(targetDocument,propertyKey){
    return propertyKey in targetDocument||(PipelineJsonDocumentFactory.isObjectOrArrayContainer(targetDocument.currentJsonRoot)&&propertyKey in targetDocument.currentJsonRoot);
  },

  // Function: enumerate document root keys for object-like tooling.
  ownKeys(targetDocument){
    // Conditional: root keys are available only when root is a container.
    if(PipelineJsonDocumentFactory.isObjectOrArrayContainer(targetDocument.currentJsonRoot)){
      return Reflect.ownKeys(targetDocument.currentJsonRoot);
    }

    return [];
  },

  // Function: get descriptor for root keys.
  getOwnPropertyDescriptor(targetDocument,propertyKey){
    // Conditional: descriptor comes from root when root owns the key.
    if(PipelineJsonDocumentFactory.isObjectOrArrayContainer(targetDocument.currentJsonRoot)&&Object.prototype.hasOwnProperty.call(targetDocument.currentJsonRoot,propertyKey)){
      return {value:targetDocument.currentJsonRoot[propertyKey],writable:true,enumerable:true,configurable:true};
    }

    return Reflect.getOwnPropertyDescriptor(targetDocument,propertyKey);
  },

  // Function: delete root keys only.
  deleteProperty(targetDocument,propertyKey){
    // Conditional: only current root own properties can be deleted through proxy syntax.
    if(PipelineJsonDocumentFactory.isObjectOrArrayContainer(targetDocument.currentJsonRoot)&&Object.prototype.hasOwnProperty.call(targetDocument.currentJsonRoot,propertyKey)){
      return targetDocument.deleteValueAtPath(String(propertyKey));
    }

    return false;
  }
});

// Function/class: shared text analysis helpers used by stateful and stream plugins.
class PipelineTextAnalysisUtility{
  // Function: tokenize source text into word, number, and symbol tokens.
  static tokenizeSourceTextIntoTokenRecords(sourceText){
    // Assignment: Unicode-aware regex recognizes words/numbers and single non-space symbols.
    const tokenScanningRegularExpression=/\b[\p{L}\p{N}_'-]+\b|[^\s]/gu;
    // Assignment: token records are collected here.
    const tokenRecords=[];
    // Assignment: regex match object for the scan loop.
    let currentRegularExpressionMatch;
    // Assignment: monotonically increasing token index.
    let tokenRecordIndex=0;

    // Loop: scan every token-like match in source text.
    while((currentRegularExpressionMatch=tokenScanningRegularExpression.exec(sourceText))!==null){
      // Assignment: matched token text.
      const tokenTextValue=currentRegularExpressionMatch[0];
      // Assignment: classify the token for downstream word counting.
      const tokenTypeName=/^[0-9]+(?:\.[0-9]+)?$/.test(tokenTextValue)?"number":/^[\p{L}_'-]+$/u.test(tokenTextValue)?"word":"symbol";

      // Assignment: append token record.
      tokenRecords.push({index:tokenRecordIndex,value:tokenTextValue,start:currentRegularExpressionMatch.index,end:currentRegularExpressionMatch.index+tokenTextValue.length,type:tokenTypeName});
      // Assignment: advance token index.
      tokenRecordIndex+=1;
    }

    // Assignment: frequency table stores lower-case token frequencies.
    const tokenFrequencyByLowerCaseValue={};

    // Loop: build token frequency table.
    for(const tokenRecord of tokenRecords){
      // Assignment: case-folded token key.
      const tokenFrequencyKey=tokenRecord.value.toLowerCase();
      // Assignment: increment frequency counter.
      tokenFrequencyByLowerCaseValue[tokenFrequencyKey]=(tokenFrequencyByLowerCaseValue[tokenFrequencyKey]||0)+1;
    }

    return {sourceLength:sourceText.length,tokenCount:tokenRecords.length,tokens:tokenRecords,frequencies:tokenFrequencyByLowerCaseValue,tokenizedAt:new Date().toISOString()};
  }

  // Function: count only word tokens from a tokenizer state object.
  static countWordTokens(tokenizerStateObject){
    // Assignment: token array fallback.
    const tokenRecords=Array.isArray(tokenizerStateObject&&tokenizerStateObject.tokens)?tokenizerStateObject.tokens:[];
    // Assignment: count matching word token records.
    const wordTokenCount=tokenRecords.filter(tokenRecord=>tokenRecord.type==="word").length;
    return wordTokenCount;
  }
}

// Function/class: context helper centralizes document wrapping and diagnostic writes.
class PipelineContextUtility{
  // Function: ensure a runtime context has required mutable arrays.
  static ensurePipelineContextShape(pipelineContext){
    // Conditional: context must be object-like.
    if(pipelineContext===null||typeof pipelineContext!=="object"){
      throw new TypeError("Pipeline context must be an object.");
    }

    // Conditional/assignment: ensure diagnostics array exists.
    if(!Array.isArray(pipelineContext.diagnostics)){
      pipelineContext.diagnostics=[];
    }

    // Conditional/assignment: ensure errors array exists.
    if(!Array.isArray(pipelineContext.errors)){
      pipelineContext.errors=[];
    }

    // Conditional/assignment: ensure stream output array exists.
    if(!Array.isArray(pipelineContext.stream)){
      pipelineContext.stream=[];
    }

    return pipelineContext;
  }

  // Function: ensure generic plugin state containers exist.
  static ensureGenericStateContainer(pipelineContext){
    // Function call: ensure base context arrays before adding state containers.
    PipelineContextUtility.ensurePipelineContextShape(pipelineContext);

    // Conditional/assignment: create the generic state root on first use.
    if(!pipelineContext.state||typeof pipelineContext.state!=="object"){
      pipelineContext.state={byInstanceId:{},byTypeId:{},timeline:[]};
    }

    // Conditional/assignment: byInstanceId stores latest records by plugin instance and namespace.
    if(!pipelineContext.state.byInstanceId||typeof pipelineContext.state.byInstanceId!=="object"||Array.isArray(pipelineContext.state.byInstanceId)){
      pipelineContext.state.byInstanceId={};
    }

    // Conditional/assignment: byTypeId stores historical records by plugin type.
    if(!pipelineContext.state.byTypeId||typeof pipelineContext.state.byTypeId!=="object"||Array.isArray(pipelineContext.state.byTypeId)){
      pipelineContext.state.byTypeId={};
    }

    // Conditional/assignment: timeline stores every generic state write in order.
    if(!Array.isArray(pipelineContext.state.timeline)){
      pipelineContext.state.timeline=[];
    }

    return pipelineContext.state;
  }

  // Function: normalize plugin ownership metadata for generic state records.
  static createPluginStateOwnerDescription(pluginStateOwner){
    // Conditional: plugins and descriptor objects are both accepted.
    const ownerInstanceId=pluginStateOwner&&typeof pluginStateOwner.instanceId==="string"&&pluginStateOwner.instanceId
      ? pluginStateOwner.instanceId
      : pluginStateOwner&&typeof pluginStateOwner.id==="string"&&pluginStateOwner.id
        ? pluginStateOwner.id
        : "unknown.instance";
    // Assignment: stable plugin type id fallback.
    const ownerTypeId=pluginStateOwner&&typeof pluginStateOwner.typeId==="string"&&pluginStateOwner.typeId
      ? pluginStateOwner.typeId
      : "unknown.type";

    return {ownerInstanceId,ownerTypeId};
  }

  // Function: write a namespaced plugin state record through the generic state container.
  static setPluginState(pipelineContext,pluginStateOwner,stateNamespace,stateValue,stateOptions={}){
    // Function call: ensure state container exists.
    const genericStateContainer=PipelineContextUtility.ensureGenericStateContainer(pipelineContext);

    // Conditional: state namespace must be a useful label.
    if(typeof stateNamespace!=="string"||!stateNamespace.trim()){
      throw new TypeError("Plugin state namespace must be a non-empty string.");
    }

    // Function call: derive plugin identity.
    const {ownerInstanceId,ownerTypeId}=PipelineContextUtility.createPluginStateOwnerDescription(pluginStateOwner);
    // Assignment: normalized namespace.
    const normalizedStateNamespace=stateNamespace.trim();
    // Assignment: state visibility is public unless the plugin marks it otherwise.
    const stateVisibility=typeof stateOptions.visibility==="string"&&stateOptions.visibility.trim()?stateOptions.visibility.trim():"public";
    // Assignment: metadata is cloned to prevent external mutation.
    const stateMetadata=PipelineSecurityUtility.cloneStrictJsonValue(stateOptions.metadata||{});

    // Conditional/assignment: create instance bucket.
    if(!genericStateContainer.byInstanceId[ownerInstanceId]||typeof genericStateContainer.byInstanceId[ownerInstanceId]!=="object"){
      genericStateContainer.byInstanceId[ownerInstanceId]={};
    }

    // Assignment: prior record determines version when the caller does not provide one.
    const previousStateRecord=genericStateContainer.byInstanceId[ownerInstanceId][normalizedStateNamespace];
    const nextStateVersion=Number.isInteger(stateOptions.version)
      ? stateOptions.version
      : previousStateRecord&&Number.isInteger(previousStateRecord.version)
        ? previousStateRecord.version+1
        : 1;

    // Assignment: normalized state record.
    const pluginStateRecord={ownerInstanceId,ownerTypeId,namespace:normalizedStateNamespace,value:PipelineSecurityUtility.cloneStrictJsonValue(stateValue),visibility:stateVisibility,version:nextStateVersion,metadata:stateMetadata,updatedAt:new Date().toISOString()};

    // Assignment: latest-by-instance namespace write.
    genericStateContainer.byInstanceId[ownerInstanceId][normalizedStateNamespace]=pluginStateRecord;

    // Conditional/assignment: create type bucket.
    if(!Array.isArray(genericStateContainer.byTypeId[ownerTypeId])){
      genericStateContainer.byTypeId[ownerTypeId]=[];
    }

    // Assignment: append to type and global timeline histories.
    genericStateContainer.byTypeId[ownerTypeId].push(pluginStateRecord);
    genericStateContainer.timeline.push(pluginStateRecord);

    return PipelineSecurityUtility.cloneStrictJsonValue(pluginStateRecord);
  }

  // Function: read generic state by plugin instance id and optional namespace.
  static getPluginStateByInstanceId(pipelineContext,pluginInstanceId,stateNamespace=undefined,defaultValue=null){
    // Function call: ensure container exists.
    const genericStateContainer=PipelineContextUtility.ensureGenericStateContainer(pipelineContext);

    // Conditional: instance id must be usable.
    if(typeof pluginInstanceId!=="string"||!pluginInstanceId){
      return defaultValue;
    }

    // Assignment: state bucket for instance.
    const instanceStateBucket=genericStateContainer.byInstanceId[pluginInstanceId];

    // Conditional: absent instance returns default.
    if(!instanceStateBucket){
      return defaultValue;
    }

    // Conditional: absent namespace returns all state for instance.
    if(typeof stateNamespace==="undefined"||stateNamespace===null){
      return PipelineSecurityUtility.cloneStrictJsonValue(instanceStateBucket);
    }

    // Assignment: normalized namespace.
    const normalizedStateNamespace=String(stateNamespace).trim();

    // Conditional: namespace bucket may be absent.
    if(!Object.prototype.hasOwnProperty.call(instanceStateBucket,normalizedStateNamespace)){
      return defaultValue;
    }

    return PipelineSecurityUtility.cloneStrictJsonValue(instanceStateBucket[normalizedStateNamespace]);
  }

  // Function: read the latest generic state record by plugin type id and optional namespace.
  static getLatestPluginStateByTypeId(pipelineContext,pluginTypeId,stateNamespace=undefined,defaultValue=null){
    // Function call: ensure container exists.
    const genericStateContainer=PipelineContextUtility.ensureGenericStateContainer(pipelineContext);

    // Conditional: type id must be usable.
    if(typeof pluginTypeId!=="string"||!pluginTypeId){
      return defaultValue;
    }

    // Assignment: type history bucket.
    const typeStateRecords=Array.isArray(genericStateContainer.byTypeId[pluginTypeId])?genericStateContainer.byTypeId[pluginTypeId]:[];

    // Conditional: absent state returns default.
    if(typeStateRecords.length===0){
      return defaultValue;
    }

    // Assignment: namespace filter is optional.
    const normalizedStateNamespace=typeof stateNamespace==="undefined"||stateNamespace===null?null:String(stateNamespace).trim();

    // Loop: scan backward for latest matching state.
    for(let stateRecordIndex=typeStateRecords.length-1;stateRecordIndex>=0;stateRecordIndex-=1){
      const stateRecord=typeStateRecords[stateRecordIndex];

      if(!normalizedStateNamespace||stateRecord.namespace===normalizedStateNamespace){
        return PipelineSecurityUtility.cloneStrictJsonValue(stateRecord);
      }
    }

    return defaultValue;
  }

  // Function: create a JSON-safe generic state snapshot for output surfaces.
  static createGenericStateSnapshot(pipelineContext,stateSnapshotPolicy={}){
    // Function call: ensure container exists.
    const genericStateContainer=PipelineContextUtility.ensureGenericStateContainer(pipelineContext);
    // Assignment: timeline can be suppressed for compact output.
    const includeTimeline=Object.prototype.hasOwnProperty.call(stateSnapshotPolicy,"includeTimeline")?Boolean(stateSnapshotPolicy.includeTimeline):true;
    // Assignment: type history can be suppressed if only instance-latest state is desired.
    const includeTypeHistory=Object.prototype.hasOwnProperty.call(stateSnapshotPolicy,"includeTypeHistory")?Boolean(stateSnapshotPolicy.includeTypeHistory):true;

    return {
      byInstanceId:PipelineSecurityUtility.cloneStrictJsonValue(genericStateContainer.byInstanceId),
      byTypeId:includeTypeHistory?PipelineSecurityUtility.cloneStrictJsonValue(genericStateContainer.byTypeId):{},
      timeline:includeTimeline?PipelineSecurityUtility.cloneStrictJsonValue(genericStateContainer.timeline):[]
    };
  }

  // Function: ensure context.data is a PipelineJsonDocument.
  static ensureContextDataDocument(pipelineContext){
    // Function call: ensure context arrays.
    PipelineContextUtility.ensurePipelineContextShape(pipelineContext);
    // Assignment: wrap data if needed.
    pipelineContext.data=PipelineJsonDocumentFactory.ensurePipelineJsonDocument(pipelineContext.data||{});
    return pipelineContext.data;
  }

  // Function: replace context.data with a new document.
  static replaceContextDataDocument(pipelineContext,jsonCompatibleValue,documentMetadata={}){
    // Function call: ensure context arrays.
    PipelineContextUtility.ensurePipelineContextShape(pipelineContext);
    // Assignment: new document replaces current data.
    pipelineContext.data=PipelineJsonDocumentFactory.createPipelineJsonDocument(jsonCompatibleValue,documentMetadata);
    return pipelineContext.data;
  }

  // Function: read current data as plain JSON.
  static getCurrentDataPlainObject(pipelineContext){
    return PipelineContextUtility.ensureContextDataDocument(pipelineContext).toPlainObject();
  }

  // Function: read original data as plain JSON.
  static getOriginalDataPlainObject(pipelineContext){
    return PipelineContextUtility.ensureContextDataDocument(pipelineContext).toOriginalPlainObject();
  }

  // Function: read source text from current context data.
  static getCurrentSourceText(pipelineContext,jsonPath="text"){
    return PipelineContextUtility.ensureContextDataDocument(pipelineContext).getTextFromPathOrStringifiedDocument(jsonPath);
  }

  // Function: set a value in the current data document.
  static setCurrentDataPathValue(pipelineContext,jsonPath,jsonCompatibleValue,pathOptions={}){
    // Function call: set path on the wrapped document.
    PipelineContextUtility.ensureContextDataDocument(pipelineContext).setValueAtPath(jsonPath,jsonCompatibleValue,pathOptions);
    return pipelineContext;
  }

  // Function: merge object into current data document.
  static mergeCurrentDataObject(pipelineContext,objectToMerge,mergeOptions={}){
    // Function call: merge object on the wrapped document.
    PipelineContextUtility.ensureContextDataDocument(pipelineContext).mergeObjectAtPath(objectToMerge,mergeOptions);
    return pipelineContext;
  }

  // Function: append a diagnostic with timestamp.
  static appendDiagnosticRecord(pipelineContext,diagnosticRecord){
    // Function call: ensure context shape.
    PipelineContextUtility.ensurePipelineContextShape(pipelineContext);
    // Function call: enforce diagnostic object shape.
    PipelineSecurityUtility.assertPlainObject(diagnosticRecord,"Diagnostic record");
    // Assignment: timestamped diagnostic clone.
    const timestampedDiagnosticRecord={time:new Date().toISOString(),...PipelineSecurityUtility.cloneStrictJsonValue(diagnosticRecord)};
    // Assignment: append diagnostic record.
    pipelineContext.diagnostics.push(timestampedDiagnosticRecord);
    return timestampedDiagnosticRecord;
  }

  // Function: append an error with timestamp.
  static appendErrorRecord(pipelineContext,errorRecord){
    // Function call: ensure context shape.
    PipelineContextUtility.ensurePipelineContextShape(pipelineContext);
    // Function call: enforce error object shape.
    PipelineSecurityUtility.assertPlainObject(errorRecord,"Error record");
    // Assignment: timestamped error clone.
    const timestampedErrorRecord={time:new Date().toISOString(),...PipelineSecurityUtility.cloneStrictJsonValue(errorRecord)};
    // Assignment: append error record.
    pipelineContext.errors.push(timestampedErrorRecord);
    return timestampedErrorRecord;
  }

  // Function: append a stream emission record.
  static appendStreamRecord(pipelineContext,streamRecord){
    // Function call: ensure context shape.
    PipelineContextUtility.ensurePipelineContextShape(pipelineContext);
    // Function call: enforce stream object shape.
    PipelineSecurityUtility.assertPlainObject(streamRecord,"Stream record");
    // Assignment: timestamped stream record.
    const timestampedStreamRecord={emittedAt:new Date().toISOString(),...PipelineSecurityUtility.cloneStrictJsonValue(streamRecord)};
    // Assignment: append stream record.
    pipelineContext.stream.push(timestampedStreamRecord);
    return timestampedStreamRecord;
  }

  // Function: create stable output snapshot for stateful output plugins.
  static createStatefulOutputSnapshot(pipelineContext,snapshotOptions={}){
    // Function call: ensure context shape.
    PipelineContextUtility.ensurePipelineContextShape(pipelineContext);
    // Assignment: current document is captured once for stable output fields.
    const currentDataDocument=PipelineContextUtility.ensureContextDataDocument(pipelineContext);
    // Assignment: generic state snapshot replaces hardwired plugin-specific output ownership.
    const genericStateSnapshot=PipelineContextUtility.createGenericStateSnapshot(pipelineContext,snapshotOptions.stateSnapshotPolicy||{});

    return {
      runId:pipelineContext.runId,
      cycle:pipelineContext.cycle,
      mode:"stateful",
      data:currentDataDocument.toPlainObject(),
      originalData:currentDataDocument.toOriginalPlainObject(),
      dataHistory:typeof currentDataDocument.toHistoryPlainArray==="function"?currentDataDocument.toHistoryPlainArray():[currentDataDocument.toPlainObject()],
      dataHistoryMetadata:typeof currentDataDocument.toHistoryMetadataPlainArray==="function"?currentDataDocument.toHistoryMetadataPlainArray():[],
      state:genericStateSnapshot,
      // Compatibility aliases retained until tests, UI output, and downstream consumers migrate to `state`.
      tokenizerState:PipelineSecurityUtility.cloneStrictJsonValue(pipelineContext.tokenizerState||{}),
      regexState:PipelineSecurityUtility.cloneStrictJsonValue(pipelineContext.regexState||{}),
      diagnostics:PipelineSecurityUtility.cloneStrictJsonValue(pipelineContext.diagnostics||[]),
      errors:PipelineSecurityUtility.cloneStrictJsonValue(pipelineContext.errors||[])
    };
  }
}

// Function/class: bounded in-memory debug logger.
class PipelineDebugLogger{
  // Function: construct logger.
  constructor({enabled=true,maxEntries=1000}={}){
    // Assignment: global logging enable flag.
    this.enabled=enabled;
    // Assignment: maximum retained log entries.
    this.maxEntries=maxEntries;
    // Assignment: retained log entries.
    this.entries=[];
  }

  // Function: append a debug entry when logging is enabled.
  log(logLevelName,logMessage,logMetadata={}){
    // Conditional: disabled logger drops messages.
    if(!this.enabled){
      return;
    }

    // Assignment: append timestamped log entry.
    this.entries.push({time:new Date().toISOString(),level:logLevelName,message:logMessage,meta:PipelineSecurityUtility.deepCloneJsonSafeValue(logMetadata)});

    // Conditional: trim oldest entries after cap is exceeded.
    if(this.entries.length>this.maxEntries){
      // Assignment: discard oldest entry.
      this.entries.shift();
    }
  }

  // Function: clear all debug records.
  clear(){
    // Assignment: replace entries with empty array.
    this.entries=[];
  }

  // Function: clone debug entries for UI rendering.
  toJsonSerializableArray(){
    return PipelineSecurityUtility.deepCloneJsonSafeValue(this.entries);
  }
}


// Assignment: define lifecycle phases emitted by the out-of-band pipeline execution hook bus.
const pipelineExecutionHookPhaseNameByEvent=Object.freeze({
  PIPELINE_EXECUTION_STARTED:"pipeline-execution-started",
  PIPELINE_EXECUTION_FINISHED:"pipeline-execution-finished",
  PIPELINE_EXECUTION_FAILED:"pipeline-execution-failed",
  MODULE_EXECUTION_STARTED:"module-execution-started",
  MODULE_EXECUTION_FINISHED:"module-execution-finished",
  MODULE_EXECUTION_FAILED:"module-execution-failed",
  MODULE_EXECUTION_SKIPPED:"module-execution-skipped",
  JUNCTION_PIPELINE_ENTERED:"junction-pipeline-entered",
  JUNCTION_PIPELINE_RETURNED:"junction-pipeline-returned",
  FILTER_DECISION_RECORDED:"filter-decision-recorded",
  BREAK_SIGNAL_RECORDED:"break-signal-recorded",
  INSPECTION_SNAPSHOT_RECORDED:"inspection-snapshot-recorded",
  CONTEXT_PATCH_APPLIED:"context-patch-applied"
});

// Assignment: define what happens when a hook callback itself fails.
const pipelineExecutionHookFailurePolicyNameByAction=Object.freeze({
  RECORD_AND_CONTINUE:"record-and-continue",
  DISABLE_HOOK_AND_CONTINUE:"disable-hook-and-continue",
  FAIL_PIPELINE_EXECUTION:"fail-pipeline-execution"
});

// Assignment: define hook callback execution mode metadata for later sync-runner validation.
const pipelineExecutionHookExecutionModeNameByMode=Object.freeze({
  SYNCHRONOUS:"synchronous",
  ASYNCHRONOUS:"asynchronous"
});

// Function/class: security guard for immutable and redacted hook event payloads.
class PipelineExecutionHookSecurityGuard{
  // Function: deeply freeze plain event data so observer hooks cannot mutate emitted events.
  static deepFreezePlainData(valueToFreeze,objectReferenceSet=new WeakSet()){
    // Conditional: primitives and null require no freezing.
    if(valueToFreeze===null||typeof valueToFreeze!=="object"){
      return valueToFreeze;
    }

    // Conditional: repeated object references are already processed.
    if(objectReferenceSet.has(valueToFreeze)){
      return valueToFreeze;
    }

    // Assignment: remember this object before recursing into child values.
    objectReferenceSet.add(valueToFreeze);

    // Conditional: arrays and plain objects receive recursive freezing.
    if(Array.isArray(valueToFreeze)||PipelineSecurityUtility.isPlainObject(valueToFreeze)){
      // Loop: freeze every own child value before freezing the container.
      for(const ownPropertyKey of Reflect.ownKeys(valueToFreeze)){
        // Assignment: child value read from the event payload.
        const childValueToFreeze=valueToFreeze[ownPropertyKey];
        // Function call: recursively freeze child payloads.
        PipelineExecutionHookSecurityGuard.deepFreezePlainData(childValueToFreeze,objectReferenceSet);
      }
    }

    // Function call: freeze the current object or array.
    return Object.freeze(valueToFreeze);
  }

  // Function: clone an event payload and deeply freeze the cloned copy before dispatch.
  static createSafeFrozenHookEventPayload(hookEventPayload){
    // Assignment: cloned event prevents hooks from holding live runtime objects.
    const clonedHookEventPayload=PipelineSecurityUtility.cloneStrictJsonValue(hookEventPayload);
    // Function call: freeze the cloned payload before handing it to hooks.
    return PipelineExecutionHookSecurityGuard.deepFreezePlainData(clonedHookEventPayload);
  }

  // Function: create a compact error object safe for logs, hook records, and JSON output.
  static createSafeErrorSummary(errorValue){
    // Assignment: fallback message for non-Error throws.
    const errorMessage=errorValue&&typeof errorValue.message==="string"?errorValue.message:String(errorValue);
    // Assignment: stack is included for local debugging when available.
    const errorStack=errorValue&&typeof errorValue.stack==="string"?errorValue.stack:null;
    return {message:errorMessage,stack:errorStack};
  }
}

// Function/class: immutable registration metadata plus mutable enabled flag for one hook listener.
class PipelineExecutionHookRegistrationRecord{
  // Function: validate and store one hook registration.
  constructor(hookRegistrationRecord){
    // Function call: ensure registration is an object.
    PipelineSecurityUtility.assertPlainObject(hookRegistrationRecord,"Pipeline execution hook registration record");

    // Assignment: hook id uniquely identifies one hook listener.
    const hookRegistrationId=hookRegistrationRecord.hookRegistrationId;
    // Assignment: human readable label appears in debug records.
    const hookHumanReadableLabel=hookRegistrationRecord.hookHumanReadableLabel;
    // Assignment: lifecycle phase determines when the hook fires.
    const hookPhase=hookRegistrationRecord.hookPhase;
    // Assignment: hook priority controls deterministic ordering.
    const hookPriority=Object.prototype.hasOwnProperty.call(hookRegistrationRecord,"hookPriority")?Number(hookRegistrationRecord.hookPriority):100;
    // Assignment: callback invoked by the hook bus.
    const hookCallbackFunction=hookRegistrationRecord.hookCallbackFunction;
    // Assignment: failure policy defaults to non-fatal recording.
    const hookFailurePolicy=hookRegistrationRecord.hookFailurePolicy||pipelineExecutionHookFailurePolicyNameByAction.RECORD_AND_CONTINUE;
    // Assignment: execution mode records whether a future sync runner may use the hook.
    const hookExecutionMode=hookRegistrationRecord.hookExecutionMode||pipelineExecutionHookExecutionModeNameByMode.ASYNCHRONOUS;

    // Conditional: hook id must be a non-empty string.
    if(typeof hookRegistrationId!=="string"||!hookRegistrationId.trim()){
      throw new TypeError("Hook registration id must be a non-empty string.");
    }

    // Conditional: hook label must be a non-empty string.
    if(typeof hookHumanReadableLabel!=="string"||!hookHumanReadableLabel.trim()){
      throw new TypeError("Hook human readable label must be a non-empty string.");
    }

    // Conditional: hook phase must be one of the stable known lifecycle phase names.
    if(!Object.values(pipelineExecutionHookPhaseNameByEvent).includes(hookPhase)){
      throw new TypeError("Unknown pipeline execution hook phase: "+hookPhase);
    }

    // Conditional: priority must be a finite number so sorting remains deterministic.
    if(!Number.isFinite(hookPriority)){
      throw new TypeError("Hook priority must be a finite number.");
    }

    // Conditional: callback must be executable code supplied by trusted application code.
    if(typeof hookCallbackFunction!=="function"){
      throw new TypeError("Hook callback function must be callable.");
    }

    // Conditional: failure policy must be known.
    if(!Object.values(pipelineExecutionHookFailurePolicyNameByAction).includes(hookFailurePolicy)){
      throw new TypeError("Unknown hook failure policy: "+hookFailurePolicy);
    }

    // Conditional: execution mode must be known.
    if(!Object.values(pipelineExecutionHookExecutionModeNameByMode).includes(hookExecutionMode)){
      throw new TypeError("Unknown hook execution mode: "+hookExecutionMode);
    }

    // Assignment: stable hook id.
    this.hookRegistrationId=hookRegistrationId.trim();
    // Assignment: stable human-readable hook label.
    this.hookHumanReadableLabel=hookHumanReadableLabel.trim();
    // Assignment: registered lifecycle phase.
    this.hookPhase=hookPhase;
    // Assignment: priority used during phase dispatch.
    this.hookPriority=hookPriority;
    // Assignment: callback function kept private to JSON snapshots.
    this.hookCallbackFunction=hookCallbackFunction;
    // Assignment: failure policy for callback exceptions.
    this.hookFailurePolicy=hookFailurePolicy;
    // Assignment: execution mode metadata.
    this.hookExecutionMode=hookExecutionMode;
    // Assignment: enabled flag can be changed by failure policy or public API.
    this.hookIsEnabled=true;
    // Assignment: creation timestamp for debugging.
    this.registeredAt=new Date().toISOString();
  }

  // Function: create JSON-safe hook description without exposing callback source code.
  toJsonSerializableHookDescription(){
    return {hookRegistrationId:this.hookRegistrationId,hookHumanReadableLabel:this.hookHumanReadableLabel,hookPhase:this.hookPhase,hookPriority:this.hookPriority,hookExecutionMode:this.hookExecutionMode,hookFailurePolicy:this.hookFailurePolicy,hookIsEnabled:this.hookIsEnabled,registeredAt:this.registeredAt};
  }
}

// Function/class: builds compact lifecycle event payloads for hook dispatch.
class PipelineExecutionHookEventFactory{
  // Function: create a compact context summary without copying the full context data payload.
  static createSafeContextSummary(pipelineContext){
    // Assignment: fallback summary for absent contexts.
    if(!pipelineContext||typeof pipelineContext!=="object"){
      return {contextIsAvailable:false};
    }

    // Assignment: data document may be a wrapped PipelineJsonDocument or a plain JSON value.
    const contextDataValue=pipelineContext.data;
    // Assignment: top-level data keys default to empty when the data cannot be summarized cheaply.
    let dataTopLevelKeys=[];

    try{
      // Assignment: current plain data is used only for key extraction, not full payload output.
      const currentDataPlainObject=PipelineJsonDocumentFactory.isPipelineJsonDocument(contextDataValue)?contextDataValue.toPlainObject():contextDataValue;
      // Conditional: object roots expose only their top-level keys.
      if(currentDataPlainObject&&typeof currentDataPlainObject==="object"&&!Array.isArray(currentDataPlainObject)){
        dataTopLevelKeys=Object.keys(currentDataPlainObject).slice(0,50);
      }
    }catch(contextSummaryError){
      // Assignment: summary errors are reduced to a small marker.
      dataTopLevelKeys=["<data-summary-unavailable>"];
      void contextSummaryError;
    }

    // Assignment: tokenizer token count is available in several pipeline families.
    const tokenizerTokenCount=Number.isFinite(pipelineContext.tokenizerState&&pipelineContext.tokenizerState.tokenCount)?pipelineContext.tokenizerState.tokenCount:0;
    // Assignment: regex state count summarizes regex result buckets.
    const regexStateInstanceCount=pipelineContext.regexState&&typeof pipelineContext.regexState==="object"?Object.keys(pipelineContext.regexState).length:0;
    // Assignment: stream revision count summarizes stream transformations.
    const streamRevisionCount=pipelineContext.streamState&&Array.isArray(pipelineContext.streamState.revisions)?pipelineContext.streamState.revisions.length:0;
    // Assignment: stream emission count summarizes output stream records.
    const streamEmissionRecordCount=Array.isArray(pipelineContext.stream)?pipelineContext.stream.length:0;
    // Assignment: generic plugin state count summarizes extensible state records.
    const genericStateTimelineRecordCount=pipelineContext.state&&Array.isArray(pipelineContext.state.timeline)?pipelineContext.state.timeline.length:0;
    // Assignment: diagnostic count is bounded metadata.
    const diagnosticRecordCount=Array.isArray(pipelineContext.diagnostics)?pipelineContext.diagnostics.length:0;
    // Assignment: error count is bounded metadata.
    const errorRecordCount=Array.isArray(pipelineContext.errors)?pipelineContext.errors.length:0;
    // Assignment: control state counts are included when control-flow plugins are active.
    const skipRecordCount=pipelineContext.pipelineControlState&&Array.isArray(pipelineContext.pipelineControlState.skipRecords)?pipelineContext.pipelineControlState.skipRecords.length:0;
    // Assignment: break record count is included when break plugins are active.
    const breakRecordCount=pipelineContext.pipelineControlState&&Array.isArray(pipelineContext.pipelineControlState.breakRecords)?pipelineContext.pipelineControlState.breakRecords.length:0;
    // Assignment: junction record count is included when junction plugins are active.
    const junctionRecordCount=pipelineContext.pipelineControlState&&Array.isArray(pipelineContext.pipelineControlState.junctionRecords)?pipelineContext.pipelineControlState.junctionRecords.length:0;
    // Assignment: inspection record count is included when inspection plugins are active.
    const inspectionRecordCount=pipelineContext.pipelineInspectionState&&Array.isArray(pipelineContext.pipelineInspectionState.inspectionRecords)?pipelineContext.pipelineInspectionState.inspectionRecords.length:0;

    return {contextIsAvailable:true,contextStatus:pipelineContext.status||null,contextCycleNumber:Number.isFinite(pipelineContext.cycle)?pipelineContext.cycle:null,dataTopLevelKeys,tokenizerTokenCount,regexStateInstanceCount,streamRevisionCount,streamEmissionRecordCount,genericStateTimelineRecordCount,diagnosticRecordCount,errorRecordCount,skipRecordCount,breakRecordCount,junctionRecordCount,inspectionRecordCount};
  }

  // Function: create an event payload for a pipeline pass start.
  static createPipelineExecutionStartedEvent(pipelineRuntime,pipelineContext,eventMetadata={}){
    // Assignment: node order is reduced to stable node ids.
    const pipelineNodeOrder=Array.isArray(eventMetadata.executablePipelineNodes)?eventMetadata.executablePipelineNodes.map(pipelineNode=>pipelineNode.id):[];
    return {pipelineRunId:pipelineContext&&pipelineContext.runId?pipelineContext.runId:null,pipelineName:eventMetadata.pipelineName||"main",pipelineCycleNumber:pipelineContext&&Number.isFinite(pipelineContext.cycle)?pipelineContext.cycle:null,pipelineRuntimeState:pipelineRuntime?pipelineRuntime.state:null,pipelineNodeOrder,contextSummary:PipelineExecutionHookEventFactory.createSafeContextSummary(pipelineContext)};
  }

  // Function: create an event payload for a pipeline pass finish.
  static createPipelineExecutionFinishedEvent(pipelineRuntime,pipelineContext,eventMetadata={}){
    return {pipelineRunId:pipelineContext&&pipelineContext.runId?pipelineContext.runId:null,pipelineName:eventMetadata.pipelineName||"main",pipelineCycleNumber:pipelineContext&&Number.isFinite(pipelineContext.cycle)?pipelineContext.cycle:null,pipelineRuntimeState:pipelineRuntime?pipelineRuntime.state:null,elapsedMilliseconds:Number.isFinite(eventMetadata.elapsedMilliseconds)?eventMetadata.elapsedMilliseconds:null,contextSummary:PipelineExecutionHookEventFactory.createSafeContextSummary(pipelineContext)};
  }

  // Function: create an event payload for a pipeline pass failure.
  static createPipelineExecutionFailedEvent(pipelineRuntime,pipelineContext,pipelineExecutionError,eventMetadata={}){
    return {pipelineRunId:pipelineContext&&pipelineContext.runId?pipelineContext.runId:null,pipelineName:eventMetadata.pipelineName||"main",pipelineCycleNumber:pipelineContext&&Number.isFinite(pipelineContext.cycle)?pipelineContext.cycle:null,pipelineRuntimeState:pipelineRuntime?pipelineRuntime.state:null,error:PipelineExecutionHookSecurityGuard.createSafeErrorSummary(pipelineExecutionError),contextSummary:PipelineExecutionHookEventFactory.createSafeContextSummary(pipelineContext)};
  }

  // Function: create an event payload for a node or module start.
  static createModuleExecutionStartedEvent(pipelineRuntime,pipelineContext,eventMetadata={}){
    return PipelineExecutionHookEventFactory.createModuleLifecycleEvent(pipelineRuntime,pipelineContext,eventMetadata);
  }

  // Function: create an event payload for a node or module finish.
  static createModuleExecutionFinishedEvent(pipelineRuntime,pipelineContext,eventMetadata={}){
    return PipelineExecutionHookEventFactory.createModuleLifecycleEvent(pipelineRuntime,pipelineContext,eventMetadata);
  }

  // Function: create an event payload for a node or module failure.
  static createModuleExecutionFailedEvent(pipelineRuntime,pipelineContext,eventMetadata={}){
    const moduleLifecycleEvent=PipelineExecutionHookEventFactory.createModuleLifecycleEvent(pipelineRuntime,pipelineContext,eventMetadata);
    // Assignment: include compact error when provided.
    moduleLifecycleEvent.error=eventMetadata.error?PipelineExecutionHookSecurityGuard.createSafeErrorSummary(eventMetadata.error):null;
    // Assignment: include compact pipeline error record when provided.
    moduleLifecycleEvent.pipelineErrorRecord=eventMetadata.pipelineErrorRecord?PipelineSecurityUtility.cloneStrictJsonValue(eventMetadata.pipelineErrorRecord):null;
    return moduleLifecycleEvent;
  }

  // Function: create an event payload for a skipped node.
  static createModuleExecutionSkippedEvent(pipelineRuntime,pipelineContext,eventMetadata={}){
    const moduleLifecycleEvent=PipelineExecutionHookEventFactory.createModuleLifecycleEvent(pipelineRuntime,pipelineContext,eventMetadata);
    // Assignment: skip record is a compact control-flow payload.
    moduleLifecycleEvent.skipRecord=eventMetadata.skipRecord?PipelineSecurityUtility.cloneStrictJsonValue(eventMetadata.skipRecord):null;
    return moduleLifecycleEvent;
  }

  // Function: create a shared node or module lifecycle event payload.
  static createModuleLifecycleEvent(pipelineRuntime,pipelineContext,eventMetadata={}){
    // Assignment: current pipeline node may be a sentinel or a plugin.
    const currentPipelineNode=eventMetadata.currentPipelineNode||{};
    return {pipelineRunId:pipelineContext&&pipelineContext.runId?pipelineContext.runId:null,pipelineName:eventMetadata.pipelineName||"main",pipelineCycleNumber:pipelineContext&&Number.isFinite(pipelineContext.cycle)?pipelineContext.cycle:null,moduleExecutionIndex:Number.isInteger(eventMetadata.nodeIndex)?eventMetadata.nodeIndex:null,moduleInstanceId:currentPipelineNode.instanceId||currentPipelineNode.id||null,moduleTypeId:currentPipelineNode.typeId||null,moduleHumanReadableLabel:currentPipelineNode.label||null,moduleRuntimeType:currentPipelineNode.type||null,moduleIsSystemSentinel:Boolean(currentPipelineNode.system),moduleRequires:Array.isArray(currentPipelineNode.requires)?[...currentPipelineNode.requires]:[],moduleSupports:Array.isArray(currentPipelineNode.supports)?[...currentPipelineNode.supports]:[],elapsedMilliseconds:Number.isFinite(eventMetadata.elapsedMilliseconds)?eventMetadata.elapsedMilliseconds:null,contextSummary:PipelineExecutionHookEventFactory.createSafeContextSummary(pipelineContext)};
  }

  // Function: create a compact event for control-flow modules that already produce records.
  static createControlFlowRecordEvent(pipelineRuntime,pipelineContext,eventMetadata={}){
    return {pipelineRunId:pipelineContext&&pipelineContext.runId?pipelineContext.runId:null,pipelineName:eventMetadata.pipelineName||(pipelineRuntime&&pipelineRuntime.currentExecutionFrame&&pipelineRuntime.currentExecutionFrame.pipelineName)||"main",pipelineCycleNumber:pipelineContext&&Number.isFinite(pipelineContext.cycle)?pipelineContext.cycle:null,sourceInstanceId:eventMetadata.sourceInstanceId||null,sourceTypeId:eventMetadata.sourceTypeId||null,controlFlowRecord:eventMetadata.controlFlowRecord?PipelineSecurityUtility.cloneStrictJsonValue(eventMetadata.controlFlowRecord):null,contextSummary:PipelineExecutionHookEventFactory.createSafeContextSummary(pipelineContext)};
  }
}

// Function/class: robust hook bus for out-of-band observation of pipeline execution.
class PipelineExecutionHookBus{
  // Function: initialize hook registration storage and diagnostic buffers.
  constructor({debugLogger=null,maxRetainedHookDispatchRecords=500,maxRetainedHookFailureRecords=200}={}){
    // Assignment: debug logger receives hook bus debug records when available.
    this.debugLogger=debugLogger;
    // Assignment: registered hook records grouped by phase.
    this.hookRegistrationRecordsByHookPhase=new Map();
    // Assignment: registered hook records by id prevents duplicates and supports enable/disable.
    this.hookRegistrationRecordsByRegistrationId=new Map();
    // Assignment: retained dispatch records support debug UI and tests.
    this.retainedHookDispatchRecords=[];
    // Assignment: retained hook failures support debug UI and tests.
    this.retainedHookFailureRecords=[];
    // Assignment: maximum retained dispatch records.
    this.maxRetainedHookDispatchRecords=maxRetainedHookDispatchRecords;
    // Assignment: maximum retained hook failure records.
    this.maxRetainedHookFailureRecords=maxRetainedHookFailureRecords;
    // Assignment: monotonic event counter creates readable hook event ids.
    this.hookEventCounter=0;
  }

  // Function: register one hook listener.
  registerPipelineExecutionHook(hookRegistrationRecord){
    // Assignment: normalized registration record validates caller input.
    const normalizedHookRegistrationRecord=new PipelineExecutionHookRegistrationRecord(hookRegistrationRecord);

    // Conditional: duplicate hook ids are rejected to preserve deterministic manipulation.
    if(this.hookRegistrationRecordsByRegistrationId.has(normalizedHookRegistrationRecord.hookRegistrationId)){
      throw new Error("Duplicate pipeline execution hook registration id: "+normalizedHookRegistrationRecord.hookRegistrationId);
    }

    // Conditional/assignment: create phase bucket on first registration for a phase.
    if(!this.hookRegistrationRecordsByHookPhase.has(normalizedHookRegistrationRecord.hookPhase)){
      this.hookRegistrationRecordsByHookPhase.set(normalizedHookRegistrationRecord.hookPhase,[]);
    }

    // Assignment: store hook by phase.
    this.hookRegistrationRecordsByHookPhase.get(normalizedHookRegistrationRecord.hookPhase).push(normalizedHookRegistrationRecord);
    // Assignment: store hook by id.
    this.hookRegistrationRecordsByRegistrationId.set(normalizedHookRegistrationRecord.hookRegistrationId,normalizedHookRegistrationRecord);
    // Function call: sort matching phase bucket immediately for deterministic dispatch.
    this.sortHookRegistrationRecordsForPhase(normalizedHookRegistrationRecord.hookPhase);
    // Function call: write debug registration record.
    this.writeDebugRecord("info","Pipeline execution hook registered.",normalizedHookRegistrationRecord.toJsonSerializableHookDescription());
    return normalizedHookRegistrationRecord.toJsonSerializableHookDescription();
  }

  // Function: unregister one hook listener by id.
  unregisterPipelineExecutionHookByRegistrationId(hookRegistrationId){
    // Assignment: existing hook record.
    const existingHookRegistrationRecord=this.hookRegistrationRecordsByRegistrationId.get(hookRegistrationId);

    // Conditional: unknown ids return false without mutating state.
    if(!existingHookRegistrationRecord){
      return false;
    }

    // Assignment: matching phase bucket.
    const hookRegistrationRecordsForPhase=this.hookRegistrationRecordsByHookPhase.get(existingHookRegistrationRecord.hookPhase)||[];
    // Assignment: filtered phase bucket.
    const filteredHookRegistrationRecordsForPhase=hookRegistrationRecordsForPhase.filter(hookRecord=>hookRecord.hookRegistrationId!==hookRegistrationId);
    // Assignment: replace phase bucket.
    this.hookRegistrationRecordsByHookPhase.set(existingHookRegistrationRecord.hookPhase,filteredHookRegistrationRecordsForPhase);
    // Assignment: delete global id lookup.
    this.hookRegistrationRecordsByRegistrationId.delete(hookRegistrationId);
    // Function call: write debug unregister record.
    this.writeDebugRecord("info","Pipeline execution hook unregistered.",{hookRegistrationId});
    return true;
  }

  // Function: disable one hook without unregistering it.
  disablePipelineExecutionHookByRegistrationId(hookRegistrationId){
    // Assignment: hook record lookup.
    const existingHookRegistrationRecord=this.hookRegistrationRecordsByRegistrationId.get(hookRegistrationId);

    // Conditional: unknown ids cannot be disabled.
    if(!existingHookRegistrationRecord){
      return false;
    }

    // Assignment: set enabled flag false.
    existingHookRegistrationRecord.hookIsEnabled=false;
    // Function call: write debug disable record.
    this.writeDebugRecord("warn","Pipeline execution hook disabled.",{hookRegistrationId});
    return true;
  }

  // Function: enable one registered hook.
  enablePipelineExecutionHookByRegistrationId(hookRegistrationId){
    // Assignment: hook record lookup.
    const existingHookRegistrationRecord=this.hookRegistrationRecordsByRegistrationId.get(hookRegistrationId);

    // Conditional: unknown ids cannot be enabled.
    if(!existingHookRegistrationRecord){
      return false;
    }

    // Assignment: set enabled flag true.
    existingHookRegistrationRecord.hookIsEnabled=true;
    // Function call: write debug enable record.
    this.writeDebugRecord("info","Pipeline execution hook enabled.",{hookRegistrationId});
    return true;
  }

  // Function: clear all registered hooks and retained hook diagnostics.
  clearPipelineExecutionHooks(){
    // Assignment: reset phase map.
    this.hookRegistrationRecordsByHookPhase=new Map();
    // Assignment: reset id map.
    this.hookRegistrationRecordsByRegistrationId=new Map();
    // Assignment: reset dispatch records.
    this.retainedHookDispatchRecords=[];
    // Assignment: reset failure records.
    this.retainedHookFailureRecords=[];
    // Function call: write debug clear record.
    this.writeDebugRecord("info","All pipeline execution hooks cleared.",{});
  }

  // Function: list all registered hooks without exposing callbacks.
  listRegisteredPipelineExecutionHooks(){
    return [...this.hookRegistrationRecordsByRegistrationId.values()].map(hookRecord=>hookRecord.toJsonSerializableHookDescription());
  }

  // Function: list registered hooks for one phase without exposing callbacks.
  listRegisteredPipelineExecutionHooksForPhase(hookPhase){
    // Conditional: hook phase must be valid.
    if(!Object.values(pipelineExecutionHookPhaseNameByEvent).includes(hookPhase)){
      throw new TypeError("Unknown pipeline execution hook phase: "+hookPhase);
    }

    return (this.hookRegistrationRecordsByHookPhase.get(hookPhase)||[]).map(hookRecord=>hookRecord.toJsonSerializableHookDescription());
  }

  // Function: emit one lifecycle phase to every enabled hook registered for that phase.
  async emitPipelineExecutionHooksForLifecyclePhase(hookPhase,hookEventPayload={}){
    // Conditional: unknown phases are caller errors.
    if(!Object.values(pipelineExecutionHookPhaseNameByEvent).includes(hookPhase)){
      throw new TypeError("Unknown pipeline execution hook phase: "+hookPhase);
    }

    // Assignment: phase hook records are copied so hooks can unregister themselves safely.
    const hookRegistrationRecordsForPhase=[...(this.hookRegistrationRecordsByHookPhase.get(hookPhase)||[])].filter(hookRecord=>hookRecord.hookIsEnabled===true);
    // Assignment: event id is created once per phase dispatch.
    const hookEventId=this.createNextHookEventId();
    // Assignment: event payload receives standard envelope fields.
    const hookEventEnvelope={hookEventId,hookPhase,eventCreatedAt:new Date().toISOString(),...hookEventPayload};
    // Assignment: safe immutable event payload passed to each hook.
    const safeFrozenHookEventPayload=PipelineExecutionHookSecurityGuard.createSafeFrozenHookEventPayload(hookEventEnvelope);
    // Assignment: dispatch record tracks the phase emission.
    const hookDispatchRecord={hookEventId,hookPhase,hookCountForPhase:hookRegistrationRecordsForPhase.length,startedAt:new Date().toISOString(),completedAt:null,successfulHookCount:0,failedHookCount:0};
    // Function call: retain dispatch start record.
    this.appendRetainedHookDispatchRecord(hookDispatchRecord);
    // Function call: write debug phase start.
    this.writeDebugRecord("debug","Pipeline execution hook phase dispatch started.",{hookEventId,hookPhase,hookCountForPhase:hookRegistrationRecordsForPhase.length});

    // Loop: call every enabled hook in deterministic order.
    for(const hookRegistrationRecord of hookRegistrationRecordsForPhase){
      // Assignment: hook start time supports duration diagnostics.
      const hookCallbackStartedAtMilliseconds=Date.now();

      try{
        // Assignment: hook callback result may be direct or promise-like.
        const hookCallbackReturnValue=hookRegistrationRecord.hookCallbackFunction(safeFrozenHookEventPayload);

        // Conditional: promise-like hook results are awaited by the async hook bus.
        if(hookCallbackReturnValue&&typeof hookCallbackReturnValue.then==="function"){
          await hookCallbackReturnValue;
        }

        // Assignment: increment success count.
        hookDispatchRecord.successfulHookCount+=1;
        // Function call: write debug hook success.
        this.writeDebugRecord("debug","Pipeline execution hook completed.",{hookEventId,hookRegistrationId:hookRegistrationRecord.hookRegistrationId,hookHumanReadableLabel:hookRegistrationRecord.hookHumanReadableLabel,hookPhase,elapsedMilliseconds:Date.now()-hookCallbackStartedAtMilliseconds});
      }catch(hookCallbackError){
        // Assignment: increment failure count.
        hookDispatchRecord.failedHookCount+=1;
        // Assignment: failure record is safe for retained diagnostics.
        const hookFailureRecord={hookEventId,hookRegistrationId:hookRegistrationRecord.hookRegistrationId,hookHumanReadableLabel:hookRegistrationRecord.hookHumanReadableLabel,hookPhase,hookFailurePolicy:hookRegistrationRecord.hookFailurePolicy,error:PipelineExecutionHookSecurityGuard.createSafeErrorSummary(hookCallbackError),failedAt:new Date().toISOString()};
        // Function call: store failure.
        this.appendRetainedHookFailureRecord(hookFailureRecord);
        // Function call: write debug failure.
        this.writeDebugRecord("error","Pipeline execution hook failed.",hookFailureRecord);

        // Conditional: disable hook when the hook selected that failure policy.
        if(hookRegistrationRecord.hookFailurePolicy===pipelineExecutionHookFailurePolicyNameByAction.DISABLE_HOOK_AND_CONTINUE){
          // Assignment: disable hook after current failure.
          hookRegistrationRecord.hookIsEnabled=false;
          // Function call: write debug disable-on-failure record.
          this.writeDebugRecord("warn","Pipeline execution hook disabled after failure.",{hookRegistrationId:hookRegistrationRecord.hookRegistrationId,hookPhase});
        }

        // Conditional: fail the pipeline only when explicitly configured.
        if(hookRegistrationRecord.hookFailurePolicy===pipelineExecutionHookFailurePolicyNameByAction.FAIL_PIPELINE_EXECUTION){
          throw hookCallbackError;
        }
      }
    }

    // Assignment: dispatch completion timestamp.
    hookDispatchRecord.completedAt=new Date().toISOString();
    // Function call: write debug phase completion.
    this.writeDebugRecord("debug","Pipeline execution hook phase dispatch completed.",{hookEventId,hookPhase,successfulHookCount:hookDispatchRecord.successfulHookCount,failedHookCount:hookDispatchRecord.failedHookCount});
    return PipelineSecurityUtility.cloneStrictJsonValue(hookDispatchRecord);
  }

  // Function: sort a phase bucket by numeric priority and stable hook id.
  sortHookRegistrationRecordsForPhase(hookPhase){
    // Assignment: bucket is reused when present.
    const hookRegistrationRecordsForPhase=this.hookRegistrationRecordsByHookPhase.get(hookPhase)||[];
    // Function call: deterministic sort by priority and then id.
    hookRegistrationRecordsForPhase.sort((leftHookRecord,rightHookRecord)=>{
      // Conditional: priority decides when different.
      if(leftHookRecord.hookPriority!==rightHookRecord.hookPriority){
        return leftHookRecord.hookPriority-rightHookRecord.hookPriority;
      }

      return leftHookRecord.hookRegistrationId.localeCompare(rightHookRecord.hookRegistrationId);
    });
  }

  // Function: create the next readable hook event id.
  createNextHookEventId(){
    // Assignment: increment event counter.
    this.hookEventCounter+=1;
    return "hook-event-"+String(this.hookEventCounter).padStart(6,"0");
  }

  // Function: append retained dispatch record and trim oldest records past cap.
  appendRetainedHookDispatchRecord(hookDispatchRecord){
    // Assignment: append cloned dispatch record.
    this.retainedHookDispatchRecords.push(PipelineSecurityUtility.cloneStrictJsonValue(hookDispatchRecord));

    // Conditional: trim dispatch records when cap is exceeded.
    while(this.retainedHookDispatchRecords.length>this.maxRetainedHookDispatchRecords){
      // Assignment: remove oldest dispatch record.
      this.retainedHookDispatchRecords.shift();
    }
  }

  // Function: append retained failure record and trim oldest records past cap.
  appendRetainedHookFailureRecord(hookFailureRecord){
    // Assignment: append cloned failure record.
    this.retainedHookFailureRecords.push(PipelineSecurityUtility.cloneStrictJsonValue(hookFailureRecord));

    // Conditional: trim failure records when cap is exceeded.
    while(this.retainedHookFailureRecords.length>this.maxRetainedHookFailureRecords){
      // Assignment: remove oldest failure record.
      this.retainedHookFailureRecords.shift();
    }
  }

  // Function: write hook bus debug records through the shared debug logger when available.
  writeDebugRecord(logLevelName,logMessage,logMetadata={}){
    // Conditional: absent logger means hook bus still functions without debug output.
    if(this.debugLogger&&typeof this.debugLogger.log==="function"){
      this.debugLogger.log(logLevelName,logMessage,logMetadata);
    }
  }

  // Function: produce a compact JSON-safe hook bus snapshot for UI and tests.
  getPipelineExecutionHookBusSnapshot(){
    // Assignment: registered hook descriptions.
    const registeredHookDescriptions=this.listRegisteredPipelineExecutionHooks();
    return {registeredHookCount:registeredHookDescriptions.length,enabledHookCount:registeredHookDescriptions.filter(hookDescription=>hookDescription.hookIsEnabled===true).length,registeredHooks:registeredHookDescriptions,retainedHookDispatchRecords:PipelineSecurityUtility.cloneStrictJsonValue(this.retainedHookDispatchRecords),retainedHookFailureRecords:PipelineSecurityUtility.cloneStrictJsonValue(this.retainedHookFailureRecords),hookEventCounter:this.hookEventCounter};
  }
}

// Function/class: immutable plugin registry metadata entry.
class PipelineRegistryEntry{
  // Function: construct registry metadata.
  constructor({typeId,label,type,requires=[],supports=[],description=""}){
    // Conditional: type id is required.
    if(!typeId||typeof typeId!=="string"){
      throw new TypeError("Registry typeId required.");
    }

    // Conditional: label is required.
    if(!label||typeof label!=="string"){
      throw new TypeError("Registry label required.");
    }

    // Conditional: node type must be known.
    if(!Object.values(pipelineNodeTypeNameByRole).includes(type)){
      throw new TypeError("Invalid registry type: "+type);
    }

    // Function call: enforce dependency/support arrays.
    PipelineSecurityUtility.assertStringArray(requires,"requires");
    PipelineSecurityUtility.assertStringArray(supports,"supports");

    // Assignment: plugin type identity.
    this.typeId=typeId;
    // Assignment: display label.
    this.label=label;
    // Assignment: node type category.
    this.type=type;
    // Assignment: immutable dependency list.
    this.requires=Object.freeze([...requires]);
    // Assignment: immutable supported format list.
    this.supports=Object.freeze([...supports]);
    // Assignment: description for UI and debug output.
    this.description=description;
    // Assignment: freeze registry entry.
    Object.freeze(this);
  }
}

// Function/class: executable node base class.
class ExecutablePipelineNode{
  // Function: construct executable node metadata.
  constructor({typeId,instanceId=null,label,system=false,type=pipelineNodeTypeNameByRole.EXTENSION_PLUGIN,requires=[],supports=[supportedPipelineDataFormat.JSON],description=""}){
    // Conditional: every node needs a type id.
    if(!typeId||typeof typeId!=="string"){
      throw new TypeError("Node typeId required.");
    }

    // Conditional: every node needs a display label.
    if(!label||typeof label!=="string"){
      throw new TypeError("Node label required.");
    }

    // Assignment: registry type identity.
    this.typeId=typeId;
    // Assignment: runtime instance identity.
    this.instanceId=instanceId||typeId;
    // Assignment: legacy id alias for older UI/test code.
    this.id=this.instanceId;
    // Assignment: display label.
    this.label=label;
    // Assignment: system sentinel flag.
    this.system=Boolean(system);
    // Assignment: node type category.
    this.type=type;
    // Assignment: immutable dependency list.
    this.requires=Object.freeze([...requires]);
    // Assignment: immutable support list.
    this.supports=Object.freeze([...supports]);
    // Assignment: human-readable description.
    this.description=description;
  }

  // Function: convert node metadata to registry entry.
  getRegistryEntry(){
    return new PipelineRegistryEntry({typeId:this.typeId,label:this.label,type:this.type,requires:this.requires,supports:this.supports,description:this.description});
  }

  // Function: subclasses must implement execution.
  async execute(){
    throw new Error("Node "+this.id+" did not implement execute().");
  }
}

// Function/class: start sentinel node.
class PipelineStartSentinelNode extends ExecutablePipelineNode{
  // Function: construct start sentinel.
  constructor(){
    super({typeId:"system.start",instanceId:"system.start",label:"Initial Execution Starter",system:true,type:pipelineNodeTypeNameByRole.SYSTEM_SENTINEL,requires:[],supports:[supportedPipelineDataFormat.UNIVERSAL],description:"Universal entry sentinel. No I/O."});
    // Assignment: start sentinel metadata is immutable.
    Object.freeze(this);
  }

  // Function: record pipeline entry.
  async execute(pipelineContext,pipelineRuntime){
    // Function call: write debug log.
    pipelineRuntime.debug.log("debug","Entered START sentinel.",{nodeId:this.id});
    // Function call: write diagnostic record.
    PipelineContextUtility.appendDiagnosticRecord(pipelineContext,{node:this.id,typeId:this.typeId,event:"entry",io:false});
    return pipelineContext;
  }
}

// Function/class: stop sentinel node.
class PipelineStopSentinelNode extends ExecutablePipelineNode{
  // Function: construct stop sentinel.
  constructor(){
    super({typeId:"system.stop",instanceId:"system.stop",label:"Stop Execution",system:true,type:pipelineNodeTypeNameByRole.SYSTEM_SENTINEL,requires:[],supports:[supportedPipelineDataFormat.UNIVERSAL],description:"Universal terminal sentinel."});
    // Assignment: stop sentinel metadata is immutable.
    Object.freeze(this);
  }

  // Function: record pipeline termination.
  async execute(pipelineContext,pipelineRuntime){
    // Function call: write debug log.
    pipelineRuntime.debug.log("debug","Entered STOP sentinel.",{nodeId:this.id});
    // Function call: write diagnostic record.
    PipelineContextUtility.appendDiagnosticRecord(pipelineContext,{node:this.id,typeId:this.typeId,event:"terminate-pass"});
    return pipelineContext;
  }
}

// Function/class: plugin base class for user-loadable nodes.
class PipelinePluginNode extends ExecutablePipelineNode{
  // Function: construct plugin node metadata.
  constructor({typeId,instanceId=null,label,type=pipelineNodeTypeNameByRole.EXTENSION_PLUGIN,requires=[],supports=[supportedPipelineDataFormat.JSON],description=""}){
    // Conditional: plugin types must use the plugin namespace.
    if(!typeId.startsWith("plugin.")){
      throw new Error("Plugin typeIds must start with plugin.");
    }

    super({typeId,instanceId:instanceId||typeId,label,system:false,type,requires,supports,description});
  }
}

// Function/class: normalized plugin factory wrapper.
class PipelinePluginFactory{
  // Function: create factory wrapper from a create callback.
  constructor({createPluginInstance}){
    // Conditional: factory must provide a creation callback.
    if(typeof createPluginInstance!=="function"){
      throw new TypeError("PipelinePluginFactory requires createPluginInstance().");
    }

    // Assignment: factory callback.
    this.createPluginInstance=createPluginInstance;
    // Assignment: prototype plugin used to harvest immutable metadata.
    const prototypePluginInstance=this.createInstance();
    // Assignment: plugin type identity.
    this.typeId=prototypePluginInstance.typeId;
    // Assignment: display label.
    this.label=prototypePluginInstance.label;
    // Assignment: plugin type category.
    this.type=prototypePluginInstance.type;
    // Assignment: immutable dependency list.
    this.requires=Object.freeze([...prototypePluginInstance.requires]);
    // Assignment: immutable support list.
    this.supports=Object.freeze([...prototypePluginInstance.supports]);
    // Assignment: description.
    this.description=prototypePluginInstance.description;
    // Assignment: freeze factory metadata.
    Object.freeze(this);
  }

  // Function: normalize constructors, callback factories, and factory objects.
  static fromFactorySource(factorySource,factoryArguments=[]){
    // Conditional: already-normalized factories pass through.
    if(factorySource instanceof PipelinePluginFactory){
      return factorySource;
    }

    // Conditional: factory object with createInstance method.
    if(factorySource&&typeof factorySource.createInstance==="function"){
      return new PipelinePluginFactory({createPluginInstance:()=>factorySource.createInstance()});
    }

    // Conditional: factory object with create method.
    if(factorySource&&typeof factorySource.create==="function"){
      return new PipelinePluginFactory({createPluginInstance:()=>factorySource.create(...factoryArguments)});
    }

    // Conditional: class constructors and function factories are both supported.
    if(typeof factorySource==="function"){
      // Assignment: source text identifies ES class constructors.
      const factorySourceText=Function.prototype.toString.call(factorySource);
      // Assignment: class-like test covers syntax classes and subclasses.
      const factoryLooksLikeClass=/^class\s/.test(factorySourceText)||Boolean(factorySource.prototype&&factorySource.prototype instanceof PipelinePluginNode);
      return new PipelinePluginFactory({createPluginInstance:()=>factoryLooksLikeClass?new factorySource(...factoryArguments):factorySource(...factoryArguments)});
    }

    throw new TypeError("Plugin install requires a PipelinePluginFactory, factory object, factory function, or plugin constructor.");
  }

  // Function: create a plugin instance and validate metadata stability.
  createInstance(){
    // Assignment: plugin created by caller factory.
    const pluginInstance=this.createPluginInstance();

    // Conditional: factories must produce plugin nodes.
    if(!(pluginInstance instanceof PipelinePluginNode)){
      throw new TypeError("Plugin factory must create a PipelinePluginNode instance.");
    }

    // Conditional: once factory metadata is known, produced instances must match its type id.
    if(this.typeId&&pluginInstance.typeId!==this.typeId){
      throw new Error("Plugin factory produced mismatched typeId: "+pluginInstance.typeId+" !== "+this.typeId);
    }

    return pluginInstance;
  }

  // Function: convert factory metadata to registry entry.
  getRegistryEntry(){
    return new PipelineRegistryEntry({typeId:this.typeId,label:this.label,type:this.type,requires:this.requires,supports:this.supports,description:this.description});
  }

  // Function: describe factory for UI/debug surfaces.
  describeFactory(){
    return {typeId:this.typeId,label:this.label,type:this.type,requires:[...this.requires],supports:[...this.supports],description:this.description,factoryMode:"create-instance"};
  }
}

// Function/class: plugin type registry.
class PipelinePluginRegistry{
  // Function: construct registry.
  constructor(debugLogger){
    // Assignment: debug logger reference.
    this.debug=debugLogger;
    // Assignment: registry entries by type id.
    this.entriesByTypeId=new Map();
  }

  // Function: register node/factory/entry metadata.
  register(registrySource){
    // Assignment: normalize source into registry entry.
    const registryEntry=registrySource instanceof ExecutablePipelineNode||registrySource instanceof PipelinePluginFactory
      ? registrySource.getRegistryEntry()
      : new PipelineRegistryEntry(registrySource);

    // Assignment: store registry entry by type id.
    this.entriesByTypeId.set(registryEntry.typeId,registryEntry);

    // Conditional: debug registry writes when logger exists.
    if(this.debug){
      this.debug.log("info","Registry entry registered.",{typeId:registryEntry.typeId,type:registryEntry.type,requires:registryEntry.requires,supports:registryEntry.supports});
    }

    return registryEntry;
  }

  // Function: list cloned registry entries.
  listRegistryEntries(){
    return [...this.entriesByTypeId.values()].map(registryEntry=>PipelineSecurityUtility.deepCloneJsonSafeValue(registryEntry));
  }

  // Function: validate dependency order for loaded plugins.
  validateLoadedPluginOrder(loadedPluginRecords){
    // Assignment: loaded type ids in actual order.
    const loadedPluginTypeIds=loadedPluginRecords.map(loadedPluginRecord=>loadedPluginRecord.plugin.typeId);
    // Assignment: set allows fast membership checks.
    const loadedPluginTypeIdSet=new Set(loadedPluginTypeIds);
    // Assignment: warnings are collected here.
    const dependencyWarningRecords=[];

    // Loop: inspect each loaded plugin.
    for(const loadedPluginRecord of loadedPluginRecords){
      // Loop: inspect each dependency of the current plugin.
      for(const requiredPluginTypeId of loadedPluginRecord.plugin.requires){
        // Conditional: required type must exist somewhere in the loaded list.
        if(!loadedPluginTypeIdSet.has(requiredPluginTypeId)){
          // Assignment: missing dependency warning.
          dependencyWarningRecords.push({instanceId:loadedPluginRecord.plugin.instanceId,typeId:loadedPluginRecord.plugin.typeId,requiredTypeId:requiredPluginTypeId,problem:"missing-required-plugin-type"});
        }else if(loadedPluginTypeIds.indexOf(requiredPluginTypeId)>loadedPluginTypeIds.indexOf(loadedPluginRecord.plugin.typeId)){
          // Assignment: dependency appears after dependent warning.
          dependencyWarningRecords.push({instanceId:loadedPluginRecord.plugin.instanceId,typeId:loadedPluginRecord.plugin.typeId,requiredTypeId:requiredPluginTypeId,problem:"required-plugin-type-loaded-after-dependent"});
        }
      }
    }

    return dependencyWarningRecords;
  }
}

// Function/class: manages installed plugin factories and loaded plugin instances.
class LoadedPipelinePluginManager{
  // Function: construct plugin manager.
  constructor(debugLogger,pluginRegistry){
    // Assignment: debug logger reference.
    this.debug=debugLogger;
    // Assignment: registry reference or new registry.
    this.registry=pluginRegistry||new PipelinePluginRegistry(debugLogger);
    // Assignment: installed factories by plugin type id.
    this.installedPluginFactoryRecordsByTypeId=new Map();
    // Assignment: loaded runtime plugin records in execution order.
    this.loadedPluginRecords=[];
    // Assignment: monotonic counter for instance id allocation.
    this.pluginInstanceCounter=0;
    // Function call: register start sentinel.
    this.registry.register(new PipelineStartSentinelNode());
    // Function call: register stop sentinel.
    this.registry.register(new PipelineStopSentinelNode());
  }

  // Function: install plugin factory, constructor, or callback.
  installPluginFactory(factorySource,factoryArguments=[]){
    // Assignment: normalize source to PipelinePluginFactory.
    const pluginFactory=PipelinePluginFactory.fromFactorySource(factorySource,factoryArguments);
    // Assignment: descriptor for UI/debug.
    const pluginFactoryDescriptor=pluginFactory.describeFactory();
    // Assignment: store factory record.
    this.installedPluginFactoryRecordsByTypeId.set(pluginFactory.typeId,{pluginFactory,...pluginFactoryDescriptor});
    // Function call: register factory metadata.
    this.registry.register(pluginFactory);
    // Function call: log installation.
    this.debug.log("info","Plugin factory installed.",{typeId:pluginFactory.typeId,factoryMode:pluginFactoryDescriptor.factoryMode});
    return pluginFactory.typeId;
  }

  // Function: allocate readable plugin instance id.
  makePluginInstanceId(pluginTypeId){
    // Assignment: advance counter.
    this.pluginInstanceCounter+=1;
    return pluginTypeId+"#"+String(this.pluginInstanceCounter).padStart(3,"0");
  }

  // Function: load plugin instance at insertion index.
  loadPluginInstance(pluginTypeId,insertionIndex=this.loadedPluginRecords.length){
    // Assignment: factory record for requested type.
    const installedPluginFactoryRecord=this.installedPluginFactoryRecordsByTypeId.get(pluginTypeId);

    // Conditional: type must be installed before loading.
    if(!installedPluginFactoryRecord){
      throw new Error("Plugin type not installed: "+pluginTypeId);
    }

    // Conditional: insertion index must be inside the loaded plugin boundary.
    if(!Number.isInteger(insertionIndex)||insertionIndex<0||insertionIndex>this.loadedPluginRecords.length){
      throw new RangeError("Invalid interior insertion index.");
    }

    // Assignment: create plugin instance.
    const pluginInstance=installedPluginFactoryRecord.pluginFactory.createInstance();
    // Assignment: give plugin a unique runtime instance id.
    pluginInstance.instanceId=this.makePluginInstanceId(pluginTypeId);
    // Assignment: legacy id alias follows instance id.
    pluginInstance.id=pluginInstance.instanceId;
    // Assignment: insert plugin record into execution order.
    this.loadedPluginRecords.splice(insertionIndex,0,{plugin:pluginInstance});
    // Function call: log load event.
    this.debug.log("info","Plugin instance loaded.",{typeId:pluginTypeId,instanceId:pluginInstance.instanceId,index:insertionIndex});
    // Function call: report dependency warnings.
    this.logDependencyWarnings("load");
    return pluginInstance.instanceId;
  }

  // Function: unload selected plugin instance and return removed metadata.
  unloadPluginInstance(pluginInstanceId){
    // Conditional: selected instance id is required.
    if(typeof pluginInstanceId!=="string"||!pluginInstanceId){
      throw new TypeError("Plugin unload requires a selected instance id.");
    }

    // Assignment: find loaded plugin index.
    const loadedPluginIndex=this.loadedPluginRecords.findIndex(loadedPluginRecord=>loadedPluginRecord.plugin.instanceId===pluginInstanceId);

    // Conditional: instance must be loaded.
    if(loadedPluginIndex<0){
      throw new Error("Plugin instance is not loaded: "+pluginInstanceId);
    }

    // Assignment: remove the plugin record.
    const [removedLoadedPluginRecord]=this.loadedPluginRecords.splice(loadedPluginIndex,1);
    // Assignment: removed metadata returned to UI for selection recovery.
    const removedPluginSummary={index:loadedPluginIndex,instanceId:removedLoadedPluginRecord.plugin.instanceId,typeId:removedLoadedPluginRecord.plugin.typeId};
    // Function call: log unload event.
    this.debug.log("info","Plugin instance unloaded.",removedPluginSummary);
    // Function call: report dependency warnings.
    this.logDependencyWarnings("unload");
    return removedPluginSummary;
  }

  // Function: move loaded plugin instance to a new index.
  movePluginInstance(pluginInstanceId,targetIndex){
    // Assignment: current index for selected instance.
    const currentIndex=this.loadedPluginRecords.findIndex(loadedPluginRecord=>loadedPluginRecord.plugin.instanceId===pluginInstanceId);

    // Conditional: instance must exist.
    if(currentIndex<0){
      throw new Error("Plugin instance is not loaded: "+pluginInstanceId);
    }

    // Assignment: temporarily remove record.
    const [loadedPluginRecord]=this.loadedPluginRecords.splice(currentIndex,1);

    // Conditional: target index must be valid after removal.
    if(!Number.isInteger(targetIndex)||targetIndex<0||targetIndex>this.loadedPluginRecords.length){
      // Assignment: restore removed record before throwing.
      this.loadedPluginRecords.splice(currentIndex,0,loadedPluginRecord);
      throw new RangeError("Invalid move index.");
    }

    // Assignment: insert record at target index.
    this.loadedPluginRecords.splice(targetIndex,0,loadedPluginRecord);
    // Function call: log move event.
    this.debug.log("info","Plugin instance moved.",{instanceId:pluginInstanceId,index:targetIndex});
    // Function call: report dependency warnings.
    this.logDependencyWarnings("move");
  }

  // Function: remove all loaded plugin instances.
  clearLoadedPluginInstances(){
    // Assignment: reset loaded plugin records.
    this.loadedPluginRecords=[];
    // Function call: log clear event.
    this.debug.log("info","Loaded plugin pipeline cleared.",{});
  }

  // Function: log dependency warnings for current loaded order.
  logDependencyWarnings(actionName){
    // Assignment: current warnings.
    const dependencyWarningRecords=this.getDependencyWarnings();

    // Conditional: write warnings only when at least one exists.
    if(dependencyWarningRecords.length){
      this.debug.log("warn","Dependency warning after "+actionName+".",{warnings:dependencyWarningRecords});
    }
  }

  // Function: return dependency warnings.
  getDependencyWarnings(){
    return this.registry.validateLoadedPluginOrder(this.loadedPluginRecords);
  }

  // Function: describe installed plugin types.
  getInstalledPluginDescriptions(){
    return [...this.installedPluginFactoryRecordsByTypeId.entries()].map(([pluginTypeId,installedPluginFactoryRecord])=>({typeId:pluginTypeId,label:installedPluginFactoryRecord.label,type:installedPluginFactoryRecord.type,requires:[...installedPluginFactoryRecord.requires],supports:[...installedPluginFactoryRecord.supports],description:installedPluginFactoryRecord.description,factoryMode:installedPluginFactoryRecord.factoryMode}));
  }

  // Function: describe loaded plugin instances.
  getLoadedPluginDescriptions(){
    return this.loadedPluginRecords.map((loadedPluginRecord,loadedPluginIndex)=>({index:loadedPluginIndex,typeId:loadedPluginRecord.plugin.typeId,instanceId:loadedPluginRecord.plugin.instanceId,id:loadedPluginRecord.plugin.instanceId,label:loadedPluginRecord.plugin.label,type:loadedPluginRecord.plugin.type,requires:[...loadedPluginRecord.plugin.requires],supports:[...loadedPluginRecord.plugin.supports],description:loadedPluginRecord.plugin.description}));
  }

  // Function: return executable loaded plugin instances.
  getLoadedPluginInstances(){
    return this.loadedPluginRecords.map(loadedPluginRecord=>loadedPluginRecord.plugin);
  }

  // Function: count loaded instances.
  countLoadedPluginInstances(){
    return this.loadedPluginRecords.length;
  }

  // Function: return registry snapshot.
  getRegistrySnapshot(){
    return this.registry.listRegistryEntries();
  }
}

// Function/class: recursive pipeline runtime.
class RecursivePipelineRuntime{
  // Function: construct runtime and plugin manager.
  constructor({debug,maximumNodesPerPass=DEFAULT_MAXIMUM_NODES_PER_PASS,maximumElapsedMsPerPass=DEFAULT_MAXIMUM_ELAPSED_MS_PER_PASS,maximumBranchEntriesPerPass=DEFAULT_MAXIMUM_BRANCH_ENTRIES_PER_PASS}={}){
    // Assignment: debug logger.
    this.debug=debug||new PipelineDebugLogger();
    // Assignment: out-of-band lifecycle hook bus for robust execution observation.
    this.pipelineExecutionHookBus=new PipelineExecutionHookBus({debugLogger:this.debug});
    // Assignment: plugin registry.
    this.registry=new PipelinePluginRegistry(this.debug);
    // Assignment: plugin manager.
    this.pluginManager=new LoadedPipelinePluginManager(this.debug,this.registry);
    // Assignment: current runtime state.
    this.state=pipelineRuntimeStateNameByStatus.IDLE;
    // Assignment: loop delay in milliseconds.
    this.cycleDelayMs=300;
    // Assignment: loop timeout handle.
    this.loopHandle=null;
    // Assignment: stop flag used by loop and recursive executor.
    this.stopRequested=false;
    // Assignment: UI/runtime guard prevents overlapping one-pass executions.
    this.isPipelinePassRunning=false;
    // Assignment: maximum number of node executions permitted in one top-level pass.
    this.maximumNodesPerPass=RecursivePipelineRuntime.normalizePositiveIntegerBudget(maximumNodesPerPass,DEFAULT_MAXIMUM_NODES_PER_PASS,"maximumNodesPerPass");
    // Assignment: maximum elapsed wall-clock milliseconds permitted in one top-level pass.
    this.maximumElapsedMsPerPass=RecursivePipelineRuntime.normalizePositiveIntegerBudget(maximumElapsedMsPerPass,DEFAULT_MAXIMUM_ELAPSED_MS_PER_PASS,"maximumElapsedMsPerPass");
    // Assignment: maximum branch/junction entries permitted in one top-level pass.
    this.maximumBranchEntriesPerPass=RecursivePipelineRuntime.normalizeNonNegativeIntegerBudget(maximumBranchEntriesPerPass,DEFAULT_MAXIMUM_BRANCH_ENTRIES_PER_PASS,"maximumBranchEntriesPerPass");
    // Assignment: current pass execution-budget state is allocated at run start.
    this.currentExecutionBudgetState=null;
    // Assignment: current context is created after budget defaults are available.
    this.context=this.createFreshPipelineContext();
  }

  // Function: normalize a positive integer execution budget.
  static normalizePositiveIntegerBudget(candidateValue,defaultValue,diagnosticLabel){
    const numericCandidate=Number(candidateValue);

    if(!Number.isInteger(numericCandidate)||numericCandidate<1){
      if(typeof candidateValue==="undefined"||candidateValue===null){
        return defaultValue;
      }

      throw new RangeError(diagnosticLabel+" must be a positive integer.");
    }

    return numericCandidate;
  }

  // Function: normalize a non-negative integer execution budget.
  static normalizeNonNegativeIntegerBudget(candidateValue,defaultValue,diagnosticLabel){
    const numericCandidate=Number(candidateValue);

    if(!Number.isInteger(numericCandidate)||numericCandidate<0){
      if(typeof candidateValue==="undefined"||candidateValue===null){
        return defaultValue;
      }

      throw new RangeError(diagnosticLabel+" must be a non-negative integer.");
    }

    return numericCandidate;
  }

  // Function: change execution budgets in one validated operation.
  configureExecutionBudgets(executionBudgetOptions={}){
    PipelineSecurityUtility.assertPlainObject(executionBudgetOptions,"Execution budget options");

    if(Object.prototype.hasOwnProperty.call(executionBudgetOptions,"maximumNodesPerPass")){
      this.maximumNodesPerPass=RecursivePipelineRuntime.normalizePositiveIntegerBudget(executionBudgetOptions.maximumNodesPerPass,this.maximumNodesPerPass,"maximumNodesPerPass");
    }

    if(Object.prototype.hasOwnProperty.call(executionBudgetOptions,"maximumElapsedMsPerPass")){
      this.maximumElapsedMsPerPass=RecursivePipelineRuntime.normalizePositiveIntegerBudget(executionBudgetOptions.maximumElapsedMsPerPass,this.maximumElapsedMsPerPass,"maximumElapsedMsPerPass");
    }

    if(Object.prototype.hasOwnProperty.call(executionBudgetOptions,"maximumBranchEntriesPerPass")){
      this.maximumBranchEntriesPerPass=RecursivePipelineRuntime.normalizeNonNegativeIntegerBudget(executionBudgetOptions.maximumBranchEntriesPerPass,this.maximumBranchEntriesPerPass,"maximumBranchEntriesPerPass");
    }

    return this.getExecutionBudgetSnapshot();
  }

  // Function: initialize execution-budget counters for the current top-level pass.
  resetExecutionBudgetForPass(pipelineName="main"){
    this.currentExecutionBudgetState={pipelineName,startedAtMilliseconds:Date.now(),nodesExecuted:0,branchEntries:0,maximumNodesPerPass:this.maximumNodesPerPass,maximumElapsedMsPerPass:this.maximumElapsedMsPerPass,maximumBranchEntriesPerPass:this.maximumBranchEntriesPerPass};
    return this.getExecutionBudgetSnapshot();
  }

  // Function: return a JSON-safe execution-budget snapshot.
  getExecutionBudgetSnapshot(){
    const executionBudgetState=this.currentExecutionBudgetState||{pipelineName:null,startedAtMilliseconds:null,nodesExecuted:0,branchEntries:0,maximumNodesPerPass:this.maximumNodesPerPass,maximumElapsedMsPerPass:this.maximumElapsedMsPerPass,maximumBranchEntriesPerPass:this.maximumBranchEntriesPerPass};
    return PipelineSecurityUtility.cloneStrictJsonValue({...executionBudgetState,elapsedMilliseconds:Number.isFinite(executionBudgetState.startedAtMilliseconds)?Date.now()-executionBudgetState.startedAtMilliseconds:0});
  }

  // Function: build and throw a budget error with a compact snapshot.
  throwExecutionBudgetExceeded(reason,metadata={}){
    const budgetError=new Error("Execution budget exceeded: "+reason);
    budgetError.name="PipelineExecutionBudgetError";
    budgetError.executionBudget=this.getExecutionBudgetSnapshot();
    budgetError.executionBudgetReason=reason;
    budgetError.executionBudgetMetadata=PipelineSecurityUtility.cloneStrictJsonValue(metadata);
    throw budgetError;
  }

  // Function: consume one node-execution unit and fail closed if a pass budget is exceeded.
  recordNodeExecutionAgainstBudget(pipelineContext,metadata={}){
    if(!this.currentExecutionBudgetState){
      this.resetExecutionBudgetForPass(metadata.pipelineName||"main");
    }

    this.currentExecutionBudgetState.nodesExecuted+=1;

    if(this.currentExecutionBudgetState.nodesExecuted>this.maximumNodesPerPass){
      this.throwExecutionBudgetExceeded("maximumNodesPerPass",metadata);
    }

    const elapsedMilliseconds=Date.now()-this.currentExecutionBudgetState.startedAtMilliseconds;

    if(elapsedMilliseconds>this.maximumElapsedMsPerPass){
      this.throwExecutionBudgetExceeded("maximumElapsedMsPerPass",{...metadata,elapsedMilliseconds});
    }

    if(pipelineContext&&typeof pipelineContext==="object"){
      pipelineContext.executionBudget=this.getExecutionBudgetSnapshot();
    }

    return this.getExecutionBudgetSnapshot();
  }

  // Function: consume one branch-entry unit and fail closed if branch traversal is excessive.
  recordBranchEntryAgainstBudget(pipelineContext,metadata={}){
    if(!this.currentExecutionBudgetState){
      this.resetExecutionBudgetForPass(metadata.pipelineName||"main");
    }

    this.currentExecutionBudgetState.branchEntries+=1;

    if(this.currentExecutionBudgetState.branchEntries>this.maximumBranchEntriesPerPass){
      this.throwExecutionBudgetExceeded("maximumBranchEntriesPerPass",metadata);
    }

    if(pipelineContext&&typeof pipelineContext==="object"){
      pipelineContext.executionBudget=this.getExecutionBudgetSnapshot();
    }

    return this.getExecutionBudgetSnapshot();
  }

  // Function: create a fresh context for reset/startup.
  createFreshPipelineContext(){
    // Assignment: crypto object may be absent in some runtimes.
    const cryptoObject=globalObject.crypto;
    // Assignment: run id from crypto when available, otherwise timestamp fallback.
    const generatedRunId=(cryptoObject&&typeof cryptoObject.randomUUID==="function")?cryptoObject.randomUUID():"run-"+Date.now()+"-"+Math.random();

    return {runId:generatedRunId,cycle:0,status:pipelineRuntimeStateNameByStatus.IDLE,input:{},data:PipelineJsonDocumentFactory.createPipelineJsonDocument({}),state:{byInstanceId:{},byTypeId:{},timeline:[]},tokenizerState:{},regexState:{},streamState:null,executionBudget:this.getExecutionBudgetSnapshot(),output:{},stream:[],diagnostics:[],errors:[]};
  }

  // Function: build executable nodes including start/stop sentinels.
  buildExecutablePipelineNodes(){
    // Assignment: full node order.
    const executablePipelineNodes=[new PipelineStartSentinelNode(),...this.pluginManager.getLoadedPluginInstances(),new PipelineStopSentinelNode()];
    // Function call: validate order and node integrity.
    this.validateExecutablePipelineNodes(executablePipelineNodes);
    return executablePipelineNodes;
  }

  // Function: validate node order and duplicate instance ids.
  validateExecutablePipelineNodes(executablePipelineNodes){
    // Conditional: pipeline must start with start sentinel.
    if(!(executablePipelineNodes[0] instanceof PipelineStartSentinelNode)){
      throw new Error("Pipeline must begin with START.");
    }

    // Conditional: pipeline must end with stop sentinel.
    if(!(executablePipelineNodes[executablePipelineNodes.length-1] instanceof PipelineStopSentinelNode)){
      throw new Error("Pipeline must end with STOP.");
    }

    // Assignment: track node ids already seen.
    const nodeInstanceIdSet=new Set();

    // Loop: validate every executable node.
    executablePipelineNodes.forEach((pipelineNode,pipelineNodeIndex)=>{
      // Conditional: duplicate ids would corrupt diagnostics and UI selection.
      if(nodeInstanceIdSet.has(pipelineNode.id)){
        throw new Error("Duplicate node instance id: "+pipelineNode.id);
      }

      // Assignment: record node id.
      nodeInstanceIdSet.add(pipelineNode.id);

      // Conditional: system nodes are allowed only at start/end.
      if(pipelineNodeIndex>0&&pipelineNodeIndex<executablePipelineNodes.length-1&&pipelineNode.system){
        throw new Error("Only sentinels may be system nodes.");
      }

      // Conditional: interior nodes must be plugins.
      if(pipelineNodeIndex>0&&pipelineNodeIndex<executablePipelineNodes.length-1&&!(pipelineNode instanceof PipelinePluginNode)){
        throw new Error("Interior nodes must be plugins.");
      }
    });

    // Assignment: dependency warnings are non-fatal but visible.
    const dependencyWarningRecords=this.pluginManager.getDependencyWarnings();

    // Conditional: log warnings when dependencies are missing or ordered late.
    if(dependencyWarningRecords.length){
      this.debug.log("warn","Pipeline dependency warnings detected.",{warnings:dependencyWarningRecords});
    }
  }

  // Function: recursively execute nodes in order.
  async executePipelineNodesRecursively(executablePipelineNodes,nodeIndex,pipelineContext){
    // Conditional: stop recursion at end, pause, or stop request.
    if(nodeIndex>=executablePipelineNodes.length||this.state===pipelineRuntimeStateNameByStatus.PAUSED||this.stopRequested){
      return pipelineContext;
    }

    // Assignment: current node.
    const currentPipelineNode=executablePipelineNodes[nodeIndex];
    // Assignment: node execution start timestamp for timing diagnostics.
    const nodeExecutionStartedAtMilliseconds=Date.now();
    // Function call: log node entry.
    this.debug.log("debug","Executing node.",{index:nodeIndex,instanceId:currentPipelineNode.instanceId,typeId:currentPipelineNode.typeId,type:currentPipelineNode.type,requires:currentPipelineNode.requires,supports:currentPipelineNode.supports});

    try{
      // Function call: consume one node budget unit before plugin execution.
      this.recordNodeExecutionAgainstBudget(pipelineContext,{pipelineName:"main",nodeIndex,moduleInstanceId:currentPipelineNode.instanceId,moduleTypeId:currentPipelineNode.typeId});
      // Function call: ensure the context is well-formed before plugin execution.
      PipelineContextUtility.ensureContextDataDocument(pipelineContext);
      // Function call: emit out-of-band module-start hook event.
      await this.pipelineExecutionHookBus.emitPipelineExecutionHooksForLifecyclePhase(pipelineExecutionHookPhaseNameByEvent.MODULE_EXECUTION_STARTED,PipelineExecutionHookEventFactory.createModuleExecutionStartedEvent(this,pipelineContext,{pipelineName:"main",nodeIndex,currentPipelineNode}));
      // Assignment: result context from current node.
      const nextPipelineContext=await currentPipelineNode.execute(pipelineContext,this);
      // Function call: ensure result context is still well-formed.
      PipelineContextUtility.ensureContextDataDocument(nextPipelineContext);
      // Function call: emit out-of-band module-finish hook event.
      await this.pipelineExecutionHookBus.emitPipelineExecutionHooksForLifecyclePhase(pipelineExecutionHookPhaseNameByEvent.MODULE_EXECUTION_FINISHED,PipelineExecutionHookEventFactory.createModuleExecutionFinishedEvent(this,nextPipelineContext,{pipelineName:"main",nodeIndex,currentPipelineNode,elapsedMilliseconds:Date.now()-nodeExecutionStartedAtMilliseconds}));
      return await this.executePipelineNodesRecursively(executablePipelineNodes,nodeIndex+1,nextPipelineContext);
    }catch(nodeExecutionError){
      // Assignment: error record for diagnostics.
      const pipelineErrorRecord={instanceId:currentPipelineNode.instanceId,typeId:currentPipelineNode.typeId,message:nodeExecutionError.message,stack:nodeExecutionError.stack,errorName:nodeExecutionError.name||"Error",executionBudget:nodeExecutionError.executionBudget||null,executionBudgetReason:nodeExecutionError.executionBudgetReason||null};
      // Function call: append runtime error.
      PipelineContextUtility.appendErrorRecord(pipelineContext,pipelineErrorRecord);
      // Function call: emit out-of-band module-failure hook event.
      await this.pipelineExecutionHookBus.emitPipelineExecutionHooksForLifecyclePhase(pipelineExecutionHookPhaseNameByEvent.MODULE_EXECUTION_FAILED,PipelineExecutionHookEventFactory.createModuleExecutionFailedEvent(this,pipelineContext,{pipelineName:"main",nodeIndex,currentPipelineNode,error:nodeExecutionError,pipelineErrorRecord,elapsedMilliseconds:Date.now()-nodeExecutionStartedAtMilliseconds}));
      // Function call: log runtime error.
      this.debug.log("error","Node execution failed.",{...pipelineErrorRecord,time:new Date().toISOString()});
      // Assignment: runtime enters error state.
      this.state=pipelineRuntimeStateNameByStatus.ERROR;
      return pipelineContext;
    }
  }

  // Function: execute one full pipeline pass.
  async runOnce(){
    // Conditional: paused runtime does not execute new passes.
    if(this.state===pipelineRuntimeStateNameByStatus.PAUSED){
      return this.context;
    }

    // Conditional: overlapping one-pass executions are blocked by the runtime itself.
    if(this.isPipelinePassRunning){
      this.debug.log("warn","Pipeline pass ignored because another pass is already running.",{});
      return this.context;
    }

    // Assignment: pipeline pass start timestamp for hook timing.
    const pipelineExecutionStartedAtMilliseconds=Date.now();
    // Function call: reset execution-budget counters for this pass.
    this.resetExecutionBudgetForPass("main");
    // Assignment: mark pass as active for UI/runtime action locks.
    this.isPipelinePassRunning=true;
    // Assignment: runtime is actively running.
    this.state=pipelineRuntimeStateNameByStatus.RUNNING;
    // Assignment: mirror state into context.
    this.context.status=this.state;
    // Assignment: increment cycle count.
    this.context.cycle+=1;
    // Assignment: build nodes for this pass.
    const executablePipelineNodes=this.buildExecutablePipelineNodes();
    try{
      // Function call: emit pipeline-start hook event.
    await this.pipelineExecutionHookBus.emitPipelineExecutionHooksForLifecyclePhase(pipelineExecutionHookPhaseNameByEvent.PIPELINE_EXECUTION_STARTED,PipelineExecutionHookEventFactory.createPipelineExecutionStartedEvent(this,this.context,{pipelineName:"main",executablePipelineNodes}));
    // Function call: log pass start.
    this.debug.log("info","Pipeline pass started.",{cycle:this.context.cycle,nodeOrder:executablePipelineNodes.map(pipelineNode=>pipelineNode.id)});

      // Assignment: execute full pass.
      this.context=await this.executePipelineNodesRecursively(executablePipelineNodes,0,this.context);

      // Conditional: non-error and non-stopped pass remains in running state.
      if(this.state!==pipelineRuntimeStateNameByStatus.ERROR&&!this.stopRequested){
        // Assignment: runtime remains running after pass.
        this.state=pipelineRuntimeStateNameByStatus.RUNNING;
        // Assignment: context mirrors runtime state.
        this.context.status=this.state;
      }

      // Function call: emit pipeline-finished hook event.
      await this.pipelineExecutionHookBus.emitPipelineExecutionHooksForLifecyclePhase(pipelineExecutionHookPhaseNameByEvent.PIPELINE_EXECUTION_FINISHED,PipelineExecutionHookEventFactory.createPipelineExecutionFinishedEvent(this,this.context,{pipelineName:"main",elapsedMilliseconds:Date.now()-pipelineExecutionStartedAtMilliseconds}));
      // Function call: log pass completion.
      this.debug.log("info","Pipeline pass completed.",{cycle:this.context.cycle,errors:this.context.errors.length});
      return this.context;
    }catch(pipelineExecutionError){
      // Function call: emit pipeline-failed hook event before rethrowing.
      await this.pipelineExecutionHookBus.emitPipelineExecutionHooksForLifecyclePhase(pipelineExecutionHookPhaseNameByEvent.PIPELINE_EXECUTION_FAILED,PipelineExecutionHookEventFactory.createPipelineExecutionFailedEvent(this,this.context,pipelineExecutionError,{pipelineName:"main"}));
      throw pipelineExecutionError;
    }finally{
      // Assignment: release the runtime pass lock after completion, failure, or budget abort.
      this.isPipelinePassRunning=false;
    }
  }

  // Function: start repeated execution loop.
  async startLoop(onTickCallback){
    // Conditional: avoid multiple concurrent loops.
    if(this.loopHandle){
      return;
    }

    // Assignment: reset stop flag.
    this.stopRequested=false;
    // Assignment: runtime is running.
    this.state=pipelineRuntimeStateNameByStatus.RUNNING;

    // Assignment/function: loop body executes one pass and schedules the next.
    const executeLoopIteration=async()=>{
      // Conditional: stop request terminates loop.
      if(this.stopRequested||this.state===pipelineRuntimeStateNameByStatus.STOPPED){
        // Assignment: clear loop handle.
        this.loopHandle=null;
        return;
      }

      // Conditional: only run passes when state is running.
      if(this.state===pipelineRuntimeStateNameByStatus.RUNNING){
        // Function call: execute one pass.
        await this.runOnce();

        // Conditional: optional UI callback after pass.
        if(typeof onTickCallback==="function"){
          onTickCallback(this.context);
        }
      }

      // Assignment: schedule next pass.
      this.loopHandle=setTimeout(executeLoopIteration,this.cycleDelayMs);
    };

    await executeLoopIteration();
  }

  // Function: process external control signal.
  signal(signalName){
    // Conditional: reject unknown signals.
    if(!Object.values(pipelineSignalNameByAction).includes(signalName)){
      throw new Error("Unsupported signal: "+signalName);
    }

    // Function call: log signal.
    this.debug.log("info","Signal received.",{signal:signalName});

    // Conditional: start signal moves runtime to running.
    if(signalName===pipelineSignalNameByAction.START){
      // Assignment: clear stop flag.
      this.stopRequested=false;
      // Assignment: set runtime state.
      this.state=pipelineRuntimeStateNameByStatus.RUNNING;
    }else if(signalName===pipelineSignalNameByAction.STOP){
      // Assignment: set stop flag.
      this.stopRequested=true;
      // Assignment: set runtime state.
      this.state=pipelineRuntimeStateNameByStatus.STOPPED;

      // Conditional: clear active timer if present.
      if(this.loopHandle){
        // Function call: cancel timeout.
        clearTimeout(this.loopHandle);
        // Assignment: clear handle.
        this.loopHandle=null;
      }
    }else if(signalName===pipelineSignalNameByAction.PAUSE&&this.state===pipelineRuntimeStateNameByStatus.RUNNING){
      // Assignment: set paused state.
      this.state=pipelineRuntimeStateNameByStatus.PAUSED;
    }else if(signalName===pipelineSignalNameByAction.RESUME&&this.state===pipelineRuntimeStateNameByStatus.PAUSED){
      // Assignment: set running state.
      this.state=pipelineRuntimeStateNameByStatus.RUNNING;
    }else if(signalName===pipelineSignalNameByAction.RESET){
      // Assignment: clear stop flag.
      this.stopRequested=false;
      // Assignment: set idle state.
      this.state=pipelineRuntimeStateNameByStatus.IDLE;
      // Assignment: replace context.
      this.context=this.createFreshPipelineContext();
      // Function call: clear debug history.
      this.debug.clear();
    }

    // Assignment: mirror state into context.
    this.context.status=this.state;
  }
}

// Assignment: publish core API with old aliases preserved where useful.
globalObject.PipelineCore={
  Signal:pipelineSignalNameByAction,
  RuntimeState:pipelineRuntimeStateNameByStatus,
  NodeType:pipelineNodeTypeNameByRole,
  DataSupport:supportedPipelineDataFormat,
  RuntimeBudgetDefaults:Object.freeze({maximumNodesPerPass:DEFAULT_MAXIMUM_NODES_PER_PASS,maximumElapsedMsPerPass:DEFAULT_MAXIMUM_ELAPSED_MS_PER_PASS,maximumBranchEntriesPerPass:DEFAULT_MAXIMUM_BRANCH_ENTRIES_PER_PASS}),
  SecurityUtil:PipelineSecurityUtility,
  PipelineSecurityUtility,
  PipelineJsonDocument,
  PipelineJsonDocumentFactory,
  PipelineContextHelper:PipelineContextUtility,
  PipelineContextUtility,
  PipelineTextAnalysisUtility,
  DebugLogger:PipelineDebugLogger,
  PipelineDebugLogger,
  PipelineExecutionHookPhase:pipelineExecutionHookPhaseNameByEvent,
  PipelineExecutionHookFailurePolicy:pipelineExecutionHookFailurePolicyNameByAction,
  PipelineExecutionHookExecutionMode:pipelineExecutionHookExecutionModeNameByMode,
  PipelineExecutionHookSecurityGuard,
  PipelineExecutionHookRegistrationRecord,
  PipelineExecutionHookEventFactory,
  PipelineExecutionHookBus,
  RegistryEntry:PipelineRegistryEntry,
  PipelineRegistryEntry,
  PipelineNode:ExecutablePipelineNode,
  ExecutablePipelineNode,
  StartNode:PipelineStartSentinelNode,
  PipelineStartSentinelNode,
  StopNode:PipelineStopSentinelNode,
  PipelineStopSentinelNode,
  PipelinePlugin:PipelinePluginNode,
  PipelinePluginNode,
  PluginFactory:PipelinePluginFactory,
  PipelinePluginFactory,
  PluginRegistry:PipelinePluginRegistry,
  PipelinePluginRegistry,
  PluginManager:LoadedPipelinePluginManager,
  LoadedPipelinePluginManager,
  RecursivePipelineRuntime
};
})(globalThis);
