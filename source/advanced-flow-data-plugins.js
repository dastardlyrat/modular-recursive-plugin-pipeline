(function attachAdvancedFlowDataPluginsToGlobalObject(globalObject){
"use strict";

// Advanced Flow/Data Plugins
// Load after: pipeline-core.js and control-flow-plugins.js when branch plugins are used.

if(!globalObject.PipelineCore){
  throw new Error("PipelineAdvancedPlugins requires globalObject.PipelineCore to be loaded first.");
}

const {
  PipelinePluginNode,
  PipelinePluginFactory,
  PipelineSecurityUtility,
  PipelineJsonDocumentFactory,
  PipelineContextUtility,
  NodeType,
  DataSupport
}=globalObject.PipelineCore;

const missingValueSentinel=Object.freeze({__advancedPluginMissingValue:true});

function appendAdvancedDiagnostic(pipelineContext,diagnosticRecord){
  return PipelineContextUtility.appendDiagnosticRecord(pipelineContext,diagnosticRecord);
}

function setAdvancedPluginState(pipelineContext,plugin,stateNamespace,stateValue,stateOptions={}){
  if(typeof PipelineContextUtility.setPluginState==="function"){
    return PipelineContextUtility.setPluginState(pipelineContext,plugin,stateNamespace,stateValue,stateOptions);
  }

  if(!pipelineContext.advancedPluginState||typeof pipelineContext.advancedPluginState!=="object"){
    pipelineContext.advancedPluginState={};
  }

  const stateRecord={
    ownerInstanceId:plugin.instanceId||plugin.id||"unknown.instance",
    ownerTypeId:plugin.typeId||"unknown.type",
    namespace:stateNamespace,
    value:PipelineSecurityUtility.cloneStrictJsonValue(stateValue),
    visibility:stateOptions.visibility||"public",
    updatedAt:new Date().toISOString()
  };

  pipelineContext.advancedPluginState[stateRecord.ownerInstanceId+":"+stateNamespace]=stateRecord;
  return PipelineSecurityUtility.cloneStrictJsonValue(stateRecord);
}

function ensureRuntimeCanBranchToNamedPipeline(pipelineRuntime){
  if(globalObject.PipelineControlFlowPlugins&&typeof globalObject.PipelineControlFlowPlugins.ensureRuntimeControlFlowCapabilities==="function"){
    globalObject.PipelineControlFlowPlugins.ensureRuntimeControlFlowCapabilities(pipelineRuntime);
  }

  if(!pipelineRuntime||typeof pipelineRuntime.executeNamedPipelineFromStart!=="function"){
    throw new Error("Named pipeline branching requires control-flow runtime capabilities. Load control-flow-plugins.js before this plugin file.");
  }
}

function normalizePathParts(jsonPath){
  return PipelineJsonDocumentFactory.convertJsonPathToPathParts(jsonPath);
}

function getPlainCurrentData(pipelineContext){
  return PipelineContextUtility.getCurrentDataPlainObject(pipelineContext);
}

function hasValueAtJsonPath(jsonCompatibleRoot,jsonPath){
  const pathParts=normalizePathParts(jsonPath);

  if(pathParts.length===0){
    return true;
  }

  let cursor=jsonCompatibleRoot;

  for(const pathPart of pathParts){
    if(cursor===null||typeof cursor!=="object"||!(pathPart in cursor)){
      return false;
    }

    cursor=cursor[pathPart];
  }

  return true;
}

function getValueAtJsonPath(jsonCompatibleRoot,jsonPath,defaultValue=missingValueSentinel){
  const pathParts=normalizePathParts(jsonPath);

  if(pathParts.length===0){
    return jsonCompatibleRoot;
  }

  let cursor=jsonCompatibleRoot;

  for(const pathPart of pathParts){
    if(cursor===null||typeof cursor!=="object"||!(pathPart in cursor)){
      return defaultValue;
    }

    cursor=cursor[pathPart];
  }

  return cursor;
}

function writeJsonValueAtPath(pipelineContext,jsonPath,nextJsonCompatibleValue,transformationMetadata={}){
  const normalizedPathParts=normalizePathParts(jsonPath);
  const currentDocument=PipelineContextUtility.ensureContextDataDocument(pipelineContext);

  if(normalizedPathParts.length===0){
    if(typeof currentDocument.appendTransformationSnapshot==="function"){
      currentDocument.appendTransformationSnapshot(nextJsonCompatibleValue,{event:"advanced-plugin-root-write",...transformationMetadata});
    }else{
      currentDocument.replaceCurrentRoot(nextJsonCompatibleValue);
    }

    return pipelineContext;
  }

  currentDocument.setValueAtPath(normalizedPathParts,nextJsonCompatibleValue,{transformationMetadata:{event:"advanced-plugin-path-write",...transformationMetadata}});
  return pipelineContext;
}

function cloneJsonSafe(valueToClone){
  return PipelineSecurityUtility.cloneStrictJsonValue(valueToClone);
}

function normalizeJson(valueToNormalize,label="advanced plugin value"){
  return PipelineJsonDocumentFactory.normalizeJsonCompatibleValue(valueToNormalize,label);
}

function stableJsonStringify(valueToStringify){
  return JSON.stringify(valueToStringify,function sortObjectKeys(_key,value){
    if(value&&typeof value==="object"&&!Array.isArray(value)&&!(value instanceof Map)&&!(value instanceof Set)){
      const sortedObject={};
      for(const objectKey of Object.keys(value).sort()){
        sortedObject[objectKey]=value[objectKey];
      }
      return sortedObject;
    }

    return value;
  });
}

function jsonDeepEqual(leftValue,rightValue){
  return stableJsonStringify(leftValue)===stableJsonStringify(rightValue);
}

function valueLength(value){
  if(typeof value==="string"||Array.isArray(value)){
    return value.length;
  }

  if(value&&typeof value==="object"){
    return Object.keys(value).length;
  }

  return 0;
}

function evaluateConfiguredTest(actualValue,testOptions={}){
  const operator=String(testOptions.operator||"truthy").trim();
  const expectedValue=Object.prototype.hasOwnProperty.call(testOptions,"expectedValue")?testOptions.expectedValue:undefined;
  const actualIsMissing=actualValue===missingValueSentinel;

  let passed=false;

  if(operator==="always"){
    passed=true;
  }else if(operator==="never"){
    passed=false;
  }else if(operator==="exists"){
    passed=!actualIsMissing;
  }else if(operator==="notExists"){
    passed=actualIsMissing;
  }else if(operator==="truthy"){
    passed=!actualIsMissing&&Boolean(actualValue)===true;
  }else if(operator==="falsy"){
    passed=actualIsMissing||Boolean(actualValue)===false;
  }else if(operator==="isNull"){
    passed=actualValue===null;
  }else if(operator==="isArray"){
    passed=Array.isArray(actualValue);
  }else if(operator==="isPlainObject"){
    passed=PipelineSecurityUtility.isPlainObject(actualValue);
  }else if(operator==="isString"){
    passed=typeof actualValue==="string";
  }else if(operator==="isNumber"){
    passed=typeof actualValue==="number"&&Number.isFinite(actualValue);
  }else if(operator==="isBoolean"){
    passed=typeof actualValue==="boolean";
  }else if(operator==="strictEquals"){
    passed=Object.is(actualValue,expectedValue);
  }else if(operator==="notStrictEquals"){
    passed=!Object.is(actualValue,expectedValue);
  }else if(operator==="deepEquals"||operator==="equals"){
    passed=jsonDeepEqual(actualValue,expectedValue);
  }else if(operator==="deepNotEquals"||operator==="notEquals"){
    passed=!jsonDeepEqual(actualValue,expectedValue);
  }else if(operator==="greaterThan"){
    passed=Number(actualValue)>Number(expectedValue);
  }else if(operator==="greaterThanOrEqual"){
    passed=Number(actualValue)>=Number(expectedValue);
  }else if(operator==="lessThan"){
    passed=Number(actualValue)<Number(expectedValue);
  }else if(operator==="lessThanOrEqual"){
    passed=Number(actualValue)<=Number(expectedValue);
  }else if(operator==="contains"){
    if(typeof actualValue==="string"){
      passed=actualValue.includes(String(expectedValue));
    }else if(Array.isArray(actualValue)){
      passed=actualValue.some(arrayItem=>jsonDeepEqual(arrayItem,expectedValue));
    }
  }else if(operator==="includesKey"){
    passed=actualValue!==null&&typeof actualValue==="object"&&!Array.isArray(actualValue)&&Object.prototype.hasOwnProperty.call(actualValue,String(expectedValue));
  }else if(operator==="lengthEquals"){
    passed=valueLength(actualValue)===Number(expectedValue);
  }else if(operator==="lengthGreaterThan"){
    passed=valueLength(actualValue)>Number(expectedValue);
  }else if(operator==="lengthGreaterThanOrEqual"){
    passed=valueLength(actualValue)>=Number(expectedValue);
  }else if(operator==="lengthLessThan"){
    passed=valueLength(actualValue)<Number(expectedValue);
  }else if(operator==="matchesRegex"){
    const regularExpression=PipelineSecurityUtility.createBoundedRegularExpression(String(expectedValue),String(testOptions.regexFlags||""));
    passed=regularExpression.test(String(actualValue));
  }else{
    throw new Error("Unsupported advanced plugin test operator: "+operator);
  }

  return {
    operator,
    expectedValue:cloneJsonSafe(expectedValue),
    actualValue:actualIsMissing?null:cloneJsonSafe(actualValue),
    actualWasMissing:actualIsMissing,
    passed
  };
}

function ensureArrayAtInputPath(jsonRoot,inputPath,pluginLabel){
  const valueAtPath=getValueAtJsonPath(jsonRoot,inputPath,missingValueSentinel);

  if(!Array.isArray(valueAtPath)){
    throw new TypeError(pluginLabel+" expected an array at path '"+(inputPath||"<root>")+"'.");
  }

  return valueAtPath;
}

function resolveGlobalPath(pathText){
  if(typeof pathText!=="string"||!pathText.trim()){
    return undefined;
  }

  const pathParts=pathText.split(".").filter(Boolean);
  let cursor=globalObject;

  for(const pathPart of pathParts){
    if(!cursor||!(pathPart in cursor)){
      return undefined;
    }

    cursor=cursor[pathPart];
  }

  return cursor;
}

function createChildPipelineContextForItem(parentContext,itemValue,itemIndex){
  return {
    runId:parentContext.runId,
    cycle:parentContext.cycle,
    status:parentContext.status,
    input:{foreachIndex:itemIndex,parentRunId:parentContext.runId},
    data:PipelineJsonDocumentFactory.createPipelineJsonDocument(itemValue,{source:"foreach-plugin-runner",foreachIndex:itemIndex}),
    tokenizerState:{},
    regexState:{},
    state:{byInstanceId:{},byTypeId:{},timeline:[]},
    streamState:null,
    output:{},
    stream:[],
    diagnostics:[],
    errors:[]
  };
}

function ensurePipelineObjectStore(pipelineContext){
  if(!Object.prototype.hasOwnProperty.call(pipelineContext,"pipelineObjectStore")){
    Object.defineProperty(pipelineContext,"pipelineObjectStore",{
      value:{byName:{}},
      writable:true,
      enumerable:false,
      configurable:true
    });
  }

  if(!pipelineContext.pipelineObjectStore||typeof pipelineContext.pipelineObjectStore!=="object"){
    pipelineContext.pipelineObjectStore={byName:{}};
  }

  if(!pipelineContext.pipelineObjectStore.byName||typeof pipelineContext.pipelineObjectStore.byName!=="object"){
    pipelineContext.pipelineObjectStore.byName={};
  }

  return pipelineContext.pipelineObjectStore;
}

class JsonPipelineQueue{
  constructor(initialItems=[]){
    if(!Array.isArray(initialItems)){
      throw new TypeError("JsonPipelineQueue requires an array of initial items.");
    }

    this.items=cloneJsonSafe(initialItems);
  }

  enqueue(itemValue){
    this.items.push(cloneJsonSafe(itemValue));
    return this.items.length;
  }

  dequeue(){
    return this.items.length?this.items.shift():undefined;
  }

  peek(){
    return this.items.length?cloneJsonSafe(this.items[0]):undefined;
  }

  toArray(){
    return cloneJsonSafe(this.items);
  }

  get length(){
    return this.items.length;
  }
}

function jsonToArrayRepresentation(jsonValue){
  if(Array.isArray(jsonValue)){
    return cloneJsonSafe(jsonValue);
  }

  if(jsonValue&&typeof jsonValue==="object"){
    return Object.entries(jsonValue).map(([key,value])=>({key,value:cloneJsonSafe(value)}));
  }

  return [cloneJsonSafe(jsonValue)];
}

function jsonToMapObject(jsonValue){
  if(Array.isArray(jsonValue)){
    const mapValue=new Map();

    for(const arrayItem of jsonValue){
      if(Array.isArray(arrayItem)&&arrayItem.length>=2){
        mapValue.set(String(arrayItem[0]),cloneJsonSafe(arrayItem[1]));
      }else if(arrayItem&&typeof arrayItem==="object"&&Object.prototype.hasOwnProperty.call(arrayItem,"key")){
        mapValue.set(String(arrayItem.key),cloneJsonSafe(arrayItem.value));
      }else{
        mapValue.set(String(mapValue.size),cloneJsonSafe(arrayItem));
      }
    }

    return mapValue;
  }

  if(jsonValue&&typeof jsonValue==="object"){
    return new Map(Object.entries(jsonValue).map(([key,value])=>[key,cloneJsonSafe(value)]));
  }

  return new Map([["value",cloneJsonSafe(jsonValue)]]);
}

function jsonToQueueObject(jsonValue){
  if(Array.isArray(jsonValue)){
    return new JsonPipelineQueue(jsonValue);
  }

  if(jsonValue&&typeof jsonValue==="object"){
    return new JsonPipelineQueue(Object.entries(jsonValue).map(([key,value])=>({key,value:cloneJsonSafe(value)})));
  }

  return new JsonPipelineQueue([jsonValue]);
}

function objectToJsonRepresentation(objectValue,options={}){
  if(objectValue instanceof JsonPipelineQueue){
    return {type:"queue",items:objectValue.toArray()};
  }

  if(objectValue instanceof Map){
    const mapOutputMode=options.mapOutputMode||"object";

    if(mapOutputMode==="entries"){
      return [...objectValue.entries()].map(([key,value])=>({key:String(key),value:normalizeJson(value,"map entry value")}));
    }

    const outputObject={};
    for(const [key,value] of objectValue.entries()){
      outputObject[String(key)]=normalizeJson(value,"map value");
    }
    return outputObject;
  }

  if(objectValue instanceof Set){
    return [...objectValue.values()].map(setItem=>normalizeJson(setItem,"set item"));
  }

  if(Array.isArray(objectValue)){
    return normalizeJson(objectValue,"array object");
  }

  if(objectValue&&typeof objectValue==="object"){
    return normalizeJson(objectValue,"object value");
  }

  return normalizeJson(objectValue,"primitive object value");
}

function buildObjectEmissionFromJson(jsonValue,targetObjectType){
  if(targetObjectType==="array"){
    return {objectType:"array",objectValue:jsonToArrayRepresentation(jsonValue),jsonRepresentation:jsonToArrayRepresentation(jsonValue)};
  }

  if(targetObjectType==="map"){
    const mapObject=jsonToMapObject(jsonValue);
    return {objectType:"map",objectValue:mapObject,jsonRepresentation:objectToJsonRepresentation(mapObject,{mapOutputMode:"entries"})};
  }

  if(targetObjectType==="queue"){
    const queueObject=jsonToQueueObject(jsonValue);
    return {objectType:"queue",objectValue:queueObject,jsonRepresentation:objectToJsonRepresentation(queueObject)};
  }

  if(targetObjectType==="entries"){
    const entriesArray=jsonToArrayRepresentation(jsonValue);
    return {objectType:"entries",objectValue:entriesArray,jsonRepresentation:entriesArray};
  }

  if(targetObjectType==="object"){
    const objectValue=cloneJsonSafe(jsonValue);
    return {objectType:"object",objectValue,jsonRepresentation:objectToJsonRepresentation(objectValue)};
  }

  throw new Error("Unsupported emitter target object type: "+targetObjectType);
}

function applyBuiltInItemOperation(itemValue,itemIndex,itemCount,operationName,operationOptions={}){
  const clonedItem=cloneJsonSafe(itemValue);

  if(operationName==="identity"||operationName==="clone"){
    return clonedItem;
  }

  if(operationName==="addIndex"){
    if(clonedItem&&typeof clonedItem==="object"&&!Array.isArray(clonedItem)){
      return {...clonedItem,index:itemIndex,itemCount};
    }

    return {index:itemIndex,itemCount,value:clonedItem};
  }

  if(operationName==="wrap"){
    return {index:itemIndex,value:clonedItem};
  }

  if(operationName==="extractPath"){
    return getValueAtJsonPath(clonedItem,operationOptions.path||"",null);
  }

  if(operationName==="stringify"){
    return JSON.stringify(clonedItem);
  }

  if(operationName==="parseJsonText"){
    if(typeof clonedItem!=="string"){
      throw new TypeError("parseJsonText operation requires string items.");
    }

    return PipelineSecurityUtility.parseJsonTextSafely(clonedItem,"foreach item JSON text");
  }

  if(operationName==="uppercaseText"||operationName==="lowercaseText"||operationName==="trimText"){
    const textPath=operationOptions.textPath||"text";
    const currentText=getValueAtJsonPath(clonedItem,textPath,typeof clonedItem==="string"?clonedItem:"");

    if(typeof currentText!=="string"){
      return clonedItem;
    }

    let nextText=currentText;
    if(operationName==="uppercaseText"){
      nextText=currentText.toUpperCase();
    }else if(operationName==="lowercaseText"){
      nextText=currentText.toLowerCase();
    }else{
      nextText=currentText.trim();
    }

    if(typeof clonedItem==="string"){
      return nextText;
    }

    const itemDocument=PipelineJsonDocumentFactory.createPipelineJsonDocument(clonedItem);
    itemDocument.setValueAtPath(textPath,nextText,{transformationMetadata:{event:"foreach-builtin-text-operation",operationName}});
    return itemDocument.toPlainObject();
  }

  throw new Error("Unsupported foreach built-in operation: "+operationName);
}

class ConditionalTestBranchToNamedPipelinePlugin extends PipelinePluginNode{
  constructor(options={}){
    PipelineSecurityUtility.assertPlainObject(options,"ConditionalTestBranchToNamedPipelinePlugin options");
    super({
      typeId:options.typeId||"plugin.advanced.control.branch.byJsonTest",
      label:options.label||"Conditional JSON Test Branch Plugin",
      type:NodeType.EXTENSION_PLUGIN,
      requires:options.requires||[],
      supports:options.supports||[DataSupport.JSON],
      description:options.description||"Branches to a named pipeline when a configured JSON-path test passes."
    });

    this.targetPipelineName=options.targetPipelineName||"secondary";
    this.inputPath=Object.prototype.hasOwnProperty.call(options,"inputPath")?options.inputPath:"";
    this.operator=options.operator||"truthy";
    this.expectedValue=Object.prototype.hasOwnProperty.call(options,"expectedValue")?cloneJsonSafe(options.expectedValue):undefined;
    this.regexFlags=options.regexFlags||"";
    this.branchWhen=Object.prototype.hasOwnProperty.call(options,"branchWhen")?Boolean(options.branchWhen):true;
  }

  async execute(pipelineContext,pipelineRuntime){
    ensureRuntimeCanBranchToNamedPipeline(pipelineRuntime);
    const currentData=getPlainCurrentData(pipelineContext);
    const actualValue=getValueAtJsonPath(currentData,this.inputPath,missingValueSentinel);
    const testResult=evaluateConfiguredTest(actualValue,{operator:this.operator,expectedValue:this.expectedValue,regexFlags:this.regexFlags});
    const shouldBranch=testResult.passed===this.branchWhen;
    const branchRecord={targetPipelineName:this.targetPipelineName,inputPath:this.inputPath,testResult,branchWhen:this.branchWhen,branched:shouldBranch,checkedAt:new Date().toISOString()};

    setAdvancedPluginState(pipelineContext,this,"conditionalBranch",branchRecord,{visibility:"public"});
    appendAdvancedDiagnostic(pipelineContext,{node:this.id,typeId:this.typeId,event:"advanced-conditional-json-branch-evaluated",...branchRecord});

    if(!shouldBranch){
      return pipelineContext;
    }

    return pipelineRuntime.executeNamedPipelineFromStart(this.targetPipelineName,pipelineContext,{sourcePlugin:this.typeId,sourceInstanceId:this.instanceId,branchRecord});
  }
}

class ExternalSignalBranchToNamedPipelinePlugin extends PipelinePluginNode{
  constructor(options={}){
    PipelineSecurityUtility.assertPlainObject(options,"ExternalSignalBranchToNamedPipelinePlugin options");
    super({
      typeId:options.typeId||"plugin.advanced.control.branch.byExternalSignal",
      label:options.label||"External Signal Branch Plugin",
      type:NodeType.EXTENSION_PLUGIN,
      requires:options.requires||[],
      supports:options.supports||[DataSupport.JSON],
      description:options.description||"Branches to a named pipeline when a runtime external signal matches a configured test."
    });

    this.targetPipelineName=options.targetPipelineName||"secondary";
    this.externalSignalName=options.externalSignalName||"branch-control-signal";
    this.operator=options.operator||"truthy";
    this.expectedValue=Object.prototype.hasOwnProperty.call(options,"expectedValue")?cloneJsonSafe(options.expectedValue):true;
    this.branchWhen=Object.prototype.hasOwnProperty.call(options,"branchWhen")?Boolean(options.branchWhen):true;
  }

  async execute(pipelineContext,pipelineRuntime){
    ensureRuntimeCanBranchToNamedPipeline(pipelineRuntime);

    if(typeof pipelineRuntime.getExternalSignalValue!=="function"){
      throw new Error("ExternalSignalBranchToNamedPipelinePlugin requires runtime.getExternalSignalValue().");
    }

    const externalSignalValue=pipelineRuntime.getExternalSignalValue(this.externalSignalName,undefined);
    const testResult=evaluateConfiguredTest(externalSignalValue,{operator:this.operator,expectedValue:this.expectedValue});
    const shouldBranch=testResult.passed===this.branchWhen;
    const branchRecord={targetPipelineName:this.targetPipelineName,externalSignalName:this.externalSignalName,testResult,branchWhen:this.branchWhen,branched:shouldBranch,checkedAt:new Date().toISOString()};

    setAdvancedPluginState(pipelineContext,this,"externalSignalBranch",branchRecord,{visibility:"public"});
    appendAdvancedDiagnostic(pipelineContext,{node:this.id,typeId:this.typeId,event:"advanced-external-signal-branch-evaluated",...branchRecord});

    if(!shouldBranch){
      return pipelineContext;
    }

    return pipelineRuntime.executeNamedPipelineFromStart(this.targetPipelineName,pipelineContext,{sourcePlugin:this.typeId,sourceInstanceId:this.instanceId,branchRecord});
  }
}

class ForEachJsonFunctionPlugin extends PipelinePluginNode{
  constructor(options={}){
    PipelineSecurityUtility.assertPlainObject(options,"ForEachJsonFunctionPlugin options");
    super({
      typeId:options.typeId||"plugin.advanced.foreach.function",
      label:options.label||"For Each JSON Function Plugin",
      type:NodeType.EXTENSION_PLUGIN,
      requires:options.requires||[],
      supports:options.supports||[DataSupport.JSON],
      description:options.description||"Runs a configured function or built-in operation on every item in a JSON array."
    });

    if(Object.prototype.hasOwnProperty.call(options,"itemMapper")&&typeof options.itemMapper!=="function"){
      throw new TypeError("ForEachJsonFunctionPlugin itemMapper must be a function when provided.");
    }

    this.inputPath=Object.prototype.hasOwnProperty.call(options,"inputPath")?options.inputPath:"";
    this.outputPath=Object.prototype.hasOwnProperty.call(options,"outputPath")?options.outputPath:this.inputPath;
    this.itemMapper=options.itemMapper||null;
    this.operationName=options.operationName||"identity";
    this.operationOptions=cloneJsonSafe(options.operationOptions||{});
    this.maximumItems=Number.isInteger(options.maximumItems)?options.maximumItems:10000;
  }

  async execute(pipelineContext,pipelineRuntime){
    const currentData=getPlainCurrentData(pipelineContext);
    const inputArray=ensureArrayAtInputPath(currentData,this.inputPath,this.label);

    if(inputArray.length>this.maximumItems){
      throw new RangeError(this.label+" refused to process "+inputArray.length+" items; maximumItems is "+this.maximumItems+".");
    }

    const outputArray=[];

    for(let itemIndex=0;itemIndex<inputArray.length;itemIndex+=1){
      const inputItem=cloneJsonSafe(inputArray[itemIndex]);
      const mappedItem=this.itemMapper
        ? this.itemMapper(inputItem,{index:itemIndex,itemCount:inputArray.length,inputPath:this.inputPath,outputPath:this.outputPath,pipelineContext,pipelineRuntime,plugin:this})
        : applyBuiltInItemOperation(inputItem,itemIndex,inputArray.length,this.operationName,this.operationOptions);

      outputArray.push(normalizeJson(mappedItem,"foreach mapped item"));
    }

    writeJsonValueAtPath(pipelineContext,this.outputPath,outputArray,{typeId:this.typeId,instanceId:this.instanceId,inputPath:this.inputPath,outputPath:this.outputPath});
    const stateRecord={inputPath:this.inputPath,outputPath:this.outputPath,itemCount:outputArray.length,operation:this.itemMapper?"custom-itemMapper":this.operationName,processedAt:new Date().toISOString()};
    setAdvancedPluginState(pipelineContext,this,"foreachFunction",stateRecord,{visibility:"public"});
    appendAdvancedDiagnostic(pipelineContext,{node:this.id,typeId:this.typeId,event:"advanced-foreach-function-complete",...stateRecord});
    return pipelineContext;
  }
}

class ForEachJsonPluginRunnerPlugin extends PipelinePluginNode{
  constructor(options={}){
    PipelineSecurityUtility.assertPlainObject(options,"ForEachJsonPluginRunnerPlugin options");
    super({
      typeId:options.typeId||"plugin.advanced.foreach.pluginRunner",
      label:options.label||"For Each JSON Plugin Runner Plugin",
      type:NodeType.EXTENSION_PLUGIN,
      requires:options.requires||[],
      supports:options.supports||[DataSupport.JSON],
      description:options.description||"Creates and runs a configured plugin once for every item in a JSON array."
    });

    this.inputPath=Object.prototype.hasOwnProperty.call(options,"inputPath")?options.inputPath:"";
    this.outputPath=Object.prototype.hasOwnProperty.call(options,"outputPath")?options.outputPath:this.inputPath;
    this.pluginFactorySource=options.pluginFactorySource||null;
    this.pluginGlobalPath=options.pluginGlobalPath||null;
    this.pluginFactoryArguments=Array.isArray(options.pluginFactoryArguments)?options.pluginFactoryArguments:[];
    this.maximumItems=Number.isInteger(options.maximumItems)?options.maximumItems:1000;
    this.includeChildDiagnostics=Boolean(options.includeChildDiagnostics);
    this.includeChildErrors=Object.prototype.hasOwnProperty.call(options,"includeChildErrors")?Boolean(options.includeChildErrors):true;
  }

  resolvePluginFactorySource(){
    if(this.pluginFactorySource){
      return this.pluginFactorySource;
    }

    const resolvedSource=resolveGlobalPath(this.pluginGlobalPath);

    if(!resolvedSource){
      throw new Error("ForEachJsonPluginRunnerPlugin requires pluginFactorySource or a resolvable pluginGlobalPath.");
    }

    return resolvedSource;
  }

  async execute(pipelineContext,pipelineRuntime){
    const currentData=getPlainCurrentData(pipelineContext);
    const inputArray=ensureArrayAtInputPath(currentData,this.inputPath,this.label);

    if(inputArray.length>this.maximumItems){
      throw new RangeError(this.label+" refused to process "+inputArray.length+" items; maximumItems is "+this.maximumItems+".");
    }

    const pluginFactory=PipelinePluginFactory.fromFactorySource(this.resolvePluginFactorySource(),this.pluginFactoryArguments);
    const outputArray=[];
    const childRunRecords=[];

    for(let itemIndex=0;itemIndex<inputArray.length;itemIndex+=1){
      const childPlugin=pluginFactory.createInstance();
      childPlugin.instanceId=(childPlugin.typeId||"plugin.advanced.child")+"#foreach-"+String(itemIndex).padStart(4,"0");
      childPlugin.id=childPlugin.instanceId;

      const childContext=createChildPipelineContextForItem(pipelineContext,inputArray[itemIndex],itemIndex);
      const resultContext=await childPlugin.execute(childContext,pipelineRuntime);
      const resultItem=PipelineContextUtility.getCurrentDataPlainObject(resultContext);
      outputArray.push(normalizeJson(resultItem,"foreach plugin result item"));

      const childRunRecord={index:itemIndex,childTypeId:childPlugin.typeId,childInstanceId:childPlugin.instanceId,diagnosticCount:resultContext.diagnostics.length,errorCount:resultContext.errors.length};
      childRunRecords.push(childRunRecord);

      if(this.includeChildDiagnostics){
        for(const childDiagnostic of resultContext.diagnostics){
          appendAdvancedDiagnostic(pipelineContext,{node:this.id,typeId:this.typeId,event:"advanced-foreach-child-diagnostic",foreachIndex:itemIndex,childDiagnostic});
        }
      }

      if(this.includeChildErrors&&resultContext.errors.length){
        for(const childError of resultContext.errors){
          PipelineContextUtility.appendErrorRecord(pipelineContext,{node:this.id,typeId:this.typeId,event:"advanced-foreach-child-error",foreachIndex:itemIndex,childError});
        }
      }
    }

    writeJsonValueAtPath(pipelineContext,this.outputPath,outputArray,{typeId:this.typeId,instanceId:this.instanceId,inputPath:this.inputPath,outputPath:this.outputPath,childPluginTypeId:pluginFactory.typeId});
    const stateRecord={inputPath:this.inputPath,outputPath:this.outputPath,itemCount:outputArray.length,childPluginTypeId:pluginFactory.typeId,childRuns:childRunRecords,processedAt:new Date().toISOString()};
    setAdvancedPluginState(pipelineContext,this,"foreachPluginRunner",stateRecord,{visibility:"public"});
    appendAdvancedDiagnostic(pipelineContext,{node:this.id,typeId:this.typeId,event:"advanced-foreach-plugin-runner-complete",itemCount:outputArray.length,childPluginTypeId:pluginFactory.typeId});
    return pipelineContext;
  }
}

class JsonObjectEmitterPlugin extends PipelinePluginNode{
  constructor(options={}){
    PipelineSecurityUtility.assertPlainObject(options,"JsonObjectEmitterPlugin options");
    super({
      typeId:options.typeId||"plugin.advanced.emitter.jsonObject",
      label:options.label||"JSON Object Emitter Plugin",
      type:NodeType.EXTENSION_PLUGIN,
      requires:options.requires||[],
      supports:options.supports||[DataSupport.JSON],
      description:options.description||"Converts JSON into arrays, maps, queues, or objects, and converts emitted objects back into JSON."
    });

    this.mode=options.mode||"jsonToObject";
    this.inputPath=Object.prototype.hasOwnProperty.call(options,"inputPath")?options.inputPath:"";
    this.outputPath=Object.prototype.hasOwnProperty.call(options,"outputPath")?options.outputPath:"";
    this.objectName=options.objectName||"default";
    this.sourceObjectName=options.sourceObjectName||this.objectName;
    this.targetObjectType=options.targetObjectType||"array";
    this.mapOutputMode=options.mapOutputMode||"object";
    this.writeJsonRepresentationToData=Boolean(options.writeJsonRepresentationToData);
    this.sourceObject=Object.prototype.hasOwnProperty.call(options,"sourceObject")?options.sourceObject:null;
  }

  async execute(pipelineContext,pipelineRuntime){
    const objectStore=ensurePipelineObjectStore(pipelineContext);
    let emittedObjectType=null;
    let jsonRepresentation=null;

    if(this.mode==="jsonToObject"||this.mode==="jsonToObjectAndWriteJson"||this.mode==="roundTrip"){
      const currentData=getPlainCurrentData(pipelineContext);
      const inputValue=getValueAtJsonPath(currentData,this.inputPath,missingValueSentinel);

      if(inputValue===missingValueSentinel){
        throw new Error("JsonObjectEmitterPlugin input path not found: "+(this.inputPath||"<root>"));
      }

      const emission=buildObjectEmissionFromJson(inputValue,this.targetObjectType);
      emittedObjectType=emission.objectType;
      jsonRepresentation=emission.jsonRepresentation;
      objectStore.byName[this.objectName]={objectType:emission.objectType,objectValue:emission.objectValue,createdByInstanceId:this.instanceId,createdByTypeId:this.typeId,createdAt:new Date().toISOString()};

      if(this.mode==="jsonToObjectAndWriteJson"||this.mode==="roundTrip"||this.writeJsonRepresentationToData){
        writeJsonValueAtPath(pipelineContext,this.outputPath,jsonRepresentation,{typeId:this.typeId,instanceId:this.instanceId,emitterMode:this.mode,objectName:this.objectName,objectType:emittedObjectType});
      }
    }else if(this.mode==="objectToJson"){
      let sourceObjectValue=this.sourceObject;
      let sourceObjectType="direct";

      if(sourceObjectValue===null){
        const objectRecord=objectStore.byName[this.sourceObjectName];

        if(!objectRecord){
          throw new Error("JsonObjectEmitterPlugin object store name not found: "+this.sourceObjectName);
        }

        sourceObjectValue=objectRecord.objectValue;
        sourceObjectType=objectRecord.objectType;
      }

      jsonRepresentation=objectToJsonRepresentation(sourceObjectValue,{mapOutputMode:this.mapOutputMode});
      emittedObjectType=sourceObjectType;
      writeJsonValueAtPath(pipelineContext,this.outputPath,jsonRepresentation,{typeId:this.typeId,instanceId:this.instanceId,emitterMode:this.mode,sourceObjectName:this.sourceObjectName,objectType:emittedObjectType});
    }else{
      throw new Error("Unsupported JsonObjectEmitterPlugin mode: "+this.mode);
    }

    const stateRecord={mode:this.mode,objectName:this.objectName,sourceObjectName:this.sourceObjectName,targetObjectType:this.targetObjectType,emittedObjectType,jsonRepresentation,processedAt:new Date().toISOString()};
    setAdvancedPluginState(pipelineContext,this,"jsonObjectEmitter",stateRecord,{visibility:"public"});
    appendAdvancedDiagnostic(pipelineContext,{node:this.id,typeId:this.typeId,event:"advanced-json-object-emitter-complete",mode:this.mode,objectName:this.objectName,sourceObjectName:this.sourceObjectName,objectType:emittedObjectType,wroteJsonRepresentation:this.mode==="objectToJson"||this.mode==="jsonToObjectAndWriteJson"||this.mode==="roundTrip"||this.writeJsonRepresentationToData});
    void pipelineRuntime;
    return pipelineContext;
  }
}

function createConditionalTestBranchToNamedPipelinePluginFactory(options={}){
  return new PipelinePluginFactory({createPluginInstance:()=>new ConditionalTestBranchToNamedPipelinePlugin(options)});
}

function createExternalSignalBranchToNamedPipelinePluginFactory(options={}){
  return new PipelinePluginFactory({createPluginInstance:()=>new ExternalSignalBranchToNamedPipelinePlugin(options)});
}

function createForEachJsonFunctionPluginFactory(options={}){
  return new PipelinePluginFactory({createPluginInstance:()=>new ForEachJsonFunctionPlugin(options)});
}

function createForEachJsonPluginRunnerPluginFactory(options={}){
  return new PipelinePluginFactory({createPluginInstance:()=>new ForEachJsonPluginRunnerPlugin(options)});
}

function createJsonObjectEmitterPluginFactory(options={}){
  return new PipelinePluginFactory({createPluginInstance:()=>new JsonObjectEmitterPlugin(options)});
}

globalObject.PipelineAdvancedPlugins={
  ConditionalTestBranchToNamedPipelinePlugin,
  ExternalSignalBranchToNamedPipelinePlugin,
  ForEachJsonFunctionPlugin,
  ForEachJsonPluginRunnerPlugin,
  JsonObjectEmitterPlugin,
  JsonPipelineQueue,
  createConditionalTestBranchToNamedPipelinePluginFactory,
  createExternalSignalBranchToNamedPipelinePluginFactory,
  createForEachJsonFunctionPluginFactory,
  createForEachJsonPluginRunnerPluginFactory,
  createJsonObjectEmitterPluginFactory,
  helpers:{
    evaluateConfiguredTest,
    getValueAtJsonPath,
    hasValueAtJsonPath,
    writeJsonValueAtPath,
    ensurePipelineObjectStore,
    objectToJsonRepresentation,
    buildObjectEmissionFromJson
  }
};

if(typeof module!=="undefined"&&module.exports){
  module.exports=globalObject.PipelineAdvancedPlugins;
}
})(globalThis);
