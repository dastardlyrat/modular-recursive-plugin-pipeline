(function attachStreamPipelinePluginsToGlobalObject(globalObject){
"use strict";

// Assignment: read core symbols once for stream plugin definitions.
const {PipelinePluginNode,PipelinePluginFactory,PipelineSecurityUtility,PipelineJsonDocumentFactory,PipelineContextUtility,PipelineTextAnalysisUtility,NodeType,DataSupport}=globalObject.PipelineCore;

// Assignment: reuse stateful helpers for reader parsing and bounded regex scanning.
const {normalizeReaderEnvelopeIntoJsonData,runBoundedRegexSearch}=globalObject.PipelinePlugins.helpers;

// Function: ensure stream state exists and has required containers.
function ensureStreamProcessingState(pipelineContext){
  // Function call: ensure base context shape.
  PipelineContextUtility.ensurePipelineContextShape(pipelineContext);

  // Conditional/assignment: create stream state on first use.
  if(!pipelineContext.streamState||typeof pipelineContext.streamState!=="object"){
    pipelineContext.streamState={originalJsonDocument:null,transformedJsonDocument:null,revisions:[],tokenizerState:null,regexState:{},wordCountState:null};
  }

  // Conditional/assignment: revisions must be an array.
  if(!Array.isArray(pipelineContext.streamState.revisions)){
    pipelineContext.streamState.revisions=[];
  }

  // Conditional/assignment: regex state must be an object.
  if(!pipelineContext.streamState.regexState||typeof pipelineContext.streamState.regexState!=="object"){
    pipelineContext.streamState.regexState={};
  }

  return pipelineContext.streamState;
}

// Function: initialize original and transformed JSON documents from reader data.
function initializeStreamStateFromJsonData(pipelineContext,jsonCompatibleData,streamMetadata={}){
  // Assignment: ensure stream state exists.
  const streamProcessingState=ensureStreamProcessingState(pipelineContext);
  // Assignment: original JSON document preserves the reader input.
  streamProcessingState.originalJsonDocument=PipelineJsonDocumentFactory.createPipelineJsonDocument(jsonCompatibleData,{...streamMetadata,streamRole:"original"});
  // Assignment: transformed JSON document starts as a copy of the reader input.
  streamProcessingState.transformedJsonDocument=PipelineJsonDocumentFactory.createPipelineJsonDocument(jsonCompatibleData,{...streamMetadata,streamRole:"transformed"});
  // Assignment: clear stream revision history on new read.
  streamProcessingState.revisions=[];
  // Assignment: clear stream tokenizer state.
  streamProcessingState.tokenizerState=null;
  // Assignment: clear stream regex state.
  streamProcessingState.regexState={};
  // Assignment: clear stream word-count state.
  streamProcessingState.wordCountState=null;
  // Assignment: context data follows transformed document for compatibility with ordinary plugins.
  pipelineContext.data=streamProcessingState.transformedJsonDocument;
  // Assignment: context regex state mirrors stream regex state.
  pipelineContext.regexState={};
  // Assignment: context tokenizer state starts empty.
  pipelineContext.tokenizerState={};
  return streamProcessingState;
}

// Function: get current transformed stream document.
function getTransformedStreamJsonDocument(pipelineContext){
  // Assignment: ensure stream state exists.
  const streamProcessingState=ensureStreamProcessingState(pipelineContext);

  // Conditional/assignment: if stream transformed document is missing, create it from context data.
  if(!PipelineJsonDocumentFactory.isPipelineJsonDocument(streamProcessingState.transformedJsonDocument)){
    streamProcessingState.transformedJsonDocument=PipelineJsonDocumentFactory.ensurePipelineJsonDocument(pipelineContext.data||{});
  }

  return streamProcessingState.transformedJsonDocument;
}

// Function: get original stream plain JSON.
function getOriginalStreamJsonPlainObject(pipelineContext){
  // Assignment: ensure stream state exists.
  const streamProcessingState=ensureStreamProcessingState(pipelineContext);

  // Conditional: return stored original when available.
  if(PipelineJsonDocumentFactory.isPipelineJsonDocument(streamProcessingState.originalJsonDocument)){
    return streamProcessingState.originalJsonDocument.toPlainObject();
  }

  return PipelineContextUtility.getOriginalDataPlainObject(pipelineContext);
}

// Function: get transformed stream plain JSON.
function getTransformedStreamJsonPlainObject(pipelineContext){
  return getTransformedStreamJsonDocument(pipelineContext).toPlainObject();
}

// Function: replace transformed JSON and mirror it into context.data.
function replaceTransformedStreamJsonDocument(pipelineContext,nextJsonCompatibleData,revisionMetadata){
  // Assignment: ensure stream state exists.
  const streamProcessingState=ensureStreamProcessingState(pipelineContext);
  // Conditional: append to existing transformed document history when available.
  if(PipelineJsonDocumentFactory.isPipelineJsonDocument(streamProcessingState.transformedJsonDocument)&&typeof streamProcessingState.transformedJsonDocument.appendTransformationSnapshot==="function"){
    // Function call: append next transformed JSON as a new immutable history snapshot.
    streamProcessingState.transformedJsonDocument.appendTransformationSnapshot(nextJsonCompatibleData,{streamRole:"transformed",updatedBy:revisionMetadata.typeId,event:revisionMetadata.event,node:revisionMetadata.node});
  }else{
    // Assignment: new transformed document.
    streamProcessingState.transformedJsonDocument=PipelineJsonDocumentFactory.createPipelineJsonDocument(nextJsonCompatibleData,{streamRole:"transformed",updatedBy:revisionMetadata.typeId});
  }
  // Assignment: context data mirrors transformed document.
  pipelineContext.data=streamProcessingState.transformedJsonDocument;

  // Assignment: transformed keys help debug object-level changes.
  const transformedPlainJson=streamProcessingState.transformedJsonDocument.toPlainObject();
  // Assignment: revision record.
  const revisionRecord={node:revisionMetadata.node,typeId:revisionMetadata.typeId,event:revisionMetadata.event,time:new Date().toISOString(),transformedKeys:transformedPlainJson&&typeof transformedPlainJson==="object"&&!Array.isArray(transformedPlainJson)?Object.keys(transformedPlainJson):[]};
  // Assignment: append revision record.
  streamProcessingState.revisions.push(revisionRecord);
  return streamProcessingState.transformedJsonDocument;
}

// Function: read text from a plain JSON value.
function getTextFromJsonCompatibleValue(jsonCompatibleValue){
  // Conditional: prefer text field when it exists and is textual.
  if(jsonCompatibleValue&&typeof jsonCompatibleValue==="object"&&typeof jsonCompatibleValue.text==="string"){
    return jsonCompatibleValue.text;
  }

  return JSON.stringify(jsonCompatibleValue||{},null,2);
}

// Function/class: stream reader preserves original JSON and initializes transformed JSON.
class StreamJsonReaderPlugin extends PipelinePluginNode{
  // Function: construct stream reader plugin.
  constructor(readerProvider){
    // Conditional: reader provider must expose read().
    if(!readerProvider||typeof readerProvider.read!=="function"){
      throw new TypeError("StreamJsonReaderPlugin requires readerProvider.read().");
    }

    super({typeId:"plugin.stream.reader",label:"Stream JSON Reader Plugin",type:NodeType.STREAM_PLUGIN,requires:[],supports:[DataSupport.JSON],description:"Reads JSON and initializes stream original/transformed PipelineJsonDocument copies."});
    // Assignment: reader provider dependency.
    this.readerProvider=readerProvider;
  }

  // Function: read and initialize stream state.
  async execute(pipelineContext,pipelineRuntime){
    // Assignment: normalized reader data.
    const normalizedReaderEnvelope=normalizeReaderEnvelopeIntoJsonData(await this.readerProvider.read());
    // Function call: initialize stream state.
    initializeStreamStateFromJsonData(pipelineContext,normalizedReaderEnvelope.data,{sourceMode:normalizedReaderEnvelope.mode,readerTypeId:this.typeId});
    // Assignment: input metadata.
    pipelineContext.input={mode:normalizedReaderEnvelope.mode,receivedAt:new Date().toISOString(),pipelineFamily:"stream",originalPreserved:true,transformedInitialized:true};
    // Function call: append diagnostic.
    PipelineContextUtility.appendDiagnosticRecord(pipelineContext,{node:this.id,typeId:this.typeId,event:"stream-read",mode:normalizedReaderEnvelope.mode,originalPreserved:true,transformedInitialized:true});
    // Function call: append debug.
    pipelineRuntime.debug.log("debug","Stream reader complete.",{instanceId:this.instanceId,typeId:this.typeId,mode:normalizedReaderEnvelope.mode});
    return pipelineContext;
  }
}

// Function/class: stream regex filter replaces transformed text with found matches.
class StreamRegexFilterPlugin extends PipelinePluginNode{
  // Function: construct stream regex filter.
  constructor(regexSettingsProvider){
    // Conditional: regex provider must expose read().
    if(!regexSettingsProvider||typeof regexSettingsProvider.read!=="function"){
      throw new TypeError("StreamRegexFilterPlugin requires regexSettingsProvider.read().");
    }

    super({typeId:"plugin.stream.regex.filter",label:"Stream Regex Filter Plugin",type:NodeType.STREAM_PLUGIN,requires:["plugin.stream.reader"],supports:[DataSupport.JSON],description:"Runs regex against transformed JSON and replaces transformed text with matching instances only."});
    // Assignment: regex settings provider.
    this.regexSettingsProvider=regexSettingsProvider;
    // Assignment: maximum regex matches.
    this.maximumMatchCount=1000;
    // Assignment: maximum text length scanned by this regex plugin.
    this.maximumInputLength=100000;
  }

  // Function: transform current text into regex matches joined by spaces.
  async execute(pipelineContext,pipelineRuntime){
    // Assignment: current transformed plain JSON.
    const currentTransformedPlainJson=getTransformedStreamJsonPlainObject(pipelineContext);
    // Assignment: source text from transformed JSON.
    const sourceText=getTextFromJsonCompatibleValue(currentTransformedPlainJson);
    // Assignment: regex result.
    const regexSearchResult=runBoundedRegexSearch(sourceText,this.regexSettingsProvider,this.maximumMatchCount,this.maximumInputLength);
    // Assignment: next transformed object preserves existing keys when current value is object-like.
    const nextTransformedPlainJson=currentTransformedPlainJson&&typeof currentTransformedPlainJson==="object"&&!Array.isArray(currentTransformedPlainJson)
      ? PipelineSecurityUtility.cloneStrictJsonValue(currentTransformedPlainJson)
      : {text:""};
    // Assignment: filtered text is only the matched values, joined by spaces.
    const filteredTextValue=regexSearchResult.matches.map(matchRecord=>matchRecord.match).join(" ");
    // Assignment: replace text in transformed JSON.
    nextTransformedPlainJson.text=filteredTextValue;
    // Assignment: include filtered text in regex result.
    regexSearchResult.filteredText=filteredTextValue;

    // Assignment: stream state.
    const streamProcessingState=ensureStreamProcessingState(pipelineContext);
    // Assignment: save regex result under instance id.
    streamProcessingState.regexState[this.instanceId]=PipelineSecurityUtility.cloneStrictJsonValue(regexSearchResult);
    // Function call: store regex result through the generic state handler.
    PipelineContextUtility.setPluginState(pipelineContext,this,"regexFilter",regexSearchResult,{visibility:"public",metadata:{source:"streamState.transformedJsonDocument"}});
    // Assignment: mirror regex state onto context as a temporary compatibility alias.
    pipelineContext.regexState=PipelineSecurityUtility.cloneStrictJsonValue(streamProcessingState.regexState);
    // Function call: replace transformed document.
    replaceTransformedStreamJsonDocument(pipelineContext,nextTransformedPlainJson,{node:this.id,typeId:this.typeId,event:"stream-regex-filter"});
    // Function call: append diagnostic.
    PipelineContextUtility.appendDiagnosticRecord(pipelineContext,{node:this.id,typeId:this.typeId,event:"stream-regex-filter",matchCount:regexSearchResult.matchCount,transformedTextLength:filteredTextValue.length});
    // Function call: append debug.
    pipelineRuntime.debug.log("debug","Stream regex filter complete.",{instanceId:this.instanceId,matchCount:regexSearchResult.matchCount,transformedText:filteredTextValue});
    return pipelineContext;
  }
}

// Function/class: stream tokenizer tokenizes transformed JSON text.
class StreamJsonTokenizerPlugin extends PipelinePluginNode{
  // Function: construct stream tokenizer.
  constructor(){
    super({typeId:"plugin.stream.tokenizer",label:"Stream JSON Tokenizer Plugin",type:NodeType.STREAM_PLUGIN,requires:["plugin.stream.reader"],supports:[DataSupport.JSON],description:"Tokenizes the current transformed stream JSON text."});
  }

  // Function: tokenize transformed text.
  async execute(pipelineContext,pipelineRuntime){
    // Assignment: transformed plain JSON.
    const currentTransformedPlainJson=getTransformedStreamJsonPlainObject(pipelineContext);
    // Assignment: transformed source text.
    const sourceText=getTextFromJsonCompatibleValue(currentTransformedPlainJson);
    // Assignment: tokenizer state.
    const tokenizerState=PipelineTextAnalysisUtility.tokenizeSourceTextIntoTokenRecords(sourceText);
    // Assignment: annotate source.
    tokenizerState.source="streamState.transformedJsonDocument";
    // Assignment: stream state.
    const streamProcessingState=ensureStreamProcessingState(pipelineContext);
    // Assignment: store stream tokenizer state.
    streamProcessingState.tokenizerState=PipelineSecurityUtility.cloneStrictJsonValue(tokenizerState);
    // Function call: store tokenizer state through the generic state handler.
    PipelineContextUtility.setPluginState(pipelineContext,this,"tokenizer",tokenizerState,{visibility:"public",metadata:{source:"streamState.transformedJsonDocument"}});
    // Assignment: mirror tokenizer state onto context as a temporary compatibility alias.
    pipelineContext.tokenizerState=PipelineSecurityUtility.cloneStrictJsonValue(tokenizerState);
    // Function call: append diagnostic.
    PipelineContextUtility.appendDiagnosticRecord(pipelineContext,{node:this.id,typeId:this.typeId,event:"stream-tokenized",tokenCount:tokenizerState.tokenCount});
    // Function call: append debug.
    pipelineRuntime.debug.log("debug","Stream tokenizer complete.",{instanceId:this.instanceId,tokenCount:tokenizerState.tokenCount});
    return pipelineContext;
  }
}

// Function/class: stream no-op plugin preserves transformed JSON and records a revision.
class StreamPassThroughDiagnosticPlugin extends PipelinePluginNode{
  // Function: construct stream pass-through plugin.
  constructor(options={}){
    // Function call: validate options.
    PipelineSecurityUtility.assertPlainObject(options,"StreamPassThroughDiagnosticPlugin options");
    // Assignment: dependency list.
    const requiredPluginTypeIds=options.requires||["plugin.stream.reader"];
    // Assignment: supported formats.
    const supportedDataFormats=options.supports||[DataSupport.JSON];
    // Function call: validate dependency list.
    PipelineSecurityUtility.assertStringArray(requiredPluginTypeIds,"Stream dummy requires");
    // Function call: validate support list.
    PipelineSecurityUtility.assertStringArray(supportedDataFormats,"Stream dummy supports");

    super({typeId:options.typeId||"plugin.stream.dummy.passThrough",label:options.label||"Stream Pass-Through Diagnostic Plugin",type:NodeType.STREAM_PLUGIN,requires:requiredPluginTypeIds,supports:supportedDataFormats,description:options.description||"Stream no-op plugin that records a revision and preserves transformed JSON."});
    // Assignment: diagnostic event name.
    this.diagnosticEvent=options.diagnosticEvent||"stream-pass-through";
  }

  // Function: record no-op revision.
  async execute(pipelineContext,pipelineRuntime){
    // Assignment: current transformed plain JSON.
    const currentTransformedPlainJson=getTransformedStreamJsonPlainObject(pipelineContext);
    // Function call: write same transformed value back to record a pass-through revision.
    replaceTransformedStreamJsonDocument(pipelineContext,currentTransformedPlainJson,{node:this.id,typeId:this.typeId,event:this.diagnosticEvent});
    // Function call: append diagnostic.
    PipelineContextUtility.appendDiagnosticRecord(pipelineContext,{node:this.id,typeId:this.typeId,event:this.diagnosticEvent,transform:"none",dataMutation:false,transformedPreserved:true});
    // Function call: append debug.
    pipelineRuntime.debug.log("debug","Stream dummy pass-through complete.",{instanceId:this.instanceId,typeId:this.typeId});
    return pipelineContext;
  }
}

// Function/class: stream word counter counts words from transformed JSON and writes result to transformed JSON.
class StreamWordCountPlugin extends PipelinePluginNode{
  // Function: construct stream word count plugin.
  constructor(){
    super({typeId:"plugin.stream.wordCount",label:"Stream Word Count Plugin",type:NodeType.STREAM_PLUGIN,requires:["plugin.stream.reader"],supports:[DataSupport.JSON],description:"Counts word tokens from current transformed JSON and writes _streamWordCount."});
  }

  // Function: count transformed words.
  async execute(pipelineContext,pipelineRuntime){
    // Assignment: current transformed plain JSON.
    const currentTransformedPlainJson=getTransformedStreamJsonPlainObject(pipelineContext);
    // Assignment: source text.
    const sourceText=getTextFromJsonCompatibleValue(currentTransformedPlainJson);
    // Assignment: tokenizer state based on current transformed text.
    const tokenizerState=PipelineTextAnalysisUtility.tokenizeSourceTextIntoTokenRecords(sourceText);
    // Assignment: word token count.
    const wordTokenCount=PipelineTextAnalysisUtility.countWordTokens(tokenizerState);
    // Assignment: next transformed object preserves object keys when possible.
    const nextTransformedPlainJson=currentTransformedPlainJson&&typeof currentTransformedPlainJson==="object"&&!Array.isArray(currentTransformedPlainJson)
      ? PipelineSecurityUtility.cloneStrictJsonValue(currentTransformedPlainJson)
      : {text:sourceText};
    // Assignment: write stream word count.
    nextTransformedPlainJson._streamWordCount=wordTokenCount;

    // Assignment: stream state.
    const streamProcessingState=ensureStreamProcessingState(pipelineContext);
    // Assignment: store word count state.
    streamProcessingState.wordCountState={source:"streamState.transformedJsonDocument",wordCount:wordTokenCount,countedAt:new Date().toISOString()};
    // Function call: store word count through the generic state handler.
    PipelineContextUtility.setPluginState(pipelineContext,this,"wordCount",streamProcessingState.wordCountState,{visibility:"public"});
    // Assignment: store tokenizer state.
    streamProcessingState.tokenizerState=PipelineSecurityUtility.cloneStrictJsonValue(tokenizerState);
    // Assignment: mirror tokenizer state as a temporary compatibility alias.
    pipelineContext.tokenizerState=PipelineSecurityUtility.cloneStrictJsonValue(tokenizerState);
    // Function call: replace transformed JSON.
    replaceTransformedStreamJsonDocument(pipelineContext,nextTransformedPlainJson,{node:this.id,typeId:this.typeId,event:"stream-word-count"});
    // Function call: append diagnostic.
    PipelineContextUtility.appendDiagnosticRecord(pipelineContext,{node:this.id,typeId:this.typeId,event:"stream-word-count",wordCount:wordTokenCount});
    // Function call: append debug.
    pipelineRuntime.debug.log("debug","Stream word count complete.",{instanceId:this.instanceId,wordCount:wordTokenCount});
    return pipelineContext;
  }
}

// Function/class: stream output writes original, transformed, states, diagnostics, and errors.
class StreamJsonOutputPlugin extends PipelinePluginNode{
  // Function: construct stream output plugin.
  constructor(outputSink){
    // Conditional: output sink must expose write().
    if(!outputSink||typeof outputSink.write!=="function"){
      throw new TypeError("StreamJsonOutputPlugin requires outputSink.write().");
    }

    super({typeId:"plugin.stream.output",label:"Stream JSON Output Plugin",type:NodeType.STREAM_PLUGIN,requires:["plugin.stream.reader"],supports:[DataSupport.JSON],description:"Writes original JSON, transformed JSON, stream state, diagnostics, and errors."});
    // Assignment: output sink dependency.
    this.outputSink=outputSink;
  }

  // Function: write stream output snapshot.
  async execute(pipelineContext,pipelineRuntime){
    // Assignment: stream state.
    const streamProcessingState=ensureStreamProcessingState(pipelineContext);
    // Assignment: output-safe stream state with documents unwrapped.
    const transformedStreamJsonDocument=getTransformedStreamJsonDocument(pipelineContext);
    // Assignment: output-safe stream state with documents unwrapped and history exposed.
    const outputStreamState={originalJson:getOriginalStreamJsonPlainObject(pipelineContext),transformedJson:transformedStreamJsonDocument.toPlainObject(),originalJsonHistory:streamProcessingState.originalJsonDocument&&typeof streamProcessingState.originalJsonDocument.toHistoryPlainArray==="function"?streamProcessingState.originalJsonDocument.toHistoryPlainArray():[getOriginalStreamJsonPlainObject(pipelineContext)],transformedJsonHistory:typeof transformedStreamJsonDocument.toHistoryPlainArray==="function"?transformedStreamJsonDocument.toHistoryPlainArray():[transformedStreamJsonDocument.toPlainObject()],transformedJsonHistoryMetadata:typeof transformedStreamJsonDocument.toHistoryMetadataPlainArray==="function"?transformedStreamJsonDocument.toHistoryMetadataPlainArray():[],revisions:PipelineSecurityUtility.cloneStrictJsonValue(streamProcessingState.revisions),tokenizerState:PipelineSecurityUtility.cloneStrictJsonValue(streamProcessingState.tokenizerState),regexState:PipelineSecurityUtility.cloneStrictJsonValue(streamProcessingState.regexState),wordCountState:PipelineSecurityUtility.cloneStrictJsonValue(streamProcessingState.wordCountState)};
    // Assignment: full stream output snapshot.
    const outputSnapshot={runId:pipelineContext.runId,cycle:pipelineContext.cycle,mode:"stream",originalJson:outputStreamState.originalJson,transformedJson:outputStreamState.transformedJson,data:getTransformedStreamJsonPlainObject(pipelineContext),streamState:outputStreamState,state:PipelineContextUtility.createGenericStateSnapshot(pipelineContext),tokenizerState:PipelineSecurityUtility.cloneStrictJsonValue(pipelineContext.tokenizerState||{}),regexState:PipelineSecurityUtility.cloneStrictJsonValue(pipelineContext.regexState||{}),diagnostics:PipelineSecurityUtility.cloneStrictJsonValue(pipelineContext.diagnostics||[]),errors:PipelineSecurityUtility.cloneStrictJsonValue(pipelineContext.errors||[])};
    // Assignment: context output.
    pipelineContext.output={mode:"stream-object",jsonObject:PipelineSecurityUtility.cloneStrictJsonValue(outputSnapshot),emittedAt:new Date().toISOString()};
    // Function call: append stream line.
    PipelineContextUtility.appendStreamRecord(pipelineContext,{type:"stream-json-line",value:JSON.stringify(outputSnapshot)});
    // Function call: write to sink.
    this.outputSink.write(pipelineContext.output.jsonObject);
    // Function call: append diagnostic.
    PipelineContextUtility.appendDiagnosticRecord(pipelineContext,{node:this.id,typeId:this.typeId,event:"stream-output-written"});
    // Function call: append debug.
    pipelineRuntime.debug.log("debug","Stream output complete.",{instanceId:this.instanceId});
    return pipelineContext;
  }
}

// Function: create stream reader factory.
function createStreamJsonReaderPluginFactory(readerProvider){
  return new PipelinePluginFactory({createPluginInstance:()=>new StreamJsonReaderPlugin(readerProvider)});
}

// Function: create stream regex filter factory.
function createStreamRegexFilterPluginFactory(regexSettingsProvider){
  return new PipelinePluginFactory({createPluginInstance:()=>new StreamRegexFilterPlugin(regexSettingsProvider)});
}

// Function: create stream tokenizer factory.
function createStreamJsonTokenizerPluginFactory(){
  return new PipelinePluginFactory({createPluginInstance:()=>new StreamJsonTokenizerPlugin()});
}

// Function: create stream pass-through factory.
function createStreamPassThroughDiagnosticPluginFactory(options={}){
  return new PipelinePluginFactory({createPluginInstance:()=>new StreamPassThroughDiagnosticPlugin(options)});
}

// Function: create stream word count factory.
function createStreamWordCountPluginFactory(){
  return new PipelinePluginFactory({createPluginInstance:()=>new StreamWordCountPlugin()});
}

// Function: create stream output factory.
function createStreamJsonOutputPluginFactory(outputSink){
  return new PipelinePluginFactory({createPluginInstance:()=>new StreamJsonOutputPlugin(outputSink)});
}

// Assignment: publish stream plugins and helpers.
globalObject.PipelineStreamPlugins={
  StreamReaderPlugin:StreamJsonReaderPlugin,
  StreamRegexFilterPlugin,
  StreamTokenizerPlugin:StreamJsonTokenizerPlugin,
  StreamPassThroughDummyPlugin:StreamPassThroughDiagnosticPlugin,
  StreamWordCountPlugin,
  StreamOutputPlugin:StreamJsonOutputPlugin,
  StreamJsonReaderPlugin,
  StreamJsonTokenizerPlugin,
  StreamPassThroughDiagnosticPlugin,
  StreamJsonOutputPlugin,
  createStreamJsonReaderPluginFactory,
  createStreamRegexFilterPluginFactory,
  createStreamJsonTokenizerPluginFactory,
  createStreamPassThroughDiagnosticPluginFactory,
  createStreamWordCountPluginFactory,
  createStreamJsonOutputPluginFactory,
  helpers:{ensureStreamProcessingState,initializeStreamStateFromJsonData,getTransformedStreamJsonDocument,getOriginalStreamJsonPlainObject,getTransformedStreamJsonPlainObject,replaceTransformedStreamJsonDocument,getTextFromJsonCompatibleValue}
};
})(globalThis);
