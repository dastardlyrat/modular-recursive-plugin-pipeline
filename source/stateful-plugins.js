(function attachStatefulPipelinePluginsToGlobalObject(globalObject){
"use strict";

// Assignment: read core symbols once for shorter class definitions.
const {PipelinePluginNode,PipelinePluginFactory,PipelineSecurityUtility,PipelineContextUtility,PipelineTextAnalysisUtility,NodeType,DataSupport}=globalObject.PipelineCore;

// Function: normalize a reader envelope into JSON-compatible data.
function normalizeReaderEnvelopeIntoJsonData(readerEnvelope){
  // Function call: validate reader envelope object.
  PipelineSecurityUtility.assertPlainObject(readerEnvelope,"Reader input envelope");
  // Assignment: selected input mode.
  const selectedInputMode=readerEnvelope.mode;
  // Assignment: reader payload.
  const readerPayloadValue=readerEnvelope.value;

  // Conditional: object mode expects a plain JSON object.
  if(selectedInputMode==="jsonObject"){
    // Function call: enforce plain object input for object mode.
    PipelineSecurityUtility.assertPlainObject(readerPayloadValue,"JSON object input");
    return {mode:selectedInputMode,data:PipelineSecurityUtility.cloneStrictJsonValue(readerPayloadValue)};
  }

  // Conditional: file mode parses JSON text from the file.
  if(selectedInputMode==="jsonFile"){
    return {mode:selectedInputMode,data:PipelineSecurityUtility.parseJsonTextSafely(readerPayloadValue,"JSON file content")};
  }

  // Conditional: text mode either parses JSON text or wraps raw text in {text}.
  if(selectedInputMode==="jsonText"){
    return {mode:selectedInputMode,data:PipelineSecurityUtility.normalizeReaderTextIntoJsonValue(readerPayloadValue)};
  }

  throw new Error("Unsupported reader mode: "+selectedInputMode);
}

// Function: run a bounded regex search against source text.
function runBoundedRegexSearch(sourceText,regexSettingsProvider,maximumMatchCount,maximumInputLength=100000){
  // Conditional: source text must stay bounded so user-supplied regex cannot scan unbounded input.
  if(typeof sourceText!=="string"){
    throw new TypeError("Regex source text must be a string.");
  }

  if(sourceText.length>maximumInputLength){
    throw new RangeError("Regex source text too long: "+sourceText.length+" > "+maximumInputLength);
  }

  // Assignment: current regex settings.
  const regularExpressionSettings=regexSettingsProvider.read();
  // Assignment: validated caller regex.
  const configuredRegularExpression=PipelineSecurityUtility.createBoundedRegularExpression(regularExpressionSettings.regexSource,regularExpressionSettings.flags);
  // Assignment: ensure scanning regex has the global flag.
  const scanningRegularExpressionFlags=configuredRegularExpression.global?configuredRegularExpression.flags:configuredRegularExpression.flags+"g";
  // Assignment: new regex instance performs bounded scan.
  const scanningRegularExpression=PipelineSecurityUtility.createBoundedRegularExpression(configuredRegularExpression.source,scanningRegularExpressionFlags);
  // Assignment: match records are collected here.
  const regexMatchRecords=[];
  // Assignment: current regex match.
  let currentRegexMatch;

  // Loop: scan source text until no matches or cap reached.
  while((currentRegexMatch=scanningRegularExpression.exec(sourceText))!==null){
    // Assignment: record match text, index, and named groups.
    regexMatchRecords.push({match:currentRegexMatch[0],index:currentRegexMatch.index,groups:currentRegexMatch.groups?PipelineSecurityUtility.cloneStrictJsonValue(currentRegexMatch.groups):null});

    // Conditional: zero-length matches must manually advance to avoid infinite loops.
    if(currentRegexMatch[0]===""){
      // Assignment: advance scanner position.
      scanningRegularExpression.lastIndex+=1;
    }

    // Conditional: stop at configured maximum.
    if(regexMatchRecords.length>=maximumMatchCount){
      break;
    }
  }

  return {regex:{source:configuredRegularExpression.source,flags:scanningRegularExpression.flags},sourceLength:sourceText.length,matchCount:regexMatchRecords.length,matches:regexMatchRecords,executedAt:new Date().toISOString()};
}

// Function/class: stateful reader wraps input data in PipelineJsonDocument.
class StatefulJsonReaderPlugin extends PipelinePluginNode{
  // Function: construct stateful reader.
  constructor(readerProvider){
    // Conditional: provider must expose read().
    if(!readerProvider||typeof readerProvider.read!=="function"){
      throw new TypeError("StatefulJsonReaderPlugin requires readerProvider.read().");
    }

    super({typeId:"plugin.stateful.reader",label:"Stateful JSON Reader Plugin",type:NodeType.STANDARD_PLUGIN,requires:[],supports:[DataSupport.JSON],description:"Reads JSON from file, object input, or text converted to JSON, then wraps it in PipelineJsonDocument."});
    // Assignment: reader provider dependency.
    this.readerProvider=readerProvider;
  }

  // Function: read input and replace context data document.
  async execute(pipelineContext,pipelineRuntime){
    // Assignment: normalized reader data.
    const normalizedReaderEnvelope=normalizeReaderEnvelopeIntoJsonData(await this.readerProvider.read());
    // Function call: replace context document.
    PipelineContextUtility.replaceContextDataDocument(pipelineContext,normalizedReaderEnvelope.data,{sourceMode:normalizedReaderEnvelope.mode,readerTypeId:this.typeId});
    // Assignment: input metadata.
    pipelineContext.input={mode:normalizedReaderEnvelope.mode,receivedAt:new Date().toISOString(),pipelineFamily:"stateful"};
    // Function call: append diagnostic.
    PipelineContextUtility.appendDiagnosticRecord(pipelineContext,{node:this.id,typeId:this.typeId,event:"stateful-read",mode:normalizedReaderEnvelope.mode,documentClass:"PipelineJsonDocument"});
    // Function call: append debug.
    pipelineRuntime.debug.log("debug","Stateful reader complete.",{instanceId:this.instanceId,typeId:this.typeId,mode:normalizedReaderEnvelope.mode});
    return pipelineContext;
  }
}

// Function/class: tokenizes the current JSON document text.
class StatefulJsonTokenizerPlugin extends PipelinePluginNode{
  // Function: construct tokenizer plugin.
  constructor(){
    super({typeId:"plugin.stateful.tokenizer",label:"Stateful JSON Tokenizer Plugin",type:NodeType.STANDARD_PLUGIN,requires:["plugin.stateful.reader"],supports:[DataSupport.JSON],description:"Tokenizes current PipelineJsonDocument text and stores parser state."});
  }

  // Function: tokenize current source text.
  async execute(pipelineContext,pipelineRuntime){
    // Assignment: current source text from document.
    const sourceText=PipelineContextUtility.getCurrentSourceText(pipelineContext,"text");
    // Assignment: tokenizer result.
    const tokenizerState=PipelineTextAnalysisUtility.tokenizeSourceTextIntoTokenRecords(sourceText);
    // Assignment: annotate source for debug.
    tokenizerState.source="PipelineJsonDocument.currentJsonRoot";
    // Function call: store tokenizer state through the generic state handler.
    PipelineContextUtility.setPluginState(pipelineContext,this,"tokenizer",tokenizerState,{visibility:"public",metadata:{source:"PipelineJsonDocument.currentJsonRoot"}});
    // Assignment: temporary compatibility alias for older output/tests.
    pipelineContext.tokenizerState=PipelineSecurityUtility.deepCloneJsonSafeValue(tokenizerState);
    // Function call: append diagnostic.
    PipelineContextUtility.appendDiagnosticRecord(pipelineContext,{node:this.id,typeId:this.typeId,event:"stateful-tokenized",tokenCount:tokenizerState.tokenCount});
    // Function call: append debug.
    pipelineRuntime.debug.log("debug","Stateful tokenizer complete.",{instanceId:this.instanceId,tokenCount:tokenizerState.tokenCount});
    return pipelineContext;
  }
}

// Function/class: stateful regex search does not alter current JSON data.
class StatefulRegexSearchPlugin extends PipelinePluginNode{
  // Function: construct regex search plugin.
  constructor(regexSettingsProvider){
    // Conditional: provider must expose read().
    if(!regexSettingsProvider||typeof regexSettingsProvider.read!=="function"){
      throw new TypeError("StatefulRegexSearchPlugin requires regexSettingsProvider.read().");
    }

    super({typeId:"plugin.stateful.regex.search",label:"Stateful Regex Search Plugin",type:NodeType.EXTENSION_PLUGIN,requires:["plugin.stateful.reader"],supports:[DataSupport.JSON],description:"Runs a bounded regex search against current PipelineJsonDocument data and stores matches in generic plugin state without mutating data."});
    // Assignment: regex settings provider dependency.
    this.regexSettingsProvider=regexSettingsProvider;
    // Assignment: max regex matches per pass.
    this.maximumMatchCount=1000;
    // Assignment: maximum text length scanned by this regex plugin.
    this.maximumInputLength=100000;
  }

  // Function: execute regex search without mutating current document.
  async execute(pipelineContext,pipelineRuntime){
    // Assignment: current source text.
    const sourceText=PipelineContextUtility.getCurrentSourceText(pipelineContext,"text");
    // Assignment: regex search result.
    const regexSearchResult=runBoundedRegexSearch(sourceText,this.regexSettingsProvider,this.maximumMatchCount,this.maximumInputLength);

    // Conditional/assignment: ensure regex state object.
    if(!pipelineContext.regexState||typeof pipelineContext.regexState!=="object"){
      pipelineContext.regexState={};
    }

    // Function call: store result through the generic state handler.
    PipelineContextUtility.setPluginState(pipelineContext,this,"regexSearch",regexSearchResult,{visibility:"public",metadata:{source:"PipelineJsonDocument.currentJsonRoot"}});
    // Assignment: store result under instance id as a temporary compatibility alias.
    pipelineContext.regexState[this.instanceId]=PipelineSecurityUtility.deepCloneJsonSafeValue(regexSearchResult);
    // Function call: append diagnostic.
    PipelineContextUtility.appendDiagnosticRecord(pipelineContext,{node:this.id,typeId:this.typeId,event:"stateful-regex-search",matchCount:regexSearchResult.matchCount});
    // Function call: append debug.
    pipelineRuntime.debug.log("debug","Stateful regex search complete.",{instanceId:this.instanceId,matchCount:regexSearchResult.matchCount});
    return pipelineContext;
  }

  // Function: run search outside the loaded pipeline for the UI button.
  executeOutOfBand(pipelineContext){
    // Assignment: current source text.
    const sourceText=PipelineContextUtility.getCurrentSourceText(pipelineContext,"text");
    return {plugin:this.typeId,mode:"out-of-band",...runBoundedRegexSearch(sourceText,this.regexSettingsProvider,this.maximumMatchCount,this.maximumInputLength)};
  }
}

// Function/class: stateful no-op plugin used for load/unload/dependency tests.
class StatefulPassThroughDiagnosticPlugin extends PipelinePluginNode{
  // Function: construct no-op plugin.
  constructor(options={}){
    // Function call: validate options.
    PipelineSecurityUtility.assertPlainObject(options,"StatefulPassThroughDiagnosticPlugin options");
    // Assignment: dependency list.
    const requiredPluginTypeIds=options.requires||[];
    // Assignment: support list.
    const supportedDataFormats=options.supports||[DataSupport.JSON];
    // Function call: validate dependency list.
    PipelineSecurityUtility.assertStringArray(requiredPluginTypeIds,"Stateful dummy requires");
    // Function call: validate support list.
    PipelineSecurityUtility.assertStringArray(supportedDataFormats,"Stateful dummy supports");

    super({typeId:options.typeId||"plugin.stateful.dummy.passThrough",label:options.label||"Stateful Pass-Through Diagnostic Plugin",type:NodeType.EXTENSION_PLUGIN,requires:requiredPluginTypeIds,supports:supportedDataFormats,description:options.description||"Stateful no-op plugin that records a diagnostic and leaves data unchanged."});
    // Assignment: diagnostic event name.
    this.diagnosticEvent=options.diagnosticEvent||"stateful-pass-through";
  }

  // Function: record no-op diagnostic.
  async execute(pipelineContext,pipelineRuntime){
    // Function call: ensure diagnostics array.
    PipelineContextUtility.ensurePipelineContextShape(pipelineContext);
    // Function call: append diagnostic.
    PipelineContextUtility.appendDiagnosticRecord(pipelineContext,{node:this.id,typeId:this.typeId,event:this.diagnosticEvent,transform:"none",dataMutation:false});
    // Function call: append debug.
    pipelineRuntime.debug.log("debug","Stateful dummy pass-through complete.",{instanceId:this.instanceId,typeId:this.typeId});
    return pipelineContext;
  }
}

// Function/class: stateful word count writes the count into current document.
class StatefulWordCountPlugin extends PipelinePluginNode{
  // Function: construct word count plugin.
  constructor(){
    super({typeId:"plugin.stateful.wordCount",label:"Stateful Word Count Plugin",type:NodeType.EXTENSION_PLUGIN,requires:["plugin.stateful.tokenizer"],supports:[DataSupport.JSON],description:"Counts word tokens from stateful tokenizer output and writes _statefulWordCount."});
  }

  // Function: count word tokens.
  async execute(pipelineContext,pipelineRuntime){
    // Assignment: word count reads tokenizer state from the generic state handler.
    const tokenizerStateRecord=PipelineContextUtility.getLatestPluginStateByTypeId(pipelineContext,"plugin.stateful.tokenizer","tokenizer",null);
    // Assignment: compatibility fallback remains during migration.
    const tokenizerState=tokenizerStateRecord&&tokenizerStateRecord.value?tokenizerStateRecord.value:pipelineContext.tokenizerState;
    // Assignment: word count from tokenizer state.
    const wordTokenCount=PipelineTextAnalysisUtility.countWordTokens(tokenizerState);
    // Function call: store word count through the generic state handler.
    PipelineContextUtility.setPluginState(pipelineContext,this,"wordCount",{source:"plugin.stateful.tokenizer",wordCount:wordTokenCount,countedAt:new Date().toISOString()},{visibility:"public"});
    // Function call: write count into current data document.
    PipelineContextUtility.setCurrentDataPathValue(pipelineContext,"_statefulWordCount",wordTokenCount);
    // Function call: append diagnostic.
    PipelineContextUtility.appendDiagnosticRecord(pipelineContext,{node:this.id,typeId:this.typeId,event:"stateful-word-count",wordCount:wordTokenCount});
    // Function call: append debug.
    pipelineRuntime.debug.log("debug","Stateful word count complete.",{instanceId:this.instanceId,wordCount:wordTokenCount});
    return pipelineContext;
  }
}

// Function/class: stateful output writes current snapshot to sink.
class StatefulJsonOutputPlugin extends PipelinePluginNode{
  // Function: construct output plugin.
  constructor(outputSink){
    // Conditional: output sink must expose write().
    if(!outputSink||typeof outputSink.write!=="function"){
      throw new TypeError("StatefulJsonOutputPlugin requires outputSink.write().");
    }

    super({typeId:"plugin.stateful.output",label:"Stateful JSON Output Plugin",type:NodeType.STANDARD_PLUGIN,requires:["plugin.stateful.reader"],supports:[DataSupport.JSON],description:"Writes JSON output for the stateful pipeline."});
    // Assignment: output sink dependency.
    this.outputSink=outputSink;
  }

  // Function: write output snapshot.
  async execute(pipelineContext,pipelineRuntime){
    // Assignment: output snapshot.
    const outputSnapshot=PipelineContextUtility.createStatefulOutputSnapshot(pipelineContext);
    // Assignment: context output object.
    pipelineContext.output={mode:"stateful-object",jsonObject:PipelineSecurityUtility.deepCloneJsonSafeValue(outputSnapshot),emittedAt:new Date().toISOString()};
    // Function call: append stream line.
    PipelineContextUtility.appendStreamRecord(pipelineContext,{type:"stateful-json-line",value:JSON.stringify(outputSnapshot)});
    // Function call: write to sink.
    this.outputSink.write(pipelineContext.output.jsonObject);
    // Function call: append diagnostic.
    PipelineContextUtility.appendDiagnosticRecord(pipelineContext,{node:this.id,typeId:this.typeId,event:"stateful-output-written"});
    // Function call: append debug.
    pipelineRuntime.debug.log("debug","Stateful output complete.",{instanceId:this.instanceId});
    return pipelineContext;
  }
}

// Function: create reader plugin factory.
function createStatefulJsonReaderPluginFactory(readerProvider){
  return new PipelinePluginFactory({createPluginInstance:()=>new StatefulJsonReaderPlugin(readerProvider)});
}

// Function: create tokenizer plugin factory.
function createStatefulJsonTokenizerPluginFactory(){
  return new PipelinePluginFactory({createPluginInstance:()=>new StatefulJsonTokenizerPlugin()});
}

// Function: create regex plugin factory.
function createStatefulRegexSearchPluginFactory(regexSettingsProvider){
  return new PipelinePluginFactory({createPluginInstance:()=>new StatefulRegexSearchPlugin(regexSettingsProvider)});
}

// Function: create dummy plugin factory.
function createStatefulPassThroughDiagnosticPluginFactory(options={}){
  return new PipelinePluginFactory({createPluginInstance:()=>new StatefulPassThroughDiagnosticPlugin(options)});
}

// Function: create word count plugin factory.
function createStatefulWordCountPluginFactory(){
  return new PipelinePluginFactory({createPluginInstance:()=>new StatefulWordCountPlugin()});
}

// Function: create output plugin factory.
function createStatefulJsonOutputPluginFactory(outputSink){
  return new PipelinePluginFactory({createPluginInstance:()=>new StatefulJsonOutputPlugin(outputSink)});
}

// Assignment: publish stateful plugins with compatibility aliases.
globalObject.PipelinePlugins={
  StandardReaderPlugin:StatefulJsonReaderPlugin,
  StatefulTokenizerPlugin:StatefulJsonTokenizerPlugin,
  WordCountPlugin:StatefulWordCountPlugin,
  StandardOutputPlugin:StatefulJsonOutputPlugin,
  StatefulJsonReaderPlugin,
  StatefulJsonTokenizerPlugin,
  StatefulRegexSearchPlugin,
  StatefulPassThroughDiagnosticPlugin,
  StatefulWordCountPlugin,
  StatefulJsonOutputPlugin,
  createStandardReaderPluginFactory:createStatefulJsonReaderPluginFactory,
  createStatefulTokenizerPluginFactory:createStatefulJsonTokenizerPluginFactory,
  createWordCountPluginFactory:createStatefulWordCountPluginFactory,
  createStandardOutputPluginFactory:createStatefulJsonOutputPluginFactory,
  createStatefulJsonReaderPluginFactory,
  createStatefulJsonTokenizerPluginFactory,
  createStatefulRegexSearchPluginFactory,
  createStatefulPassThroughDiagnosticPluginFactory,
  createStatefulWordCountPluginFactory,
  createStatefulJsonOutputPluginFactory,
  helpers:{normalizeReaderEnvelopeIntoJsonData,runBoundedRegexSearch}
};

// Assignment: publish regex namespace compatibility.
globalObject.PipelineRegexPlugins={
  RegexPipelinePlugin:StatefulRegexSearchPlugin,
  StatefulRegexSearchPlugin,
  createRegexPipelinePluginFactory:createStatefulRegexSearchPluginFactory,
  createStatefulRegexSearchPluginFactory
};

// Assignment: publish dummy namespace compatibility.
globalObject.PipelineDummyPlugins={
  PassThroughDummyPlugin:StatefulPassThroughDiagnosticPlugin,
  StatefulPassThroughDiagnosticPlugin,
  createStatefulPassThroughDiagnosticPluginFactory
};
})(globalThis);
