(function attachMergedPipelineSelfTestsToGlobalObject(globalObject){
"use strict";

// Function: assert a condition and throw with a diagnostic label when it fails.
function assertCondition(conditionValue,failureMessage){
  // Conditional: failed assertion stops the test harness.
  if(!conditionValue){
    throw new Error(failureMessage);
  }
}

// Function/class: in-memory reader provider for tests.
class InMemoryJsonInputReaderProvider{
  // Function: store input mode and value.
  constructor(readerEnvelope){
    // Assignment: reader envelope for repeated test reads.
    this.readerEnvelope=readerEnvelope;
  }

  // Function: return cloned reader envelope.
  async read(){
    return globalObject.PipelineCore.PipelineSecurityUtility.deepCloneJsonSafeValue(this.readerEnvelope);
  }
}

// Function/class: in-memory output sink for tests.
class InMemoryJsonOutputSink{
  // Function: initialize sink.
  constructor(){
    // Assignment: last output starts empty.
    this.lastWrittenObject=null;
  }

  // Function: store cloned output.
  write(outputObject){
    // Assignment: clone output so tests do not share mutable runtime objects.
    this.lastWrittenObject=globalObject.PipelineCore.PipelineSecurityUtility.deepCloneJsonSafeValue(outputObject);
  }
}

// Function/class: fixed regex provider for tests.
class FixedRegexSettingsProvider{
  // Function: store regex source and flags.
  constructor(regexSource,regexFlags){
    // Assignment: regex source text.
    this.regexSource=regexSource;
    // Assignment: regex flags text.
    this.regexFlags=regexFlags;
  }

  // Function: return regex settings.
  read(){
    return {regexSource:this.regexSource,flags:this.regexFlags};
  }
}

// Function: install all stream test plugins and run one pass.
async function runStreamPipelineTransformationTest(){
  // Assignment: destructure core namespace.
  const {RecursivePipelineRuntime,PipelineDebugLogger}=globalObject.PipelineCore;
  // Assignment: input reader used by test.
  const readerProvider=new InMemoryJsonInputReaderProvider({mode:"jsonText",value:'{"text":"The quick brown fox jumps over 13 lazy dogs. The fox is quick."}'});
  // Assignment: regex provider filters fox matches.
  const regexProvider=new FixedRegexSettingsProvider("\\bfox\\b","gi");
  // Assignment: output sink captures output object.
  const outputSink=new InMemoryJsonOutputSink();
  // Assignment: runtime under test.
  const runtime=new RecursivePipelineRuntime({debug:new PipelineDebugLogger({enabled:true})});
  // Assignment: plugin id catalog.
  const pluginTypeIds={
    reader:runtime.pluginManager.installPluginFactory(globalObject.PipelineStreamPlugins.StreamJsonReaderPlugin,[readerProvider]),
    regexFilter:runtime.pluginManager.installPluginFactory(globalObject.PipelineStreamPlugins.StreamRegexFilterPlugin,[regexProvider]),
    tokenizer:runtime.pluginManager.installPluginFactory(globalObject.PipelineStreamPlugins.StreamJsonTokenizerPlugin),
    dummy:runtime.pluginManager.installPluginFactory(globalObject.PipelineStreamPlugins.StreamPassThroughDiagnosticPlugin,[{requires:["plugin.stream.reader"]}]),
    wordCount:runtime.pluginManager.installPluginFactory(globalObject.PipelineStreamPlugins.StreamWordCountPlugin),
    output:runtime.pluginManager.installPluginFactory(globalObject.PipelineStreamPlugins.StreamJsonOutputPlugin,[outputSink])
  };

  // Assignment: load default stream order.
  runtime.pluginManager.loadPluginInstance(pluginTypeIds.reader,0);
  // Assignment: load regex filter after reader.
  runtime.pluginManager.loadPluginInstance(pluginTypeIds.regexFilter,1);
  // Assignment: load tokenizer after transform.
  runtime.pluginManager.loadPluginInstance(pluginTypeIds.tokenizer,2);
  // Assignment: load pass-through diagnostic plugin.
  runtime.pluginManager.loadPluginInstance(pluginTypeIds.dummy,3);
  // Assignment: load word counter after tokenizer.
  runtime.pluginManager.loadPluginInstance(pluginTypeIds.wordCount,4);
  // Assignment: load output plugin last.
  runtime.pluginManager.loadPluginInstance(pluginTypeIds.output,5);

  // Function call: run one complete pass.
  await runtime.runOnce();

  // Assignment: captured output.
  const outputObject=outputSink.lastWrittenObject;
  // Function call: verify stream mode.
  assertCondition(outputObject.mode==="stream","Expected stream output mode.");
  // Function call: verify original text survived unchanged.
  assertCondition(outputObject.originalJson.text==="The quick brown fox jumps over 13 lazy dogs. The fox is quick.","Expected original JSON text to be preserved.");
  // Function call: verify regex filter transformed text to only matched words.
  assertCondition(outputObject.transformedJson.text==="fox fox","Expected transformed text to contain the two regex matches only.");
  // Function call: verify transformed word count is two.
  assertCondition(outputObject.transformedJson._streamWordCount===2,"Expected transformed stream word count to be 2.");
  // Function call: verify regex match count is two.
  assertCondition(Object.values(outputObject.regexState)[0].matchCount===2,"Expected regex match count to be 2.");

  return {name:"stream transformation preserves original and counts transformed words",passed:true,details:{transformedText:outputObject.transformedJson.text,wordCount:outputObject.transformedJson._streamWordCount,matchCount:Object.values(outputObject.regexState)[0].matchCount}};
}

// Function: verify unload returns metadata and updates loaded list.
async function runPluginUnloadBehaviorTest(){
  // Assignment: destructure core namespace.
  const {RecursivePipelineRuntime,PipelineDebugLogger}=globalObject.PipelineCore;
  // Assignment: output sink captures output object if needed.
  const outputSink=new InMemoryJsonOutputSink();
  // Assignment: reader provider.
  const readerProvider=new InMemoryJsonInputReaderProvider({mode:"jsonText",value:"hello"});
  // Assignment: runtime under test.
  const runtime=new RecursivePipelineRuntime({debug:new PipelineDebugLogger({enabled:true})});
  // Assignment: reader plugin id.
  const readerTypeId=runtime.pluginManager.installPluginFactory(globalObject.PipelinePlugins.StatefulJsonReaderPlugin,[readerProvider]);
  // Assignment: output plugin id.
  const outputTypeId=runtime.pluginManager.installPluginFactory(globalObject.PipelinePlugins.StatefulJsonOutputPlugin,[outputSink]);
  // Assignment: loaded reader instance id.
  const readerInstanceId=runtime.pluginManager.loadPluginInstance(readerTypeId,0);
  // Assignment: loaded output instance id.
  runtime.pluginManager.loadPluginInstance(outputTypeId,1);
  // Assignment: removed plugin summary.
  const removedPluginSummary=runtime.pluginManager.unloadPluginInstance(readerInstanceId);
  // Function call: verify removed index.
  assertCondition(removedPluginSummary.index===0,"Expected removed plugin index 0.");
  // Function call: verify remaining count.
  assertCondition(runtime.pluginManager.countLoadedPluginInstances()===1,"Expected one plugin to remain after unload.");

  return {name:"plugin unload removes selected instance and returns removed metadata",passed:true,details:removedPluginSummary};
}

// Function: verify conditional skip, junction branch entry, and inspection records.
async function runControlFlowJunctionAndSkipTest(){
  // Assignment: destructure core namespace.
  const {RecursivePipelineRuntime,PipelineDebugLogger}=globalObject.PipelineCore;
  // Assignment: input reader used by test.
  const readerProvider=new InMemoryJsonInputReaderProvider({mode:"jsonText",value:'{"text":"The quick brown fox jumps over 13 lazy dogs. The fox is quick."}'});
  // Assignment: regex provider would normally filter fox matches.
  const regexProvider=new FixedRegexSettingsProvider("\\bfox\\b","gi");
  // Assignment: output sink captures output object.
  const outputSink=new InMemoryJsonOutputSink();
  // Assignment: runtime under test.
  const runtime=new RecursivePipelineRuntime({debug:new PipelineDebugLogger({enabled:true})});
  // Function call: enable named pipelines and control signals.
  globalObject.PipelineControlFlowPlugins.ensureRuntimeControlFlowCapabilities(runtime);
  // Assignment: main pipeline manager.
  const mainPipelinePluginManager=runtime.getPluginManagerByPipelineName("main");
  // Assignment: secondary pipeline manager.
  const secondaryPipelinePluginManager=runtime.getPluginManagerByPipelineName("secondary");
  // Assignment: main plugin ids.
  const mainPluginTypeIds={
    reader:mainPipelinePluginManager.installPluginFactory(globalObject.PipelineStreamPlugins.StreamJsonReaderPlugin,[readerProvider]),
    conditionalFilter:mainPipelinePluginManager.installPluginFactory(globalObject.PipelineControlFlowPlugins.ConditionalNextModuleFilterPlugin,[{externalSignalName:"allow-next-module",defaultSignalValue:true}]),
    regexFilter:mainPipelinePluginManager.installPluginFactory(globalObject.PipelineStreamPlugins.StreamRegexFilterPlugin,[regexProvider]),
    tokenizer:mainPipelinePluginManager.installPluginFactory(globalObject.PipelineStreamPlugins.StreamJsonTokenizerPlugin),
    junction:mainPipelinePluginManager.installPluginFactory(globalObject.PipelineControlFlowPlugins.PipelineJunctionToNamedPipelinePlugin,[{targetPipelineName:"secondary"}]),
    wordCount:mainPipelinePluginManager.installPluginFactory(globalObject.PipelineStreamPlugins.StreamWordCountPlugin),
    output:mainPipelinePluginManager.installPluginFactory(globalObject.PipelineStreamPlugins.StreamJsonOutputPlugin,[outputSink])
  };
  // Assignment: secondary plugin ids.
  const secondaryPluginTypeIds={
    inspect:secondaryPipelinePluginManager.installPluginFactory(globalObject.PipelineControlFlowPlugins.PipelineInspectionPlugin,[{includeCurrentDataSnapshot:true}])
  };

  // Function call: deny the next module after the conditional filter.
  runtime.setExternalSignalValue("allow-next-module",false);
  // Function call: keep break signal passive.
  runtime.setExternalSignalValue("break-control-signal","NONE");
  // Assignment: load main reader.
  mainPipelinePluginManager.loadPluginInstance(mainPluginTypeIds.reader,0);
  // Assignment: load conditional filter.
  mainPipelinePluginManager.loadPluginInstance(mainPluginTypeIds.conditionalFilter,1);
  // Assignment: load regex filter that should be skipped.
  mainPipelinePluginManager.loadPluginInstance(mainPluginTypeIds.regexFilter,2);
  // Assignment: load tokenizer after skipped module.
  mainPipelinePluginManager.loadPluginInstance(mainPluginTypeIds.tokenizer,3);
  // Assignment: load junction after tokenizer.
  mainPipelinePluginManager.loadPluginInstance(mainPluginTypeIds.junction,4);
  // Assignment: load word count.
  mainPipelinePluginManager.loadPluginInstance(mainPluginTypeIds.wordCount,5);
  // Assignment: load output.
  mainPipelinePluginManager.loadPluginInstance(mainPluginTypeIds.output,6);
  // Assignment: load secondary inspection plugin.
  secondaryPipelinePluginManager.loadPluginInstance(secondaryPluginTypeIds.inspect,0);

  // Function call: run one complete primary pass.
  await runtime.runOnce();

  // Assignment: captured output.
  const outputObject=outputSink.lastWrittenObject;
  // Assignment: inspection records.
  const inspectionRecords=runtime.context.pipelineInspectionState.inspectionRecords;
  // Assignment: skip records.
  const skipRecords=runtime.context.pipelineControlState.skipRecords;
  // Assignment: junction records.
  const junctionRecords=runtime.context.pipelineControlState.junctionRecords;
  // Function call: verify regex module was skipped.
  assertCondition(skipRecords.length===1,"Expected exactly one skipped module record.");
  // Function call: verify skipped type id.
  assertCondition(skipRecords[0].skippedTypeId==="plugin.stream.regex.filter","Expected regex filter to be skipped.");
  // Function call: verify branch inspection ran in secondary pipeline.
  assertCondition(inspectionRecords.some(record=>record.executionFrame&&record.executionFrame.pipelineName==="secondary"),"Expected secondary pipeline inspection record.");
  // Function call: verify junction was recorded.
  assertCondition(junctionRecords.length===1,"Expected one junction record.");
  // Function call: verify transformed text was not filtered because regex was skipped.
  assertCondition(outputObject.transformedJson.text==="The quick brown fox jumps over 13 lazy dogs. The fox is quick.","Expected transformed text to remain unfiltered after skipping regex.");
  // Function call: verify word count continued after skip.
  assertCondition(outputObject.transformedJson._streamWordCount===12,"Expected word count to continue after skip and count original words.");

  return {name:"control flow skip continues and junction enters secondary pipeline",passed:true,details:{skipRecords:skipRecords.length,junctionRecords:junctionRecords.length,inspectionRecords:inspectionRecords.length,wordCount:outputObject.transformedJson._streamWordCount}};
}

// Function: verify break module can pause execution before the next module.
async function runSignalControlledBreakPauseTest(){
  // Assignment: destructure core namespace.
  const {RecursivePipelineRuntime,PipelineDebugLogger,RuntimeState}=globalObject.PipelineCore;
  // Assignment: reader provider.
  const readerProvider=new InMemoryJsonInputReaderProvider({mode:"jsonText",value:"pause test"});
  // Assignment: output sink captures output if execution reaches output.
  const outputSink=new InMemoryJsonOutputSink();
  // Assignment: runtime under test.
  const runtime=new RecursivePipelineRuntime({debug:new PipelineDebugLogger({enabled:true})});
  // Function call: enable control flow capabilities.
  globalObject.PipelineControlFlowPlugins.ensureRuntimeControlFlowCapabilities(runtime);
  // Assignment: main plugin manager.
  const mainPipelinePluginManager=runtime.getPluginManagerByPipelineName("main");
  // Assignment: plugin ids.
  const pluginTypeIds={
    reader:mainPipelinePluginManager.installPluginFactory(globalObject.PipelinePlugins.StatefulJsonReaderPlugin,[readerProvider]),
    breakBySignal:mainPipelinePluginManager.installPluginFactory(globalObject.PipelineControlFlowPlugins.SignalControlledBreakPlugin,[{externalSignalName:"break-control-signal",defaultControlSignal:"NONE"}]),
    output:mainPipelinePluginManager.installPluginFactory(globalObject.PipelinePlugins.StatefulJsonOutputPlugin,[outputSink])
  };

  // Function call: configure break module to pause.
  runtime.setExternalSignalValue("break-control-signal","PAUSE");
  // Assignment: load reader.
  mainPipelinePluginManager.loadPluginInstance(pluginTypeIds.reader,0);
  // Assignment: load break before output.
  mainPipelinePluginManager.loadPluginInstance(pluginTypeIds.breakBySignal,1);
  // Assignment: load output that should not execute while paused.
  mainPipelinePluginManager.loadPluginInstance(pluginTypeIds.output,2);

  // Function call: run one pass.
  await runtime.runOnce();

  // Function call: verify runtime paused.
  assertCondition(runtime.state===RuntimeState.PAUSED,"Expected runtime state to be paused.");
  // Function call: verify output was not reached after break pause.
  assertCondition(outputSink.lastWrittenObject===null,"Expected output not to run after pause break.");

  return {name:"signal controlled break pauses execution before following module",passed:true,details:{state:runtime.state,outputWritten:outputSink.lastWrittenObject!==null}};
}


// Function: verify PipelineJsonDocument keeps an append-only frozen JSON history buffer.
function runPipelineJsonDocumentHistoryBufferTest(){
  // Assignment: destructure JSON document factory.
  const {PipelineJsonDocumentFactory}=globalObject.PipelineCore;
  // Assignment: create document with initial JSON.
  const jsonDocument=PipelineJsonDocumentFactory.createPipelineJsonDocument({text:"first",nested:{count:1}});
  // Function call: append one path mutation.
  jsonDocument.setValueAtPath("text","second");
  // Function call: append one merge mutation.
  jsonDocument.mergeObjectAtPath({added:true});
  // Assignment: history and metadata snapshots.
  const historySnapshots=jsonDocument.toHistoryPlainArray();
  const historyMetadata=jsonDocument.toHistoryMetadataPlainArray();

  // Function call: verify history length.
  assertCondition(historySnapshots.length===3,"Expected three JSON history snapshots.");
  // Function call: verify immutable original was preserved.
  assertCondition(historySnapshots[0].text==="first","Expected first history snapshot to preserve original text.");
  // Function call: verify first transformation snapshot.
  assertCondition(historySnapshots[1].text==="second","Expected second history snapshot to contain transformed text.");
  // Function call: verify second transformation snapshot.
  assertCondition(historySnapshots[2].added===true,"Expected third history snapshot to contain merged value.");
  // Function call: verify current plain output remains latest snapshot for compatibility.
  assertCondition(jsonDocument.toPlainObject().added===true,"Expected toPlainObject to return latest snapshot.");
  // Function call: verify immutable original output remains first input.
  assertCondition(jsonDocument.toOriginalPlainObject().text==="first","Expected immutable original root to remain first input.");
  // Function call: verify internal roots and history arrays are frozen.
  assertCondition(Object.isFrozen(jsonDocument.immutableOriginalRoot),"Expected immutable original root to be frozen.");
  assertCondition(Object.isFrozen(jsonDocument.jsonHistoryBuffer),"Expected JSON history buffer array to be frozen.");
  assertCondition(Object.isFrozen(jsonDocument.jsonHistoryBuffer[0]),"Expected first history snapshot to be frozen.");
  // Function call: verify metadata count matches history count.
  assertCondition(historyMetadata.length===historySnapshots.length,"Expected history metadata count to match history snapshot count.");

  return {name:"json document history buffer preserves immutable frozen snapshots",passed:true,details:{historyLength:historySnapshots.length,latestHistoryIndex:jsonDocument.getLatestHistoryIndex(),latestText:jsonDocument.toPlainObject().text,originalText:jsonDocument.toOriginalPlainObject().text,historyFrozen:Object.isFrozen(jsonDocument.jsonHistoryBuffer),originalFrozen:Object.isFrozen(jsonDocument.immutableOriginalRoot)}};
}

// Function: verify generic plugin state helper records tokenizer, regex, and word-count state.
async function runGenericStatefulPluginStateHandlerTest(){
  // Assignment: destructure core namespace.
  const {RecursivePipelineRuntime,PipelineDebugLogger,PipelineContextUtility}=globalObject.PipelineCore;
  // Assignment: reader provider supplies repeated fox text.
  const readerProvider=new InMemoryJsonInputReaderProvider({mode:"jsonText",value:'{"text":"fox fox dog"}'});
  // Assignment: regex provider detects fox matches.
  const regexProvider=new FixedRegexSettingsProvider("\\bfox\\b","gi");
  // Assignment: output sink captures stateful output snapshot.
  const outputSink=new InMemoryJsonOutputSink();
  // Assignment: runtime under test.
  const runtime=new RecursivePipelineRuntime({debug:new PipelineDebugLogger({enabled:true})});
  // Assignment: plugin ids.
  const pluginTypeIds={
    reader:runtime.pluginManager.installPluginFactory(globalObject.PipelinePlugins.StatefulJsonReaderPlugin,[readerProvider]),
    tokenizer:runtime.pluginManager.installPluginFactory(globalObject.PipelinePlugins.StatefulJsonTokenizerPlugin),
    regexSearch:runtime.pluginManager.installPluginFactory(globalObject.PipelinePlugins.StatefulRegexSearchPlugin,[regexProvider]),
    wordCount:runtime.pluginManager.installPluginFactory(globalObject.PipelinePlugins.StatefulWordCountPlugin),
    output:runtime.pluginManager.installPluginFactory(globalObject.PipelinePlugins.StatefulJsonOutputPlugin,[outputSink])
  };

  // Assignment: load stateful pipeline.
  runtime.pluginManager.loadPluginInstance(pluginTypeIds.reader,0);
  runtime.pluginManager.loadPluginInstance(pluginTypeIds.tokenizer,1);
  runtime.pluginManager.loadPluginInstance(pluginTypeIds.regexSearch,2);
  runtime.pluginManager.loadPluginInstance(pluginTypeIds.wordCount,3);
  runtime.pluginManager.loadPluginInstance(pluginTypeIds.output,4);

  // Function call: run one pass.
  await runtime.runOnce();

  // Assignment: output snapshot.
  const outputObject=outputSink.lastWrittenObject;
  // Assignment: latest tokenizer state from generic handler.
  const latestTokenizerRecord=PipelineContextUtility.getLatestPluginStateByTypeId(runtime.context,"plugin.stateful.tokenizer","tokenizer",null);
  // Assignment: latest regex state from generic handler.
  const latestRegexRecord=PipelineContextUtility.getLatestPluginStateByTypeId(runtime.context,"plugin.stateful.regex.search","regexSearch",null);
  // Assignment: latest word-count state from generic handler.
  const latestWordCountRecord=PipelineContextUtility.getLatestPluginStateByTypeId(runtime.context,"plugin.stateful.wordCount","wordCount",null);

  // Function call: verify output emits generic state.
  assertCondition(outputObject&&outputObject.state&&Array.isArray(outputObject.state.timeline),"Expected stateful output to emit generic state timeline.");
  // Function call: verify state timeline contains the three state-producing plugins.
  assertCondition(outputObject.state.timeline.length>=3,"Expected generic state timeline to contain tokenizer, regex, and word-count records.");
  // Function call: verify tokenizer state is available generically.
  assertCondition(latestTokenizerRecord&&latestTokenizerRecord.value&&latestTokenizerRecord.value.tokenCount===3,"Expected tokenizer state from generic handler.");
  // Function call: verify regex state is available generically.
  assertCondition(latestRegexRecord&&latestRegexRecord.value&&latestRegexRecord.value.matchCount===2,"Expected regex state from generic handler.");
  // Function call: verify word count state is available generically.
  assertCondition(latestWordCountRecord&&latestWordCountRecord.value&&latestWordCountRecord.value.wordCount===3,"Expected word-count state from generic handler.");
  // Function call: verify word count still writes data.
  assertCondition(outputObject.data._statefulWordCount===3,"Expected stateful word count to write data field.");
  // Function call: verify temporary compatibility aliases are still present.
  assertCondition(outputObject.tokenizerState&&outputObject.tokenizerState.tokenCount===3,"Expected tokenizerState compatibility alias.");
  assertCondition(outputObject.regexState&&Object.values(outputObject.regexState)[0].matchCount===2,"Expected regexState compatibility alias.");

  return {name:"generic state handler stores stateful tokenizer regex and word-count records",passed:true,details:{stateTimelineCount:outputObject.state.timeline.length,tokenCount:latestTokenizerRecord.value.tokenCount,matchCount:latestRegexRecord.value.matchCount,wordCount:latestWordCountRecord.value.wordCount,aliasesPresent:Boolean(outputObject.tokenizerState&&outputObject.regexState)}};
}


// Function: verify hook bus lifecycle events fire in deterministic order.
async function runHookBusLifecycleOrderTest(){
  // Assignment: destructure core namespace.
  const {RecursivePipelineRuntime,PipelineDebugLogger,PipelineExecutionHookPhase}=globalObject.PipelineCore;
  // Assignment: reader provider supplies a tiny object.
  const readerProvider=new InMemoryJsonInputReaderProvider({mode:"jsonText",value:"hook lifecycle test"});
  // Assignment: output sink captures output.
  const outputSink=new InMemoryJsonOutputSink();
  // Assignment: runtime under test.
  const runtime=new RecursivePipelineRuntime({debug:new PipelineDebugLogger({enabled:true})});
  // Assignment: recorded hook phases.
  const recordedHookPhaseNames=[];
  // Function call: register pipeline start hook.
  runtime.pipelineExecutionHookBus.registerPipelineExecutionHook({hookRegistrationId:"test.lifecycle.pipeline-start",hookHumanReadableLabel:"Test Lifecycle Pipeline Start Hook",hookPhase:PipelineExecutionHookPhase.PIPELINE_EXECUTION_STARTED,hookPriority:10,hookCallbackFunction:(safeHookEvent)=>{recordedHookPhaseNames.push(safeHookEvent.hookPhase+":"+safeHookEvent.pipelineName);}});
  // Function call: register module start hook.
  runtime.pipelineExecutionHookBus.registerPipelineExecutionHook({hookRegistrationId:"test.lifecycle.module-start",hookHumanReadableLabel:"Test Lifecycle Module Start Hook",hookPhase:PipelineExecutionHookPhase.MODULE_EXECUTION_STARTED,hookPriority:10,hookCallbackFunction:(safeHookEvent)=>{recordedHookPhaseNames.push(safeHookEvent.hookPhase+":"+safeHookEvent.moduleTypeId);}});
  // Function call: register pipeline finish hook.
  runtime.pipelineExecutionHookBus.registerPipelineExecutionHook({hookRegistrationId:"test.lifecycle.pipeline-finish",hookHumanReadableLabel:"Test Lifecycle Pipeline Finish Hook",hookPhase:PipelineExecutionHookPhase.PIPELINE_EXECUTION_FINISHED,hookPriority:10,hookCallbackFunction:(safeHookEvent)=>{recordedHookPhaseNames.push(safeHookEvent.hookPhase+":"+safeHookEvent.pipelineName);}});
  // Assignment: install reader plugin.
  const readerTypeId=runtime.pluginManager.installPluginFactory(globalObject.PipelinePlugins.StatefulJsonReaderPlugin,[readerProvider]);
  // Assignment: install output plugin.
  const outputTypeId=runtime.pluginManager.installPluginFactory(globalObject.PipelinePlugins.StatefulJsonOutputPlugin,[outputSink]);
  // Assignment: load reader first.
  runtime.pluginManager.loadPluginInstance(readerTypeId,0);
  // Assignment: load output second.
  runtime.pluginManager.loadPluginInstance(outputTypeId,1);
  // Function call: run one pass.
  await runtime.runOnce();
  // Function call: verify pipeline start was first.
  assertCondition(recordedHookPhaseNames[0]==="pipeline-execution-started:main","Expected first hook event to be pipeline start.");
  // Function call: verify start sentinel was observed.
  assertCondition(recordedHookPhaseNames.includes("module-execution-started:system.start"),"Expected module start hook for system.start.");
  // Function call: verify reader was observed.
  assertCondition(recordedHookPhaseNames.includes("module-execution-started:plugin.stateful.reader"),"Expected module start hook for stateful reader.");
  // Function call: verify output was reached.
  assertCondition(outputSink.lastWrittenObject!==null,"Expected output sink to receive output.");
  // Function call: verify pipeline finish was last.
  assertCondition(recordedHookPhaseNames[recordedHookPhaseNames.length-1]==="pipeline-execution-finished:main","Expected last hook event to be pipeline finish.");
  return {name:"hook bus lifecycle hooks fire in deterministic order",passed:true,details:{recordedHookPhases:recordedHookPhaseNames}};
}

// Function: verify hook events are frozen summaries rather than mutable full contexts.
async function runHookBusFrozenRedactedEventTest(){
  // Assignment: destructure core namespace.
  const {RecursivePipelineRuntime,PipelineDebugLogger,PipelineExecutionHookPhase}=globalObject.PipelineCore;
  // Assignment: reader provider supplies JSON with text and extra key.
  const readerProvider=new InMemoryJsonInputReaderProvider({mode:"jsonText",value:'{"text":"hook redaction test","extra":"value"}'});
  // Assignment: output sink captures output.
  const outputSink=new InMemoryJsonOutputSink();
  // Assignment: runtime under test.
  const runtime=new RecursivePipelineRuntime({debug:new PipelineDebugLogger({enabled:true})});
  // Assignment: captured event summary.
  let capturedEventSummary=null;
  // Function call: register one hook that inspects event immutability.
  runtime.pipelineExecutionHookBus.registerPipelineExecutionHook({hookRegistrationId:"test.security.frozen-event",hookHumanReadableLabel:"Test Security Frozen Event Hook",hookPhase:PipelineExecutionHookPhase.MODULE_EXECUTION_STARTED,hookPriority:10,hookCallbackFunction:(safeHookEvent)=>{capturedEventSummary={eventFrozen:Object.isFrozen(safeHookEvent),contextSummaryFrozen:Object.isFrozen(safeHookEvent.contextSummary),hasContextSummary:Boolean(safeHookEvent.contextSummary),hasFullContextProperty:Object.prototype.hasOwnProperty.call(safeHookEvent,"pipelineContext"),dataTopLevelKeys:Array.isArray(safeHookEvent.contextSummary&&safeHookEvent.contextSummary.dataTopLevelKeys)?safeHookEvent.contextSummary.dataTopLevelKeys:[]};}});
  // Assignment: install reader plugin.
  const readerTypeId=runtime.pluginManager.installPluginFactory(globalObject.PipelinePlugins.StatefulJsonReaderPlugin,[readerProvider]);
  // Assignment: install output plugin.
  const outputTypeId=runtime.pluginManager.installPluginFactory(globalObject.PipelinePlugins.StatefulJsonOutputPlugin,[outputSink]);
  // Assignment: load reader.
  runtime.pluginManager.loadPluginInstance(readerTypeId,0);
  // Assignment: load output.
  runtime.pluginManager.loadPluginInstance(outputTypeId,1);
  // Function call: run one pass.
  await runtime.runOnce();
  // Function call: verify hook captured an event.
  assertCondition(capturedEventSummary!==null,"Expected hook to capture event summary.");
  // Function call: verify event root is frozen.
  assertCondition(capturedEventSummary.eventFrozen===true,"Expected hook event payload to be frozen.");
  // Function call: verify nested context summary is frozen.
  assertCondition(capturedEventSummary.contextSummaryFrozen===true,"Expected hook event context summary to be frozen.");
  // Function call: verify full mutable context is not present.
  assertCondition(capturedEventSummary.hasFullContextProperty===false,"Expected hook event to omit full mutable pipeline context.");
  // Function call: verify data keys are summarized after reader has run or at least summary exists.
  assertCondition(capturedEventSummary.hasContextSummary===true,"Expected hook event to include a compact context summary.");
  return {name:"hook event payloads are frozen and redacted",passed:true,details:capturedEventSummary};
}

// Function: verify hook failure can be recorded while pipeline execution continues.
async function runHookBusFailureRecordAndContinueTest(){
  // Assignment: destructure core namespace.
  const {RecursivePipelineRuntime,PipelineDebugLogger,PipelineExecutionHookPhase,PipelineExecutionHookFailurePolicy}=globalObject.PipelineCore;
  // Assignment: reader provider supplies small text.
  const readerProvider=new InMemoryJsonInputReaderProvider({mode:"jsonText",value:"hook failure continue"});
  // Assignment: output sink captures output.
  const outputSink=new InMemoryJsonOutputSink();
  // Assignment: runtime under test.
  const runtime=new RecursivePipelineRuntime({debug:new PipelineDebugLogger({enabled:true})});
  // Function call: register throwing hook with record-and-continue policy.
  runtime.pipelineExecutionHookBus.registerPipelineExecutionHook({hookRegistrationId:"test.failure.record-and-continue",hookHumanReadableLabel:"Test Failure Record And Continue Hook",hookPhase:PipelineExecutionHookPhase.MODULE_EXECUTION_STARTED,hookPriority:10,hookFailurePolicy:PipelineExecutionHookFailurePolicy.RECORD_AND_CONTINUE,hookCallbackFunction:()=>{throw new Error("intentional hook failure for test");}});
  // Assignment: install reader plugin.
  const readerTypeId=runtime.pluginManager.installPluginFactory(globalObject.PipelinePlugins.StatefulJsonReaderPlugin,[readerProvider]);
  // Assignment: install output plugin.
  const outputTypeId=runtime.pluginManager.installPluginFactory(globalObject.PipelinePlugins.StatefulJsonOutputPlugin,[outputSink]);
  // Assignment: load reader.
  runtime.pluginManager.loadPluginInstance(readerTypeId,0);
  // Assignment: load output.
  runtime.pluginManager.loadPluginInstance(outputTypeId,1);
  // Function call: run one pass.
  await runtime.runOnce();
  // Assignment: hook snapshot after run.
  const hookBusSnapshot=runtime.pipelineExecutionHookBus.getPipelineExecutionHookBusSnapshot();
  // Function call: verify the pipeline still reached output.
  assertCondition(outputSink.lastWrittenObject!==null,"Expected output to be written even though hook failed.");
  // Function call: verify hook failures were retained.
  assertCondition(hookBusSnapshot.retainedHookFailureRecords.length>0,"Expected retained hook failure records.");
  // Function call: verify runtime did not enter error because hook policy continues.
  assertCondition(runtime.state!==globalObject.PipelineCore.RuntimeState.ERROR,"Expected runtime not to enter error for record-and-continue hook failure.");
  return {name:"hook failures are recorded and can continue without stopping the pipeline",passed:true,details:{hookFailureCount:hookBusSnapshot.retainedHookFailureRecords.length,outputWritten:outputSink.lastWrittenObject!==null,state:runtime.state}};
}



// Function: verify the abstract hypervisor observes runtime lifecycle hooks and builds a snapshot.
async function runHypervisorOversightSnapshotTest(){
  // Assignment: destructure core namespace.
  const {RecursivePipelineRuntime,PipelineDebugLogger}=globalObject.PipelineCore;
  // Assignment: reader provider supplies small text.
  const readerProvider=new InMemoryJsonInputReaderProvider({mode:"jsonText",value:"hypervisor observes pipeline"});
  // Assignment: output sink captures output.
  const outputSink=new InMemoryJsonOutputSink();
  // Assignment: runtime under test.
  const runtime=new RecursivePipelineRuntime({debug:new PipelineDebugLogger({enabled:true})});
  // Function call: control-flow capabilities expose named-pipeline diagnostics used by the hypervisor.
  globalObject.PipelineControlFlowPlugins.ensureRuntimeControlFlowCapabilities(runtime);
  // Assignment: hypervisor under test.
  const hypervisor=globalObject.PipelineHypervisor.createDefaultPipelineHypervisor({runtime,debug:runtime.debug,slowPipelineThresholdMs:100000,slowModuleThresholdMs:100000});
  // Assignment: install reader plugin.
  const readerTypeId=runtime.pluginManager.installPluginFactory(globalObject.PipelinePlugins.StatefulJsonReaderPlugin,[readerProvider]);
  // Assignment: install output plugin.
  const outputTypeId=runtime.pluginManager.installPluginFactory(globalObject.PipelinePlugins.StatefulJsonOutputPlugin,[outputSink]);
  // Assignment: load reader.
  runtime.pluginManager.loadPluginInstance(readerTypeId,0);
  // Assignment: load output.
  runtime.pluginManager.loadPluginInstance(outputTypeId,1);
  // Function call: run one pass.
  await runtime.runOnce();
  // Assignment: snapshot after one pass.
  const hypervisorSnapshot=hypervisor.createSnapshot();
  // Function call: verify hook observation.
  assertCondition(hypervisorSnapshot.counters.hookEventsObserved>0,"Expected hypervisor to observe lifecycle hook events.");
  // Function call: verify pipeline timing.
  assertCondition(Boolean(hypervisorSnapshot.timing.pipelineTimingByName.main),"Expected hypervisor to capture main pipeline timing.");
  // Function call: verify module timing.
  assertCondition(Object.keys(hypervisorSnapshot.timing.moduleTimingByTypeId).length>0,"Expected hypervisor to capture module timing.");
  // Function call: verify namespace detection.
  assertCondition(hypervisorSnapshot.loadedNamespaces.hypervisor===true,"Expected hypervisor namespace to be detected.");
  // Function call: verify no test alert created.
  assertCondition(hypervisorSnapshot.hypervisor.healthState!=="error","Expected hypervisor health not to be error.");
  return {name:"hypervisor observes lifecycle hooks and emits oversight snapshot",passed:true,details:{healthState:hypervisorSnapshot.hypervisor.healthState,eventCount:hypervisorSnapshot.counters.hookEventsObserved,moduleTimingKeys:Object.keys(hypervisorSnapshot.timing.moduleTimingByTypeId)}};
}

// Function: verify callable-registry browser shim is stitched into the global namespace.
async function runCallableRegistryShimSelfTest(){
  // Conditional: skip only when the shim is not loaded by the current harness.
  assertCondition(Boolean(globalObject.PipelineCallableRegistry),"Expected PipelineCallableRegistry namespace to be loaded.");
  // Function call: run the callable-registry module's own smoke test.
  const result=globalObject.PipelineCallableRegistry.selfTestDeepCloneModule();
  // Function call: verify smoke test success.
  assertCondition(result.ok===true,"Expected callable registry self-test to pass.");
  return {name:"callable registry shim supports registered callable deep cloning",passed:true,details:result};
}


// Function: verify the hypervisor observes junction enter and return lifecycle phases.
async function runHypervisorJunctionPhaseOversightTest(){
  // Assignment: destructure core namespace.
  const {RecursivePipelineRuntime,PipelineDebugLogger}=globalObject.PipelineCore;
  // Assignment: reader provider supplies small branchable text.
  const readerProvider=new InMemoryJsonInputReaderProvider({mode:"jsonText",value:"hypervisor junction phase test"});
  // Assignment: output sink captures output.
  const outputSink=new InMemoryJsonOutputSink();
  // Assignment: runtime under test.
  const runtime=new RecursivePipelineRuntime({debug:new PipelineDebugLogger({enabled:true})});
  // Function call: enable named pipelines.
  globalObject.PipelineControlFlowPlugins.ensureRuntimeControlFlowCapabilities(runtime);
  // Assignment: attach hypervisor before the pass starts.
  const hypervisor=globalObject.PipelineHypervisor.createDefaultPipelineHypervisor({runtime,debug:runtime.debug,slowPipelineThresholdMs:100000,slowModuleThresholdMs:100000});
  // Assignment: managers.
  const mainPipelinePluginManager=runtime.getPluginManagerByPipelineName("main");
  const secondaryPipelinePluginManager=runtime.getPluginManagerByPipelineName("secondary");
  // Assignment: install main plugins.
  const readerTypeId=mainPipelinePluginManager.installPluginFactory(globalObject.PipelinePlugins.StatefulJsonReaderPlugin,[readerProvider]);
  const junctionTypeId=mainPipelinePluginManager.installPluginFactory(globalObject.PipelineControlFlowPlugins.PipelineJunctionToNamedPipelinePlugin,[{targetPipelineName:"secondary"}]);
  const outputTypeId=mainPipelinePluginManager.installPluginFactory(globalObject.PipelinePlugins.StatefulJsonOutputPlugin,[outputSink]);
  // Assignment: install secondary inspection plugin.
  const inspectTypeId=secondaryPipelinePluginManager.installPluginFactory(globalObject.PipelineControlFlowPlugins.PipelineInspectionPlugin,[{includeCurrentDataSnapshot:false}]);
  // Function call: load main pipeline.
  mainPipelinePluginManager.loadPluginInstance(readerTypeId,0);
  mainPipelinePluginManager.loadPluginInstance(junctionTypeId,1);
  mainPipelinePluginManager.loadPluginInstance(outputTypeId,2);
  // Function call: load secondary pipeline.
  secondaryPipelinePluginManager.loadPluginInstance(inspectTypeId,0);
  // Function call: run one pass.
  await runtime.runOnce();
  // Assignment: hypervisor snapshot.
  const hypervisorSnapshot=hypervisor.createSnapshot();
  // Assignment: hook phases observed by hypervisor.
  const observedHookPhases=hypervisorSnapshot.recentEvents.map(eventRecord=>eventRecord.hookPhase);
  // Function call: verify concrete junction phase names are observed.
  assertCondition(observedHookPhases.includes("junction-pipeline-entered"),"Expected hypervisor to observe junction-pipeline-entered.");
  assertCondition(observedHookPhases.includes("junction-pipeline-returned"),"Expected hypervisor to observe junction-pipeline-returned.");
  // Function call: verify control-flow counter changed.
  assertCondition(hypervisorSnapshot.counters.controlFlowRecords>=2,"Expected hypervisor control-flow records for junction enter and return.");
  return {name:"hypervisor observes junction enter and return hook phases",passed:true,details:{controlFlowRecords:hypervisorSnapshot.counters.controlFlowRecords,observedHookPhases}};
}

// Function: verify strict JSON cloning rejects non-JSON runtime objects and descriptor tricks.
function runStrictJsonCloneRejectionTest(){
  // Assignment: strict clone helper.
  const strictClone=globalObject.PipelineCore.PipelineSecurityUtility.cloneStrictJsonValue;
  // Assignment: rejected labels are collected here.
  const rejectedLabels=[];
  // Function: assert a value is rejected by strict JSON clone.
  const expectRejected=(label,value)=>{
    try{
      strictClone(value);
    }catch(expectedError){
      rejectedLabels.push(label+":"+(expectedError.name||"Error"));
      return;
    }

    throw new Error("Expected strict JSON clone to reject "+label+".");
  };
  // Assignment: object with accessor descriptor.
  const accessorObject={};
  Object.defineProperty(accessorObject,"dangerous",{enumerable:true,get(){return "executed";}});
  // Assignment: object with a symbol key.
  const symbolKeyObject={};
  Object.defineProperty(symbolKeyObject,Symbol("hidden"),{enumerable:true,value:"hidden"});
  // Function calls: reject non-JSON and descriptor trick values.
  expectRejected("Map",new Map([["a",1]]));
  expectRejected("Set",new Set([1]));
  expectRejected("Date",new Date(0));
  expectRejected("RegExp",/x/g);
  expectRejected("function",function rejectedFunction(){});
  expectRejected("accessor",accessorObject);
  expectRejected("symbol-key",symbolKeyObject);
  // Assignment: accepted JSON clone.
  const acceptedClone=strictClone({text:"safe",items:[1,true,null]});
  // Function call: verify accepted clone remains intact.
  assertCondition(acceptedClone.text==="safe"&&acceptedClone.items.length===3,"Expected strict clone to accept ordinary JSON.");
  return {name:"strict JSON clone rejects runtime objects and descriptor tricks",passed:true,details:{rejectedLabels,acceptedClone}};
}

// Function: verify regex hardening rejects a known catastrophic-backtracking shape.
function runRegexReDosGuardTest(){
  // Assignment: regex helper.
  const {PipelineSecurityUtility}=globalObject.PipelineCore;
  // Assignment: rejection marker.
  let rejectedMessage=null;

  try{
    PipelineSecurityUtility.createBoundedRegularExpression("(a+)+$","g");
  }catch(regexGuardError){
    rejectedMessage=regexGuardError.message;
  }

  // Function call: verify high-risk regex was rejected.
  assertCondition(Boolean(rejectedMessage),"Expected nested quantified regex to be rejected.");
  // Function call: verify ordinary regex still works.
  const safeRegex=PipelineSecurityUtility.createBoundedRegularExpression("\\bfox\\b","gi");
  assertCondition(safeRegex.test("fox"),"Expected safe regex to remain accepted.");
  return {name:"regex ReDoS guard rejects nested quantified groups",passed:true,details:{rejectedMessage,safeRegex:safeRegex.source}};
}

// Function: verify node execution budget stops runaway plugin chains.
async function runExecutionNodeBudgetAbortTest(){
  // Assignment: destructure core namespace.
  const {RecursivePipelineRuntime,PipelineDebugLogger,RuntimeState}=globalObject.PipelineCore;
  // Assignment: reader provider supplies text.
  const readerProvider=new InMemoryJsonInputReaderProvider({mode:"jsonText",value:"budget abort test"});
  // Assignment: output sink captures output.
  const outputSink=new InMemoryJsonOutputSink();
  // Assignment: runtime permits only start and reader before budget expires.
  const runtime=new RecursivePipelineRuntime({debug:new PipelineDebugLogger({enabled:true}),maximumNodesPerPass:2});
  // Assignment: install plugins.
  const readerTypeId=runtime.pluginManager.installPluginFactory(globalObject.PipelinePlugins.StatefulJsonReaderPlugin,[readerProvider]);
  const outputTypeId=runtime.pluginManager.installPluginFactory(globalObject.PipelinePlugins.StatefulJsonOutputPlugin,[outputSink]);
  // Function call: load reader and output.
  runtime.pluginManager.loadPluginInstance(readerTypeId,0);
  runtime.pluginManager.loadPluginInstance(outputTypeId,1);
  // Function call: run one pass; recursive executor records the budget failure in context.
  await runtime.runOnce();
  // Assignment: budget errors.
  const budgetErrors=runtime.context.errors.filter(errorRecord=>errorRecord.executionBudgetReason==="maximumNodesPerPass");
  // Function calls: verify budget abort.
  assertCondition(runtime.state===RuntimeState.ERROR,"Expected runtime to enter error state after node budget exhaustion.");
  assertCondition(budgetErrors.length>=1,"Expected maximumNodesPerPass error record.");
  assertCondition(outputSink.lastWrittenObject===null,"Expected output not to be written after node budget abort.");
  return {name:"execution node budget aborts excessive passes",passed:true,details:{state:runtime.state,errorCount:runtime.context.errors.length,budgetReason:budgetErrors[0].executionBudgetReason}};
}

// Function: verify branch budget stops excessive junction traversal.
async function runExecutionBranchBudgetAbortTest(){
  // Assignment: destructure core namespace.
  const {RecursivePipelineRuntime,PipelineDebugLogger,RuntimeState}=globalObject.PipelineCore;
  // Assignment: reader provider supplies branchable text.
  const readerProvider=new InMemoryJsonInputReaderProvider({mode:"jsonText",value:"branch budget abort"});
  // Assignment: output sink captures output.
  const outputSink=new InMemoryJsonOutputSink();
  // Assignment: runtime allows no branch entries.
  const runtime=new RecursivePipelineRuntime({debug:new PipelineDebugLogger({enabled:true}),maximumBranchEntriesPerPass:0});
  // Function call: enable named pipelines.
  globalObject.PipelineControlFlowPlugins.ensureRuntimeControlFlowCapabilities(runtime);
  // Assignment: managers.
  const mainPipelinePluginManager=runtime.getPluginManagerByPipelineName("main");
  const secondaryPipelinePluginManager=runtime.getPluginManagerByPipelineName("secondary");
  // Assignment: install plugins.
  const readerTypeId=mainPipelinePluginManager.installPluginFactory(globalObject.PipelinePlugins.StatefulJsonReaderPlugin,[readerProvider]);
  const junctionTypeId=mainPipelinePluginManager.installPluginFactory(globalObject.PipelineControlFlowPlugins.PipelineJunctionToNamedPipelinePlugin,[{targetPipelineName:"secondary"}]);
  const outputTypeId=mainPipelinePluginManager.installPluginFactory(globalObject.PipelinePlugins.StatefulJsonOutputPlugin,[outputSink]);
  const inspectTypeId=secondaryPipelinePluginManager.installPluginFactory(globalObject.PipelineControlFlowPlugins.PipelineInspectionPlugin,[{includeCurrentDataSnapshot:false}]);
  // Function call: load pipelines.
  mainPipelinePluginManager.loadPluginInstance(readerTypeId,0);
  mainPipelinePluginManager.loadPluginInstance(junctionTypeId,1);
  mainPipelinePluginManager.loadPluginInstance(outputTypeId,2);
  secondaryPipelinePluginManager.loadPluginInstance(inspectTypeId,0);
  // Function call: run one pass; junction should exhaust branch budget before entering secondary.
  await runtime.runOnce();
  // Assignment: budget errors.
  const budgetErrors=runtime.context.errors.filter(errorRecord=>errorRecord.executionBudgetReason==="maximumBranchEntriesPerPass");
  // Function calls: verify budget abort.
  assertCondition(runtime.state===RuntimeState.ERROR,"Expected runtime to enter error state after branch budget exhaustion.");
  assertCondition(budgetErrors.length>=1,"Expected maximumBranchEntriesPerPass error record.");
  assertCondition(outputSink.lastWrittenObject===null,"Expected output not to be written after branch budget abort.");
  return {name:"execution branch budget aborts excessive junction traversal",passed:true,details:{state:runtime.state,errorCount:runtime.context.errors.length,budgetReason:budgetErrors[0].executionBudgetReason}};
}

// Function: run all available self-tests.
async function runMergedPipelineSelfTests(){
  // Assignment: self-test results are collected here.
  const selfTestResults=[];
  // Assignment: test functions in deterministic order.
  const testFunctions=[runPipelineJsonDocumentHistoryBufferTest,runGenericStatefulPluginStateHandlerTest,runStreamPipelineTransformationTest,runPluginUnloadBehaviorTest,runControlFlowJunctionAndSkipTest,runSignalControlledBreakPauseTest,runHookBusLifecycleOrderTest,runHookBusFrozenRedactedEventTest,runHookBusFailureRecordAndContinueTest,runHypervisorOversightSnapshotTest,runHypervisorJunctionPhaseOversightTest,runStrictJsonCloneRejectionTest,runRegexReDosGuardTest,runExecutionNodeBudgetAbortTest,runExecutionBranchBudgetAbortTest,runCallableRegistryShimSelfTest];

  // Loop: execute each self-test.
  for(const testFunction of testFunctions){
    try{
      // Assignment: test result.
      const testResult=await testFunction();
      // Assignment: append success result.
      selfTestResults.push(testResult);
    }catch(testError){
      // Assignment: append failure result.
      selfTestResults.push({name:testFunction.name,passed:false,error:{message:testError.message,stack:testError.stack}});
    }
  }

  // Assignment: aggregate pass flag.
  const allTestsPassed=selfTestResults.every(testResult=>testResult.passed===true);
  return {passed:allTestsPassed,results:selfTestResults,completedAt:new Date().toISOString()};
}

// Assignment: expose browser/global self-test entry point.
globalObject.runMergedPipelineSelfTests=runMergedPipelineSelfTests;

// Conditional: export for Node-based verification when CommonJS is present.
if(typeof module!=="undefined"&&module.exports){
  // Assignment: CommonJS export.
  module.exports={runMergedPipelineSelfTests};
}
})(globalThis);
