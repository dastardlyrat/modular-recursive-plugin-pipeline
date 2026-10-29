(function attachPipelineControlFlowPluginsToGlobalObject(globalObject){
"use strict";

// Assignment: read core symbols once for control-flow plugin definitions.
const {PipelinePluginNode,PipelinePluginFactory,PipelineSecurityUtility,PipelineContextUtility,PipelineJsonDocumentFactory,PipelineExecutionHookPhase,PipelineExecutionHookEventFactory,NodeType,DataSupport,RuntimeState,Signal,StartNode,StopNode,PipelinePlugin,PluginManager}=globalObject.PipelineCore;

// Assignment: store the original recursive executor so the patch can avoid double wrapping.
const originalRecursiveExecutor=globalObject.PipelineCore.RecursivePipelineRuntime.prototype.executePipelineNodesRecursively;

// Function: ensure pipeline control state containers exist on the context.
function ensurePipelineControlState(pipelineContext){
  // Function call: ensure base context shape before adding control fields.
  PipelineContextUtility.ensurePipelineContextShape(pipelineContext);

  // Conditional/assignment: create control state on first use.
  if(!pipelineContext.pipelineControlState||typeof pipelineContext.pipelineControlState!=="object"){
    pipelineContext.pipelineControlState={skipRecords:[],breakRecords:[],junctionRecords:[]};
  }

  // Conditional/assignment: ensure skip records array exists.
  if(!Array.isArray(pipelineContext.pipelineControlState.skipRecords)){
    pipelineContext.pipelineControlState.skipRecords=[];
  }

  // Conditional/assignment: ensure break records array exists.
  if(!Array.isArray(pipelineContext.pipelineControlState.breakRecords)){
    pipelineContext.pipelineControlState.breakRecords=[];
  }

  // Conditional/assignment: ensure junction records array exists.
  if(!Array.isArray(pipelineContext.pipelineControlState.junctionRecords)){
    pipelineContext.pipelineControlState.junctionRecords=[];
  }

  return pipelineContext.pipelineControlState;
}

// Function: ensure pipeline inspection state exists on the context.
function ensurePipelineInspectionState(pipelineContext){
  // Function call: ensure base context shape before adding inspection fields.
  PipelineContextUtility.ensurePipelineContextShape(pipelineContext);

  // Conditional/assignment: create inspection state on first use.
  if(!pipelineContext.pipelineInspectionState||typeof pipelineContext.pipelineInspectionState!=="object"){
    pipelineContext.pipelineInspectionState={inspectionRecords:[]};
  }

  // Conditional/assignment: ensure inspection records array exists.
  if(!Array.isArray(pipelineContext.pipelineInspectionState.inspectionRecords)){
    pipelineContext.pipelineInspectionState.inspectionRecords=[];
  }

  return pipelineContext.pipelineInspectionState;
}

// Function: normalize pipeline names consistently across modules.
function normalizePipelineName(pipelineRuntime,pipelineName){
  // Assignment: runtime primary name fallback.
  const fallbackPipelineName=pipelineRuntime&&pipelineRuntime.primaryPipelineName?pipelineRuntime.primaryPipelineName:"main";
  // Assignment: normalized caller name.
  const normalizedPipelineName=(typeof pipelineName==="string"&&pipelineName.trim())?pipelineName.trim():fallbackPipelineName;
  return normalizedPipelineName;
}

// Function: install control-flow capabilities on one runtime instance.
function ensureRuntimeControlFlowCapabilities(pipelineRuntime){
  // Conditional: runtime object is required.
  if(!pipelineRuntime||typeof pipelineRuntime!=="object"){
    throw new TypeError("A pipeline runtime instance is required.");
  }

  // Conditional/assignment: initialize primary pipeline name.
  if(!pipelineRuntime.primaryPipelineName){
    pipelineRuntime.primaryPipelineName="main";
  }

  // Conditional/assignment: initialize secondary pipeline name.
  if(!pipelineRuntime.secondaryPipelineName){
    pipelineRuntime.secondaryPipelineName="secondary";
  }

  // Conditional/assignment: initialize named pipeline manager map.
  if(!(pipelineRuntime.pipelineManagerByPipelineName instanceof Map)){
    pipelineRuntime.pipelineManagerByPipelineName=new Map();
  }

  // Conditional/assignment: register primary manager.
  if(!pipelineRuntime.pipelineManagerByPipelineName.has(pipelineRuntime.primaryPipelineName)){
    pipelineRuntime.pipelineManagerByPipelineName.set(pipelineRuntime.primaryPipelineName,pipelineRuntime.pluginManager);
  }

  // Conditional/assignment: create the second pipeline manager requested for junction execution.
  if(!pipelineRuntime.pipelineManagerByPipelineName.has(pipelineRuntime.secondaryPipelineName)){
    pipelineRuntime.pipelineManagerByPipelineName.set(pipelineRuntime.secondaryPipelineName,new PluginManager(pipelineRuntime.debug,pipelineRuntime.registry));
  }

  // Conditional/assignment: external signal map stores control values supplied by UI or tests.
  if(!(pipelineRuntime.externalSignalValueByName instanceof Map)){
    pipelineRuntime.externalSignalValueByName=new Map();
  }

  // Conditional/assignment: one-shot skip request starts empty.
  if(typeof pipelineRuntime.skipNextPipelineNodeRequest==="undefined"){
    pipelineRuntime.skipNextPipelineNodeRequest=null;
  }

  // Conditional/assignment: execution frame stack starts empty.
  if(!Array.isArray(pipelineRuntime.executionFrameStack)){
    pipelineRuntime.executionFrameStack=[];
  }

  return pipelineRuntime;
}

// Function: patch the runtime prototype once so filtering, inspection, and junction modules can cooperate with core execution.
function patchRuntimePrototypeForControlFlow(){
  // Assignment: runtime prototype.
  const runtimePrototype=globalObject.PipelineCore.RecursivePipelineRuntime.prototype;

  // Conditional: avoid double patching if this file is loaded twice.
  if(runtimePrototype.hasControlFlowPatch===true){
    return;
  }

  // Assignment: mark prototype as patched.
  runtimePrototype.hasControlFlowPatch=true;

  // Function: create or return a named pipeline plugin manager.
  runtimePrototype.createNamedPipelinePluginManager=function createNamedPipelinePluginManager(pipelineName){
    // Function call: ensure instance fields exist.
    ensureRuntimeControlFlowCapabilities(this);
    // Assignment: normalized pipeline name.
    const normalizedPipelineName=normalizePipelineName(this,pipelineName);

    // Conditional: existing pipeline manager is reused.
    if(this.pipelineManagerByPipelineName.has(normalizedPipelineName)){
      return this.pipelineManagerByPipelineName.get(normalizedPipelineName);
    }

    // Assignment: new pipeline manager shares the runtime registry and logger.
    const namedPipelinePluginManager=new PluginManager(this.debug,this.registry);
    // Assignment: store pipeline manager.
    this.pipelineManagerByPipelineName.set(normalizedPipelineName,namedPipelinePluginManager);
    // Function call: log pipeline creation.
    this.debug.log("info","Named pipeline manager created.",{pipelineName:normalizedPipelineName});
    return namedPipelinePluginManager;
  };

  // Function: fetch an existing named pipeline plugin manager.
  runtimePrototype.getPluginManagerByPipelineName=function getPluginManagerByPipelineName(pipelineName){
    // Function call: ensure instance fields exist.
    ensureRuntimeControlFlowCapabilities(this);
    // Assignment: normalized pipeline name.
    const normalizedPipelineName=normalizePipelineName(this,pipelineName);
    // Assignment: lookup result.
    const namedPipelinePluginManager=this.pipelineManagerByPipelineName.get(normalizedPipelineName);

    // Conditional: unknown pipelines are configuration errors.
    if(!namedPipelinePluginManager){
      throw new Error("Unknown pipeline name: "+normalizedPipelineName);
    }

    return namedPipelinePluginManager;
  };

  // Function: list named pipeline names.
  runtimePrototype.getPipelineNames=function getPipelineNames(){
    // Function call: ensure instance fields exist.
    ensureRuntimeControlFlowCapabilities(this);
    return [...this.pipelineManagerByPipelineName.keys()];
  };

  // Function: return a serializable snapshot of every named pipeline.
  runtimePrototype.getNamedPipelineSnapshot=function getNamedPipelineSnapshot(){
    // Function call: ensure instance fields exist.
    ensureRuntimeControlFlowCapabilities(this);
    // Assignment: snapshot output array.
    const namedPipelineDescriptions=[];

    // Loop: describe every named pipeline manager.
    for(const [pipelineName,namedPipelinePluginManager] of this.pipelineManagerByPipelineName.entries()){
      // Assignment: append pipeline description.
      namedPipelineDescriptions.push({pipelineName,loadedPluginCount:namedPipelinePluginManager.countLoadedPluginInstances(),loadedPlugins:namedPipelinePluginManager.getLoadedPluginDescriptions(),dependencyWarnings:namedPipelinePluginManager.getDependencyWarnings()});
    }

    return namedPipelineDescriptions;
  };

  // Function: build executable nodes for a named pipeline.
  runtimePrototype.buildExecutablePipelineNodesForPipeline=function buildExecutablePipelineNodesForPipeline(pipelineName){
    // Function call: ensure instance fields exist.
    ensureRuntimeControlFlowCapabilities(this);
    // Assignment: normalized pipeline name.
    const normalizedPipelineName=normalizePipelineName(this,pipelineName);
    // Assignment: named manager.
    const namedPipelinePluginManager=this.getPluginManagerByPipelineName(normalizedPipelineName);
    // Assignment: full branch node order.
    const executablePipelineNodes=[new StartNode(),...namedPipelinePluginManager.getLoadedPluginInstances(),new StopNode()];
    // Function call: use core validator for structural checks.
    this.validateExecutablePipelineNodes(executablePipelineNodes);
    return executablePipelineNodes;
  };

  // Function: set an external signal value.
  runtimePrototype.setExternalSignalValue=function setExternalSignalValue(signalName,signalValue){
    // Function call: ensure instance fields exist.
    ensureRuntimeControlFlowCapabilities(this);

    // Conditional: signal name must be a useful string.
    if(typeof signalName!=="string"||!signalName.trim()){
      throw new TypeError("External signal name must be a non-empty string.");
    }

    // Assignment: normalized signal name.
    const normalizedSignalName=signalName.trim();
    // Assignment: cloned signal value.
    const clonedSignalValue=PipelineSecurityUtility.cloneStrictJsonValue(signalValue);
    // Assignment: store external signal.
    this.externalSignalValueByName.set(normalizedSignalName,clonedSignalValue);
    // Function call: log signal update.
    this.debug.log("info","External signal value set.",{signalName:normalizedSignalName,signalValue:clonedSignalValue});
    return clonedSignalValue;
  };

  // Function: read an external signal value.
  runtimePrototype.getExternalSignalValue=function getExternalSignalValue(signalName,defaultSignalValue=undefined){
    // Function call: ensure instance fields exist.
    ensureRuntimeControlFlowCapabilities(this);

    // Conditional: signal name must be a useful string.
    if(typeof signalName!=="string"||!signalName.trim()){
      throw new TypeError("External signal name must be a non-empty string.");
    }

    // Assignment: normalized signal name.
    const normalizedSignalName=signalName.trim();

    // Conditional: absent signal returns caller default.
    if(!this.externalSignalValueByName.has(normalizedSignalName)){
      return defaultSignalValue;
    }

    return PipelineSecurityUtility.cloneStrictJsonValue(this.externalSignalValueByName.get(normalizedSignalName));
  };

  // Function: request skipping the next module.
  runtimePrototype.requestSkipNextPipelineNode=function requestSkipNextPipelineNode(skipRequestMetadata={}){
    // Function call: ensure instance fields exist.
    ensureRuntimeControlFlowCapabilities(this);
    // Assignment: skip request record.
    this.skipNextPipelineNodeRequest={requested:true,requestedAt:new Date().toISOString(),metadata:PipelineSecurityUtility.cloneStrictJsonValue(skipRequestMetadata)};
    // Function call: log request.
    this.debug.log("info","Skip-next pipeline node requested.",this.skipNextPipelineNodeRequest);
    return this.skipNextPipelineNodeRequest;
  };

  // Function: consume pending skip request.
  runtimePrototype.consumeSkipNextPipelineNodeRequest=function consumeSkipNextPipelineNodeRequest(){
    // Function call: ensure instance fields exist.
    ensureRuntimeControlFlowCapabilities(this);
    // Assignment: request snapshot.
    const skipRequest=this.skipNextPipelineNodeRequest;
    // Assignment: clear request.
    this.skipNextPipelineNodeRequest=null;
    return skipRequest;
  };

  // Function: apply a break-control signal.
  runtimePrototype.applyBreakControlSignal=function applyBreakControlSignal(controlSignalName,controlSignalMetadata={}){
    // Function call: ensure instance fields exist.
    ensureRuntimeControlFlowCapabilities(this);
    // Assignment: normalized signal name.
    const normalizedControlSignalName=typeof controlSignalName==="string"?controlSignalName.trim().toUpperCase():"";

    // Conditional: NONE means the break module is passive.
    if(!normalizedControlSignalName||normalizedControlSignalName==="NONE"){
      this.debug.log("debug","Break control signal ignored.",{controlSignal:"NONE",metadata:controlSignalMetadata});
      return {applied:false,controlSignal:"NONE"};
    }

    // Conditional: STOP delegates to the runtime signal handler.
    if(normalizedControlSignalName===Signal.STOP){
      this.signal(Signal.STOP);
      return {applied:true,controlSignal:normalizedControlSignalName};
    }

    // Conditional: PAUSE stops recursive continuation after this module.
    if(normalizedControlSignalName===Signal.PAUSE){
      this.state=RuntimeState.PAUSED;
      this.context.status=this.state;
      this.debug.log("info","Break module paused execution.",{controlSignal:normalizedControlSignalName,metadata:controlSignalMetadata});
      return {applied:true,controlSignal:normalizedControlSignalName};
    }

    // Conditional: RESUME clears paused/stopped flags without creating a second loop.
    if(normalizedControlSignalName===Signal.RESUME){
      this.stopRequested=false;
      this.state=RuntimeState.RUNNING;
      this.context.status=this.state;
      this.debug.log("info","Break module resumed execution.",{controlSignal:normalizedControlSignalName,metadata:controlSignalMetadata});
      return {applied:true,controlSignal:normalizedControlSignalName};
    }

    throw new Error("Unsupported break control signal: "+normalizedControlSignalName);
  };

  // Function: execute a named pipeline from its start sentinel.
  runtimePrototype.executeNamedPipelineFromStart=async function executeNamedPipelineFromStart(pipelineName,pipelineContext,junctionMetadata={}){
    // Function call: ensure instance fields exist.
    ensureRuntimeControlFlowCapabilities(this);
    // Assignment: normalized branch name.
    const normalizedPipelineName=normalizePipelineName(this,pipelineName);
    // Function call: consume one branch-entry budget unit before branch execution.
    this.recordBranchEntryAgainstBudget(pipelineContext,{sourcePipelineName:(this.currentExecutionFrame&&this.currentExecutionFrame.pipelineName)||"unknown",targetPipelineName:normalizedPipelineName,entryReason:junctionMetadata&&junctionMetadata.sourceTypeId?"junction":"named-pipeline"});
    // Assignment: branch node order.
    const branchExecutablePipelineNodes=this.buildExecutablePipelineNodesForPipeline(normalizedPipelineName);
    // Function call: log branch entry.
    this.debug.log("info","Entering named pipeline from junction.",{pipelineName:normalizedPipelineName,junctionMetadata,nodeOrder:branchExecutablePipelineNodes.map(pipelineNode=>pipelineNode.id)});
    return await this.executePipelineNodesRecursively(branchExecutablePipelineNodes,0,pipelineContext,{pipelineName:normalizedPipelineName,entryReason:"junction",junctionMetadata});
  };

  // Function: replacement recursive executor with skip and inspection-frame support.
  runtimePrototype.executePipelineNodesRecursively=async function executePipelineNodesRecursively(executablePipelineNodes,nodeIndex,pipelineContext,executionOptions={}){
    // Function call: ensure instance fields exist.
    ensureRuntimeControlFlowCapabilities(this);
    // Assignment: pipeline name for this executor call.
    const executionPipelineName=normalizePipelineName(this,executionOptions.pipelineName);

    // Conditional: stop recursion at end, pause, or stop request.
    if(nodeIndex>=executablePipelineNodes.length||this.state===RuntimeState.PAUSED||this.stopRequested){
      return pipelineContext;
    }

    // Assignment: current node.
    const currentPipelineNode=executablePipelineNodes[nodeIndex];
    // Assignment: node execution start timestamp for hook timing.
    const nodeExecutionStartedAtMilliseconds=Date.now();
    // Assignment: execution frame visible to inspection modules.
    const executionFrameRecord={pipelineName:executionPipelineName,nodeIndex,instanceId:currentPipelineNode.instanceId,typeId:currentPipelineNode.typeId,nodeType:currentPipelineNode.type,nodeOrder:executablePipelineNodes.map(pipelineNode=>pipelineNode.id),entryReason:executionOptions.entryReason||"runOnce",enteredAt:new Date().toISOString()};
    // Assignment: previous frame restored after execution.
    const previousExecutionFrame=this.currentExecutionFrame;
    // Assignment: publish current frame.
    this.currentExecutionFrame=executionFrameRecord;
    // Assignment: push current frame.
    this.executionFrameStack.push(executionFrameRecord);
    // Function call: log execution.
    this.debug.log("debug","Executing node.",{pipelineName:executionPipelineName,index:nodeIndex,instanceId:currentPipelineNode.instanceId,typeId:currentPipelineNode.typeId,type:currentPipelineNode.type,requires:currentPipelineNode.requires,supports:currentPipelineNode.supports});

    try{
      // Function call: consume one node budget unit before plugin execution.
      this.recordNodeExecutionAgainstBudget(pipelineContext,{pipelineName:executionPipelineName,nodeIndex,moduleInstanceId:currentPipelineNode.instanceId,moduleTypeId:currentPipelineNode.typeId});
      // Function call: ensure context before execution.
      PipelineContextUtility.ensureContextDataDocument(pipelineContext);
      // Function call: emit out-of-band module-start hook event.
      await this.pipelineExecutionHookBus.emitPipelineExecutionHooksForLifecyclePhase(PipelineExecutionHookPhase.MODULE_EXECUTION_STARTED,PipelineExecutionHookEventFactory.createModuleExecutionStartedEvent(this,pipelineContext,{pipelineName:executionPipelineName,nodeIndex,currentPipelineNode}));
      // Assignment: execute node.
      const nextPipelineContext=await currentPipelineNode.execute(pipelineContext,this);
      // Function call: ensure context after execution.
      PipelineContextUtility.ensureContextDataDocument(nextPipelineContext);
      // Function call: emit out-of-band module-finish hook event.
      await this.pipelineExecutionHookBus.emitPipelineExecutionHooksForLifecyclePhase(PipelineExecutionHookPhase.MODULE_EXECUTION_FINISHED,PipelineExecutionHookEventFactory.createModuleExecutionFinishedEvent(this,nextPipelineContext,{pipelineName:executionPipelineName,nodeIndex,currentPipelineNode,elapsedMilliseconds:Date.now()-nodeExecutionStartedAtMilliseconds}));
      // Assignment: default next index.
      let nextNodeIndex=nodeIndex+1;
      // Assignment: skip request possibly created by current node.
      const skipNextPipelineNodeRequest=this.consumeSkipNextPipelineNodeRequest();

      // Conditional: consume skip request and continue after the skipped module.
      if(skipNextPipelineNodeRequest&&nextNodeIndex<executablePipelineNodes.length){
        // Assignment: candidate skipped node.
        const candidateSkippedPipelineNode=executablePipelineNodes[nextNodeIndex];

        // Conditional: never skip start/stop sentinels.
        if(candidateSkippedPipelineNode&&!candidateSkippedPipelineNode.system){
          // Assignment: control state.
          const pipelineControlState=ensurePipelineControlState(nextPipelineContext);
          // Assignment: skip record.
          const skipRecord={pipelineName:executionPipelineName,requestedByInstanceId:currentPipelineNode.instanceId,requestedByTypeId:currentPipelineNode.typeId,skippedInstanceId:candidateSkippedPipelineNode.instanceId,skippedTypeId:candidateSkippedPipelineNode.typeId,request:skipNextPipelineNodeRequest,skippedAt:new Date().toISOString()};
          // Assignment: append skip record.
          pipelineControlState.skipRecords.push(skipRecord);
          // Function call: append diagnostic.
          PipelineContextUtility.appendDiagnosticRecord(nextPipelineContext,{node:currentPipelineNode.id,typeId:currentPipelineNode.typeId,event:"skip-next-module-consumed",...skipRecord});
          // Function call: log skip.
          this.debug.log("info","Pipeline node skipped by filter module.",skipRecord);
          // Function call: emit out-of-band module-skipped hook event.
          await this.pipelineExecutionHookBus.emitPipelineExecutionHooksForLifecyclePhase(PipelineExecutionHookPhase.MODULE_EXECUTION_SKIPPED,PipelineExecutionHookEventFactory.createModuleExecutionSkippedEvent(this,nextPipelineContext,{pipelineName:executionPipelineName,nodeIndex:nextNodeIndex,currentPipelineNode:candidateSkippedPipelineNode,skipRecord}));
          // Assignment: skip exactly one module.
          nextNodeIndex+=1;
        }
      }

      return await this.executePipelineNodesRecursively(executablePipelineNodes,nextNodeIndex,nextPipelineContext,executionOptions);
    }catch(nodeExecutionError){
      // Assignment: runtime error record.
      const pipelineErrorRecord={pipelineName:executionPipelineName,instanceId:currentPipelineNode.instanceId,typeId:currentPipelineNode.typeId,message:nodeExecutionError.message,stack:nodeExecutionError.stack,errorName:nodeExecutionError.name||"Error",executionBudget:nodeExecutionError.executionBudget||null,executionBudgetReason:nodeExecutionError.executionBudgetReason||null};
      // Function call: append error.
      PipelineContextUtility.appendErrorRecord(pipelineContext,pipelineErrorRecord);
      // Function call: emit out-of-band module-failure hook event.
      await this.pipelineExecutionHookBus.emitPipelineExecutionHooksForLifecyclePhase(PipelineExecutionHookPhase.MODULE_EXECUTION_FAILED,PipelineExecutionHookEventFactory.createModuleExecutionFailedEvent(this,pipelineContext,{pipelineName:executionPipelineName,nodeIndex,currentPipelineNode,error:nodeExecutionError,pipelineErrorRecord,elapsedMilliseconds:Date.now()-nodeExecutionStartedAtMilliseconds}));
      // Function call: log error.
      this.debug.log("error","Node execution failed.",{...pipelineErrorRecord,time:new Date().toISOString()});
      // Assignment: runtime enters error state.
      this.state=RuntimeState.ERROR;
      return pipelineContext;
    }finally{
      // Assignment: pop current execution frame.
      this.executionFrameStack.pop();
      // Assignment: restore previous execution frame.
      this.currentExecutionFrame=previousExecutionFrame;
    }
  };

  // Function: replacement runOnce that preserves PAUSED state created by a break module.
  runtimePrototype.runOnce=async function runOnce(){
    // Function call: ensure instance fields exist.
    ensureRuntimeControlFlowCapabilities(this);

    // Conditional: paused runtime does not execute a new pass.
    if(this.state===RuntimeState.PAUSED){
      return this.context;
    }

    // Conditional: overlapping one-pass executions are blocked by the runtime itself.
    if(this.isPipelinePassRunning){
      this.debug.log("warn","Pipeline pass ignored because another pass is already running.",{});
      return this.context;
    }

    // Assignment: primary pipeline name for this execution.
    const primaryPipelineName=this.primaryPipelineName||"main";
    // Assignment: pipeline pass start timestamp for hook timing.
    const pipelineExecutionStartedAtMilliseconds=Date.now();
    // Function call: reset execution-budget counters for this pass.
    this.resetExecutionBudgetForPass(primaryPipelineName);
    // Assignment: mark pass as active for UI/runtime action locks.
    this.isPipelinePassRunning=true;
    // Assignment: runtime is actively running.
    this.state=RuntimeState.RUNNING;
    // Assignment: mirror state into context.
    this.context.status=this.state;
    // Assignment: increment cycle count.
    this.context.cycle+=1;
    // Assignment: build executable nodes for the primary pipeline.
    const executablePipelineNodes=this.buildExecutablePipelineNodes();
    try{
      // Function call: emit pipeline-start hook event.
    await this.pipelineExecutionHookBus.emitPipelineExecutionHooksForLifecyclePhase(PipelineExecutionHookPhase.PIPELINE_EXECUTION_STARTED,PipelineExecutionHookEventFactory.createPipelineExecutionStartedEvent(this,this.context,{pipelineName:primaryPipelineName,executablePipelineNodes}));
    // Function call: log pass start.
    this.debug.log("info","Pipeline pass started.",{pipelineName:primaryPipelineName,cycle:this.context.cycle,nodeOrder:executablePipelineNodes.map(pipelineNode=>pipelineNode.id)});

      // Assignment: execute full primary pass.
      this.context=await this.executePipelineNodesRecursively(executablePipelineNodes,0,this.context,{pipelineName:primaryPipelineName,entryReason:"runOnce"});

      // Conditional: non-error, non-stopped, and non-paused pass remains running.
      if(this.state!==RuntimeState.ERROR&&!this.stopRequested&&this.state!==RuntimeState.PAUSED){
        // Assignment: runtime remains running after pass.
        this.state=RuntimeState.RUNNING;
        // Assignment: context mirrors runtime state.
        this.context.status=this.state;
      }

      // Function call: emit pipeline-finished hook event.
      await this.pipelineExecutionHookBus.emitPipelineExecutionHooksForLifecyclePhase(PipelineExecutionHookPhase.PIPELINE_EXECUTION_FINISHED,PipelineExecutionHookEventFactory.createPipelineExecutionFinishedEvent(this,this.context,{pipelineName:primaryPipelineName,elapsedMilliseconds:Date.now()-pipelineExecutionStartedAtMilliseconds}));
      // Function call: log pass completion.
      this.debug.log("info","Pipeline pass completed.",{pipelineName:primaryPipelineName,cycle:this.context.cycle,errors:this.context.errors.length,state:this.state});
      return this.context;
    }catch(pipelineExecutionError){
      // Function call: emit pipeline-failed hook event before rethrowing.
      await this.pipelineExecutionHookBus.emitPipelineExecutionHooksForLifecyclePhase(PipelineExecutionHookPhase.PIPELINE_EXECUTION_FAILED,PipelineExecutionHookEventFactory.createPipelineExecutionFailedEvent(this,this.context,pipelineExecutionError,{pipelineName:primaryPipelineName}));
      throw pipelineExecutionError;
    }finally{
      // Assignment: release the runtime pass lock after completion, failure, or budget abort.
      this.isPipelinePassRunning=false;
    }
  };

  // Assignment: keep a reference to the original executor for debugging.
  runtimePrototype.originalRecursiveExecutorBeforeControlFlowPatch=originalRecursiveExecutor;
}

// Function call: patch runtime as soon as this module loads.
patchRuntimePrototypeForControlFlow();

// Function/class: junction plugin enters another named pipeline from its start sentinel.
class PipelineJunctionToNamedPipelinePlugin extends PipelinePluginNode{
  // Function: construct junction plugin.
  constructor(options={}){
    // Function call: validate options.
    PipelineSecurityUtility.assertPlainObject(options,"PipelineJunctionToNamedPipelinePlugin options");
    // Assignment: target pipeline name.
    const targetPipelineName=options.targetPipelineName||"secondary";

    super({typeId:options.typeId||"plugin.control.junction.toNamedPipeline",label:options.label||"Junction To Named Pipeline Plugin",type:NodeType.EXTENSION_PLUGIN,requires:options.requires||[],supports:options.supports||[DataSupport.JSON],description:options.description||"Enters another named pipeline from that pipeline's start sentinel, then returns to the current pipeline."});
    // Assignment: target pipeline name.
    this.targetPipelineName=targetPipelineName;
    // Assignment: maximum nested junction depth prevents accidental infinite recursion.
    this.maximumJunctionDepth=Number.isInteger(options.maximumJunctionDepth)?options.maximumJunctionDepth:8;
  }

  // Function: enter target pipeline and then return to caller pipeline.
  async execute(pipelineContext,pipelineRuntime){
    // Function call: ensure runtime control capabilities.
    ensureRuntimeControlFlowCapabilities(pipelineRuntime);
    // Assignment: control state.
    const pipelineControlState=ensurePipelineControlState(pipelineContext);
    // Assignment: current junction depth.
    const currentJunctionDepth=Number.isInteger(pipelineRuntime.currentJunctionDepth)?pipelineRuntime.currentJunctionDepth:0;

    // Conditional: prevent recursive runaway.
    if(currentJunctionDepth>=this.maximumJunctionDepth){
      throw new Error("Maximum junction depth exceeded: "+this.maximumJunctionDepth);
    }

    // Assignment: entry frame.
    const sourceExecutionFrame=PipelineSecurityUtility.cloneStrictJsonValue(pipelineRuntime.currentExecutionFrame||{});
    // Assignment: junction entry record.
    const junctionRecord={sourcePipelineName:sourceExecutionFrame.pipelineName||"unknown",targetPipelineName:this.targetPipelineName,sourceInstanceId:this.instanceId,sourceTypeId:this.typeId,enteredAt:new Date().toISOString(),depth:currentJunctionDepth+1};
    // Assignment: append junction record.
    pipelineControlState.junctionRecords.push(junctionRecord);
    // Function call: append diagnostic.
    PipelineContextUtility.appendDiagnosticRecord(pipelineContext,{node:this.id,typeId:this.typeId,event:"junction-enter",...junctionRecord});
    // Function call: log junction entry.
    pipelineRuntime.debug.log("info","Junction entering target pipeline.",junctionRecord);
    // Function call: emit junction-entered hook event.
    await pipelineRuntime.pipelineExecutionHookBus.emitPipelineExecutionHooksForLifecyclePhase(PipelineExecutionHookPhase.JUNCTION_PIPELINE_ENTERED,PipelineExecutionHookEventFactory.createControlFlowRecordEvent(pipelineRuntime,pipelineContext,{sourceInstanceId:this.instanceId,sourceTypeId:this.typeId,controlFlowRecord:junctionRecord}));

    try{
      // Assignment: increment runtime junction depth.
      pipelineRuntime.currentJunctionDepth=currentJunctionDepth+1;
      // Assignment: branch result context.
      const branchResultContext=await pipelineRuntime.executeNamedPipelineFromStart(this.targetPipelineName,pipelineContext,junctionRecord);
      // Assignment: junction return record.
      const junctionReturnRecord={sourceInstanceId:this.instanceId,sourceTypeId:this.typeId,targetPipelineName:this.targetPipelineName,returnedAt:new Date().toISOString()};
      // Function call: append exit diagnostic.
      PipelineContextUtility.appendDiagnosticRecord(branchResultContext,{node:this.id,typeId:this.typeId,event:"junction-return",targetPipelineName:this.targetPipelineName,returnedAt:junctionReturnRecord.returnedAt});
      // Function call: emit junction-returned hook event.
      await pipelineRuntime.pipelineExecutionHookBus.emitPipelineExecutionHooksForLifecyclePhase(PipelineExecutionHookPhase.JUNCTION_PIPELINE_RETURNED,PipelineExecutionHookEventFactory.createControlFlowRecordEvent(pipelineRuntime,branchResultContext,{sourceInstanceId:this.instanceId,sourceTypeId:this.typeId,controlFlowRecord:junctionReturnRecord}));
      return branchResultContext;
    }finally{
      // Assignment: restore previous runtime junction depth.
      pipelineRuntime.currentJunctionDepth=currentJunctionDepth;
    }
  }
}

// Function/class: conditionally skip the next module based on an external signal.
class ConditionalNextModuleFilterPlugin extends PipelinePluginNode{
  // Function: construct filter plugin.
  constructor(options={}){
    // Function call: validate options.
    PipelineSecurityUtility.assertPlainObject(options,"ConditionalNextModuleFilterPlugin options");
    super({typeId:options.typeId||"plugin.control.filter.nextModuleBySignal",label:options.label||"Conditional Next Module Filter Plugin",type:NodeType.EXTENSION_PLUGIN,requires:options.requires||[],supports:options.supports||[DataSupport.JSON],description:options.description||"Reads an external boolean signal and skips the next module when the signal does not allow execution."});
    // Assignment: signal name read from runtime external signal store.
    this.externalSignalName=options.externalSignalName||"allow-next-module";
    // Assignment: default signal value.
    this.defaultSignalValue=Object.prototype.hasOwnProperty.call(options,"defaultSignalValue")?Boolean(options.defaultSignalValue):true;
  }

  // Function: read signal and optionally request a one-module skip.
  async execute(pipelineContext,pipelineRuntime){
    // Function call: ensure runtime control capabilities.
    ensureRuntimeControlFlowCapabilities(pipelineRuntime);
    // Assignment: signal value from runtime external store.
    const externalSignalValue=pipelineRuntime.getExternalSignalValue(this.externalSignalName,this.defaultSignalValue);
    // Assignment: next module is allowed only when signal is true.
    const nextModuleExecutionAllowed=Boolean(externalSignalValue)===true;

    // Conditional: request skip when signal denies execution.
    if(!nextModuleExecutionAllowed){
      // Function call: request executor skip.
      pipelineRuntime.requestSkipNextPipelineNode({requestedByInstanceId:this.instanceId,requestedByTypeId:this.typeId,externalSignalName:this.externalSignalName,externalSignalValue,nextModuleExecutionAllowed});
    }

    // Function call: append diagnostic.
    PipelineContextUtility.appendDiagnosticRecord(pipelineContext,{node:this.id,typeId:this.typeId,event:"conditional-next-module-filter",externalSignalName:this.externalSignalName,externalSignalValue,nextModuleExecutionAllowed,action:nextModuleExecutionAllowed?"continue":"skip-next-module"});
    // Function call: log result.
    pipelineRuntime.debug.log("debug","Conditional next-module filter evaluated.",{instanceId:this.instanceId,externalSignalName:this.externalSignalName,externalSignalValue,nextModuleExecutionAllowed});
    // Function call: emit filter decision hook event.
    await pipelineRuntime.pipelineExecutionHookBus.emitPipelineExecutionHooksForLifecyclePhase(PipelineExecutionHookPhase.FILTER_DECISION_RECORDED,PipelineExecutionHookEventFactory.createControlFlowRecordEvent(pipelineRuntime,pipelineContext,{sourceInstanceId:this.instanceId,sourceTypeId:this.typeId,controlFlowRecord:{externalSignalName:this.externalSignalName,externalSignalValue,nextModuleExecutionAllowed,action:nextModuleExecutionAllowed?"continue":"skip-next-module"}}));
    return pipelineContext;
  }
}

// Function/class: break plugin controls stop, pause, or resume from an external signal.
class SignalControlledBreakPlugin extends PipelinePluginNode{
  // Function: construct break plugin.
  constructor(options={}){
    // Function call: validate options.
    PipelineSecurityUtility.assertPlainObject(options,"SignalControlledBreakPlugin options");
    super({typeId:options.typeId||"plugin.control.break.signal",label:options.label||"Signal Controlled Break Plugin",type:NodeType.EXTENSION_PLUGIN,requires:options.requires||[],supports:options.supports||[DataSupport.JSON],description:options.description||"Reads an external signal and applies STOP, PAUSE, RESUME, or NONE at this point in the pipeline."});
    // Assignment: signal name read from runtime external signal store.
    this.externalSignalName=options.externalSignalName||"break-control-signal";
    // Assignment: default break control is passive.
    this.defaultControlSignal=options.defaultControlSignal||"NONE";
  }

  // Function: apply break signal.
  async execute(pipelineContext,pipelineRuntime){
    // Function call: ensure runtime control capabilities.
    ensureRuntimeControlFlowCapabilities(pipelineRuntime);
    // Assignment: control state.
    const pipelineControlState=ensurePipelineControlState(pipelineContext);
    // Assignment: external signal value.
    const externalSignalValue=pipelineRuntime.getExternalSignalValue(this.externalSignalName,this.defaultControlSignal);
    // Assignment: result from runtime control application.
    const breakApplicationResult=pipelineRuntime.applyBreakControlSignal(externalSignalValue,{requestedByInstanceId:this.instanceId,requestedByTypeId:this.typeId,externalSignalName:this.externalSignalName});
    // Assignment: break record.
    const breakRecord={node:this.id,typeId:this.typeId,externalSignalName:this.externalSignalName,externalSignalValue,breakApplicationResult,appliedAt:new Date().toISOString()};
    // Assignment: append break record.
    pipelineControlState.breakRecords.push(breakRecord);
    // Function call: append diagnostic.
    PipelineContextUtility.appendDiagnosticRecord(pipelineContext,{event:"break-signal-evaluated",...breakRecord});
    // Function call: log result.
    pipelineRuntime.debug.log("info","Break signal evaluated.",breakRecord);
    // Function call: emit break signal hook event.
    await pipelineRuntime.pipelineExecutionHookBus.emitPipelineExecutionHooksForLifecyclePhase(PipelineExecutionHookPhase.BREAK_SIGNAL_RECORDED,PipelineExecutionHookEventFactory.createControlFlowRecordEvent(pipelineRuntime,pipelineContext,{sourceInstanceId:this.instanceId,sourceTypeId:this.typeId,controlFlowRecord:breakRecord}));
    return pipelineContext;
  }
}

// Function/class: inspection plugin captures pipeline and context state at its position.
class PipelineInspectionPlugin extends PipelinePluginNode{
  // Function: construct inspection plugin.
  constructor(options={}){
    // Function call: validate options.
    PipelineSecurityUtility.assertPlainObject(options,"PipelineInspectionPlugin options");
    super({typeId:options.typeId||"plugin.control.inspect.pipeline",label:options.label||"Pipeline Inspection Plugin",type:NodeType.EXTENSION_PLUGIN,requires:options.requires||[],supports:options.supports||[DataSupport.JSON],description:options.description||"Captures the current pipeline execution frame, named pipeline layout, and JSON context snapshot."});
    // Assignment: include data snapshot flag.
    this.includeCurrentDataSnapshot=Object.prototype.hasOwnProperty.call(options,"includeCurrentDataSnapshot")?Boolean(options.includeCurrentDataSnapshot):true;
  }

  // Function: capture point-in-pipeline inspection snapshot.
  async execute(pipelineContext,pipelineRuntime){
    // Function call: ensure runtime control capabilities.
    ensureRuntimeControlFlowCapabilities(pipelineRuntime);
    // Assignment: inspection state.
    const pipelineInspectionState=ensurePipelineInspectionState(pipelineContext);
    // Assignment: execution frame snapshot.
    const executionFrameSnapshot=PipelineSecurityUtility.cloneStrictJsonValue(pipelineRuntime.currentExecutionFrame||{});
    // Assignment: named pipeline snapshot.
    const namedPipelineSnapshot=pipelineRuntime.getNamedPipelineSnapshot();
    // Assignment: context data snapshot if enabled.
    const currentDataSnapshot=this.includeCurrentDataSnapshot?PipelineContextUtility.getCurrentDataPlainObject(pipelineContext):null;
    // Assignment: inspection record.
    const inspectionRecord={node:this.id,typeId:this.typeId,event:"pipeline-inspection",executionFrame:executionFrameSnapshot,namedPipelines:namedPipelineSnapshot,currentData:currentDataSnapshot,diagnosticCount:Array.isArray(pipelineContext.diagnostics)?pipelineContext.diagnostics.length:0,errorCount:Array.isArray(pipelineContext.errors)?pipelineContext.errors.length:0,inspectedAt:new Date().toISOString()};
    // Assignment: append inspection record.
    pipelineInspectionState.inspectionRecords.push(inspectionRecord);
    // Function call: append diagnostic.
    PipelineContextUtility.appendDiagnosticRecord(pipelineContext,{node:this.id,typeId:this.typeId,event:"pipeline-inspection",pipelineName:executionFrameSnapshot.pipelineName,nodeIndex:executionFrameSnapshot.nodeIndex,loadedPipelineCount:namedPipelineSnapshot.length});
    // Function call: log inspection.
    pipelineRuntime.debug.log("info","Pipeline inspection captured.",{instanceId:this.instanceId,pipelineName:executionFrameSnapshot.pipelineName,nodeIndex:executionFrameSnapshot.nodeIndex});
    // Function call: emit inspection snapshot hook event with the compact inspection record.
    await pipelineRuntime.pipelineExecutionHookBus.emitPipelineExecutionHooksForLifecyclePhase(PipelineExecutionHookPhase.INSPECTION_SNAPSHOT_RECORDED,PipelineExecutionHookEventFactory.createControlFlowRecordEvent(pipelineRuntime,pipelineContext,{sourceInstanceId:this.instanceId,sourceTypeId:this.typeId,controlFlowRecord:{event:inspectionRecord.event,executionFrame:inspectionRecord.executionFrame,diagnosticCount:inspectionRecord.diagnosticCount,errorCount:inspectionRecord.errorCount,inspectedAt:inspectionRecord.inspectedAt}}));
    return pipelineContext;
  }
}

// Function: create junction plugin factory.
function createPipelineJunctionToNamedPipelinePluginFactory(options={}){
  return new PipelinePluginFactory({createPluginInstance:()=>new PipelineJunctionToNamedPipelinePlugin(options)});
}

// Function: create conditional filter plugin factory.
function createConditionalNextModuleFilterPluginFactory(options={}){
  return new PipelinePluginFactory({createPluginInstance:()=>new ConditionalNextModuleFilterPlugin(options)});
}

// Function: create signal-controlled break plugin factory.
function createSignalControlledBreakPluginFactory(options={}){
  return new PipelinePluginFactory({createPluginInstance:()=>new SignalControlledBreakPlugin(options)});
}

// Function: create inspection plugin factory.
function createPipelineInspectionPluginFactory(options={}){
  return new PipelinePluginFactory({createPluginInstance:()=>new PipelineInspectionPlugin(options)});
}

// Assignment: publish control-flow plugins.
globalObject.PipelineControlFlowPlugins={
  ensureRuntimeControlFlowCapabilities,
  ensurePipelineControlState,
  ensurePipelineInspectionState,
  PipelineJunctionToNamedPipelinePlugin,
  ConditionalNextModuleFilterPlugin,
  SignalControlledBreakPlugin,
  PipelineInspectionPlugin,
  createPipelineJunctionToNamedPipelinePluginFactory,
  createConditionalNextModuleFilterPluginFactory,
  createSignalControlledBreakPluginFactory,
  createPipelineInspectionPluginFactory
};
})(globalThis);
