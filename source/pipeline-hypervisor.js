(function attachPipelineHypervisorToGlobalObject(globalObject){
"use strict";

const DEFAULT_SLOW_PIPELINE_THRESHOLD_MS=250;
const DEFAULT_SLOW_MODULE_THRESHOLD_MS=75;
const DEFAULT_MAX_EVENT_RECORDS=1000;
const DEFAULT_MAX_ALERT_RECORDS=300;
const DEFAULT_MAX_DIAGNOSTIC_RECORDS=500;

function nowIsoString(){
  return new Date().toISOString();
}

function cloneJsonSafe(value){
  const core=globalObject.PipelineCore;

  if(core&&core.PipelineSecurityUtility&&typeof core.PipelineSecurityUtility.cloneStrictJsonValue==="function"){
    return core.PipelineSecurityUtility.cloneStrictJsonValue(value);
  }

  if(core&&core.PipelineSecurityUtility&&typeof core.PipelineSecurityUtility.deepCloneJsonSafeValue==="function"){
    return core.PipelineSecurityUtility.deepCloneJsonSafeValue(value);
  }

  if(typeof structuredClone==="function"){
    try{
      return structuredClone(value);
    }catch(structuredCloneError){
      void structuredCloneError;
    }
  }

  return JSON.parse(JSON.stringify(value));
}

function summarizeError(errorValue){
  if(!errorValue){
    return null;
  }

  return {
    name:errorValue.name||"Error",
    message:errorValue.message||String(errorValue),
    stack:errorValue.stack||null
  };
}

function limitArrayLength(arrayValue,maximumLength){
  while(arrayValue.length>maximumLength){
    arrayValue.shift();
  }

  return arrayValue;
}

function createEmptyTimingMetric(){
  return {
    count:0,
    totalElapsedMilliseconds:0,
    averageElapsedMilliseconds:0,
    minimumElapsedMilliseconds:null,
    maximumElapsedMilliseconds:null,
    lastElapsedMilliseconds:null,
    lastObservedAt:null
  };
}

function recordElapsedTimingMetric(timingMetric,elapsedMilliseconds){
  const normalizedElapsedMilliseconds=Number.isFinite(elapsedMilliseconds)?elapsedMilliseconds:0;
  timingMetric.count+=1;
  timingMetric.totalElapsedMilliseconds+=normalizedElapsedMilliseconds;
  timingMetric.averageElapsedMilliseconds=timingMetric.totalElapsedMilliseconds/timingMetric.count;
  timingMetric.minimumElapsedMilliseconds=timingMetric.minimumElapsedMilliseconds===null?normalizedElapsedMilliseconds:Math.min(timingMetric.minimumElapsedMilliseconds,normalizedElapsedMilliseconds);
  timingMetric.maximumElapsedMilliseconds=timingMetric.maximumElapsedMilliseconds===null?normalizedElapsedMilliseconds:Math.max(timingMetric.maximumElapsedMilliseconds,normalizedElapsedMilliseconds);
  timingMetric.lastElapsedMilliseconds=normalizedElapsedMilliseconds;
  timingMetric.lastObservedAt=nowIsoString();
  return timingMetric;
}

function incrementCounter(counterObject,counterName,incrementAmount=1){
  counterObject[counterName]=(counterObject[counterName]||0)+incrementAmount;
  return counterObject[counterName];
}

function normalizeHookPhaseConstant(phaseName){
  return typeof phaseName==="string"&&phaseName.length?phaseName:"unknown-hook-phase";
}

class PipelineHypervisor{
  constructor(options={}){
    this.hypervisorId=options.hypervisorId||"pipeline.hypervisor.abstract.orchestration.v1";
    this.label=options.label||"Abstract Pipeline Hypervisor";
    this.runtime=options.runtime||null;
    this.debug=options.debug||null;
    this.startedAt=nowIsoString();
    this.attachedAt=null;
    this.detachedAt=null;
    this.isAttached=false;
    this.healthState="unattached";
    this.mode="observational";
    this.slowPipelineThresholdMs=Number.isFinite(options.slowPipelineThresholdMs)?Math.max(1,options.slowPipelineThresholdMs):DEFAULT_SLOW_PIPELINE_THRESHOLD_MS;
    this.slowModuleThresholdMs=Number.isFinite(options.slowModuleThresholdMs)?Math.max(1,options.slowModuleThresholdMs):DEFAULT_SLOW_MODULE_THRESHOLD_MS;
    this.maxEventRecords=Number.isInteger(options.maxEventRecords)?Math.max(10,options.maxEventRecords):DEFAULT_MAX_EVENT_RECORDS;
    this.maxAlertRecords=Number.isInteger(options.maxAlertRecords)?Math.max(10,options.maxAlertRecords):DEFAULT_MAX_ALERT_RECORDS;
    this.maxDiagnosticRecords=Number.isInteger(options.maxDiagnosticRecords)?Math.max(10,options.maxDiagnosticRecords):DEFAULT_MAX_DIAGNOSTIC_RECORDS;
    this.registeredHookIds=[];
    this.eventRecords=[];
    this.alertRecords=[];
    this.diagnosticRecords=[];
    this.failureRecords=[];
    this.counters={
      hookEventsObserved:0,
      pipelineStarted:0,
      pipelineFinished:0,
      pipelineFailed:0,
      moduleStarted:0,
      moduleFinished:0,
      moduleFailed:0,
      moduleSkipped:0,
      controlFlowRecords:0,
      alertsRaised:0,
      diagnosticsRaised:0,
      topologyAudits:0
    };
    this.pipelineTimingByName=Object.create(null);
    this.moduleTimingByTypeId=Object.create(null);
    this.moduleFailureCountByTypeId=Object.create(null);
    this.moduleSkipCountByTypeId=Object.create(null);
    this.controlFlowCountBySourceTypeId=Object.create(null);
    this.lastContextSummary=null;
    this.lastRuntimeSnapshot=null;
    this.lastEventAt=null;
  }

  attach(runtime=this.runtime){
    if(!runtime||!runtime.pipelineExecutionHookBus){
      throw new Error("PipelineHypervisor requires a runtime with pipelineExecutionHookBus.");
    }

    if(this.isAttached&&this.runtime===runtime){
      return this;
    }

    if(this.isAttached){
      this.detach();
    }

    this.runtime=runtime;
    this.debug=this.debug||runtime.debug||null;
    this.attachedAt=nowIsoString();
    this.detachedAt=null;
    this.isAttached=true;
    this.healthState="observing";
    this.installStandardOversightHooks();
    this.recordDiagnostic("hypervisor-attached","Hypervisor attached to runtime lifecycle hook bus.",{runtimeState:runtime.state});
    return this;
  }

  detach(){
    if(!this.runtime||!this.runtime.pipelineExecutionHookBus){
      this.isAttached=false;
      return this;
    }

    for(const hookRegistrationId of this.registeredHookIds){
      try{
        this.runtime.pipelineExecutionHookBus.unregisterPipelineExecutionHookByRegistrationId(hookRegistrationId);
      }catch(unregisterError){
        this.recordFailure("hook-unregister-failed",unregisterError,{hookRegistrationId});
      }
    }

    this.registeredHookIds=[];
    this.isAttached=false;
    this.detachedAt=nowIsoString();
    this.healthState="detached";
    this.recordDiagnostic("hypervisor-detached","Hypervisor detached from runtime lifecycle hook bus.",{});
    return this;
  }

  installStandardOversightHooks(){
    const core=globalObject.PipelineCore;

    if(!core||!core.PipelineExecutionHookPhase){
      throw new Error("PipelineCore hook phase constants are not loaded.");
    }

    const phase=core.PipelineExecutionHookPhase;
    const register=(suffix,hookPhase,callbackFunction)=>{
      const hookRegistrationId=this.hypervisorId+"."+suffix;
      this.runtime.pipelineExecutionHookBus.registerPipelineExecutionHook({
        hookRegistrationId,
        hookHumanReadableLabel:"Hypervisor Oversight Hook - "+suffix.replaceAll("-"," "),
        hookPhase,
        hookPriority:9000,
        hookFailurePolicy:core.PipelineExecutionHookFailurePolicy?core.PipelineExecutionHookFailurePolicy.RECORD_AND_CONTINUE:undefined,
        hookCallbackFunction:callbackFunction
      });
      this.registeredHookIds.push(hookRegistrationId);
    };

    register("pipeline-started",phase.PIPELINE_EXECUTION_STARTED,(eventPayload)=>this.observePipelineStarted(eventPayload));
    register("pipeline-finished",phase.PIPELINE_EXECUTION_FINISHED,(eventPayload)=>this.observePipelineFinished(eventPayload));
    register("pipeline-failed",phase.PIPELINE_EXECUTION_FAILED,(eventPayload)=>this.observePipelineFailed(eventPayload));
    register("module-started",phase.MODULE_EXECUTION_STARTED,(eventPayload)=>this.observeModuleStarted(eventPayload));
    register("module-finished",phase.MODULE_EXECUTION_FINISHED,(eventPayload)=>this.observeModuleFinished(eventPayload));
    register("module-failed",phase.MODULE_EXECUTION_FAILED,(eventPayload)=>this.observeModuleFailed(eventPayload));
    register("module-skipped",phase.MODULE_EXECUTION_SKIPPED,(eventPayload)=>this.observeModuleSkipped(eventPayload));

    if(phase.FILTER_DECISION_RECORDED){
      register("filter-decision",phase.FILTER_DECISION_RECORDED,(eventPayload)=>this.observeControlFlowRecord(eventPayload));
    }

    if(phase.JUNCTION_PIPELINE_ENTERED){
      register("junction-pipeline-entered",phase.JUNCTION_PIPELINE_ENTERED,(eventPayload)=>this.observeControlFlowRecord(eventPayload));
    }

    if(phase.JUNCTION_PIPELINE_RETURNED){
      register("junction-pipeline-returned",phase.JUNCTION_PIPELINE_RETURNED,(eventPayload)=>this.observeControlFlowRecord(eventPayload));
    }

    if(phase.BREAK_SIGNAL_RECORDED){
      register("break-signal",phase.BREAK_SIGNAL_RECORDED,(eventPayload)=>this.observeControlFlowRecord(eventPayload));
    }
  }

  observeBaseEvent(eventPayload,category){
    const eventRecord={
      category,
      hookEventId:eventPayload&&eventPayload.hookEventId?eventPayload.hookEventId:null,
      hookPhase:normalizeHookPhaseConstant(eventPayload&&eventPayload.hookPhase),
      pipelineName:eventPayload&&eventPayload.pipelineName?eventPayload.pipelineName:null,
      moduleTypeId:eventPayload&&eventPayload.moduleTypeId?eventPayload.moduleTypeId:null,
      moduleInstanceId:eventPayload&&eventPayload.moduleInstanceId?eventPayload.moduleInstanceId:null,
      observedAt:nowIsoString()
    };

    this.lastEventAt=eventRecord.observedAt;
    this.lastContextSummary=eventPayload&&eventPayload.contextSummary?cloneJsonSafe(eventPayload.contextSummary):this.lastContextSummary;
    this.eventRecords.push(eventRecord);
    limitArrayLength(this.eventRecords,this.maxEventRecords);
    incrementCounter(this.counters,"hookEventsObserved");
    return eventRecord;
  }

  observePipelineStarted(eventPayload){
    this.observeBaseEvent(eventPayload,"pipeline-started");
    incrementCounter(this.counters,"pipelineStarted");
    this.auditRuntimeTopology("pipeline-started");
  }

  observePipelineFinished(eventPayload){
    this.observeBaseEvent(eventPayload,"pipeline-finished");
    incrementCounter(this.counters,"pipelineFinished");

    const pipelineName=eventPayload&&eventPayload.pipelineName?eventPayload.pipelineName:"main";
    const elapsedMilliseconds=Number.isFinite(eventPayload&&eventPayload.elapsedMilliseconds)?eventPayload.elapsedMilliseconds:0;
    this.pipelineTimingByName[pipelineName]=this.pipelineTimingByName[pipelineName]||createEmptyTimingMetric();
    recordElapsedTimingMetric(this.pipelineTimingByName[pipelineName],elapsedMilliseconds);

    if(elapsedMilliseconds>this.slowPipelineThresholdMs){
      this.recordAlert("slow-pipeline-pass","Pipeline pass exceeded the configured slow-pass threshold.",{pipelineName,elapsedMilliseconds,thresholdMilliseconds:this.slowPipelineThresholdMs},"warning");
    }
  }

  observePipelineFailed(eventPayload){
    this.observeBaseEvent(eventPayload,"pipeline-failed");
    incrementCounter(this.counters,"pipelineFailed");
    this.recordAlert("pipeline-failed","Pipeline execution failure observed.",{pipelineName:eventPayload&&eventPayload.pipelineName,error:eventPayload&&eventPayload.error},"error");
  }

  observeModuleStarted(eventPayload){
    this.observeBaseEvent(eventPayload,"module-started");
    incrementCounter(this.counters,"moduleStarted");
  }

  observeModuleFinished(eventPayload){
    this.observeBaseEvent(eventPayload,"module-finished");
    incrementCounter(this.counters,"moduleFinished");

    const moduleTypeId=eventPayload&&eventPayload.moduleTypeId?eventPayload.moduleTypeId:"unknown.module";
    const elapsedMilliseconds=Number.isFinite(eventPayload&&eventPayload.elapsedMilliseconds)?eventPayload.elapsedMilliseconds:0;
    this.moduleTimingByTypeId[moduleTypeId]=this.moduleTimingByTypeId[moduleTypeId]||createEmptyTimingMetric();
    recordElapsedTimingMetric(this.moduleTimingByTypeId[moduleTypeId],elapsedMilliseconds);

    if(elapsedMilliseconds>this.slowModuleThresholdMs){
      this.recordAlert("slow-module-execution","Module execution exceeded the configured slow-module threshold.",{moduleTypeId,elapsedMilliseconds,thresholdMilliseconds:this.slowModuleThresholdMs},"warning");
    }
  }

  observeModuleFailed(eventPayload){
    this.observeBaseEvent(eventPayload,"module-failed");
    incrementCounter(this.counters,"moduleFailed");
    const moduleTypeId=eventPayload&&eventPayload.moduleTypeId?eventPayload.moduleTypeId:"unknown.module";
    incrementCounter(this.moduleFailureCountByTypeId,moduleTypeId);
    this.recordAlert("module-failed","Module execution failure observed.",{pipelineName:eventPayload&&eventPayload.pipelineName,moduleTypeId,moduleInstanceId:eventPayload&&eventPayload.moduleInstanceId,error:eventPayload&&eventPayload.error},"error");
  }

  observeModuleSkipped(eventPayload){
    this.observeBaseEvent(eventPayload,"module-skipped");
    incrementCounter(this.counters,"moduleSkipped");
    const moduleTypeId=eventPayload&&eventPayload.moduleTypeId?eventPayload.moduleTypeId:"unknown.module";
    incrementCounter(this.moduleSkipCountByTypeId,moduleTypeId);
  }

  observeControlFlowRecord(eventPayload){
    this.observeBaseEvent(eventPayload,"control-flow");
    incrementCounter(this.counters,"controlFlowRecords");
    const sourceTypeId=eventPayload&&eventPayload.sourceTypeId?eventPayload.sourceTypeId:"unknown.control.source";
    incrementCounter(this.controlFlowCountBySourceTypeId,sourceTypeId);
  }

  auditRuntimeTopology(reason="manual-audit"){
    incrementCounter(this.counters,"topologyAudits");

    if(!this.runtime){
      this.recordAlert("runtime-missing","Hypervisor has no runtime attached.",{reason},"error");
      return {ok:false,reason};
    }

    const auditRecord={reason,checkedAt:nowIsoString(),warnings:[],errors:[]};

    if(typeof this.runtime.getNamedPipelineSnapshot==="function"){
      const namedPipelineSnapshot=this.runtime.getNamedPipelineSnapshot();
      const hasMainPipeline=namedPipelineSnapshot.some(pipelineRecord=>pipelineRecord.pipelineName==="main");

      if(!hasMainPipeline){
        auditRecord.errors.push("Missing main pipeline manager.");
      }

      for(const pipelineRecord of namedPipelineSnapshot){
        if(pipelineRecord.pipelineName==="main"&&pipelineRecord.loadedPluginCount===0){
          auditRecord.warnings.push("Main pipeline has no loaded plugin instances.");
        }

        if(Array.isArray(pipelineRecord.dependencyWarnings)&&pipelineRecord.dependencyWarnings.length>0){
          auditRecord.warnings.push("Pipeline "+pipelineRecord.pipelineName+" has dependency warnings.");
        }
      }
    }else{
      auditRecord.warnings.push("Named pipeline snapshot API is unavailable.");
    }

    if(this.runtime.state===globalObject.PipelineCore.RuntimeState.ERROR){
      auditRecord.errors.push("Runtime is in error state.");
    }

    if(auditRecord.errors.length>0){
      this.recordAlert("topology-audit-error","Topology audit found blocking errors.",auditRecord,"error");
    }else if(auditRecord.warnings.length>0){
      this.recordAlert("topology-audit-warning","Topology audit found warnings.",auditRecord,"warning");
    }else{
      this.recordDiagnostic("topology-audit-ok","Topology audit completed without warnings.",auditRecord);
    }

    return auditRecord;
  }

  inspectLoadedNamespaces(){
    return {
      pipelineCore:Boolean(globalObject.PipelineCore),
      statefulPlugins:Boolean(globalObject.PipelinePlugins),
      streamPlugins:Boolean(globalObject.PipelineStreamPlugins),
      controlFlowPlugins:Boolean(globalObject.PipelineControlFlowPlugins),
      advancedPlugins:Boolean(globalObject.PipelineAdvancedPlugins),
      callableRegistry:Boolean(globalObject.PipelineCallableRegistry),
      hypervisor:Boolean(globalObject.PipelineHypervisor)
    };
  }

  recordDiagnostic(diagnosticCode,diagnosticMessage,details={}){
    const diagnosticRecord={code:diagnosticCode,message:diagnosticMessage,details:cloneJsonSafe(details),recordedAt:nowIsoString()};
    this.diagnosticRecords.push(diagnosticRecord);
    limitArrayLength(this.diagnosticRecords,this.maxDiagnosticRecords);
    incrementCounter(this.counters,"diagnosticsRaised");

    if(this.debug&&typeof this.debug.log==="function"){
      this.debug.log("debug","Hypervisor diagnostic recorded.",diagnosticRecord);
    }

    return diagnosticRecord;
  }

  recordFailure(failureCode,errorValue,details={}){
    const failureRecord={code:failureCode,error:summarizeError(errorValue),details:cloneJsonSafe(details),recordedAt:nowIsoString()};
    this.failureRecords.push(failureRecord);
    limitArrayLength(this.failureRecords,this.maxAlertRecords);
    this.recordAlert(failureCode,"Hypervisor internal failure recorded.",failureRecord,"error");
    return failureRecord;
  }

  recordAlert(alertCode,alertMessage,details={},severity="warning"){
    const alertRecord={code:alertCode,message:alertMessage,severity,details:cloneJsonSafe(details),recordedAt:nowIsoString()};
    this.alertRecords.push(alertRecord);
    limitArrayLength(this.alertRecords,this.maxAlertRecords);
    incrementCounter(this.counters,"alertsRaised");

    if(this.debug&&typeof this.debug.log==="function"){
      this.debug.log(severity==="error"?"error":"warn","Hypervisor alert recorded.",alertRecord);
    }

    return alertRecord;
  }

  computeHealthState(){
    if(!this.isAttached){
      this.healthState="detached";
      return this.healthState;
    }

    if(!this.runtime){
      this.healthState="error";
      return this.healthState;
    }

    if(globalObject.PipelineCore&&this.runtime.state===globalObject.PipelineCore.RuntimeState.ERROR){
      this.healthState="error";
      return this.healthState;
    }

    const recentErrorAlert=this.alertRecords.slice(-25).some(alertRecord=>alertRecord.severity==="error");

    if(recentErrorAlert){
      this.healthState="error";
      return this.healthState;
    }

    const recentWarningAlert=this.alertRecords.slice(-25).some(alertRecord=>alertRecord.severity==="warning");

    if(recentWarningAlert){
      this.healthState="warning";
      return this.healthState;
    }

    this.healthState="nominal";
    return this.healthState;
  }

  createRuntimeSnapshot(){
    if(!this.runtime){
      return null;
    }

    const externalSignals=this.runtime.externalSignalValueByName instanceof Map?Object.fromEntries(this.runtime.externalSignalValueByName.entries()):{};
    const namedPipelines=typeof this.runtime.getNamedPipelineSnapshot==="function"?this.runtime.getNamedPipelineSnapshot():[];
    const hookBusSnapshot=this.runtime.pipelineExecutionHookBus&&typeof this.runtime.pipelineExecutionHookBus.getPipelineExecutionHookBusSnapshot==="function"?this.runtime.pipelineExecutionHookBus.getPipelineExecutionHookBusSnapshot():null;
    const registrySnapshot=this.runtime.pluginManager&&typeof this.runtime.pluginManager.getRegistrySnapshot==="function"?this.runtime.pluginManager.getRegistrySnapshot():[];

    return {
      runtimeState:this.runtime.state,
      cycle:this.runtime.context&&Number.isFinite(this.runtime.context.cycle)?this.runtime.context.cycle:null,
      stopRequested:Boolean(this.runtime.stopRequested),
      loopActive:Boolean(this.runtime.loopHandle),
      contextStatus:this.runtime.context?this.runtime.context.status:null,
      contextErrorCount:this.runtime.context&&Array.isArray(this.runtime.context.errors)?this.runtime.context.errors.length:0,
      contextDiagnosticCount:this.runtime.context&&Array.isArray(this.runtime.context.diagnostics)?this.runtime.context.diagnostics.length:0,
      externalSignals:cloneJsonSafe(externalSignals),
      namedPipelines:cloneJsonSafe(namedPipelines),
      hookBus:cloneJsonSafe(hookBusSnapshot),
      registry:cloneJsonSafe(registrySnapshot)
    };
  }

  createSnapshot(){
    const runtimeSnapshot=this.createRuntimeSnapshot();
    this.lastRuntimeSnapshot=runtimeSnapshot?cloneJsonSafe(runtimeSnapshot):null;
    const healthState=this.computeHealthState();
    const callableRegistryAvailable=Boolean(globalObject.PipelineCallableRegistry);
    const callableRegistrySelfTestAvailable=Boolean(globalObject.PipelineCallableRegistry&&typeof globalObject.PipelineCallableRegistry.selfTestDeepCloneModule==="function");

    return {
      schemaVersion:1,
      hypervisor:{
        id:this.hypervisorId,
        label:this.label,
        mode:this.mode,
        healthState,
        isAttached:this.isAttached,
        startedAt:this.startedAt,
        attachedAt:this.attachedAt,
        detachedAt:this.detachedAt,
        lastEventAt:this.lastEventAt,
        registeredHookIds:[...this.registeredHookIds],
        thresholds:{slowPipelineThresholdMs:this.slowPipelineThresholdMs,slowModuleThresholdMs:this.slowModuleThresholdMs}
      },
      counters:cloneJsonSafe(this.counters),
      timing:{
        pipelineTimingByName:cloneJsonSafe(this.pipelineTimingByName),
        moduleTimingByTypeId:cloneJsonSafe(this.moduleTimingByTypeId)
      },
      control:{
        moduleFailureCountByTypeId:cloneJsonSafe(this.moduleFailureCountByTypeId),
        moduleSkipCountByTypeId:cloneJsonSafe(this.moduleSkipCountByTypeId),
        controlFlowCountBySourceTypeId:cloneJsonSafe(this.controlFlowCountBySourceTypeId)
      },
      loadedNamespaces:this.inspectLoadedNamespaces(),
      cloneServices:{callableRegistryAvailable,callableRegistrySelfTestAvailable},
      recentEvents:cloneJsonSafe(this.eventRecords.slice(-50)),
      recentAlerts:cloneJsonSafe(this.alertRecords.slice(-50)),
      recentDiagnostics:cloneJsonSafe(this.diagnosticRecords.slice(-50)),
      recentFailures:cloneJsonSafe(this.failureRecords.slice(-25)),
      lastContextSummary:cloneJsonSafe(this.lastContextSummary),
      runtime:runtimeSnapshot
    };
  }

  executeSignal(signalName){
    if(!this.runtime||typeof this.runtime.signal!=="function"){
      throw new Error("Cannot send signal because no runtime is attached.");
    }

    this.recordDiagnostic("hypervisor-signal-dispatched","Hypervisor dispatched a runtime signal.",{signalName});
    return this.runtime.signal(signalName);
  }

  async runOnce(){
    if(!this.runtime||typeof this.runtime.runOnce!=="function"){
      throw new Error("Cannot run pipeline because no runtime is attached.");
    }

    this.recordDiagnostic("hypervisor-run-once-requested","Hypervisor requested one runtime pass.",{});
    return await this.runtime.runOnce();
  }
}

function createDefaultPipelineHypervisor(options={}){
  const hypervisor=new PipelineHypervisor(options);

  if(options.runtime){
    hypervisor.attach(options.runtime);
  }

  return hypervisor;
}

globalObject.PipelineHypervisor={
  PipelineHypervisor,
  createDefaultPipelineHypervisor
};

if(typeof module!=="undefined"&&module.exports){
  module.exports=globalObject.PipelineHypervisor;
}
})(globalThis);
