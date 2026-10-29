(function attachMergedPipelineBrowserApplicationToGlobalObject(globalObject){
"use strict";

// Function: resolve a required DOM element by id.
function getRequiredElementById(elementId){
  // Assignment: DOM element lookup result.
  const domElement=document.getElementById(elementId);

  // Conditional: fail fast when markup and UI script are out of sync.
  if(!domElement){
    throw new Error("Missing required UI element: "+elementId);
  }

  return domElement;
}

// Function/class: browser application controller for the merged pipeline.
class MergedPipelineBrowserApplication{
  // Function: wire adapters, runtime, plugins, signals, events, and first render.
  constructor(){
    // Assignment: DOM element map.
    this.elements=this.collectRequiredElements();
    // Assignment: debug logger.
    this.debugLogger=new globalObject.PipelineCore.PipelineDebugLogger({enabled:true,maxEntries:1500});
    // Assignment: selected plugin id remembered across renders.
    this.lastSelectedLoadedPluginInstanceId=null;
    // Assignment: selected plugin index remembered across renders.
    this.lastSelectedLoadedPluginIndex=0;
    // Assignment: UI action lock blocks overlapping async actions such as double-clicked Run Once.
    this.isUserActionRunning=false;
    // Assignment: timestamp for the most recent blocked overlapping UI action.
    this.lastBlockedUiActionAt=null;

    // Assignment: browser input reader adapter.
    this.readerProvider=new globalObject.PipelineAdapters.BrowserJsonInputReaderProvider({inputModeElement:this.elements.inputMode,inputTextElement:this.elements.inputText,fileInputElement:this.elements.jsonFile});
    // Assignment: browser output sink adapter.
    this.outputSink=new globalObject.PipelineAdapters.BrowserJsonOutputSink(this.elements.outputPane);
    // Assignment: browser regex settings adapter.
    this.regexSettingsProvider=new globalObject.PipelineAdapters.BrowserRegexSettingsProvider({regexInputElement:this.elements.regexInput,regexFlagsElement:this.elements.regexFlags});
    // Assignment: recursive runtime.
    this.pipelineRuntime=new globalObject.PipelineCore.RecursivePipelineRuntime({debug:this.debugLogger});
    // Function call: enable named pipelines, external signals, skip-next behavior, break behavior, and inspection frames.
    globalObject.PipelineControlFlowPlugins.ensureRuntimeControlFlowCapabilities(this.pipelineRuntime);
    // Assignment: abstract orchestration hypervisor attached to runtime lifecycle hooks.
    this.pipelineHypervisor=globalObject.PipelineHypervisor?globalObject.PipelineHypervisor.createDefaultPipelineHypervisor({runtime:this.pipelineRuntime,debug:this.debugLogger,slowPipelineThresholdMs:250,slowModuleThresholdMs:75}):null;
    // Assignment: primary pipeline manager reference.
    this.primaryPipelinePluginManager=this.pipelineRuntime.getPluginManagerByPipelineName("main");
    // Assignment: secondary pipeline manager reference.
    this.secondaryPipelinePluginManager=this.pipelineRuntime.getPluginManagerByPipelineName("secondary");
    // Assignment: plugin catalog for the primary pipeline.
    this.pluginTypeIdCatalog=this.installPluginCatalogIntoManager(this.primaryPipelinePluginManager);
    // Assignment: plugin catalog for the secondary branch pipeline.
    this.secondaryPluginTypeIdCatalog=this.installPluginCatalogIntoManager(this.secondaryPipelinePluginManager);

    // Function call: initialize external signal defaults.
    this.synchronizeControlSignalInputsToRuntime();
    // Function call: load the secondary branch first so junctions have a target.
    this.loadDefaultSecondaryInspectionPipeline();
    // Function call: load the default stream pipeline.
    this.loadDefaultStreamPipeline();
    // Function call: bind UI events.
    this.bindUserInterfaceEvents();
    // Function call: first render.
    this.renderApplicationState();
    // Function call: log successful boot.
    this.debugLogger.log("info","Merged pipeline v12 tabbed UI wired successfully.",{defaultPipeline:"stream-with-control-flow",namedPipelines:this.pipelineRuntime.getPipelineNames(),hypervisorAttached:Boolean(this.pipelineHypervisor)});
    // Function call: render again so boot log is visible.
    this.renderApplicationState();
  }

  // Function: collect all DOM elements needed by the UI controller.
  collectRequiredElements(){
    // Assignment: element ids required by this script.
    const requiredElementIds=["inputMode","inputText","jsonFile","outputPane","debugPane","runtimeState","cycleCount","pluginCount","errorCount","installedPluginSelect","loadedPluginSelect","insertIndex","loadPluginButton","unloadPluginButton","moveUpButton","moveDownButton","runOnceButton","regexInput","regexFlags","regexButton","loadStreamDefaultButton","loadStatefulDefaultButton","runSelfTestsButton","allowNextModuleSignal","breakControlSignal","applyControlSignalsButton","branchTargetPipelineName","branchMaximumDepth","branchJsonPath","branchOperator","branchExpectedValue","branchWhen","branchSignalName","branchSignalValue","applyBranchConfigurationButton","hypervisorPane","hypervisorHealth","hypervisorEventCount","hypervisorAlertCount","hypervisorAveragePipelineMs"];
    // Assignment: output map keyed by element id.
    const collectedElementsById={};

    // Loop: resolve each required element.
    for(const elementId of requiredElementIds){
      // Assignment: store resolved element.
      collectedElementsById[elementId]=getRequiredElementById(elementId);
    }

    return collectedElementsById;
  }

  // Function: install stateful, stream, control-flow, and optional advanced plugin families into a specific manager.
  installPluginCatalogIntoManager(pluginManager){
    const branchConfiguration=this.readBranchConfiguration();
    const pluginTypeIdCatalog={
      stateful:{
        reader:pluginManager.installPluginFactory(globalObject.PipelinePlugins.StatefulJsonReaderPlugin,[this.readerProvider]),
        tokenizer:pluginManager.installPluginFactory(globalObject.PipelinePlugins.StatefulJsonTokenizerPlugin),
        regexSearch:pluginManager.installPluginFactory(globalObject.PipelinePlugins.StatefulRegexSearchPlugin,[this.regexSettingsProvider]),
        dummy:pluginManager.installPluginFactory(globalObject.PipelinePlugins.StatefulPassThroughDiagnosticPlugin,[{typeId:"plugin.stateful.dummy.passThrough",label:"Stateful Pass-Through Diagnostic Plugin",requires:[],supports:["json"]}]),
        wordCount:pluginManager.installPluginFactory(globalObject.PipelinePlugins.StatefulWordCountPlugin),
        output:pluginManager.installPluginFactory(globalObject.PipelinePlugins.StatefulJsonOutputPlugin,[this.outputSink])
      },
      stream:{
        reader:pluginManager.installPluginFactory(globalObject.PipelineStreamPlugins.StreamJsonReaderPlugin,[this.readerProvider]),
        regexFilter:pluginManager.installPluginFactory(globalObject.PipelineStreamPlugins.StreamRegexFilterPlugin,[this.regexSettingsProvider]),
        tokenizer:pluginManager.installPluginFactory(globalObject.PipelineStreamPlugins.StreamJsonTokenizerPlugin),
        dummy:pluginManager.installPluginFactory(globalObject.PipelineStreamPlugins.StreamPassThroughDiagnosticPlugin,[{typeId:"plugin.stream.dummy.passThrough",label:"Stream Pass-Through Diagnostic Plugin",requires:["plugin.stream.reader"],supports:["json"]}]),
        wordCount:pluginManager.installPluginFactory(globalObject.PipelineStreamPlugins.StreamWordCountPlugin),
        output:pluginManager.installPluginFactory(globalObject.PipelineStreamPlugins.StreamJsonOutputPlugin,[this.outputSink])
      },
      control:{
        inspect:pluginManager.installPluginFactory(globalObject.PipelineControlFlowPlugins.PipelineInspectionPlugin,[{includeCurrentDataSnapshot:true}]),
        filterNextModuleBySignal:pluginManager.installPluginFactory(globalObject.PipelineControlFlowPlugins.ConditionalNextModuleFilterPlugin,[{externalSignalName:"allow-next-module",defaultSignalValue:true}]),
        junctionToSecondaryPipeline:pluginManager.installPluginFactory(globalObject.PipelineControlFlowPlugins.PipelineJunctionToNamedPipelinePlugin,[{targetPipelineName:branchConfiguration.targetPipelineName,maximumJunctionDepth:branchConfiguration.maximumJunctionDepth}]),
        breakBySignal:pluginManager.installPluginFactory(globalObject.PipelineControlFlowPlugins.SignalControlledBreakPlugin,[{externalSignalName:"break-control-signal",defaultControlSignal:"NONE"}])
      }
    };

    if(globalObject.PipelineAdvancedPlugins){
      pluginTypeIdCatalog.advanced={
        branchByJsonTest:pluginManager.installPluginFactory(globalObject.PipelineAdvancedPlugins.ConditionalTestBranchToNamedPipelinePlugin,[{targetPipelineName:branchConfiguration.targetPipelineName,inputPath:branchConfiguration.jsonTestPath,operator:branchConfiguration.jsonTestOperator,expectedValue:branchConfiguration.expectedValue,branchWhen:branchConfiguration.branchWhen}]),
        branchByExternalSignal:pluginManager.installPluginFactory(globalObject.PipelineAdvancedPlugins.ExternalSignalBranchToNamedPipelinePlugin,[{targetPipelineName:branchConfiguration.targetPipelineName,externalSignalName:branchConfiguration.externalSignalName,operator:"strictEquals",expectedValue:branchConfiguration.externalSignalValue,branchWhen:true}]),
        forEachFunction:pluginManager.installPluginFactory(globalObject.PipelineAdvancedPlugins.ForEachJsonFunctionPlugin,[{inputPath:"items",outputPath:"items",operationName:"identity"}]),
        forEachPluginRunner:pluginManager.installPluginFactory(globalObject.PipelineAdvancedPlugins.ForEachJsonPluginRunnerPlugin,[{inputPath:"items",outputPath:"items",pluginGlobalPath:"PipelineStreamPlugins.StreamPassThroughDiagnosticPlugin"}]),
        jsonObjectEmitter:pluginManager.installPluginFactory(globalObject.PipelineAdvancedPlugins.JsonObjectEmitterPlugin,[{mode:"jsonToObjectAndWriteJson",inputPath:"",outputPath:"",objectName:"default",targetObjectType:"array"}])
      };
    }

    return pluginTypeIdCatalog;
  }

  // Function: parse a configuration value as JSON when possible, otherwise keep it as text.
  parseConfigurationValue(configurationText){
    const rawConfigurationText=String(configurationText||"").trim();

    if(!rawConfigurationText){
      return "";
    }

    try{
      return JSON.parse(rawConfigurationText);
    }catch(ignoredParseError){
      const ignoredError=ignoredParseError;
      void ignoredError;
      return rawConfigurationText;
    }
  }

  // Function: read branching configuration from the configuration tab.
  readBranchConfiguration(){
    const targetPipelineName=(this.elements&&this.elements.branchTargetPipelineName&&this.elements.branchTargetPipelineName.value.trim())||"secondary";
    const requestedMaximumDepth=Number.parseInt(this.elements&&this.elements.branchMaximumDepth?this.elements.branchMaximumDepth.value:"4",10);
    const maximumJunctionDepth=Number.isInteger(requestedMaximumDepth)?Math.max(1,Math.min(requestedMaximumDepth,32)):4;
    const jsonTestPath=(this.elements&&this.elements.branchJsonPath)?this.elements.branchJsonPath.value.trim():"text";
    const jsonTestOperator=(this.elements&&this.elements.branchOperator)?this.elements.branchOperator.value:"exists";
    const expectedValue=this.parseConfigurationValue(this.elements&&this.elements.branchExpectedValue?this.elements.branchExpectedValue.value:"fox");
    const branchWhen=(this.elements&&this.elements.branchWhen?this.elements.branchWhen.value:"true")==="true";
    const externalSignalName=(this.elements&&this.elements.branchSignalName&&this.elements.branchSignalName.value.trim())||"branch-control-signal";
    const externalSignalValue=this.parseConfigurationValue(this.elements&&this.elements.branchSignalValue?this.elements.branchSignalValue.value:"true");

    return {targetPipelineName,maximumJunctionDepth,jsonTestPath,jsonTestOperator,expectedValue,branchWhen,externalSignalName,externalSignalValue};
  }

  // Function: reinstall branch-sensitive factories so newly loaded branch plugins use current configuration.
  refreshConfiguredBranchPluginFactories(pluginManager=this.primaryPipelinePluginManager){
    const branchConfiguration=this.readBranchConfiguration();
    const configuredJunctionTypeId=pluginManager.installPluginFactory(globalObject.PipelineControlFlowPlugins.PipelineJunctionToNamedPipelinePlugin,[{targetPipelineName:branchConfiguration.targetPipelineName,maximumJunctionDepth:branchConfiguration.maximumJunctionDepth}]);

    if(pluginManager===this.primaryPipelinePluginManager&&this.pluginTypeIdCatalog&&this.pluginTypeIdCatalog.control){
      this.pluginTypeIdCatalog.control.junctionToSecondaryPipeline=configuredJunctionTypeId;
    }

    if(globalObject.PipelineAdvancedPlugins){
      const advancedTypeIds={
        branchByJsonTest:pluginManager.installPluginFactory(globalObject.PipelineAdvancedPlugins.ConditionalTestBranchToNamedPipelinePlugin,[{targetPipelineName:branchConfiguration.targetPipelineName,inputPath:branchConfiguration.jsonTestPath,operator:branchConfiguration.jsonTestOperator,expectedValue:branchConfiguration.expectedValue,branchWhen:branchConfiguration.branchWhen}]),
        branchByExternalSignal:pluginManager.installPluginFactory(globalObject.PipelineAdvancedPlugins.ExternalSignalBranchToNamedPipelinePlugin,[{targetPipelineName:branchConfiguration.targetPipelineName,externalSignalName:branchConfiguration.externalSignalName,operator:"strictEquals",expectedValue:branchConfiguration.externalSignalValue,branchWhen:true}])
      };

      if(pluginManager===this.primaryPipelinePluginManager&&this.pluginTypeIdCatalog){
        this.pluginTypeIdCatalog.advanced={...(this.pluginTypeIdCatalog.advanced||{}),...advancedTypeIds};
      }
    }

    return configuredJunctionTypeId;
  }

  // Function: synchronize UI controls into runtime external signals.
  synchronizeControlSignalInputsToRuntime(){
    // Assignment: boolean signal text.
    const allowNextModuleText=this.elements.allowNextModuleSignal.value;
    // Assignment: boolean signal value.
    const allowNextModuleValue=allowNextModuleText==="true";
    // Assignment: break signal value.
    const breakControlSignalValue=this.elements.breakControlSignal.value||"NONE";
    // Assignment: branch signal configuration.
    const branchConfiguration=this.readBranchConfiguration();
    // Function call: store next-module condition signal.
    this.pipelineRuntime.setExternalSignalValue("allow-next-module",allowNextModuleValue);
    // Function call: store break control signal.
    this.pipelineRuntime.setExternalSignalValue("break-control-signal",breakControlSignalValue);
    // Function call: store external branch signal.
    this.pipelineRuntime.setExternalSignalValue(branchConfiguration.externalSignalName,branchConfiguration.externalSignalValue);
    // Function call: log UI sync.
    this.debugLogger.log("info","Control signals synchronized from UI.",{allowNextModule:allowNextModuleValue,breakControlSignal:breakControlSignalValue,branchSignalName:branchConfiguration.externalSignalName,branchSignalValue:branchConfiguration.externalSignalValue});
  }

  // Function: clear loaded plugins from the primary pipeline and reset selected-plugin tracking.
  clearPrimaryPipelineForDefaultLoad(){
    // Function call: remove loaded plugin instances.
    this.primaryPipelinePluginManager.clearLoadedPluginInstances();
    // Assignment: reset remembered selected id.
    this.lastSelectedLoadedPluginInstanceId=null;
    // Assignment: reset remembered selected index.
    this.lastSelectedLoadedPluginIndex=0;
  }

  // Function: load the configured branch pipeline entered by junction and branch modules.
  loadDefaultSecondaryInspectionPipeline(){
    const branchConfiguration=this.readBranchConfiguration();
    const targetPipelinePluginManager=branchConfiguration.targetPipelineName==="secondary"
      ? this.secondaryPipelinePluginManager
      : this.pipelineRuntime.createNamedPipelinePluginManager(branchConfiguration.targetPipelineName);
    const targetPluginTypeIdCatalog=this.installPluginCatalogIntoManager(targetPipelinePluginManager);

    targetPipelinePluginManager.clearLoadedPluginInstances();
    targetPipelinePluginManager.loadPluginInstance(targetPluginTypeIdCatalog.control.inspect,0);

    this.activeBranchPipelinePluginManager=targetPipelinePluginManager;
    this.activeBranchPluginTypeIdCatalog=targetPluginTypeIdCatalog;

    this.debugLogger.log("info","Default branch inspection pipeline loaded.",{pipelineName:branchConfiguration.targetPipelineName,order:targetPipelinePluginManager.getLoadedPluginDescriptions().map(pluginDescription=>pluginDescription.typeId)});
  }

  // Function: load default stream pipeline where regex can be skipped, branch can be entered, break can be applied, and output is stream-shaped.
  loadDefaultStreamPipeline(){
    // Function call: synchronize signals before load.
    this.synchronizeControlSignalInputsToRuntime();
    // Function call: ensure the configured target branch pipeline exists and has an inspection point.
    this.loadDefaultSecondaryInspectionPipeline();
    // Function call: refresh branch-sensitive factories before loading instances.
    const configuredJunctionTypeId=this.refreshConfiguredBranchPluginFactories();
    // Function call: clear current primary pipeline.
    this.clearPrimaryPipelineForDefaultLoad();
    // Assignment: stream ids.
    const streamPluginTypeIds=this.pluginTypeIdCatalog.stream;
    // Assignment: control ids.
    const controlPluginTypeIds=this.pluginTypeIdCatalog.control;
    // Assignment: load stream reader first.
    this.lastSelectedLoadedPluginInstanceId=this.primaryPipelinePluginManager.loadPluginInstance(streamPluginTypeIds.reader,0);
    // Assignment: inspect immediately after reading.
    this.primaryPipelinePluginManager.loadPluginInstance(controlPluginTypeIds.inspect,1);
    // Assignment: load conditional filter before regex so it can skip regex.
    this.primaryPipelinePluginManager.loadPluginInstance(controlPluginTypeIds.filterNextModuleBySignal,2);
    // Assignment: regex filter is the module conditionally skipped.
    this.primaryPipelinePluginManager.loadPluginInstance(streamPluginTypeIds.regexFilter,3);
    // Assignment: tokenizer follows the possible regex transform.
    this.primaryPipelinePluginManager.loadPluginInstance(streamPluginTypeIds.tokenizer,4);
    // Assignment: junction enters the configured branch pipeline from that pipeline's start sentinel.
    this.primaryPipelinePluginManager.loadPluginInstance(configuredJunctionTypeId||controlPluginTypeIds.junctionToSecondaryPipeline,5);
    // Assignment: break module can pause, stop, resume, or continue at this point.
    this.primaryPipelinePluginManager.loadPluginInstance(controlPluginTypeIds.breakBySignal,6);
    // Assignment: load diagnostic no-op.
    this.primaryPipelinePluginManager.loadPluginInstance(streamPluginTypeIds.dummy,7);
    // Assignment: load word count after tokenizer and branch.
    this.primaryPipelinePluginManager.loadPluginInstance(streamPluginTypeIds.wordCount,8);
    // Assignment: load output last.
    this.primaryPipelinePluginManager.loadPluginInstance(streamPluginTypeIds.output,9);
    // Function call: log default load.
    this.debugLogger.log("info","Default stream control-flow pipeline loaded.",{pipelineName:"main",order:this.primaryPipelinePluginManager.getLoadedPluginDescriptions().map(pluginDescription=>pluginDescription.typeId)});
  }

  // Function: load default stateful pipeline where regex search can be skipped and branch inspection can occur.
  loadDefaultStatefulPipeline(){
    // Function call: synchronize signals before load.
    this.synchronizeControlSignalInputsToRuntime();
    // Function call: ensure the configured target branch pipeline exists and has an inspection point.
    this.loadDefaultSecondaryInspectionPipeline();
    // Function call: refresh branch-sensitive factories before loading instances.
    const configuredJunctionTypeId=this.refreshConfiguredBranchPluginFactories();
    // Function call: clear current primary pipeline.
    this.clearPrimaryPipelineForDefaultLoad();
    // Assignment: stateful ids.
    const statefulPluginTypeIds=this.pluginTypeIdCatalog.stateful;
    // Assignment: control ids.
    const controlPluginTypeIds=this.pluginTypeIdCatalog.control;
    // Assignment: load stateful reader first.
    this.lastSelectedLoadedPluginInstanceId=this.primaryPipelinePluginManager.loadPluginInstance(statefulPluginTypeIds.reader,0);
    // Assignment: inspect immediately after reading.
    this.primaryPipelinePluginManager.loadPluginInstance(controlPluginTypeIds.inspect,1);
    // Assignment: filter can skip the next stateful regex search.
    this.primaryPipelinePluginManager.loadPluginInstance(controlPluginTypeIds.filterNextModuleBySignal,2);
    // Assignment: stateful regex search is conditionally skippable.
    this.primaryPipelinePluginManager.loadPluginInstance(statefulPluginTypeIds.regexSearch,3);
    // Assignment: tokenizer follows filter/regex.
    this.primaryPipelinePluginManager.loadPluginInstance(statefulPluginTypeIds.tokenizer,4);
    // Assignment: junction enters the configured branch pipeline.
    this.primaryPipelinePluginManager.loadPluginInstance(configuredJunctionTypeId||controlPluginTypeIds.junctionToSecondaryPipeline,5);
    // Assignment: break module can alter execution.
    this.primaryPipelinePluginManager.loadPluginInstance(controlPluginTypeIds.breakBySignal,6);
    // Assignment: diagnostic no-op.
    this.primaryPipelinePluginManager.loadPluginInstance(statefulPluginTypeIds.dummy,7);
    // Assignment: word count.
    this.primaryPipelinePluginManager.loadPluginInstance(statefulPluginTypeIds.wordCount,8);
    // Assignment: output.
    this.primaryPipelinePluginManager.loadPluginInstance(statefulPluginTypeIds.output,9);
    // Function call: log default load.
    this.debugLogger.log("info","Default stateful control-flow pipeline loaded.",{pipelineName:"main",order:this.primaryPipelinePluginManager.getLoadedPluginDescriptions().map(pluginDescription=>pluginDescription.typeId)});
  }

  // Function: bind multi-tab UI navigation.
  bindTabNavigation(){
    const tabButtons=[...document.querySelectorAll(".tab-button[data-tab]")];
    const tabPanels=[...document.querySelectorAll(".tab-panel[data-tab-panel]")];

    const activateTab=(tabName)=>{
      for(const tabButton of tabButtons){
        const isSelected=tabButton.dataset.tab===tabName;
        tabButton.classList.toggle("active",isSelected);
        tabButton.setAttribute("aria-selected",String(isSelected));
      }

      for(const tabPanel of tabPanels){
        const isSelected=tabPanel.dataset.tabPanel===tabName;
        tabPanel.classList.toggle("active",isSelected);
        tabPanel.hidden=!isSelected;
      }
    };

    for(const tabButton of tabButtons){
      tabButton.addEventListener("click",()=>activateTab(tabButton.dataset.tab));
    }
  }

  // Function: attach all event listeners.
  bindUserInterfaceEvents(){
    // Function call: bind tab navigation before action controls.
    this.bindTabNavigation();

    // Loop: bind every runtime signal button.
    document.querySelectorAll("button[data-signal]").forEach(signalButton=>{
      // Function call: attach click listener.
      signalButton.addEventListener("click",async()=>{
        // Assignment: signal from button attribute.
        const signalName=signalButton.getAttribute("data-signal");
        await this.tryAsyncUserAction(async()=>{
          // Function call: keep external signals current before runtime signal.
          this.synchronizeControlSignalInputsToRuntime();
          // Function call: send signal to runtime.
          this.pipelineRuntime.signal(signalName);

          // Conditional: START begins loop execution.
          if(signalName===globalObject.PipelineCore.Signal.START){
            await this.pipelineRuntime.startLoop(()=>this.renderApplicationState());
          }
        },{allowWhileRunning:signalName!==globalObject.PipelineCore.Signal.START});
      });
    });

    // Function call: bind control signal apply button.
    this.elements.applyControlSignalsButton.addEventListener("click",()=>{
      this.trySyncUserAction(()=>{
        this.synchronizeControlSignalInputsToRuntime();
      });
    });

    // Function call: bind branch configuration apply button.
    this.elements.applyBranchConfigurationButton.addEventListener("click",()=>{
      this.trySyncUserAction(()=>{
        this.synchronizeControlSignalInputsToRuntime();
        this.loadDefaultSecondaryInspectionPipeline();
        this.refreshConfiguredBranchPluginFactories();
      });
    });

    // Function call: bind control signal change events.
    this.elements.allowNextModuleSignal.addEventListener("change",()=>this.synchronizeControlSignalInputsToRuntime());
    this.elements.breakControlSignal.addEventListener("change",()=>this.synchronizeControlSignalInputsToRuntime());
    this.elements.branchSignalValue.addEventListener("change",()=>this.synchronizeControlSignalInputsToRuntime());
    this.elements.branchSignalName.addEventListener("change",()=>this.synchronizeControlSignalInputsToRuntime());

    // Function call: bind selection change.
    this.elements.loadedPluginSelect.addEventListener("change",()=>{
      // Assignment: selected plugin or null.
      const selectedLoadedPlugin=this.getSelectedLoadedPluginDescription({allowNoSelection:true});

      // Conditional: update remembered selection when one exists.
      if(selectedLoadedPlugin){
        // Assignment: remember selected instance id.
        this.lastSelectedLoadedPluginInstanceId=selectedLoadedPlugin.instanceId;
        // Assignment: remember selected index.
        this.lastSelectedLoadedPluginIndex=selectedLoadedPlugin.index;
        // Assignment: use selected index as default insert index.
        this.elements.insertIndex.value=String(selectedLoadedPlugin.index);
      }

      // Function call: refresh button disabled states.
      this.renderActionAvailability();
    });

    // Function call: bind run once.
    this.elements.runOnceButton.addEventListener("click",async()=>{
      await this.tryAsyncUserAction(async()=>{
        // Function call: keep external signals current.
        this.synchronizeControlSignalInputsToRuntime();
        // Function call: run one pass.
        await this.pipelineRuntime.runOnce();
      });
    });

    // Function call: bind plugin load button.
    this.elements.loadPluginButton.addEventListener("click",()=>{
      this.trySyncUserAction(()=>{
        // Assignment: selected plugin type id.
        const pluginTypeId=this.elements.installedPluginSelect.value;
        // Assignment: safe insert index.
        const insertionIndex=this.getSafeInsertIndex();
        // Assignment: loaded instance id.
        const pluginInstanceId=this.primaryPipelinePluginManager.loadPluginInstance(pluginTypeId,insertionIndex);
        // Assignment: remember selected instance.
        this.lastSelectedLoadedPluginInstanceId=pluginInstanceId;
        // Assignment: remember selected index.
        this.lastSelectedLoadedPluginIndex=insertionIndex;
      });
    });

    // Function call: bind plugin unload button.
    this.elements.unloadPluginButton.addEventListener("click",()=>{
      this.trySyncUserAction(()=>{
        // Assignment: selected loaded plugin.
        const selectedLoadedPlugin=this.getSelectedLoadedPluginDescription();
        // Assignment: removed plugin summary.
        const removedPluginSummary=this.primaryPipelinePluginManager.unloadPluginInstance(selectedLoadedPlugin.instanceId);
        // Assignment: remaining plugin records.
        const remainingLoadedPlugins=this.primaryPipelinePluginManager.getLoadedPluginDescriptions();

        // Conditional: empty list clears selection state.
        if(remainingLoadedPlugins.length===0){
          // Assignment: no selection.
          this.lastSelectedLoadedPluginInstanceId=null;
          // Assignment: reset selected index.
          this.lastSelectedLoadedPluginIndex=0;
          return;
        }

        // Assignment: select the plugin that moved into the removed index if possible.
        const nextSelectedIndex=Math.min(removedPluginSummary.index,remainingLoadedPlugins.length-1);
        // Assignment: remember next selection.
        this.lastSelectedLoadedPluginInstanceId=remainingLoadedPlugins[nextSelectedIndex].instanceId;
        // Assignment: remember next selected index.
        this.lastSelectedLoadedPluginIndex=nextSelectedIndex;
      });
    });

    // Function call: bind move up button.
    this.elements.moveUpButton.addEventListener("click",()=>{
      this.trySyncUserAction(()=>{
        // Assignment: selected loaded plugin.
        const selectedLoadedPlugin=this.getSelectedLoadedPluginDescription();
        // Function call: move selected plugin up.
        this.primaryPipelinePluginManager.movePluginInstance(selectedLoadedPlugin.instanceId,selectedLoadedPlugin.index-1);
        // Assignment: remember moved instance.
        this.lastSelectedLoadedPluginInstanceId=selectedLoadedPlugin.instanceId;
        // Assignment: remember new index.
        this.lastSelectedLoadedPluginIndex=selectedLoadedPlugin.index-1;
      });
    });

    // Function call: bind move down button.
    this.elements.moveDownButton.addEventListener("click",()=>{
      this.trySyncUserAction(()=>{
        // Assignment: selected loaded plugin.
        const selectedLoadedPlugin=this.getSelectedLoadedPluginDescription();
        // Function call: move selected plugin down.
        this.primaryPipelinePluginManager.movePluginInstance(selectedLoadedPlugin.instanceId,selectedLoadedPlugin.index+1);
        // Assignment: remember moved instance.
        this.lastSelectedLoadedPluginInstanceId=selectedLoadedPlugin.instanceId;
        // Assignment: remember new index.
        this.lastSelectedLoadedPluginIndex=selectedLoadedPlugin.index+1;
      });
    });

    // Function call: bind stateful regex out-of-band button.
    this.elements.regexButton.addEventListener("click",()=>{
      this.trySyncUserAction(()=>{
        // Assignment: regex plugin created only for out-of-band execution.
        const regexSearchPlugin=new globalObject.PipelinePlugins.StatefulRegexSearchPlugin(this.regexSettingsProvider);
        // Assignment: regex result.
        const regexResult=regexSearchPlugin.executeOutOfBand(this.pipelineRuntime.context);
        // Assignment: display out-of-band result.
        this.elements.outputPane.textContent=JSON.stringify(regexResult,null,2);
        // Function call: log out-of-band execution.
        this.debugLogger.log("info","Regex plugin executed out-of-band.",{matchCount:regexResult.matchCount});
      });
    });

    // Function call: bind stream default button.
    this.elements.loadStreamDefaultButton.addEventListener("click",()=>{
      this.trySyncUserAction(()=>{
        this.loadDefaultStreamPipeline();
      });
    });

    // Function call: bind stateful default button.
    this.elements.loadStatefulDefaultButton.addEventListener("click",()=>{
      this.trySyncUserAction(()=>{
        this.loadDefaultStatefulPipeline();
      });
    });

    // Function call: bind self-test button.
    this.elements.runSelfTestsButton.addEventListener("click",async()=>{
      await this.tryAsyncUserAction(async()=>{
        // Assignment: self-test result.
        const selfTestResult=await globalObject.runMergedPipelineSelfTests();
        // Assignment: show result in output pane.
        this.elements.outputPane.textContent=JSON.stringify(selfTestResult,null,2);
        // Function call: log self-test result.
        this.debugLogger.log(selfTestResult.passed?"info":"error","Built-in self-tests completed.",{passed:selfTestResult.passed});
      });
    });
  }

  // Function: clamp insert index to valid range.
  getSafeInsertIndex(){
    // Assignment: raw integer parsed from input.
    const rawInsertIndex=Number.parseInt(this.elements.insertIndex.value,10);
    // Assignment: maximum legal insertion index.
    const maximumInsertIndex=this.primaryPipelinePluginManager.countLoadedPluginInstances();

    // Conditional: invalid input defaults to append.
    if(!Number.isInteger(rawInsertIndex)){
      return maximumInsertIndex;
    }

    return Math.max(0,Math.min(rawInsertIndex,maximumInsertIndex));
  }

  // Function: return selected loaded plugin metadata.
  getSelectedLoadedPluginDescription(options={}){
    // Assignment: whether null is acceptable.
    const allowNoSelection=Boolean(options.allowNoSelection);
    // Assignment: select element.
    const loadedPluginSelectElement=this.elements.loadedPluginSelect;
    // Assignment: selected option fallback compatible with more browsers.
    const selectedOption=(loadedPluginSelectElement.selectedOptions&&loadedPluginSelectElement.selectedOptions[0])?loadedPluginSelectElement.selectedOptions[0]:loadedPluginSelectElement.options[loadedPluginSelectElement.selectedIndex];

    // Conditional: missing selection is either allowed or an error.
    if(!selectedOption){
      // Conditional: caller accepts no selection.
      if(allowNoSelection){
        return null;
      }

      throw new Error("No loaded plugin instance selected.");
    }

    // Assignment: instance id from dataset.
    const pluginInstanceId=selectedOption.dataset.instanceId;
    // Assignment: type id from dataset.
    const pluginTypeId=selectedOption.dataset.typeId;
    // Assignment: numeric index from dataset.
    const loadedPluginIndex=Number.parseInt(selectedOption.dataset.index,10);

    // Conditional: option metadata must be complete.
    if(!pluginInstanceId||!Number.isInteger(loadedPluginIndex)){
      throw new Error("Selected plugin option is missing instance metadata.");
    }

    return {instanceId:pluginInstanceId,typeId:pluginTypeId,index:loadedPluginIndex};
  }

  // Function: run a synchronous user action with debug capture.
  trySyncUserAction(userAction){
    try{
      // Function call: execute user action.
      userAction();
    }catch(userActionError){
      // Function call: log UI error.
      this.debugLogger.log("error","UI action failed.",{message:userActionError.message,stack:userActionError.stack});
    }

    // Function call: render after action.
    this.renderApplicationState();
  }

  // Function: run an asynchronous user action with debug capture and overlap protection.
  async tryAsyncUserAction(userAction,actionOptions={}){
    // Assignment: some control signals may remain available while a pass is running.
    const allowWhileRunning=Boolean(actionOptions.allowWhileRunning);

    // Conditional: block overlapping async UI work unless explicitly allowed.
    if(this.isUserActionRunning&&!allowWhileRunning){
      this.lastBlockedUiActionAt=new Date().toISOString();
      this.debugLogger.log("warn","UI action blocked because another asynchronous action is still running.",{blockedAt:this.lastBlockedUiActionAt});
      this.renderApplicationState();
      return;
    }

    // Assignment: lock async UI actions before the awaited operation begins.
    this.isUserActionRunning=true;

    try{
      // Function call: execute user action.
      await userAction();
    }catch(userActionError){
      // Function call: log UI error.
      this.debugLogger.log("error","UI action failed.",{message:userActionError.message,stack:userActionError.stack});
    }finally{
      // Assignment: release async UI lock after success, failure, or cancellation.
      this.isUserActionRunning=false;
      // Function call: render after action.
      this.renderApplicationState();
    }
  }

  // Function: render entire application state.
  renderApplicationState(){
    // Assignment: runtime state label.
    this.elements.runtimeState.textContent=this.pipelineRuntime.state;
    // Assignment: cycle count label.
    this.elements.cycleCount.textContent=String(this.pipelineRuntime.context.cycle);
    // Assignment: plugin count label.
    this.elements.pluginCount.textContent=String(this.primaryPipelinePluginManager.countLoadedPluginInstances());
    // Assignment: error count label.
    this.elements.errorCount.textContent=String(this.pipelineRuntime.context.errors.length);
    // Function call: render installed plugins.
    this.renderInstalledPluginOptions();
    // Function call: render loaded plugins.
    this.renderLoadedPluginOptions();
    // Function call: render button enabled/disabled state.
    this.renderActionAvailability();
    // Assignment: external signal snapshot.
    const externalSignals=Object.fromEntries(this.pipelineRuntime.externalSignalValueByName instanceof Map?this.pipelineRuntime.externalSignalValueByName.entries():[]);
    // Assignment: hook bus snapshot for observer diagnostics.
    const pipelineExecutionHookBusSnapshot=this.pipelineRuntime.pipelineExecutionHookBus?this.pipelineRuntime.pipelineExecutionHookBus.getPipelineExecutionHookBusSnapshot():null;
    // Assignment: hypervisor snapshot for abstract orchestration oversight.
    const hypervisorSnapshot=this.pipelineHypervisor?this.pipelineHypervisor.createSnapshot():null;
    // Conditional: render hypervisor tab when the hypervisor is available.
    if(hypervisorSnapshot){
      // Assignment: compact health value.
      this.elements.hypervisorHealth.textContent=hypervisorSnapshot.hypervisor.healthState;
      // Assignment: compact event count value.
      this.elements.hypervisorEventCount.textContent=String(hypervisorSnapshot.counters.hookEventsObserved);
      // Assignment: compact alert count value.
      this.elements.hypervisorAlertCount.textContent=String(hypervisorSnapshot.counters.alertsRaised);
      // Assignment: main pipeline average timing.
      const mainTiming=hypervisorSnapshot.timing.pipelineTimingByName.main||null;
      // Assignment: render rounded average milliseconds.
      this.elements.hypervisorAveragePipelineMs.textContent=mainTiming?String(Math.round(mainTiming.averageElapsedMilliseconds*100)/100):"0";
      // Assignment: render full hypervisor snapshot.
      this.elements.hypervisorPane.textContent=JSON.stringify(hypervisorSnapshot,null,2);
    }
    // Assignment: debug pane content.
    this.elements.debugPane.textContent=JSON.stringify({debug:this.debugLogger.toJsonSerializableArray(),hookBus:pipelineExecutionHookBusSnapshot,hypervisor:hypervisorSnapshot?{healthState:hypervisorSnapshot.hypervisor.healthState,eventCount:hypervisorSnapshot.counters.hookEventsObserved,alertCount:hypervisorSnapshot.counters.alertsRaised}:null,branchConfiguration:this.readBranchConfiguration(),namedPipelines:this.pipelineRuntime.getNamedPipelineSnapshot(),registry:this.primaryPipelinePluginManager.getRegistrySnapshot(),dependencyWarnings:this.primaryPipelinePluginManager.getDependencyWarnings(),externalSignals},null,2);
  }

  // Function: render installed plugin select options for the primary pipeline.
  renderInstalledPluginOptions(){
    // Assignment: preserve current selected type id.
    const currentSelectedPluginTypeId=this.elements.installedPluginSelect.value;
    // Function call: clear select children.
    this.elements.installedPluginSelect.replaceChildren();

    // Loop: render each installed plugin.
    for(const pluginDescription of this.primaryPipelinePluginManager.getInstalledPluginDescriptions()){
      // Assignment: new option element.
      const optionElement=document.createElement("option");
      // Assignment: option value is plugin type id.
      optionElement.value=pluginDescription.typeId;
      // Assignment: option display text uses descriptive metadata.
      optionElement.textContent=pluginDescription.typeId+" | type="+pluginDescription.type+" | supports="+pluginDescription.supports.join(",")+" | requires="+(pluginDescription.requires.join(",")||"none");
      // Function call: append option.
      this.elements.installedPluginSelect.appendChild(optionElement);
    }

    // Conditional: restore previous selection when it still exists.
    if(currentSelectedPluginTypeId&&[...this.elements.installedPluginSelect.options].some(optionElement=>optionElement.value===currentSelectedPluginTypeId)){
      // Assignment: restore selected value.
      this.elements.installedPluginSelect.value=currentSelectedPluginTypeId;
    }
  }

  // Function: render loaded plugin select options for the primary pipeline.
  renderLoadedPluginOptions(){
    // Assignment: loaded plugin descriptions.
    const loadedPluginDescriptions=this.primaryPipelinePluginManager.getLoadedPluginDescriptions();
    // Function call: clear select children.
    this.elements.loadedPluginSelect.replaceChildren();

    // Loop: render loaded plugin options in execution order.
    for(const pluginDescription of loadedPluginDescriptions){
      // Assignment: new option element.
      const optionElement=document.createElement("option");
      // Assignment: option value combines index and instance id.
      optionElement.value=pluginDescription.index+":"+pluginDescription.instanceId;
      // Assignment: dataset instance id.
      optionElement.dataset.instanceId=pluginDescription.instanceId;
      // Assignment: dataset type id.
      optionElement.dataset.typeId=pluginDescription.typeId;
      // Assignment: dataset index.
      optionElement.dataset.index=String(pluginDescription.index);
      // Assignment: display text.
      optionElement.textContent=pluginDescription.index+": "+pluginDescription.instanceId+" ("+pluginDescription.typeId+") | type="+pluginDescription.type+" | supports="+pluginDescription.supports.join(",")+" | requires="+(pluginDescription.requires.join(",")||"none");
      // Function call: append option.
      this.elements.loadedPluginSelect.appendChild(optionElement);
    }

    // Conditional: no loaded plugins means no selection.
    if(loadedPluginDescriptions.length===0){
      // Assignment: clear selected instance.
      this.lastSelectedLoadedPluginInstanceId=null;
      // Assignment: reset index.
      this.lastSelectedLoadedPluginIndex=0;
      // Assignment: max insert index is zero.
      this.elements.insertIndex.max="0";
      // Assignment: insert index text is zero.
      this.elements.insertIndex.value="0";
      return;
    }

    // Assignment: try to restore selection by instance id.
    let selectedOption=[...this.elements.loadedPluginSelect.options].find(optionElement=>optionElement.dataset.instanceId===this.lastSelectedLoadedPluginInstanceId);

    // Conditional: fall back to remembered index if the instance no longer exists.
    if(!selectedOption){
      // Assignment: safe fallback index.
      const safeSelectedIndex=Math.max(0,Math.min(this.lastSelectedLoadedPluginIndex,loadedPluginDescriptions.length-1));
      // Assignment: selected option by safe index.
      selectedOption=this.elements.loadedPluginSelect.options[safeSelectedIndex];
    }

    // Conditional: apply selected option when available.
    if(selectedOption){
      // Assignment: mark option selected.
      selectedOption.selected=true;
      // Assignment: remember selected instance.
      this.lastSelectedLoadedPluginInstanceId=selectedOption.dataset.instanceId;
      // Assignment: remember selected index.
      this.lastSelectedLoadedPluginIndex=Number.parseInt(selectedOption.dataset.index,10);
      // Assignment: set select value.
      this.elements.loadedPluginSelect.value=selectedOption.value;
    }

    // Assignment: insert max equals loaded count.
    this.elements.insertIndex.max=String(loadedPluginDescriptions.length);
    // Assignment: insert index follows current selected index.
    this.elements.insertIndex.value=String(Math.max(0,Math.min(this.lastSelectedLoadedPluginIndex,loadedPluginDescriptions.length)));
  }

  // Function: render button disabled states.
  renderActionAvailability(){
    // Assignment: selected plugin or null.
    const selectedLoadedPlugin=this.getSelectedLoadedPluginDescription({allowNoSelection:true});
    // Assignment: loaded plugin count.
    const loadedPluginCount=this.primaryPipelinePluginManager.countLoadedPluginInstances();
    // Assignment: async lock status used to prevent overlapping pass/test actions.
    const asyncActionRunning=Boolean(this.isUserActionRunning||this.pipelineRuntime.isPipelinePassRunning);
    // Assignment: unload disabled when no selection.
    this.elements.unloadPluginButton.disabled=!selectedLoadedPlugin;
    // Assignment: move up disabled at top or no selection.
    this.elements.moveUpButton.disabled=!selectedLoadedPlugin||selectedLoadedPlugin.index<=0;
    // Assignment: move down disabled at bottom or no selection.
    this.elements.moveDownButton.disabled=!selectedLoadedPlugin||selectedLoadedPlugin.index>=loadedPluginCount-1;
    // Assignment: one-pass and self-test actions are disabled while an async operation is active.
    this.elements.runOnceButton.disabled=asyncActionRunning;
    this.elements.runSelfTestsButton.disabled=asyncActionRunning;
  }
}

// Function: boot browser application.
function bootMergedPipelineBrowserApplication(){
  try{
    // Assignment: application controller is exposed for console debugging.
    globalObject.mergedPipelineApplication=new MergedPipelineBrowserApplication();
  }catch(bootError){
    // Assignment: debug pane may be available even on boot failure.
    const debugPaneElement=document.getElementById("debugPane");

    // Conditional: render boot error if possible.
    if(debugPaneElement){
      // Assignment: boot error JSON.
      debugPaneElement.textContent=JSON.stringify({fatalBootError:{message:bootError.message,stack:bootError.stack}},null,2);
    }

    throw bootError;
  }
}

// Conditional: defer boot until DOM is available.
if(document.readyState==="loading"){
  // Function call: boot after DOMContentLoaded.
  document.addEventListener("DOMContentLoaded",bootMergedPipelineBrowserApplication);
}else{
  // Function call: boot immediately.
  bootMergedPipelineBrowserApplication();
}
})(globalThis);
