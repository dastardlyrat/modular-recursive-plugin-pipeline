(function attachBrowserPipelineAdaptersToGlobalObject(globalObject){
"use strict";

// Assignment: core utilities are read from the merged pipeline core namespace.
const {PipelineSecurityUtility,PipelineJsonDocumentFactory}=globalObject.PipelineCore;

// Function/class: reads input from browser form controls and returns a normalized envelope.
class BrowserJsonInputReaderProvider{
  // Function: store browser element references.
  constructor({inputModeElement,inputTextElement,fileInputElement}){
    // Assignment: reader mode select element.
    this.inputModeElement=inputModeElement;
    // Assignment: text/json input element.
    this.inputTextElement=inputTextElement;
    // Assignment: optional JSON file input element.
    this.fileInputElement=fileInputElement;
  }

  // Function: read the selected input source.
  async read(){
    // Assignment: current input mode selected by the user.
    const selectedInputMode=this.inputModeElement.value;

    // Conditional: JSON file mode reads selected file text.
    if(selectedInputMode==="jsonFile"){
      // Assignment: first selected file.
      const selectedJsonFile=this.fileInputElement.files[0];

      // Conditional: file mode requires a selected file.
      if(!selectedJsonFile){
        throw new Error("No JSON file selected.");
      }

      // Conditional: file size is bounded for this browser demo.
      if(selectedJsonFile.size>2000000){
        throw new Error("JSON file too large.");
      }

      return {mode:selectedInputMode,value:await selectedJsonFile.text()};
    }

    // Conditional: object mode parses the text area as JSON before returning it.
    if(selectedInputMode==="jsonObject"){
      return {mode:selectedInputMode,value:PipelineSecurityUtility.parseJsonTextSafely(this.inputTextElement.value,"JSON object input")};
    }

    return {mode:"jsonText",value:this.inputTextElement.value};
  }
}

// Function/class: writes output object snapshots to a browser pre element.
class BrowserJsonOutputSink{
  // Function: store output element.
  constructor(outputPaneElement){
    // Assignment: output pre element.
    this.outputPaneElement=outputPaneElement;
    // Assignment: last written object for debugging or tests.
    this.lastWrittenObject=null;
  }

  // Function: write an object as pretty JSON.
  write(objectValue){
    // Assignment: unwrap PipelineJsonDocument when a plugin accidentally sends one directly.
    const jsonSerializableValue=PipelineJsonDocumentFactory.isPipelineJsonDocument(objectValue)?objectValue.toPlainObject():objectValue;
    // Assignment: clone last object so UI writes cannot mutate runtime state.
    this.lastWrittenObject=PipelineSecurityUtility.cloneStrictJsonValue(jsonSerializableValue);
    // Assignment: render pretty JSON into output pane.
    this.outputPaneElement.textContent=JSON.stringify(this.lastWrittenObject,null,2);
  }
}

// Function/class: reads regex settings from browser controls.
class BrowserRegexSettingsProvider{
  // Function: store regex form elements.
  constructor({regexInputElement,regexFlagsElement}){
    // Assignment: regex source input element.
    this.regexInputElement=regexInputElement;
    // Assignment: regex flags input element.
    this.regexFlagsElement=regexFlagsElement;
  }

  // Function: return current regex settings.
  read(){
    return {regexSource:this.regexInputElement.value,flags:this.regexFlagsElement.value};
  }
}

// Assignment: publish browser adapters.
globalObject.PipelineAdapters={
  BrowserReaderProvider:BrowserJsonInputReaderProvider,
  BrowserJsonInputReaderProvider,
  BrowserOutputSink:BrowserJsonOutputSink,
  BrowserJsonOutputSink,
  BrowserRegexProvider:BrowserRegexSettingsProvider,
  BrowserRegexSettingsProvider
};
})(globalThis);
