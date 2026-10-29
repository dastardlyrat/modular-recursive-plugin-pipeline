"use strict";

require("./pipeline-core.js");
require("./stateful-plugins.js");
require("./stream-plugins.js");
require("./control-flow-plugins.js");
require("./advanced-flow-data-plugins.js");
require("./deep-clone-callable-registry.browser.js");
require("./pipeline-hypervisor.js");
require("./self-tests.js");

globalThis.runMergedPipelineSelfTests()
  .then(testResult=>{
    console.log(JSON.stringify(testResult,null,2));
    process.exit(testResult.passed?0:1);
  })
  .catch(error=>{
    console.error(error);
    process.exit(1);
  });
