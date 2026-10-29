import { selfTestDeepCloneModule } from './deep_clone_callable_registry_module_hardened.js';

const result = selfTestDeepCloneModule();
console.log(JSON.stringify(result, null, 2));
