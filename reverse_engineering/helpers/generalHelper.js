const { dependencies } = require('../../shared/appDependencies');

const getNamespaceFromSchemaTopic = (schemaTopic) => schemaTopic?.split('.').slice(0, -1).join('.')
const getName = ({ name: fullName }) => dependencies.lodash.last((fullName || '').split('.'));
const getNamespace = ({ name: fullName, namespace, schemaTopic }) => (fullName || '').split('.').slice(0, -1).join('.') || namespace || getNamespaceFromSchemaTopic(schemaTopic);
const EMPTY_NAMESPACE = '#emptyNamespace';

const handleErrorObject = error => dependencies.lodash.pick(error, ['title', 'message', 'stack']);

module.exports = { getName, getNamespace, EMPTY_NAMESPACE, handleErrorObject };
