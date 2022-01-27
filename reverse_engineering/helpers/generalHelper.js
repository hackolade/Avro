const { dependencies } = require('../appDependencies');

const getName = ({ name: fullName }) => dependencies.lodash.last((fullName || '').split('.'));
const getNamespace = ({ name: fullName, namespace }) => (fullName || '').split('.').slice(0, -1).join('.') || namespace;
const EMPTY_NAMESPACE = '#emptyNamespace';

module.exports = { getName, getNamespace, EMPTY_NAMESPACE };