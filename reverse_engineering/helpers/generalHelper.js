const _ = require('lodash');

const EMPTY_NAMESPACE = '#emptyNamespace';

const getNamespaceFromSchemaTopic = schemaTopic => schemaTopic?.split('.').slice(0, -1).join('.');

const getName = ({ name: fullName }) => _.last((fullName || '').split('.'));

const getNamespace = ({ name: fullName, namespace, schemaTopic }) =>
	(fullName || '').split('.').slice(0, -1).join('.') || namespace || getNamespaceFromSchemaTopic(schemaTopic);

const handleErrorObject = error => _.pick(error, ['title', 'message', 'stack']);

module.exports = { getName, getNamespace, EMPTY_NAMESPACE, handleErrorObject };
