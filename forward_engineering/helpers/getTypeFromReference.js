
const { dependencies } = require('../../shared/appDependencies');
const { prepareName } = require('./generalHelper');

const getTypeFromReference = (schema) => {
	if (!schema.$ref) {		
        return;
	}

	if(dependencies.lodash.includes(schema.$ref, '#')) {
		const namespace = schema.namespace || '';
		const name = prepareName(dependencies.lodash.last(schema.$ref.split('/')) || '');

		return [namespace, name].filter(Boolean).join('.');
	}

	return schema.$ref;
};

module.exports = getTypeFromReference