const { prepareName } = require('./generalHelper');

const getTypeFromReference = (_, schema) => {
	if (!schema.$ref) {		
        return;
	}

	if(_.includes(schema.$ref, '#')) {
		const namespace = schema.namespace || '';
		const name = prepareName(_.last(schema.$ref.split('/')) || '');

		return [namespace, name].filter(Boolean).join('.');
	}

	return schema.$ref;
};

module.exports = getTypeFromReference