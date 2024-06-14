const _ = require('lodash');
const { setDependencies, dependencies } = require('../shared/appDependencies');
const jsonSchemaAdapter = require('./helpers/adaptJsonSchema');
const { handleErrorObject } = require('./helpers/generalHelper');

const adaptJsonSchema = (data, logger, callback, app) => {
	setDependencies(app);

	logger.log('info', 'Adaptation of JSON Schema started...', 'Adapt JSON Schema');
	try {
		const jsonSchema = JSON.parse(data.jsonSchema);
		const adaptedJsonSchema = jsonSchemaAdapter.adaptJsonSchema(jsonSchema);

		logger.log('info', 'Adaptation of JSON Schema finished.', 'Adapt JSON Schema');

		callback(null, {
			jsonSchema: JSON.stringify(adaptedJsonSchema),
			jsonSchemaName: jsonSchemaAdapter.adaptJsonSchemaName(data.jsonSchemaName),
		});
	} catch (error) {
		callback({ ...handleErrorObject(error), title: 'Adapt JSON Schema' });
	}
};

module.exports = { adaptJsonSchema };
