const { dependencies } = require('../../shared/appDependencies');
const avsc = require('../modules/avsc');
const { parseJson } = require('./generalHelper');
const { SCRIPT_TYPES } = require('../../shared/constants');

const PULSAR_CORRECT_QUERY_REGEX = /\/.+\/.+\/.+\/schema/;
const PULSAR_NAMESPACE_EXISTS_REGEX = /.+\/.+\/.*\/schema/;
const PULSAR_TOPIC_EXISTS_REGEX = /.+\/.*\/.+\/schema/;

const MISSING_PULSAR_NAMESPACE_ERROR = 'Pulsar namespace is missing';
const MISSING_PULSAR_TOPIC_ERROR = 'Pulsar topic is missing';
const AZURE_MISSING_SCHEMA_GROUP_ERROR = 'Schema Group is missing';
const SCHEMA_IS_VALID_MESSAGE = 'Avro schema is valid';
const SCHEMAS_ARE_VALID_MESSAGE = 'Avro schemas are valid';

let _;

const validateAvroScript = (script, scriptType, logger) => {
	_ = dependencies.lodash;

	const scripts = parseScript(script, scriptType);
	const validator = getScriptValidator(scriptType);

	const validationMessages = scripts.map(script => validator(script, logger));
	if (scripts.length === 1) {
		return _.first(validationMessages);
	}

	return getMessageForMultipleSchemaValidation((_.flatten(validationMessages));
};

const parseScript = (script, scriptType) =>{
	switch(scriptType){
		case SCRIPT_TYPES.CONFLUENT_SCHEMA_REGISTRY:
			return parseConfluentScript(script);
		case SCRIPT_TYPES.AZURE_SCHEMA_REGISTRY:
			return parseAzureScript(script);
		case SCRIPT_TYPES.PULSAR_SCHEMA_REGISTRY:
			return parsePulsarScript(script);
		case SCRIPT_TYPES.SCHEMA_REGISTRY:
			return [{ script: parseJson(script).schema }]
		default:
			return [{ script }]
	}
}

const parseConfluentScript = script => {
	const scripts = [...script.matchAll(/^POST \/(.*)$\n(^\{[\s\S]*?^\})/gm)];

	return scripts.map(([data, queryPath, stringifiedBody]) => {
		const { schema } = parseJson(stringifiedBody);
		if (_.isPlainObject(schema)) {
			return { script: JSON.stringify(schema) }
		}

		return { script: schema }
	});
};

const parseAzureScript = script => {
	const defaultScripts = [...script.matchAll(/^PUT \/(.*)$\n(^\{[\s\S]*?^\})/gm)];
	const scripts = _.isEmpty(defaultScripts) ? [...script.matchAll(/^PUT \/(.*)\n(\{[\s\S]*?}$)/gm)] : defaultScripts;

	return scripts.map(([data, query, script]) => ({ script, query }));
};

const parsePulsarScript = script => {
	const defaultScripts = [...script.matchAll(/^POST \/(.*)$\n(^\{[\s\S]*?^\})/gm)];
	const scripts = _.isEmpty(defaultScripts) ? [...script.matchAll(/^POST \/(.*)\n(\{[\s\S]*?}$)/gm)] : defaultScripts;

	return scripts.map(([data, query, script]) => ({ script, query }));
};

const getScriptValidator = scriptType => {
	if (scriptType === SCRIPT_TYPES.AZURE_SCHEMA_REGISTRY){
		return validateAzureScript;
	}

	if (scriptType === SCRIPT_TYPES.PULSAR_SCHEMA_REGISTRY){
		return validatePulsarScript;
	}

	return validateScriptGeneral;
};

const validateAzureScript = ({ script, query }, logger) => {
	const schemaGroupExists = /.+\/schemas/.test(query);
	if (schemaGroupExists) {
		return validateScript(script, logger)
	}

	const { namespace } = parseJson(script);

	return [getErrorMessage(namespace)({ message: AZURE_MISSING_SCHEMA_GROUP_ERROR })]
}

const validatePulsarScript = ({ script, query }, logger) => {
	const queryIsCorrect = PULSAR_CORRECT_QUERY_REGEX.test(query);
	if (queryIsCorrect) {
		return validateScript(script, logger)
	}

	const namespaceExists = PULSAR_NAMESPACE_EXISTS_REGEX.test(query);
	const topicExists = PULSAR_TOPIC_EXISTS_REGEX.test(query);

	if (namespaceExists && topicExists) {
		return [getSuccessMessage(SCHEMA_IS_VALID_MESSAGE)];
	}

	return [
		!namespaceExists && getErrorMessage()({ message: MISSING_PULSAR_NAMESPACE_ERROR }),
		!topicExists && getErrorMessage()({ message: MISSING_PULSAR_TOPIC_ERROR }),
	].filter(Boolean);
};

const validateScriptGeneral = ({ script }, logger) => validateScript(script, logger);

const validateScript = (targetScript, logger) => {
	try {
		return validate(targetScript);
	} catch (error) {
		logger.log('error', { error }, 'Avro Validation Error');

		return [getErrorMessage()(error)];
	}
};

const validate = script => {
	const entityName = parseJson(script).name;
	try {
		avsc.parse(script);

		if (_.isEmpty(avsc.errorsCollector)) {
			return [getSuccessMessage(SCHEMA_IS_VALID_MESSAGE)];
		}

		const messages = avsc.errorsCollector.map(getErrorMessage(entityName));
		clearErrorsCollector();

		return messages;
	} catch (err) {
		const errors = err instanceof TypeError ? avsc.errorsCollector : avsc.errorsCollector.concat(err);
		const errorMessages = errors.map(getErrorMessage(entityName));

		clearErrorsCollector();

		return errorMessages;
	}
};

const getMessageForMultipleSchemaValidation = (validationMessages)=> {
	const isErrorMessage = message => message?.type !== 'success';
	const isError = validationMessages.some(isErrorMessage);

	if (isError) {
		return validationMessages.filter(isErrorMessage);
	}

	return [getSuccessMessage(SCHEMAS_ARE_VALID_MESSAGE)];
};

const clearErrorsCollector = () => {
	avsc.errorsCollector.splice(0);
};

const getErrorMessage = (entityName = '') => err => ({
	label: err.fieldName || entityName || err.name || '',
	title: err.message,
	type: 'error',
	context: '',
});

const getSuccessMessage = title => ({
	type: 'success',
	label: '',
	title,
	context: '',
});

module.exports = validateAvroScript;
