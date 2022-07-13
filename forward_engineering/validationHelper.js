const avsc = require('./modules/avsc');

const toMessage = entityName => err => ({
	type: 'error',
	label: err.fieldName || entityName || err.name,
	title: err.message,
	context: '',
});

const validate = script => {
	let entityName = getEntityNameSafe(script);
	try {
		avsc.errorsCollector.splice(0, 0);

		avsc.parse(script);

		if (avsc.errorsCollector && avsc.errorsCollector.length) {
			const messages = avsc.errorsCollector.map(toMessage(entityName));
			avsc.errorsCollector.splice(0);

			return messages;
		} else {
			return [
				{
					type: 'success',
					label: entityName || '',
					title: 'Avro schema is valid',
					context: '',
				},
			];
		}
	} catch (err) {
		const errors = err instanceof TypeError ? avsc.errorsCollector : avsc.errorsCollector.concat(err);
		const errorMessages = errors.map(toMessage(entityName));

		avsc.errorsCollector.splice(0);

		return errorMessages;
	}
};

const getEntityNameSafe = script => {
	try {
		return JSON.parse(script)?.name;
	} catch (error) {
		return;
	}
};

module.exports = {
	validate,
};
