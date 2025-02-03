const _ = require('lodash');
const { GENERAL_ATTRIBUTES } = require('../../shared/constants');
const { reorderAttributes } = require('./generalHelper');

const mapAvroSchema = (avroSchema, iteratee) => {
	if (Array.isArray(avroSchema)) {
		return avroSchema.map(item => mapAvroSchema(item, iteratee));
	}

	avroSchema = iteratee(avroSchema);

	if (typeof avroSchema === 'string') {
		return avroSchema;
	}

	if (Array.isArray(avroSchema.fields)) {
		const fields = avroSchema.fields.map(field => {
			const typeSchema = mapAvroSchema(field.type, iteratee);
			if (Array.isArray(typeSchema.type)) {
				// properties of a reference (field) have higher priority except of some
				// Avro-related properties like `default` or `type`. Although, it is not possible to define these
				// properties on the reference, I made such merge to be sure that we don't overwrite
				// some definition properties that are necessary because I'm not aware of whole scope and impact
				// of the change.
				return reorderAttributes({
					..._.pick(field, GENERAL_ATTRIBUTES),
					...typeSchema,
					..._.omit(field, GENERAL_ATTRIBUTES),
					doc: field.doc ?? typeSchema.doc,
				});
			}

			return {
				...field,
				type: typeSchema,
			};
		});

		avroSchema = { ...avroSchema, fields };
	}

	if (avroSchema.values) {
		avroSchema = { ...avroSchema, values: mapAvroSchema(avroSchema.values, iteratee) };
	}

	if (avroSchema.items) {
		avroSchema = { ...avroSchema, items: mapAvroSchema(avroSchema.items, iteratee) };
	}

	return avroSchema;
};

module.exports = mapAvroSchema;
