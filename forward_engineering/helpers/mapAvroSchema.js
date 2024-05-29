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
				return {
					...field,
					...typeSchema,
				};
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
