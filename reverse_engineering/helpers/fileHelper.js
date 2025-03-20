const fs = require('fs');
const path = require('path');
const avro = require('avsc');
const snappy = require('snappyjs');
const ALLOWED_EXTENSIONS = [
	'.avro',
	'.avsc',
	'.confluent-avro',
	'.azureSchemaRegistry-avro',
	'.pulsarSchemaRegistry-avro',
];
const SNAPPY_CHECKSUM_LENGTH = 4;

const openAvroFile = async path => {
	const content = await getFileContent(path);

	return JSON.parse(content);
};

const getFileContent = path =>
	new Promise((resolve, reject) => {
		const extension = getExtension(path);
		const respond = (err, content) => (err ? reject(err) : resolve(content));

		if (!ALLOWED_EXTENSIONS.includes(extension)) {
			return respond(new Error(`The file ${path} is not recognized as Avro Schema or Data.`));
		}

		if (extension === '.avro') {
			return readAvroData(path, respond);
		}

		fs.readFile(path, 'utf-8', respond);
	});

const readAvroData = (filePath, cb) => {
	const codecs = {
		snappy: (buf, cb) => cb(snappy.uncompress(buf.slice(0, -SNAPPY_CHECKSUM_LENGTH))),
		null: (buf, cb) => cb(null, buf),
	};

	const processMetadata = metadata => {
		try {
			const schema = JSON.stringify(metadata);
			return cb(null, schema);
		} catch (error) {
			return cb(error);
		}
	};

	try {
		avro.createFileDecoder(filePath, { codecs }).on('metadata', processMetadata).on('error', cb);
	} catch (err) {
		// In browser environment file decoder is unavailable, need to use a fallback strategy
		if (err.toString().includes('createFileDecoder is not a function')) {
			const avroBuffer = fs.readFileSync(filePath);
			const avroBlob = new Blob([avroBuffer]);

			avro.createBlobDecoder(avroBlob).on('metadata', processMetadata).on('error', cb);
		} else {
			cb(err);
		}
	}
};

const getExtension = filePath => path.extname(filePath);

module.exports = { openAvroFile, getExtension };
