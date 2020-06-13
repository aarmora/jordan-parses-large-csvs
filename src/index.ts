import csvParser from 'csv-parser';
import csvSplitStream from 'csv-split-stream';
import * as fs from 'fs';

// createBigFile('big-file.csv');

(async () => {

	for (let index = 0; index < 621; index++) {
		const directory = `F:\\business-csvs\\part_3/`;
		const fileName = `output-${index}.csv`;
		const path = `${directory}${fileName}`;
		if (fs.existsSync(path)) {
			try {
				await parseCsv(path);
			}
			catch (e) {
				// console.log('caught an error from down low', e, fileName);
				if (e.message.includes('document is larger')) {
					console.log('document too big error');

					const totalChunks = await splitCsv(path, fileName);
					const basePath = `csvs/problems/${fileName}-`;

					for (let i = 0; i < totalChunks; i++) {
						await parseCsv(`${basePath}${i}.csv`);
						fs.unlinkSync(`${basePath}${i}.csv`);
					}
				}
			}
		}
	}

})();

async function parseCsv(fileName: string) {

	let counter = 0;
	let totalCounter = 0;

	console.time('parseCsv');

	fs.createReadStream(fileName)
		.pipe(csvParser())
		.on('data', (row) => {
			totalCounter++;
			counter++;

			// console.log('totalCounter', totalCounter);

			if (counter > 1000) {
				console.log('total Counter', totalCounter);
				counter = 0;

				// do something with the database here
			}
		})
		.on('end', () => {
			console.log('completed the parse!');
			console.timeEnd('parseCsv');
		});
}

async function createBigFile(fileName: string) {
	// Create file with headers
	let headers = '';
	let columns = '';
	for (let i = 0; i < 420; i++) {
		if (i < 419) {
			headers += `header-${i},`;
			columns += `columnns-${i},`;
		}
		else if (i === 419) {
			columns += `columns-${i}\n`;
		}
	}

	fs.writeFile(fileName, headers, () => {
		console.log('File created');
	});
	const stream = fs.createWriteStream(fileName, { flags: 'a' });

	for (let i = 0; i < 35e5; i++) {
		const ableToWrite = stream.write(columns);

		if (!ableToWrite) {
			await new Promise(resolve => {
				stream.once('drain', resolve);
			});
		}
	}
}

async function splitCsv(path: string, fileName: string) {

	return new Promise((resolve, reject) => {
		csvSplitStream.split(
			fs.createReadStream(path),
			{
				lineLimit: 10000
			},
			(index) => fs.createWriteStream(`csvs/problems/${fileName}-${index}.csv`)
		)
			.then(csvSplitResponse => {
				console.log('csvSplitStream succeeded.', csvSplitResponse);
				resolve(csvSplitResponse.totalChunks);
			}).catch(csvSplitError => {
				console.log('csvSplitStream failed!', csvSplitError);
				reject();
			});
	})
}