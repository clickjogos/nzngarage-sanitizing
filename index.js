const ObjectsToCsv = require('objects-to-csv')
const csv = require('csvtojson')
const datefns = require('date-fns')

const { getCsvFiles } = require ('./getCsvFiles')

async function main() {
	
	/* dates format: YYYY-MM-DD */ 
	console.log("Defining dates")
	const objectToGetCsvFiles = {
		fileName: "nzn-period-test",
		findStartDate : "2020-10-01", 
		findEndDate : "2020-10-05",
		tipoStartDate : "2020-10-18",
		tipoEndDate:  "2020-10-23",
		viewsEndDate : "2020-10-12"
	}
   
	/* getting database news, in csv format */
	console.log("Getting database news, in csv format")
	await getCsvFiles(objectToGetCsvFiles)

	let inputFileName = objectToGetCsvFiles.fileName

	/* Read file */
	console.log("Read file")
	let materiasArray = await csv().fromFile(__dirname + `/inputFiles/${inputFileName}.csv`)
	let tagArray = []

	/* Adjust day and time coluns and fill tag array*/
	console.log("Adjust day and time coluns and fill tag array")
	await materiasArray.map((row) => {
		let indexTag = tagArray.indexOf(row['Tag'])
		if (indexTag == -1) tagArray.push(row['Tag'])

		const fullDateTime = row.Data.split(' ')
		row['semana'] = datefns.differenceInCalendarWeeks(new Date(fullDateTime), new Date(datefns.getYear(new Date(fullDateTime)), 0, 1)) + 1
		row['ano'] = datefns.getYear(new Date(fullDateTime))
		row['diadasemana'] = defineWeekDays(new Date(fullDateTime).getDay())
		row['periodo'] = defineDayPeriod(new Date(fullDateTime).getHours())

		row.views_semana = parseInt(row.views_semana)

		delete row['Data']
		delete row['CodTipoMateria']
		delete row['CodMateria']
		delete row['views_mes']
		delete row['views_dia']
		delete row['views']
	})

	/* Group rows by year and week */
	console.log("Group rows by year and week")
	let materiasGrouped = await groupMateriasByYearWeek(materiasArray)

	/* Build compare object */
	console.log("Build compare object ")
	let headers = [['ano', 'semana', 'Dom', 'Seg', 'Ter', 'Qua', 'Qui', 'Sex', 'Sab']]
	headers.push(tagArray)
	headers.push(['manha', 'tarde', 'noite', 'madrugada', 'hard news', 'long tail', 'views_semana'])
	let headersFlat = headers.reduce((acc, it) => [...acc, ...it], [])

	/* Sum occurencies and build new object */
	console.log("Sum occurencies and build new object")
	let materiasAccumulate = await sumMateriasObjectValues(materiasGrouped)

	/* Adjust new object to include null occurencies */
	console.log("Adjust new object to include null occurencies")
	headersFlat.map((key) => {
		materiasAccumulate.map((mat) => {
			if (!mat[key]) mat[key] = 0
		})
		return
	})

	/* Save output file */
	console.log("Save output file")
	let outputFileName = `${objectToGetCsvFiles.fileName}_sanitized`
	objecto2csv = await new ObjectsToCsv(materiasAccumulate)
	objecto2csv.toDisk(__dirname + `/outputFiles/${outputFileName}.csv`)
	return
}

async function groupMateriasByYearWeek(materias) {
	return materias.reduce(function (acumulador, valor) {
		var indice = -1
		acumulador.map((pos, index) => {
			let indexAno = pos.findIndex((val) => val.ano == valor.ano)
			let indexSemana = pos.findIndex((val) => val.semana == valor.semana)
			if (indexAno != -1 && indexSemana != -1) {
				indice = index
			}
		})

		if (indice == -1) acumulador.push([valor])
		else acumulador[indice].push(valor)

		return acumulador
	}, [])
}

async function sumMateriasObjectValues(materias) {
	return materias.map((groupWeekYear) => {
		let materiasReduced = groupWeekYear.reduce(function (acc, it) {
			acc['ano'] = it.ano
			acc['semana'] = it.semana

			acc[it.diadasemana] = acc[it.diadasemana] + 1 || 1
			acc[it.Tag] = acc[it.Tag] + 1 || 1
			acc[it.periodo] = acc[it.periodo] + 1 || 1
			acc[it.tipo] = acc[it.tipo] + 1 || 1
			acc['views_semana'] = acc['views_semana'] + it.views_semana || it.views_semana

			return acc
		}, {})
		return materiasReduced
	})
}

function defineWeekDays(day) {
	switch (day) {
		case 0:
			return 'Dom'
		case 1:
			return 'Seg'
		case 2:
			return 'Ter'
		case 3:
			return 'Qua'
		case 4:
			return 'Qui'
		case 5:
			return 'Sex'
		case 6:
			return 'Sab'
		default:
			break
	}
}

function defineDayPeriod(hour) {
	if (hour >= 6 && hour < 12) {
	  return 'manha'
	} else if (hour >= 12 && hour < 18) {
	  return 'tarde'
	} else if (hour >= 18 && hour < 23) {
	  return 'noite'
	} else {
		
	  return 'madrugada'
	}
  }

main()