require('dotenv').config()

var sql = require('mssql')
var ObjectsToCsv = require('objects-to-csv')

var config = {
  server: process.env.HOST,
  requestTimeout: 999999999,
  options: {
    database: process.env.DB_NAME,
    encrypt: false
  },
  authentication: {
    type: 'default',
    options: {
      userName: process.env.USERNAME,
      password: process.env.PASSWORD
    }
  }
}

// listMaterias()

module.exports.getCsvFiles = async function getCsvFiles(objectToGetCsvFiles) {
  try {

    // /* dates format: YYYY-MM-DD */ 
    const findStartDate = objectToGetCsvFiles.findStartDate
    const findEndDate = objectToGetCsvFiles.findEndDate
    const tipoStartDate = objectToGetCsvFiles.tipoStartDate
    const tipoEndDate = objectToGetCsvFiles.tipoEndDate
    const viewsEndDate = objectToGetCsvFiles.viewsEndDate
    
    var pool = await sql.connect(config)

    let materias = await pool.request().query(`SELECT
          [CodMateria],
          [CodTipoMateria],
          [Data],
          [views],
          [views_dia],
          [views_semana],
          [views_mes],
          [Tag]
      FROM
          [Tec_data].[dbo].[Materias]
      WHERE
          [Data] BETWEEN '${findStartDate}'
          AND '${findEndDate}'
          AND [publieditorial] = 0
          AND([possuiLinkAfiliacao] = 0
		  OR [possuiLinkAfiliacao] IS NULL)`)

    var query = ''
    materias.recordset.map((row, index) => {
      if (index == materias.recordset.length - 1) {
        query = query + `[views_artigo] = ${row.CodMateria}`
      } else {
        query = query + `[views_artigo] = ${row.CodMateria} OR `
      }
    })

    var queryTipo = `SELECT [views_artigo], [views_total] FROM [Tec_data].[dbo].[materias_views] WHERE (` + query + `) AND [views_data] BETWEEN '${tipoStartDate}' AND '${tipoEndDate}' ORDER BY [views_data] DESC`

    var queryViews = `SELECT [views_artigo], [views_total], [views_data] FROM [Tec_data].[dbo].[materias_views]  WHERE (` + query + `) AND [views_data] BETWEEN '${findStartDate}' AND '${viewsEndDate}' ORDER BY [views_data] ASC`

    materias = await getTipo(pool, queryTipo, materias.recordset)
    console.log('Get Tipo:', materias)
    materias = await getViews(pool, queryViews, materias)
    console.log('Get Views:', materias)
    
    await saveCSV(materias, objectToGetCsvFiles.fileName)

  } catch (err) {
    console.error(err)
  }
}

async function getTipo(pool, query, materias) {
  try {

    let views = await pool.request().query(query)

    let views_artigoGrouped = views.recordset.reduce(function (acumulador, valor) {
      var indice = -1
      acumulador.map((pos, index) => {
        let indexViews_artigo = pos.findIndex((val) => val.views_artigo == valor.views_artigo)
        if (indexViews_artigo != -1) {
          indice = index
        }
      })
      if (indice == -1) {
        acumulador.push([valor])
      } else {
        acumulador[indice].push(valor)
      }

      return acumulador
    }, [])

    let views_artigoAccumulate = views_artigoGrouped.map((groupArtigo) => {
      let views_artigoReduced = groupArtigo.reduce(function (acc, it) {
        acc.views_artigo = it.views_artigo
        acc['views_total'] = acc['views_total'] + it.views_total || it.views_total
        return acc
      }, {})
      return views_artigoReduced
    })

    materias = materias.map(materia => {
      let picked = views_artigoAccumulate.find(o => o.views_artigo === materia.CodMateria)
      if (picked && picked.views_total > 100) {
        materia.tipo = 'long tail'
      } else {
        materia.tipo = 'hard news'
      }
      return materia
    })

    return materias

  } catch (err) {
    console.error(err)
  }
}

async function getViews(pool, query, materias) {
  try {

    let views = await pool.request().query(query)

    let views_artigoGrouped = views.recordset.reduce(function (acumulador, valor) {
      var indice = -1
      acumulador.map((pos, index) => {
        let indexViews_artigo = pos.findIndex((val) => val.views_artigo == valor.views_artigo)
        if (indexViews_artigo != -1) {
          indice = index
        }
      })

      if (indice == -1) {
        acumulador.push([valor])
      } else if (acumulador[indice].lenght < 7) {
        acumulador[indice].push(valor)
      }

      return acumulador
    }, [])


    let views_artigoAccumulate = views_artigoGrouped.map((groupArtigo) => {
      let views_artigoReduced = groupArtigo.reduce(function (acc, it) {
        acc.views_artigo = it.views_artigo
        acc.views_data = it.views_data
        acc['views_total'] = acc['views_total'] + it.views_total || it.views_total
        return acc
      }, {})
      return views_artigoReduced
    })

    materias = materias.map(materia => {
      let picked = views_artigoAccumulate.find(o => o.views_artigo === materia.CodMateria)
      if(picked){
        materia.views_semana = picked.views_total
      }else{
        return null
      }
      materia.Data = ((materia.Data.toISOString()).replace('T', ' ')).replace('Z', '')
      
      return materia
    })
    
    materias = materias.filter(Boolean)
    
    return materias


  } catch (err) {
    console.error(err)
  }
}

async function saveCSV(data, fileName) {
  const csv = new ObjectsToCsv(data)

  // Save to file:
  await csv.toDisk(__dirname + `/inputFiles/${fileName}.csv`)

  // Return the CSV file as string:
  console.log(await csv.toString())
}