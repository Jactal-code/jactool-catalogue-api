const mysql = require('mysql2/promise');

// Pool partagé
let pool = null;
function getPool() {
  if (!pool) {
    pool = mysql.createPool({
      host: process.env.MYSQL_HOST,
      port: process.env.MYSQL_PORT || 3306,
      user: process.env.MYSQL_USER,
      password: process.env.MYSQL_PASSWORD,
      database: process.env.MYSQL_DATABASE,
      waitForConnections: true,
      connectionLimit: 3,
      queueLimit: 0,
      enableKeepAlive: true,
    });
  }
  return pool;
}

module.exports = async function (context, req) {
  const clientPrincipal = req.headers['x-ms-client-principal'];
  if (!clientPrincipal) {
    context.res = { status: 401, body: { error: 'Unauthorized' } };
    return;
  }

  const pool = getPool();
  try {
    // On lance toutes les requêtes en parallèle
    const [
      fournisseursRes,
      marquesRes,
      licencesRes,
      statutsRes,
      catTradRes,
      sousCatTradRes,
      catWebRes,
      sousCatWebRes,
      acheteursRes,
      rayonsRes,
      rayonsClientRes
    ] = await Promise.all([
      pool.execute(`SELECT DISTINCT FF_NOM AS val FROM FICFOU WHERE FF_NOM IS NOT NULL AND FF_NOM != '' ORDER BY FF_NOM`),
      pool.execute(`SELECT DISTINCT LIBE_MARQUE AS val FROM FICMARQUE WHERE LIBE_MARQUE IS NOT NULL AND LIBE_MARQUE != '' ORDER BY LIBE_MARQUE`),
      pool.execute(`SELECT DISTINCT LIBELLE_LICENCE AS val FROM FICLICENCE WHERE LIBELLE_LICENCE IS NOT NULL AND LIBELLE_LICENCE != '' ORDER BY LIBELLE_LICENCE`),
      pool.execute(`SELECT DISTINCT FA_STA AS val FROM FICART WHERE FA_STA IS NOT NULL AND FA_STA != '' ORDER BY FA_STA`),
      pool.execute(`SELECT DISTINCT FG_LIB AS val FROM FICGRP WHERE FG_LIB IS NOT NULL AND FG_LIB != '' ORDER BY FG_LIB`),
      pool.execute(`SELECT DISTINCT SG_LIB AS val FROM FICSGRP WHERE SG_LIB IS NOT NULL AND SG_LIB != '' ORDER BY SG_LIB`),
      pool.execute(`SELECT DISTINCT C2_LIB AS val FROM CATWEB WHERE C2_LIB IS NOT NULL AND C2_LIB != '' ORDER BY C2_LIB`),
      pool.execute(`SELECT DISTINCT S1_LIB AS val FROM SCATWEB WHERE S1_LIB IS NOT NULL AND S1_LIB != '' ORDER BY S1_LIB`),
      pool.execute(`SELECT DISTINCT FA_ACHNOM AS val FROM FICART WHERE FA_ACHNOM IS NOT NULL AND FA_ACHNOM != '' ORDER BY FA_ACHNOM`),
      pool.execute(`SELECT ER_NUM AS id, TRIM(ER_REF) AS nom, ER_DATE AS date FROM FICERAY WHERE ER_MASTER = 1 AND ER_STA IN (1, 3) AND TRIM(ER_REF) != '' ORDER BY ER_DATE DESC, ER_NUM DESC`),
      pool.execute(`SELECT ER_NUM AS id, TRIM(ER_REF) AS nom, ER_DATE AS date FROM FICERAY WHERE (ER_MASTER = 0 OR ER_MASTER IS NULL) AND ER_STA IN (1, 3) AND TRIM(ER_REF) != '' ORDER BY ER_DATE DESC, ER_NUM DESC`)
    ]);

    context.res = {
      status: 200,
      headers: {
        'Content-Type': 'application/json',
        'Cache-Control': 'private, max-age=300' // 5 min de cache navigateur
      },
      body: {
        fournisseurs: fournisseursRes[0].map(r => r.val),
        marques: marquesRes[0].map(r => r.val),
        licences: licencesRes[0].map(r => r.val),
        statuts: statutsRes[0].map(r => r.val),
        cat_trad: catTradRes[0].map(r => r.val),
        sous_cat_trad: sousCatTradRes[0].map(r => r.val),
        cat_web: catWebRes[0].map(r => r.val),
        sous_cat_web: sousCatWebRes[0].map(r => r.val),
        acheteurs: acheteursRes[0].map(r => r.val),
        rayons_master: rayonsRes[0].map(r => ({
          id: r.id,
          nom: r.nom,
          date: r.date ? new Date(r.date).toISOString().slice(0, 10) : null
        })),
        rayons_client: rayonsClientRes[0].map(r => ({
          id: r.id,
          nom: r.nom,
          date: r.date ? new Date(r.date).toISOString().slice(0, 10) : null
        }))
      }
    };
  } catch (err) {
    context.log.error('Error in /api/filter-options:', err);
    context.res = { status: 500, body: { error: err.message } };
  }
};
