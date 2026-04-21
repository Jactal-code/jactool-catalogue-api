const mysql = require('mysql2/promise');

module.exports = async function (context, req) {
  const clientPrincipal = req.headers['x-ms-client-principal'];
  if (!clientPrincipal) {
    context.res = { status: 401, body: { error: 'Unauthorized' } };
    return;
  }

  let connection;
  try {
    connection = await mysql.createConnection({
      host: process.env.MYSQL_HOST,
      port: process.env.MYSQL_PORT || 3306,
      user: process.env.MYSQL_USER,
      password: process.env.MYSQL_PASSWORD,
      database: process.env.MYSQL_DATABASE
    });

    // Récupérer les listes distinctes en parallèle pour accélérer
    const [fournisseurs, marques, licences, acheteurs, catTrad, sousCatTrad, catWeb, sousCatWeb] = await Promise.all([
      connection.execute(`SELECT DISTINCT FF_NOM AS val FROM FICFOU WHERE FF_NOM IS NOT NULL AND FF_NOM != '' ORDER BY FF_NOM`),
      connection.execute(`SELECT DISTINCT LIBE_MARQUE AS val FROM FICMARQUE WHERE LIBE_MARQUE IS NOT NULL AND LIBE_MARQUE != '' ORDER BY LIBE_MARQUE`),
      connection.execute(`SELECT DISTINCT LIBELLE_LICENCE AS val FROM FICLICENCE WHERE LIBELLE_LICENCE IS NOT NULL AND LIBELLE_LICENCE != '' ORDER BY LIBELLE_LICENCE`),
      connection.execute(`SELECT DISTINCT FA_ACHNOM AS val FROM FICART WHERE FA_ACHNOM IS NOT NULL AND FA_ACHNOM != '' ORDER BY FA_ACHNOM`),
      connection.execute(`SELECT DISTINCT FG_LIB AS val FROM FICGRP WHERE FG_LIB IS NOT NULL AND FG_LIB != '' ORDER BY FG_LIB`),
      connection.execute(`SELECT DISTINCT SG_LIB AS val FROM FICSGRP WHERE SG_LIB IS NOT NULL AND SG_LIB != '' ORDER BY SG_LIB`),
      connection.execute(`SELECT DISTINCT C2_LIB AS val FROM CATWEB WHERE C2_LIB IS NOT NULL AND C2_LIB != '' ORDER BY C2_LIB`),
      connection.execute(`SELECT DISTINCT S1_LIB AS val FROM SCATWEB WHERE S1_LIB IS NOT NULL AND S1_LIB != '' ORDER BY S1_LIB`)
    ]);

    context.res = {
      status: 200,
      headers: { 'Content-Type': 'application/json' },
      body: {
        fournisseurs: fournisseurs[0].map(r => r.val),
        marques: marques[0].map(r => r.val),
        licences: licences[0].map(r => r.val),
        acheteurs: acheteurs[0].map(r => r.val),
        cat_trad: catTrad[0].map(r => r.val),
        sous_cat_trad: sousCatTrad[0].map(r => r.val),
        cat_web: catWeb[0].map(r => r.val),
        sous_cat_web: sousCatWeb[0].map(r => r.val),
        statuts: [
          { code: 'AC', label: 'Actif' },
          { code: 'AS', label: 'Actif sans stock' },
          { code: 'NV', label: 'Non vendable' },
          { code: 'AJ', label: 'Arrêt Jactal' },
          { code: 'IN', label: 'Inactif fournisseur' },
          { code: 'EP', label: 'Epuration' },
          { code: 'OC', label: 'Occasion' }
        ]
      }
    };
  } catch (err) {
    context.res = { status: 500, body: { error: err.message } };
  } finally {
    if (connection) await connection.end();
  }
};
