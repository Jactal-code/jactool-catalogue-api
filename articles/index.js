const mysql = require('mysql2/promise');

module.exports = async function (context, req) {
  const clientPrincipal = req.headers['x-ms-client-principal'];
  if (!clientPrincipal) {
    context.res = {
      status: 401,
      body: { error: 'Unauthorized - this API can only be called via the SWA' }
    };
    return;
  }

  const page = Math.max(1, parseInt(req.query.page) || 1);
  const pageSize = Math.min(200, Math.max(1, parseInt(req.query.pageSize) || 50));
  const search = (req.query.search || '').trim();
  const offset = (page - 1) * pageSize;

  let connection;
  try {
    connection = await mysql.createConnection({
      host: process.env.MYSQL_HOST,
      port: process.env.MYSQL_PORT || 3306,
      user: process.env.MYSQL_USER,
      password: process.env.MYSQL_PASSWORD,
      database: process.env.MYSQL_DATABASE
    });

    let total;
    let refsToFetch;

    if (search) {
      // RECHERCHE : on doit passer par la VIEW car on cherche dans plusieurs tables jointes
      const whereClause = `WHERE 
        REF_JACTAL LIKE ? 
        OR NOM_FOURNISSEUR LIKE ? 
        OR EAN_JACTAL LIKE ? 
        OR EAN_USINE LIKE ? 
        OR LIBELLE_STANDART LIKE ? 
        OR LIBELLE_WEB LIKE ? 
        OR DESCRIPTIF_WEB LIKE ?
        OR LICENCE_NOM LIKE ?
        OR MARQUE_NOM LIKE ?
        OR TRAD_GROUPE_NOM LIKE ?
        OR TRAD_SOUS_GROUPE_NOM LIKE ?
        OR WEB_GROUPE1_NOM LIKE ?
        OR WEB_SOUS_GROUPE1_NOM LIKE ?`;
      const searchPattern = `%${search}%`;
      const params = [];
      for (let i = 0; i < 13; i++) params.push(searchPattern);

      const [countResult] = await connection.execute(
        `SELECT COUNT(*) as total FROM V_ARTICLES_CATALOGUE ${whereClause}`,
        params
      );
      total = countResult[0].total;

      const [refResult] = await connection.execute(
        `SELECT REF_JACTAL FROM V_ARTICLES_CATALOGUE ${whereClause} 
         ORDER BY REF_JACTAL LIMIT ${pageSize} OFFSET ${offset}`,
        params
      );
      refsToFetch = refResult.map(r => r.REF_JACTAL);
    } else {
      // PAS DE RECHERCHE : on fait le tri DIRECTEMENT sur FICART qui a l'index
      const [countResult] = await connection.execute(
        `SELECT COUNT(*) as total FROM FICART`
      );
      total = countResult[0].total;

      const [refResult] = await connection.execute(
        `SELECT FA_CODE as REF_JACTAL FROM FICART 
         ORDER BY FA_CODE LIMIT ${pageSize} OFFSET ${offset}`
      );
      refsToFetch = refResult.map(r => r.REF_JACTAL);
    }

    // Si aucun résultat, renvoyer vide
    if (refsToFetch.length === 0) {
      context.res = {
        status: 200,
        headers: { 'Content-Type': 'application/json' },
        body: {
          articles: [],
          total: total,
          page: page,
          pageSize: pageSize,
          totalPages: Math.ceil(total / pageSize)
        }
      };
      return;
    }

    // Maintenant on charge uniquement les détails de ces refs (50 max)
    const placeholders = refsToFetch.map(() => '?').join(',');
    const [rows] = await connection.execute(
      `SELECT REF_JACTAL, LIBELLE_STANDART, NOM_FOURNISSEUR, 
              URL_PHOTO1, PERTINANCE,
              STOCK1, STOCK2, STOCK3 
       FROM V_ARTICLES_CATALOGUE 
       WHERE REF_JACTAL IN (${placeholders})
       ORDER BY REF_JACTAL`,
      refsToFetch
    );

    context.res = {
      status: 200,
      headers: { 'Content-Type': 'application/json' },
      body: {
        articles: rows,
        total: total,
        page: page,
        pageSize: pageSize,
        totalPages: Math.ceil(total / pageSize)
      }
    };
  } catch (err) {
    context.res = {
      status: 500,
      body: { error: err.message }
    };
  } finally {
    if (connection) await connection.end();
  }
};
