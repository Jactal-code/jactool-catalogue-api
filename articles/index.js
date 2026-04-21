const mysql = require('mysql2/promise');

module.exports = async function (context, req) {
  // Sécurité : n'accepter que les appels venant de la SWA
  const clientPrincipal = req.headers['x-ms-client-principal'];
  if (!clientPrincipal) {
    context.res = {
      status: 401,
      body: { error: 'Unauthorized - this API can only be called via the SWA' }
    };
    return;
  }

  // Lecture et validation des query params
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

    // Construction du WHERE si recherche
    let whereClause = '';
    const params = [];
    if (search) {
      whereClause = `WHERE 
        REF_JACTAL LIKE ? 
        OR NOM_FOURNISSEUR LIKE ? 
        OR EAN_JACTAL LIKE ? 
        OR EAN_USINE LIKE ? 
        OR LIBELLE_STANDART LIKE ? 
        OR LIBELLE_WEB LIKE ? 
        OR DESCRIPTIF_WEB LIKE ?`;
      const searchPattern = `%${search}%`;
      for (let i = 0; i < 7; i++) params.push(searchPattern);
    }

    // 1) Compte total
    const [countResult] = await connection.execute(
      `SELECT COUNT(*) as total FROM V_ARTICLES_CATALOGUE ${whereClause}`,
      params
    );
    const total = countResult[0].total;

    // 2) Données paginées
    const [rows] = await connection.execute(
      `SELECT REF_JACTAL, LIBELLE_STANDART, NOM_FOURNISSEUR, MARQUE_NOM, 
              STOCK1, STOCK2, STOCK3 
       FROM V_ARTICLES_CATALOGUE 
       ${whereClause}
       ORDER BY REF_JACTAL
       LIMIT ${pageSize} OFFSET ${offset}`,
      params
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
