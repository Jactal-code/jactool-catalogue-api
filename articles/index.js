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

  let connection;
  try {
    connection = await mysql.createConnection({
      host: process.env.MYSQL_HOST,
      port: process.env.MYSQL_PORT || 3306,
      user: process.env.MYSQL_USER,
      password: process.env.MYSQL_PASSWORD,
      database: process.env.MYSQL_DATABASE
    });

    const [rows] = await connection.execute(
      'SELECT REF_JACTAL, LIBELLE_STANDART, NOM_FOURNISSEUR, MARQUE_NOM, STOCK1, STOCK2, STOCK3 FROM V_ARTICLES_CATALOGUE LIMIT 100'
    );

    context.res = {
      status: 200,
      headers: { 'Content-Type': 'application/json' },
      body: rows
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
