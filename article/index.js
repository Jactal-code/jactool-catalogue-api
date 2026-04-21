const mysql = require('mysql2/promise');

module.exports = async function (context, req) {
  const clientPrincipal = req.headers['x-ms-client-principal'];
  if (!clientPrincipal) {
    context.res = { status: 401, body: { error: 'Unauthorized' } };
    return;
  }

  const ref = context.bindingData.ref;
  if (!ref) {
    context.res = { status: 400, body: { error: 'Missing ref parameter' } };
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
      `SELECT * FROM V_ARTICLES_CATALOGUE WHERE REF_JACTAL = ? LIMIT 1`,
      [ref]
    );

    if (rows.length === 0) {
      context.res = { status: 404, body: { error: 'Article not found' } };
      return;
    }

    context.res = {
      status: 200,
      headers: { 'Content-Type': 'application/json' },
      body: rows[0]
    };
  } catch (err) {
    context.res = { status: 500, body: { error: err.message } };
  } finally {
    if (connection) await connection.end();
  }
};
