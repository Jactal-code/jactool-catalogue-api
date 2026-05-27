const mysql = require('mysql2/promise');

// Pool partagé entre invocations (réutilisation)
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
  const ref = context.bindingData.ref;
  if (!ref) {
    context.res = { status: 400, body: { error: 'Missing ref parameter' } };
    return;
  }

  const pool = getPool();
  try {
    // 3 requêtes en parallèle :
    // 1) Détails article via la VIEW
    // 2) Photos depuis INDPHO (triées : principale d'abord, puis par ordre d'ajout)
    // 3) Rayons (master + autres) actifs ou en attente
    const [articleRes, photosRes, rayonsRes] = await Promise.all([
      pool.execute(
        `SELECT * FROM V_ARTICLES_CATALOGUE WHERE REF_JACTAL = ? LIMIT 1`,
        [ref]
      ),
      pool.execute(
        `SELECT I0CLEUNIK AS id, I0_FICHIER AS filename, I0_TYPE AS type, I0_URL AS url
         FROM INDPHO
         WHERE I0_ART = ?
         ORDER BY I0_TYPE DESC, I0CLEUNIK ASC`,
        [ref]
      ),
      pool.execute(
        `SELECT 
           E.ER_NUM AS id,
           TRIM(E.ER_REF) AS nom,
           E.ER_DATE AS date,
           COALESCE(E.ER_MASTER, 0) AS master_flag
         FROM FICDRAY D
         JOIN FICERAY E ON E.ER_NUM = CAST(LEFT(D.DR_NUM, 6) AS UNSIGNED)
         WHERE TRIM(D.DR_ART) = ?
           AND E.ER_STA IN (1, 3)
           AND TRIM(E.ER_REF) != ''
         ORDER BY E.ER_MASTER DESC, E.ER_DATE DESC`,
        [ref]
      )
    ]);

    if (articleRes[0].length === 0) {
      context.res = { status: 404, body: { error: 'Article not found' } };
      return;
    }

    const article = articleRes[0][0];

    // Photos : nettoyer et flagger la principale (I0_TYPE = 1)
    const photos = photosRes[0].map(p => ({
      id: p.id,
      filename: p.filename,
      url: p.url,
      is_main: p.type === 1
    }));

    // Rayons : dédupliquer (un rayon peut apparaitre plusieurs fois dans FICDRAY)
    const seenRayonIds = new Set();
    const rayons = [];
    for (const r of rayonsRes[0]) {
      if (seenRayonIds.has(r.id)) continue;
      seenRayonIds.add(r.id);
      rayons.push({
        id: r.id,
        nom: r.nom,
        date: r.date,
        master: r.master_flag === 1
      });
    }

    article.PHOTOS = photos;
    article.RAYONS = rayons;

    context.res = {
      status: 200,
      headers: { 'Content-Type': 'application/json' },
      body: article
    };
  } catch (err) {
    context.log.error('Error in /api/article/{ref}:', err);
    context.res = { status: 500, body: { error: err.message } };
  }
};
