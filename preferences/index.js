const mysql = require('mysql2/promise');

// Préférences par défaut retournées si l'utilisateur n'en a jamais enregistré
const DEFAULT_PREFS = {
  view: 'list',
  sort: 'pertinance',
  order: 'desc',
  page_size: 50,
  visible_columns: ['REF_JACTAL', 'LIBELLE_STANDART', 'NOM_FOURNISSEUR', 'STOCK1', 'STOCK2', 'STOCK3', 'PERTINANCE'],
  export_templates: []
};

module.exports = async function (context, req) {
  const clientPrincipal = req.headers['x-ms-client-principal'];
  if (!clientPrincipal) {
    context.res = { status: 401, body: { error: 'Unauthorized' } };
    return;
  }

  // Extraire l'userId depuis le header SWA (base64 JSON)
  let userId;
  try {
    const decoded = JSON.parse(Buffer.from(clientPrincipal, 'base64').toString('utf-8'));
    userId = decoded.userId || decoded.userDetails;
    if (!userId) throw new Error('No userId in principal');
  } catch (err) {
    context.res = { status: 401, body: { error: 'Invalid client principal' } };
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

    if (req.method === 'GET') {
      const [rows] = await connection.execute(
        `SELECT prefs FROM JACTOOL_USER_PREFS WHERE user_id = ? LIMIT 1`,
        [userId]
      );
      let prefs = DEFAULT_PREFS;
      if (rows.length > 0) {
        // MySQL renvoie JSON soit comme objet, soit comme string selon le driver
        const raw = rows[0].prefs;
        prefs = typeof raw === 'string' ? JSON.parse(raw) : raw;
        // Merger avec DEFAULT_PREFS pour garantir que toutes les clés attendues existent
        prefs = { ...DEFAULT_PREFS, ...prefs };
      }
      context.res = {
        status: 200,
        headers: { 'Content-Type': 'application/json' },
        body: prefs
      };
      return;
    }

    if (req.method === 'PUT') {
      const body = req.body;
      if (!body || typeof body !== 'object') {
        context.res = { status: 400, body: { error: 'Invalid payload' } };
        return;
      }

      // On valide et nettoie les clés attendues pour éviter que n'importe quoi arrive en base
      const cleanPrefs = {
        view: (body.view === 'grid' || body.view === 'list') ? body.view : DEFAULT_PREFS.view,
        sort: typeof body.sort === 'string' ? body.sort.slice(0, 30) : DEFAULT_PREFS.sort,
        order: (body.order === 'asc' || body.order === 'desc') ? body.order : DEFAULT_PREFS.order,
        page_size: (typeof body.page_size === 'number' && body.page_size > 0 && body.page_size <= 500) ? body.page_size : DEFAULT_PREFS.page_size,
        visible_columns: Array.isArray(body.visible_columns) ? body.visible_columns.slice(0, 150).map(c => String(c).slice(0, 80)) : DEFAULT_PREFS.visible_columns,
        export_templates: Array.isArray(body.export_templates) ? body.export_templates.slice(0, 50).map(t => ({
          name: typeof t.name === 'string' ? t.name.slice(0, 80) : 'Sans nom',
          columns: Array.isArray(t.columns) ? t.columns.slice(0, 150).map(c => String(c).slice(0, 80)) : []
        })) : DEFAULT_PREFS.export_templates
      };

      await connection.execute(
        `INSERT INTO JACTOOL_USER_PREFS (user_id, prefs) 
         VALUES (?, ?) 
         ON DUPLICATE KEY UPDATE prefs = VALUES(prefs)`,
        [userId, JSON.stringify(cleanPrefs)]
      );

      context.res = {
        status: 200,
        headers: { 'Content-Type': 'application/json' },
        body: cleanPrefs
      };
      return;
    }

    context.res = { status: 405, body: { error: 'Method not allowed' } };
  } catch (err) {
    context.log.error('Error in /api/preferences:', err);
    context.res = { status: 500, body: { error: err.message } };
  } finally {
    if (connection) await connection.end();
  }
};
