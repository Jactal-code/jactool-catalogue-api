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
  const sort = req.query.sort || 'pertinance';
  const order = (req.query.order || 'desc').toLowerCase() === 'asc' ? 'ASC' : 'DESC';
  const offset = (page - 1) * pageSize;

  const f = {
    fournisseurs: parseList(req.query.fournisseurs),
    marques: parseList(req.query.marques),
    licences: parseList(req.query.licences),
    actif: req.query.actif,
    statuts: parseList(req.query.statuts),
    cat_trad: parseList(req.query.cat_trad),
    sous_cat_trad: parseList(req.query.sous_cat_trad),
    cat_web: parseList(req.query.cat_web),
    sous_cat_web: parseList(req.query.sous_cat_web),
    acheteurs: parseList(req.query.acheteurs),
    stock1_op: req.query.stock1_op, stock1_val: req.query.stock1_val,
    stock2_op: req.query.stock2_op, stock2_val: req.query.stock2_val,
    stock3_op: req.query.stock3_op, stock3_val: req.query.stock3_val,
  };

  // Mapping des colonnes de tri
  const SORT_MAP = {
    'ref':         { ficart: 'FA_CODE',        view: 'REF_JACTAL' },
    'libelle':     { ficart: 'FA_LIB',         view: 'LIBELLE_STANDART' },
    'fournisseur': { ficart: null,             view: 'NOM_FOURNISSEUR' },
    'stock1':      { ficart: 'FA_STO1',        view: 'STOCK1' },
    'stock2':      { ficart: 'FA_STO2',        view: 'STOCK2' },
    'stock3':      { ficart: 'FA_STO3',        view: 'STOCK3' },
    'pertinance':  { ficart: 'FA_PERTINANCE',  view: 'PERTINANCE' },
  };
  const sortDef = SORT_MAP[sort] || SORT_MAP['pertinance'];

  let orderByFicart = null;
  if (sortDef.ficart) {
    orderByFicart = `${sortDef.ficart} ${order}, FA_CODE ASC`;
  }
  const orderByView = `${sortDef.view} ${order}, REF_JACTAL ASC`;

  const needsView = !!(search 
    || f.fournisseurs.length || f.marques.length || f.licences.length 
    || f.cat_trad.length || f.sous_cat_trad.length 
    || f.cat_web.length || f.sous_cat_web.length
    || !orderByFicart
  );

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

    if (needsView) {
      const { where, params } = buildViewWhere(search, f);

      const [countResult] = await connection.execute(
        `SELECT COUNT(*) as total FROM V_ARTICLES_CATALOGUE ${where}`,
        params
      );
      total = countResult[0].total;

      const [refResult] = await connection.execute(
        `SELECT REF_JACTAL FROM V_ARTICLES_CATALOGUE ${where} 
         ORDER BY ${orderByView} LIMIT ${pageSize} OFFSET ${offset}`,
        params
      );
      refsToFetch = refResult.map(r => r.REF_JACTAL);
    } else {
      const { where, params } = buildFicartWhere(f);

      const [countResult] = await connection.execute(
        `SELECT COUNT(*) as total FROM FICART ${where}`,
        params
      );
      total = countResult[0].total;

      const [refResult] = await connection.execute(
        `SELECT FA_CODE as REF_JACTAL FROM FICART ${where} 
         ORDER BY ${orderByFicart} LIMIT ${pageSize} OFFSET ${offset}`,
        params
      );
      refsToFetch = refResult.map(r => r.REF_JACTAL);
    }

    if (refsToFetch.length === 0) {
      context.res = {
        status: 200,
        headers: { 'Content-Type': 'application/json' },
        body: {
          articles: [], total, page, pageSize,
          totalPages: Math.ceil(total / pageSize)
        }
      };
      return;
    }

    // Requête finale : on utilise query() au lieu de execute() pour FIELD()
    // et on échappe manuellement les refs pour FIELD() via mysql.escape()
    const placeholders = refsToFetch.map(() => '?').join(',');
    const escapedRefs = refsToFetch.map(r => mysql.escape(r)).join(',');
    const [rows] = await connection.query(
      `SELECT REF_JACTAL, LIBELLE_STANDART, NOM_FOURNISSEUR, 
              URL_PHOTO1, PERTINANCE,
              STOCK1, STOCK2, STOCK3 
       FROM V_ARTICLES_CATALOGUE 
       WHERE REF_JACTAL IN (${placeholders})
       ORDER BY FIELD(REF_JACTAL, ${escapedRefs})`,
      refsToFetch
    );

    context.res = {
      status: 200,
      headers: { 'Content-Type': 'application/json' },
      body: {
        articles: rows,
        total, page, pageSize,
        totalPages: Math.ceil(total / pageSize),
        sort, order: order.toLowerCase()
      }
    };
  } catch (err) {
    context.log.error('Error in /api/articles:', err);
    context.res = { status: 500, body: { error: err.message } };
  } finally {
    if (connection) await connection.end();
  }
};

function parseList(str) {
  if (!str) return [];
  return str.split(',').map(s => s.trim()).filter(s => s.length > 0);
}

function buildViewWhere(search, f) {
  const conditions = [];
  const params = [];

  if (search) {
    conditions.push(`(REF_JACTAL LIKE ? OR NOM_FOURNISSEUR LIKE ? OR EAN_JACTAL LIKE ? OR EAN_USINE LIKE ? OR LIBELLE_STANDART LIKE ? OR LIBELLE_WEB LIKE ? OR DESCRIPTIF_WEB LIKE ? OR LICENCE_NOM LIKE ? OR MARQUE_NOM LIKE ? OR TRAD_GROUPE_NOM LIKE ? OR TRAD_SOUS_GROUPE_NOM LIKE ? OR WEB_GROUPE1_NOM LIKE ? OR WEB_SOUS_GROUPE1_NOM LIKE ?)`);
    const pattern = `%${search}%`;
    for (let i = 0; i < 13; i++) params.push(pattern);
  }

  addIn(conditions, params, 'NOM_FOURNISSEUR', f.fournisseurs);
  addIn(conditions, params, 'MARQUE_NOM', f.marques);
  addIn(conditions, params, 'LICENCE_NOM', f.licences);
  addIn(conditions, params, 'TRAD_GROUPE_NOM', f.cat_trad);
  addIn(conditions, params, 'TRAD_SOUS_GROUPE_NOM', f.sous_cat_trad);
  addIn(conditions, params, 'WEB_GROUPE1_NOM', f.cat_web);
  addIn(conditions, params, 'WEB_SOUS_GROUPE1_NOM', f.sous_cat_web);
  addIn(conditions, params, 'NOM_ACHETEUR', f.acheteurs);
  addIn(conditions, params, 'STATUT', f.statuts);

  if (f.actif === '1') conditions.push(`ACTUEL = 1`);
  else if (f.actif === '0') conditions.push(`(ACTUEL = 0 OR ACTUEL IS NULL)`);

  addStock(conditions, params, 'STOCK1', f.stock1_op, f.stock1_val);
  addStock(conditions, params, 'STOCK2', f.stock2_op, f.stock2_val);
  addStock(conditions, params, 'STOCK3', f.stock3_op, f.stock3_val);

  return { where: conditions.length ? `WHERE ${conditions.join(' AND ')}` : '', params };
}

function buildFicartWhere(f) {
  const conditions = [];
  const params = [];

  addIn(conditions, params, 'FA_ACHNOM', f.acheteurs);
  addIn(conditions, params, 'FA_STA', f.statuts);

  if (f.actif === '1') conditions.push(`FA_ACT = 1`);
  else if (f.actif === '0') conditions.push(`(FA_ACT = 0 OR FA_ACT IS NULL)`);

  addStock(conditions, params, 'FA_STO1', f.stock1_op, f.stock1_val);
  addStock(conditions, params, 'FA_STO2', f.stock2_op, f.stock2_val);
  addStock(conditions, params, 'FA_STO3', f.stock3_op, f.stock3_val);

  return { where: conditions.length ? `WHERE ${conditions.join(' AND ')}` : '', params };
}

function addIn(conditions, params, col, values) {
  if (!values || !values.length) return;
  const placeholders = values.map(() => '?').join(',');
  conditions.push(`${col} IN (${placeholders})`);
  values.forEach(v => params.push(v));
}

function addStock(conditions, params, col, op, val) {
  if (!op || val === undefined || val === '') return;
  const allowedOps = ['>', '>=', '<', '<=', '=', '!='];
  if (!allowedOps.includes(op)) return;
  const numVal = parseFloat(val);
  if (isNaN(numVal)) return;
  conditions.push(`${col} ${op} ?`);
  params.push(numVal);
}
