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
  const offset = (page - 1) * pageSize;

  // Récupération des filtres
  const f = {
    fournisseurs: parseList(req.query.fournisseurs),
    marques: parseList(req.query.marques),
    licences: parseList(req.query.licences),
    actif: req.query.actif, // '1' ou '0' ou undefined
    statuts: parseList(req.query.statuts),
    cat_trad: parseList(req.query.cat_trad),
    sous_cat_trad: parseList(req.query.sous_cat_trad),
    cat_web: parseList(req.query.cat_web),
    sous_cat_web: parseList(req.query.sous_cat_web),
    acheteurs: parseList(req.query.acheteurs),
    stock1_op: req.query.stock1_op,
    stock1_val: req.query.stock1_val,
    stock2_op: req.query.stock2_op,
    stock2_val: req.query.stock2_val,
    stock3_op: req.query.stock3_op,
    stock3_val: req.query.stock3_val,
  };

  // Tri
  let orderByFicart, orderByView;
  if (sort === 'ref') {
    orderByFicart = 'FA_CODE ASC';
    orderByView = 'REF_JACTAL ASC';
  } else {
    orderByFicart = 'FA_PERTINANCE DESC, FA_CODE ASC';
    orderByView = 'PERTINANCE DESC, REF_JACTAL ASC';
  }

  // Détection : a-t-on besoin de la VIEW ?
  const needsView = !!(search 
    || f.fournisseurs.length 
    || f.marques.length 
    || f.licences.length 
    || f.cat_trad.length 
    || f.sous_cat_trad.length 
    || f.cat_web.length 
    || f.sous_cat_web.length
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
      // Cas lent : au moins un filtre sur la VIEW, on passe par la VIEW
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
      // Cas rapide : que des filtres FICART, on reste sur FICART direct
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
          articles: [],
          total: total,
          page: page,
          pageSize: pageSize,
          totalPages: Math.ceil(total / pageSize)
        }
      };
      return;
    }

    // Charger les détails des refs trouvés via la VIEW
    const placeholders = refsToFetch.map(() => '?').join(',');
    const [rows] = await connection.execute(
      `SELECT REF_JACTAL, LIBELLE_STANDART, NOM_FOURNISSEUR, 
              URL_PHOTO1, PERTINANCE,
              STOCK1, STOCK2, STOCK3 
       FROM V_ARTICLES_CATALOGUE 
       WHERE REF_JACTAL IN (${placeholders})
       ORDER BY FIELD(REF_JACTAL, ${placeholders})`,
      [...refsToFetch, ...refsToFetch]
    );

    context.res = {
      status: 200,
      headers: { 'Content-Type': 'application/json' },
      body: {
        articles: rows,
        total: total,
        page: page,
        pageSize: pageSize,
        totalPages: Math.ceil(total / pageSize),
        sort: sort
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

// --- Helpers ---

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

  addInCondition(conditions, params, 'NOM_FOURNISSEUR', f.fournisseurs);
  addInCondition(conditions, params, 'MARQUE_NOM', f.marques);
  addInCondition(conditions, params, 'LICENCE_NOM', f.licences);
  addInCondition(conditions, params, 'TRAD_GROUPE_NOM', f.cat_trad);
  addInCondition(conditions, params, 'TRAD_SOUS_GROUPE_NOM', f.sous_cat_trad);
  addInCondition(conditions, params, 'WEB_GROUPE1_NOM', f.cat_web);
  addInCondition(conditions, params, 'WEB_SOUS_GROUPE1_NOM', f.sous_cat_web);
  addInCondition(conditions, params, 'NOM_ACHETEUR', f.acheteurs);
  addInCondition(conditions, params, 'STATUT', f.statuts);

  if (f.actif === '1') conditions.push(`ACTUEL = 1`);
  else if (f.actif === '0') conditions.push(`(ACTUEL = 0 OR ACTUEL IS NULL)`);

  addStockCondition(conditions, params, 'STOCK1', f.stock1_op, f.stock1_val);
  addStockCondition(conditions, params, 'STOCK2', f.stock2_op, f.stock2_val);
  addStockCondition(conditions, params, 'STOCK3', f.stock3_op, f.stock3_val);

  const where = conditions.length ? `WHERE ${conditions.join(' AND ')}` : '';
  return { where, params };
}

function buildFicartWhere(f) {
  const conditions = [];
  const params = [];

  addInCondition(conditions, params, 'FA_ACHNOM', f.acheteurs);
  addInCondition(conditions, params, 'FA_STA', f.statuts);

  if (f.actif === '1') conditions.push(`FA_ACT = 1`);
  else if (f.actif === '0') conditions.push(`(FA_ACT = 0 OR FA_ACT IS NULL)`);

  addStockCondition(conditions, params, 'FA_STO1', f.stock1_op, f.stock1_val);
  addStockCondition(conditions, params, 'FA_STO2', f.stock2_op, f.stock2_val);
  addStockCondition(conditions, params, 'FA_STO3', f.stock3_op, f.stock3_val);

  const where = conditions.length ? `WHERE ${conditions.join(' AND ')}` : '';
  return { where, params };
}

function addInCondition(conditions, params, column, values) {
  if (!values || !values.length) return;
  const placeholders = values.map(() => '?').join(',');
  conditions.push(`${column} IN (${placeholders})`);
  values.forEach(v => params.push(v));
}

function addStockCondition(conditions, params, column, op, val) {
  if (!op || val === undefined || val === '') return;
  const allowedOps = ['>', '>=', '<', '<=', '=', '!='];
  if (!allowedOps.includes(op)) return;
  const numVal = parseFloat(val);
  if (isNaN(numVal)) return;
  conditions.push(`${column} ${op} ?`);
  params.push(numVal);
}
