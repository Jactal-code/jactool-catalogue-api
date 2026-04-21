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
    const ALL_VIEW_COLUMNS = new Set([
      'REF_JACTAL','ACTUEL','CODE_FOURNISSEUR','NOM_FOURNISSEUR','REF_ART_FOURNISSEUR','EAN_USINE','EAN_JACTAL',
      'COMMENTAIRE_INTERNE','PA','DEVISE','PR','CODE_VALORLUX','LIBELLE_STANDART','LIBELLE_WEB','DESCRIPTIF_WEB',
      'AGE_PREVU_TW','MARQUE_TW','STOCK1','STOCK2','STOCK3','HT1_CFT','HT1_PRIX','HT2_PRIX','HT3_CFT','HT3_PRIX',
      'HT4_CFT','HT4_PRIX','HT5_CFT','HT5_PRIX','HT6_CFT','HT6_PRIX','TTC1_PRIX','TTC2_PRIX','TTC3_PRIX','TTC4_PRIX',
      'TTC5_PRIX','TTC6_PRIX','REGROUPE','GROUPE_REMISE','COLIS_INT','COLIS_EXT','MIN_CMDE','WEB_PVTTC_PROPO',
      'WEB1_CFT','WEB_HT1_PRIX','WEB1_QTE','WEB2_CFT','WEB_HT2_PRIX','WEB2_QTE','WEB3_CFT','WEB_HT3_PRIX','WEB3_QTE',
      'WEB4_CFT','WEB4_PRIX','WEB4_QTE','PRIX_GROSSISTE','QTE_GROSSISTE','REMISE_TEMPORAIRE','TRAD_GROUPE',
      'TRAD_GROUPE_NOM','TRAD_SOUS_GROUPE','TRAD_SOUS_GROUPE_NOM','CODE_DOUANIER','LARGEUR_MM_PRODUIT',
      'HAUTEUR_MM_PRODUIT','PROFONDEUR_MM_PRODUIT','POIDS_G_PRODUIT','LARGEUR_MM_COLIS','HAUTEUR_MM_COLIS',
      'PROFONDEUR_MM_COLIS','POIDS_G_COLIS','NBRE_COLIS_COUCHE','EUROPALETE_NBRE_COUCHE','NBRE_BATT_LR03',
      'NBRE_BATT_LR06','NBRE_BATT_LR14','NBRE_BATT_LR20','NBRE_BATT_9V','NBRE_BATT_AKKU','ARTICLE_WEB',
      'WEB_GROUPE1','WEB_GROUPE1_NOM','WEB_SOUS_GROUPE1','WEB_SOUS_GROUPE1_NOM','WEB_GROUPE2','WEB_GROUPE2_NOM',
      'WEB_SOUS_GROUPE2','WEB_SOUS_GROUPE2_NOM','WEB_GROUPE3','WEB_GROUPE3_NOM','WEB_SOUS_GROUPE3',
      'WEB_SOUS_GROUPE3_NOM','WEB_TOUS_PAYS','WEB_EXCLURE_LUX','WEB_EXCLURE_FRA','WEB_EXCLURE_BEL',
      'WEB_EXCLURE_ALL','WEB_DATE_MISE_WEB','WEB_PRODUIT_PERMANENT','WEB_PROD_ZONE_DEFIL','WEB_PROD_TETE_CAT',
      'WEB_PROD_PRECO','WEB_CMDE_AVANT','WEB_ARRIVAGE_PREVU','WEB_ST1','WEB_ST2','DATE_DEBUT_SAISON',
      'DATE_FIN_SAISON','CLE_MARQUE','MARQUE_NOM','CLE_LICENCE','LICENCE_NOM','LIMITE_COM','URL_PHOTO1',
      'STATUT','PERTINANCE','DANGEREUX','CLASSIFICATION','POINT_ECLAIR','IMDG','LITRAGE','DATE_VALIDITE_SECURE',
      'EAN_COLIS','EAN_PALETTE','DESCRIPTION_UR','DESCRIPTION_UC','QTE_UC_PAR_UR','DESCRIPTION_UV','QTE_UV_PAR_UC',
      'CODE_ACHETEUR','NOM_ACHETEUR','FICHE_SECURITE'
    ]);

    const requestedCols = (req.query.columns || '').split(',').map(c => c.trim()).filter(c => c && ALL_VIEW_COLUMNS.has(c));
    // Toujours inclure ces colonnes minimales pour l'UI
    const BASE_COLS = ['REF_JACTAL', 'LIBELLE_STANDART', 'NOM_FOURNISSEUR', 'URL_PHOTO1', 'PERTINANCE', 'STOCK1', 'STOCK2', 'STOCK3'];
    const finalCols = Array.from(new Set([...BASE_COLS, ...requestedCols]));
    const colsList = finalCols.map(c => `\`${c}\``).join(', ');

    const placeholders = refsToFetch.map(() => '?').join(',');
    const escapedRefs = refsToFetch.map(r => mysql.escape(r)).join(',');
    const [rows] = await connection.query(
      `SELECT ${colsList}
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
