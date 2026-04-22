const mysql = require('mysql2/promise');

// Pool de connexions partagé entre les invocations (réutilisation au lieu de reconnect)
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
      connectionLimit: 5,
      queueLimit: 0,
      enableKeepAlive: true,
      keepAliveInitialDelay: 10000
    });
  }
  return pool;
}

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

const BASE_COLS = ['REF_JACTAL', 'LIBELLE_STANDART', 'NOM_FOURNISSEUR', 'URL_PHOTO1', 'PERTINANCE', 'STOCK1', 'STOCK2', 'STOCK3'];

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
    rayons_master: parseList(req.query.rayons_master).map(r => parseInt(r, 10)).filter(n => !isNaN(n)),
    rayons_client: parseList(req.query.rayons_client).map(r => parseInt(r, 10)).filter(n => !isNaN(n)),
    stock1_op: req.query.stock1_op, stock1_val: req.query.stock1_val,
    stock2_op: req.query.stock2_op, stock2_val: req.query.stock2_val,
    stock3_op: req.query.stock3_op, stock3_val: req.query.stock3_val,
  };

  const sortDef = SORT_MAP[sort] || SORT_MAP['pertinance'];

  // Détermine si des filtres "externes" à FICART sont actifs (=> on doit passer par la VIEW)
  const needsViewFilters = !!(
    f.fournisseurs.length || f.marques.length || f.licences.length
    || f.cat_trad.length || f.sous_cat_trad.length
    || f.cat_web.length || f.sous_cat_web.length
  );

  // Filtre rayons actif ? → on doit pré-filtrer avec une sous-requête sur FICDRAY
  const needsRayonFilter = f.rayons_master.length > 0 || f.rayons_client.length > 0;

  // Détermine si on peut rester sur FICART (bien plus rapide)
  const canUseFicart = !search && !needsViewFilters && !needsRayonFilter && !!sortDef.ficart;

  const pool = getPool();
  try {
    let total = 0;
    let refsToFetch = [];

    if (canUseFicart) {
      // ===== CHEMIN 1 : FICART direct, filtres simples seulement =====
      const { where, params } = buildFicartWhere(f);
      const orderBy = `${sortDef.ficart} ${order}, FA_CODE ASC`;

      // COUNT et SELECT en parallèle
      const [countRes, refRes] = await Promise.all([
        pool.execute(`SELECT COUNT(*) as total FROM FICART ${where}`, params),
        pool.execute(
          `SELECT FA_CODE as REF_JACTAL FROM FICART ${where} ORDER BY ${orderBy} LIMIT ${pageSize} OFFSET ${offset}`,
          params
        )
      ]);
      total = countRes[0][0].total;
      refsToFetch = refRes[0].map(r => r.REF_JACTAL);

    } else if (needsRayonFilter && !search && !needsViewFilters) {
      // ===== CHEMIN 1b : Filtre rayons seul (pas de search, pas de filtre VIEW) =====
      // On utilise FICART + sous-requête sur FICDRAY pour filtrer par rayon
      const { where: ficartWhere, params: ficartParams } = buildFicartWhere(f);
      const rayonClause = buildRayonSubquery(getAllRayonIds(f));
      const fullWhere = ficartWhere
        ? `${ficartWhere} AND ${rayonClause.sql}`
        : `WHERE ${rayonClause.sql}`;
      const allParams = [...ficartParams, ...rayonClause.params];
      const orderBy = sortDef.ficart
        ? `${sortDef.ficart} ${order}, FA_CODE ASC`
        : `FA_PERTINANCE ${order}, FA_CODE ASC`;

      const [countRes, refRes] = await Promise.all([
        pool.execute(`SELECT COUNT(*) as total FROM FICART ${fullWhere}`, allParams),
        pool.execute(
          `SELECT FA_CODE as REF_JACTAL FROM FICART ${fullWhere} ORDER BY ${orderBy} LIMIT ${pageSize} OFFSET ${offset}`,
          allParams
        )
      ]);
      total = countRes[0][0].total;
      refsToFetch = refRes[0].map(r => r.REF_JACTAL);

    } else if (search && !needsViewFilters) {
      // ===== CHEMIN 2 : Recherche textuelle sur FICART via FULLTEXT (rapide) =====
      // Avec fallback LIKE si FULLTEXT ne retourne rien
      const result = await searchOnFicartWithFallback(pool, search, f, sortDef, order, offset, pageSize);
      total = result.total;
      refsToFetch = result.refs;

    } else {
      // ===== CHEMIN 3 : Besoin des JOINs de la VIEW (filtres fournisseur/marque/etc.) =====
      const result = await searchOnViewWithFallback(pool, search, f, sortDef, order, offset, pageSize);
      total = result.total;
      refsToFetch = result.refs;
    }

    // Pas de résultats → réponse vide
    if (refsToFetch.length === 0) {
      context.res = {
        status: 200,
        headers: { 'Content-Type': 'application/json' },
        body: {
          articles: [], total, page, pageSize,
          totalPages: Math.ceil(total / pageSize),
          sort, order: order.toLowerCase()
        }
      };
      return;
    }

    // ===== Récupération des détails pour les refs trouvées =====
    // On passe par la VIEW pour avoir tous les champs joints (fournisseur, marque, etc.)
    // ET on lance en PARALLÈLE la récupération des rayons MASTER
    const requestedCols = (req.query.columns || '').split(',').map(c => c.trim()).filter(c => c && ALL_VIEW_COLUMNS.has(c));
    const finalCols = Array.from(new Set([...BASE_COLS, ...requestedCols]));
    const colsList = finalCols.map(c => `\`${c}\``).join(', ');

    const placeholders = refsToFetch.map(() => '?').join(',');
    const escapedRefs = refsToFetch.map(r => mysql.escape(r)).join(',');

    // Requêtes en parallèle : détails articles + rayons MASTER
    const [detailsRes, rayonsMap] = await Promise.all([
      pool.query(
        `SELECT ${colsList}
         FROM V_ARTICLES_CATALOGUE 
         WHERE REF_JACTAL IN (${placeholders})
         ORDER BY FIELD(REF_JACTAL, ${escapedRefs})`,
        refsToFetch
      ),
      getRayonsForRefs(pool, refsToFetch)
    ]);
    const rows = detailsRes[0];

    // Injecter les rayons dans chaque article
    for (const row of rows) {
      row.RAYONS = rayonsMap.get(row.REF_JACTAL) || [];
    }

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
  }
};

// =============================================================
// Recherche FICART avec FULLTEXT + fallback LIKE
// =============================================================
async function searchOnFicartWithFallback(pool, search, f, sortDef, order, offset, pageSize) {
  const orderBy = sortDef.ficart
    ? `${sortDef.ficart} ${order}, FA_CODE ASC`
    : `FA_PERTINANCE ${order}, FA_CODE ASC`;

  // Clause rayon si filtre actif
  const allRayonIds = getAllRayonIds(f);
  const rayonClause = allRayonIds.length > 0 ? buildRayonSubquery(allRayonIds) : null;
  const rayonSqlAdd = rayonClause ? ` AND ${rayonClause.sql}` : '';
  const rayonParamsAdd = rayonClause ? rayonClause.params : [];

  // 1) Tentative FULLTEXT (rapide)
  const ftxQuery = buildFulltextQuery(search);
  if (ftxQuery) {
    const { where, params } = buildFicartWhere(f);
    const ftxClause = `MATCH(FA_CODE, FA_LIB, FA_REFI, FA_REFF, FA_BCUS) AGAINST(? IN BOOLEAN MODE)`;
    const whereWithFtx = where
      ? `${where} AND ${ftxClause}${rayonSqlAdd}`
      : `WHERE ${ftxClause}${rayonSqlAdd}`;
    const ftxParams = [...params, ftxQuery, ...rayonParamsAdd];

    const [countRes, refRes] = await Promise.all([
      pool.execute(`SELECT COUNT(*) as total FROM FICART ${whereWithFtx}`, ftxParams),
      pool.execute(
        `SELECT FA_CODE as REF_JACTAL FROM FICART ${whereWithFtx} ORDER BY ${orderBy} LIMIT ${pageSize} OFFSET ${offset}`,
        ftxParams
      )
    ]);
    const total = countRes[0][0].total;

    if (total > 0) {
      return { total, refs: refRes[0].map(r => r.REF_JACTAL) };
    }
  }

  // 2) Fallback LIKE (trouve les fragments et les termes courts)
  const { where, params } = buildFicartWhere(f);
  const likeClause = `(FA_CODE LIKE ? OR FA_LIB LIKE ? OR FA_REFI LIKE ? OR FA_REFF LIKE ? OR FA_BCUS LIKE ?)`;
  const pattern = `%${search}%`;
  const likeParams = [pattern, pattern, pattern, pattern, pattern];
  const whereWithLike = where
    ? `${where} AND ${likeClause}${rayonSqlAdd}`
    : `WHERE ${likeClause}${rayonSqlAdd}`;
  const allParams = [...params, ...likeParams, ...rayonParamsAdd];

  const [countRes, refRes] = await Promise.all([
    pool.execute(`SELECT COUNT(*) as total FROM FICART ${whereWithLike}`, allParams),
    pool.execute(
      `SELECT FA_CODE as REF_JACTAL FROM FICART ${whereWithLike} ORDER BY ${orderBy} LIMIT ${pageSize} OFFSET ${offset}`,
      allParams
    )
  ]);
  return { total: countRes[0][0].total, refs: refRes[0].map(r => r.REF_JACTAL) };
}

// =============================================================
// Recherche VIEW (filtres fournisseur/marque/etc. + éventuelle recherche)
// =============================================================
async function searchOnViewWithFallback(pool, search, f, sortDef, order, offset, pageSize) {
  const orderBy = `${sortDef.view} ${order}, REF_JACTAL ASC`;

  // Si pas de search, juste les filtres sur la VIEW
  if (!search) {
    const { where, params } = buildViewWhere('', f);
    const [countRes, refRes] = await Promise.all([
      pool.execute(`SELECT COUNT(*) as total FROM V_ARTICLES_CATALOGUE ${where}`, params),
      pool.execute(
        `SELECT REF_JACTAL FROM V_ARTICLES_CATALOGUE ${where} ORDER BY ${orderBy} LIMIT ${pageSize} OFFSET ${offset}`,
        params
      )
    ]);
    return { total: countRes[0][0].total, refs: refRes[0].map(r => r.REF_JACTAL) };
  }

  // Avec search + filtres VIEW : on tente FULLTEXT étendu (FICART + tables secondaires via JOIN via la VIEW)
  const ftxQuery = buildFulltextQuery(search);
  if (ftxQuery) {
    const { where, params } = buildViewWhereWithFulltext(ftxQuery, f);
    const [countRes, refRes] = await Promise.all([
      pool.execute(`SELECT COUNT(*) as total FROM V_ARTICLES_CATALOGUE ${where}`, params),
      pool.execute(
        `SELECT REF_JACTAL FROM V_ARTICLES_CATALOGUE ${where} ORDER BY ${orderBy} LIMIT ${pageSize} OFFSET ${offset}`,
        params
      )
    ]);
    const total = countRes[0][0].total;
    if (total > 0) {
      return { total, refs: refRes[0].map(r => r.REF_JACTAL) };
    }
  }

  // Fallback LIKE complet sur la VIEW
  const { where, params } = buildViewWhere(search, f);
  const [countRes, refRes] = await Promise.all([
    pool.execute(`SELECT COUNT(*) as total FROM V_ARTICLES_CATALOGUE ${where}`, params),
    pool.execute(
      `SELECT REF_JACTAL FROM V_ARTICLES_CATALOGUE ${where} ORDER BY ${orderBy} LIMIT ${pageSize} OFFSET ${offset}`,
      params
    )
  ]);
  return { total: countRes[0][0].total, refs: refRes[0].map(r => r.REF_JACTAL) };
}

// =============================================================
// Helpers SQL
// =============================================================
function parseList(str) {
  if (!str) return [];
  return str.split(',').map(s => s.trim()).filter(s => s.length > 0);
}

// Construit une requête FULLTEXT boolean mode pour MySQL
// Ex: "briquet rouge" → "+briquet* +rouge*"
// Retourne null si la recherche est inutilisable (mots trop courts etc.)
function buildFulltextQuery(search) {
  if (!search) return null;
  // Nettoyer les caractères spéciaux FULLTEXT (+ - * " < > ( ) ~ @)
  const cleaned = search.replace(/[+\-*"<>()~@]/g, ' ').trim();
  if (!cleaned) return null;
  const words = cleaned.split(/\s+/).filter(w => w.length >= 3); // min_token_size = 3
  if (words.length === 0) return null;
  // "+word1* +word2*" → tous les mots doivent matcher, avec wildcard
  return words.map(w => `+${w}*`).join(' ');
}

// WHERE pour FICART (filtres simples uniquement)
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

// WHERE pour la VIEW avec FULLTEXT (tables secondaires via MATCH AGAINST)
function buildViewWhereWithFulltext(ftxQuery, f) {
  const conditions = [];
  const params = [];

  // Recherche FULLTEXT sur toutes les tables qui ont un index FULLTEXT
  // Note : on ne peut pas faire MATCH sur les alias de la VIEW directement,
  // mais comme la VIEW joint les tables, on peut utiliser les colonnes underlying via leur alias
  // Ex: LIBELLE_STANDART correspond à FA.FA_LIB → mais c'est la VIEW qui le présente en LIBELLE_STANDART
  // Pour utiliser MATCH, on doit faire MATCH sur les colonnes natives des tables.
  // Dans la VIEW, impossible. On va donc faire un OR de LIKE sur la VIEW pour le search part.
  // MAIS on applique le WHERE filtres normalement pour réduire avant.
  conditions.push(`(
    LIBELLE_STANDART LIKE ?
    OR REF_JACTAL LIKE ?
    OR EAN_USINE LIKE ?
    OR EAN_JACTAL LIKE ?
    OR REF_ART_FOURNISSEUR LIKE ?
    OR LIBELLE_WEB LIKE ?
    OR DESCRIPTIF_WEB LIKE ?
    OR NOM_FOURNISSEUR LIKE ?
    OR MARQUE_NOM LIKE ?
    OR LICENCE_NOM LIKE ?
    OR TRAD_GROUPE_NOM LIKE ?
    OR TRAD_SOUS_GROUPE_NOM LIKE ?
    OR WEB_GROUPE1_NOM LIKE ?
  )`);
  // On extrait le "mot principal" du ftxQuery pour en faire un LIKE
  const mainWord = ftxQuery.replace(/[+\-*]/g, '').split(/\s+/).filter(Boolean)[0] || '';
  const pattern = `%${mainWord}%`;
  for (let i = 0; i < 13; i++) params.push(pattern);

  // Filtres
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

  // Filtre rayon (sous-requête sur FICDRAY)
  const allRayonIds = getAllRayonIds(f);
  if (allRayonIds.length > 0) {
    const rc = buildRayonSubquery(allRayonIds, 'REF_JACTAL');
    conditions.push(rc.sql);
    params.push(...rc.params);
  }

  return { where: `WHERE ${conditions.join(' AND ')}`, params };
}

// WHERE pour la VIEW avec LIKE (fallback sur fragments)
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

  // Filtre rayon (sous-requête sur FICDRAY)
  const allRayonIds = getAllRayonIds(f);
  if (allRayonIds.length > 0) {
    const rc = buildRayonSubquery(allRayonIds, 'REF_JACTAL');
    conditions.push(rc.sql);
    params.push(...rc.params);
  }

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

// =============================================================
// RAYONS — sous-requête de filtrage (WHERE FA_CODE IN (SELECT ...))
// =============================================================
// Combine rayons_master + rayons_client en une seule liste d'IDs
function getAllRayonIds(f) {
  return [...(f.rayons_master || []), ...(f.rayons_client || [])];
}

// Construit un bloc de clause SQL du type :
//   FA_CODE IN (
//     SELECT TRIM(DR_ART) FROM FICDRAY
//     WHERE CAST(LEFT(DR_NUM,6) AS UNSIGNED) IN (?, ?, ...)
//     AND TRIM(DR_ART) != ''
//   )
// `column` = nom de colonne côté article (FA_CODE ou REF_JACTAL selon contexte)
function buildRayonSubquery(rayonIds, column = 'FA_CODE') {
  const placeholders = rayonIds.map(() => '?').join(',');
  const sql = `${column} IN (
    SELECT TRIM(DR_ART) FROM FICDRAY
    WHERE CAST(LEFT(DR_NUM, 6) AS UNSIGNED) IN (${placeholders})
      AND TRIM(DR_ART) != ''
  )`;
  return { sql, params: rayonIds };
}

// =============================================================
// RAYONS — récupérer les rayons des articles d'une page
// Retourne un Map<ref, [{id, nom, date, master}, ...]>
// - master: true = rayon MASTER (ER_MASTER = 1)
// - master: false = rayon client / autre
// =============================================================
async function getRayonsForRefs(pool, refs) {
  if (!refs || refs.length === 0) return new Map();
  const placeholders = refs.map(() => '?').join(',');
  const [rows] = await pool.query(
    `SELECT 
       TRIM(D.DR_ART) AS ref,
       E.ER_NUM AS id,
       TRIM(E.ER_REF) AS nom,
       E.ER_DATE AS date,
       COALESCE(E.ER_MASTER, 0) AS master_flag
     FROM FICDRAY D
     JOIN FICERAY E ON E.ER_NUM = CAST(LEFT(D.DR_NUM, 6) AS UNSIGNED)
     WHERE TRIM(D.DR_ART) IN (${placeholders})
       AND TRIM(E.ER_REF) != ''
     ORDER BY E.ER_MASTER DESC, E.ER_DATE DESC`,
    refs
  );
  const map = new Map();
  for (const row of rows) {
    if (!map.has(row.ref)) map.set(row.ref, []);
    // Éviter les doublons (un rayon peut apparaître plusieurs fois dans FICDRAY)
    const arr = map.get(row.ref);
    if (!arr.some(x => x.id === row.id)) {
      arr.push({
        id: row.id,
        nom: row.nom,
        date: row.date,
        master: row.master_flag === 1
      });
    }
  }
  return map;
}
