const mysql = require('mysql2/promise');
const ExcelJS = require('exceljs');

module.exports = async function (context, req) {
  const clientPrincipal = req.headers['x-ms-client-principal'];
  if (!clientPrincipal) {
    context.res = { status: 401, body: { error: 'Unauthorized' } };
    return;
  }

  const search = (req.query.search || '').trim();
  const columns = parseList(req.query.columns);
  
  if (columns.length === 0) {
    context.res = { status: 400, body: { error: 'No columns selected' } };
    return;
  }

  // Validation colonnes (anti-injection)
  const ALLOWED_COLUMNS = new Set([
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
  
  const validColumns = columns.filter(c => ALLOWED_COLUMNS.has(c));
  if (validColumns.length === 0) {
    context.res = { status: 400, body: { error: 'No valid columns' } };
    return;
  }

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

  let connection;
  try {
    connection = await mysql.createConnection({
      host: process.env.MYSQL_HOST,
      port: process.env.MYSQL_PORT || 3306,
      user: process.env.MYSQL_USER,
      password: process.env.MYSQL_PASSWORD,
      database: process.env.MYSQL_DATABASE
    });

    const { where, params } = buildWhere(search, f);
    const colsList = validColumns.map(c => `\`${c}\``).join(', ');

    const [rows] = await connection.execute(
      `SELECT ${colsList} FROM V_ARTICLES_CATALOGUE ${where} ORDER BY PERTINANCE DESC, REF_JACTAL ASC`,
      params
    );

    // Construire le XLSX
    const workbook = new ExcelJS.Workbook();
    const sheet = workbook.addWorksheet('Articles');

    sheet.columns = validColumns.map(c => ({
      header: c,
      key: c,
      width: 20
    }));

    sheet.getRow(1).font = { bold: true, color: { argb: 'FFFFFFFF' } };
    sheet.getRow(1).fill = {
      type: 'pattern',
      pattern: 'solid',
      fgColor: { argb: 'FF1E3A5F' }
    };

    rows.forEach(row => sheet.addRow(row));

    const buffer = await workbook.xlsx.writeBuffer();
    
    const now = new Date();
    const filename = `articles_${now.toISOString().slice(0, 10).replace(/-/g, '')}_${now.getHours()}${String(now.getMinutes()).padStart(2, '0')}.xlsx`;

    context.res = {
      status: 200,
      headers: {
        'Content-Type': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        'Content-Disposition': `attachment; filename="${filename}"`
      },
      body: buffer,
      isRaw: true
    };
  } catch (err) {
    context.res = { status: 500, body: { error: err.message } };
  } finally {
    if (connection) await connection.end();
  }
};

function parseList(str) {
  if (!str) return [];
  return str.split(',').map(s => s.trim()).filter(s => s.length > 0);
}

function buildWhere(search, f) {
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
